/*----------------------------------------------------------------------------
 *
 * dmq.c
 *	  Distributed message queue.
 *
 *
 *	  Backend to remote backend messaging with memqueue-like interface.
 * COPY protocol is used as a transport to avoid unnecessary overhead of sql
 * parsing.
 *	  Sender is a custom bgworker that starts with postgres, can open multiple
 * remote connections and keeps open memory queue with each ordinary backend.
 * It's a sender responsibility to establish a connection with a remote
 * counterpart. Sender can send heartbeats to allow early detection of dead
 * connections. Also it can stubbornly try to reestablish dead connection.
 *	  Receiver is an ordinary backend spawned by a postmaster upon sender
 * connection request. As it first call sender will call dmq_receiver_loop()
 * function that will switch fe/be protocol to a COPY mode and enters endless
 * receiving loop.
 *
 *
 * Portions Copyright (c) 1996-2018, PostgreSQL Global Development Group
 * Portions Copyright (c) 1994, Regents of the University of California
 *
 *----------------------------------------------------------------------------
 */
#include "postgres.h"

#include "dmq.h"
#include "libpq/libpq.h"
#include "libpq/pqformat.h"
#include "miscadmin.h"
#include "pgstat.h"
#include "executor/executor.h"
#include "utils/builtins.h"
#include "utils/timestamp.h"
#include "storage/shm_toc.h"
#include "storage/shm_mq.h"
#include "storage/ipc.h"
#include "tcop/tcopprot.h"
#include "utils/dynahash.h"

#define DMQ_MQ_SIZE ((Size)1024)
#define DMQ_MQ_MAGIC 0x646d71

/*
 * Shared data structures to hold current connections topology.
 * All that stuff can be moved to persistent tables to avoid hardcoded
 * size limits, but now it doesn't seems to be worth of troubles.
 */

#define DMQ_NAME_MAXLEN 32
#define DMQ_CONNSTR_MAX_LEN 150

#define DMQ_MAX_SUBS_PER_BACKEND 10
#define DMQ_MAX_DESTINATIONS 10
#define DMQ_MAX_RECEIVERS 10

typedef enum
{
	Idle,			/* upon init or falure */
	Connecting,		/* upon PQconnectStart */
	Negotiating,	/* upon PQconnectPoll == OK */
	Active,			/* upon dmq_receiver_loop() response */
} DmqConnState;

typedef struct {
	bool			active;
	char			sender_name[DMQ_NAME_MAXLEN];
	char			connstr[DMQ_CONNSTR_MAX_LEN];
	int				ping_period;
	PGconn		   *pgconn;
	DmqConnState	state;
	int				pos;
} DmqDestination;

typedef struct
{
	char	stream_name[DMQ_NAME_MAXLEN];
	int		procno;
} DmqStreamSubscription;

typedef struct DmqSharedState
{
	LWLock		   *lock;
	dsm_handle	    out_dsm;
	DmqDestination  destinations[DMQ_MAX_DESTINATIONS];

	HTAB		   *subscriptions;
	char			receiver[DMQ_MAX_RECEIVERS][DMQ_NAME_MAXLEN];
	dsm_handle		receiver_dsm[DMQ_MAX_RECEIVERS];
	int				nreceivers;

	pid_t			sender_pid;
} DmqSharedState;

static DmqSharedState *dmq_state;

/* Backend-local i/o queues. */
typedef struct DmqBackendState
{
	shm_mq_handle *mq_outh;
	shm_mq_handle *mq_inh[DMQ_MAX_DESTINATIONS];
	int				n_inhandles;
} DmqBackendState;

/* Flags set by signal handlers */
static volatile sig_atomic_t got_SIGHUP = false;

static DmqBackendState dmq_local;

static shmem_startup_hook_type PreviousShmemStartupHook;

dmq_receiver_start_hook_type dmq_receiver_start_hook;

void dmq_sender_main(Datum main_arg);

PG_FUNCTION_INFO_V1(dmq_receiver_loop);

/*****************************************************************************
 *
 * Helpers
 *
 *****************************************************************************/

static double
dmq_now(void)
{
	instr_time cur_time;
	INSTR_TIME_SET_CURRENT(cur_time);
	return INSTR_TIME_GET_MILLISEC(cur_time);
}

/*****************************************************************************
 *
 * Initialization
 *
 *****************************************************************************/

/* SIGHUP: set flag to reload configuration at next convenient time */
static void
dmq_sighup_handler(SIGNAL_ARGS)
{
	int			save_errno = errno;

	got_SIGHUP = true;

	/* Waken anything waiting on the process latch */
	SetLatch(MyLatch);

	errno = save_errno;
}

/*
 * Set pointer to dmq shared state in all backends.
 */
static void
dmq_shmem_startup_hook(void)
{
	bool found;
	HASHCTL		hash_info;

	if (PreviousShmemStartupHook)
		PreviousShmemStartupHook();

	MemSet(&hash_info, 0, sizeof(hash_info));
	hash_info.keysize = DMQ_NAME_MAXLEN;
	hash_info.entrysize = sizeof(DmqStreamSubscription);

	LWLockAcquire(AddinShmemInitLock, LW_EXCLUSIVE);

	dmq_state = ShmemInitStruct("dmq",
								sizeof(DmqSharedState),
								&found);
	if (!found)
	{
		int i;

		dmq_state->lock = &(GetNamedLWLockTranche("dmq"))->lock;
		dmq_state->out_dsm = DSM_HANDLE_INVALID;
		memset(dmq_state->destinations, '\0', sizeof(DmqDestination)*DMQ_MAX_DESTINATIONS);

		dmq_state->sender_pid = 0;
		dmq_state->nreceivers = 0;
		for (i = 0; i < DMQ_MAX_RECEIVERS; i++)
		{
			dmq_state->receiver[i][0] = '\0';
			dmq_state->receiver_dsm[i] = DSM_HANDLE_INVALID;
		}
	}

	// XXX: move it also under !found ?
	dmq_state->subscriptions = ShmemInitHash("dmq_stream_subscriptions",
								DMQ_MAX_SUBS_PER_BACKEND*MaxBackends,
								DMQ_MAX_SUBS_PER_BACKEND*MaxBackends,
								&hash_info,
								HASH_ELEM);

	LWLockRelease(AddinShmemInitLock);
}

static Size
dmq_shmem_size(void)
{
	Size	size = 0;

	size = add_size(size, sizeof(DmqSharedState));
	size = add_size(size, hash_estimate_size(DMQ_MAX_SUBS_PER_BACKEND*MaxBackends,
											 sizeof(DmqStreamSubscription)));
	return MAXALIGN(size);
}

void
dmq_init(void)
{
	BackgroundWorker worker;

	if (!process_shared_preload_libraries_in_progress)
		return;

	/* Reserve area for our shared state */
	RequestAddinShmemSpace(dmq_shmem_size());

	RequestNamedLWLockTranche("dmq", 1);

	/* Set up common data for all our workers */
	memset(&worker, 0, sizeof(worker));
	worker.bgw_flags = BGWORKER_SHMEM_ACCESS;
	worker.bgw_start_time = BgWorkerStart_ConsistentState;
	worker.bgw_restart_time = 5;
	worker.bgw_notify_pid = 0;
	worker.bgw_main_arg = 0;
	sprintf(worker.bgw_library_name, "multimaster");
	sprintf(worker.bgw_function_name, "dmq_sender_main");
	snprintf(worker.bgw_name, BGW_MAXLEN, "dmq_sender");
	snprintf(worker.bgw_type, BGW_MAXLEN, "dmq_sender");
	RegisterBackgroundWorker(&worker);

	/* Register shmem hook for all backends */
	PreviousShmemStartupHook = shmem_startup_hook;
	shmem_startup_hook = dmq_shmem_startup_hook;

	// on_proc_exit(dmq_at_exit, 0);
}

// void _PG_init(void);

// void
// _PG_init(void)
// {
// 	dmq_init();
// }

static Size
dmq_toc_size()
{
	int i;
	shm_toc_estimator e;

	shm_toc_initialize_estimator(&e);
	for (i = 0; i < MaxBackends; i++)
		shm_toc_estimate_chunk(&e, DMQ_MQ_SIZE);
	shm_toc_estimate_keys(&e, MaxBackends);
	return shm_toc_estimate(&e);
}


/*****************************************************************************
 *
 * Sender
 *
 *****************************************************************************/

// static void
// fe_close(PGconn *conn)
// {
// 	PQputCopyEnd(conn, NULL);
// 	PQflush(conn);
// 	PQfinish(conn);
// }

static int
fe_send(PGconn *conn, char *msg, size_t len)
{
	if (PQputCopyData(conn, msg, len) < 0)
		return -1;

	// XXX: move PQflush out of the loop?
	if (PQflush(conn) < 0)
		return -1;

	return 0;
}

void
dmq_sender_main(Datum main_arg)
{
	uintptr_t		i;
	dsm_segment	   *seg;
	shm_toc		   *toc;
	shm_mq_handle **mq_handles;
	WaitEventSet   *set;
	DmqDestination	conns[DMQ_MAX_DESTINATIONS];

	double	prev_timer_at = dmq_now();

	/* init this worker */
	pqsignal(SIGHUP, dmq_sighup_handler);
	pqsignal(SIGTERM, die);
	BackgroundWorkerUnblockSignals();

	/* setup queue receivers */
	seg = dsm_create(dmq_toc_size(), 0);
	dsm_pin_segment(seg);
	toc = shm_toc_create(DMQ_MQ_MAGIC, dsm_segment_address(seg),
						 dmq_toc_size());
	mq_handles = palloc(MaxBackends*sizeof(shm_mq_handle *));
	for (i = 0; i < MaxBackends; i++)
	{
		shm_mq	   *mq;
		mq = shm_mq_create(shm_toc_allocate(toc, DMQ_MQ_SIZE), DMQ_MQ_SIZE);
		shm_toc_insert(toc, i, mq);
		shm_mq_set_receiver(mq, MyProc);
		mq_handles[i] = shm_mq_attach(mq, seg, NULL);
	}

	for (i = 0; i < DMQ_MAX_DESTINATIONS; i++)
	{
		conns[i].active = false;
	}

	LWLockAcquire(dmq_state->lock, LW_EXCLUSIVE);
	dmq_state->sender_pid = MyProcPid;
	dmq_state->out_dsm = dsm_segment_handle(seg);
	LWLockRelease(dmq_state->lock);

	set = CreateWaitEventSet(CurrentMemoryContext, 15);
	AddWaitEventToSet(set, WL_POSTMASTER_DEATH, PGINVALID_SOCKET, NULL, NULL);
	AddWaitEventToSet(set, WL_LATCH_SET, PGINVALID_SOCKET, MyLatch, NULL);

	got_SIGHUP = true;

	for (;;)
	{
		WaitEvent	event;
		int			nevents;
		bool		wait = true;
		double		now_millisec;
		bool		timer_event = false;

		if (ProcDiePending)
			break;

		/*
		 * Read new connections from shared memory.
		 */
		if (got_SIGHUP)
		{
			got_SIGHUP = false;

			LWLockAcquire(dmq_state->lock, LW_SHARED);
			for (i = 0; i < DMQ_MAX_DESTINATIONS; i++)
			{
				DmqDestination *dest = &(dmq_state->destinations[i]);
				if (dest->active && !conns[i].active)
				{
					conns[i] = *dest;
					Assert(conns[i].pgconn == NULL);
					conns[i].state = Idle;
					prev_timer_at = 0; /* do not wait for timer event */
				}
			}
			LWLockRelease(dmq_state->lock);
		}

		/*
		 * Transfer data from backend queues to their remote counterparts.
		 */
		for (i = 0; i < MaxBackends; i++)
		{
			void	   *data;
			Size		len;
			shm_mq_result res;

			res = shm_mq_receive(mq_handles[i], &len, &data, true);
			if (res == SHM_MQ_SUCCESS)
			{
				int conn_id;

				/* first byte is connection_id */
				conn_id = * (char *) data;
				data = (char *) data + 1;
				len -= 1;
				Assert(0 <= conn_id && conn_id < DMQ_MAX_DESTINATIONS);

				if (conns[conn_id].state == Active)
				{
					int ret = fe_send(conns[conn_id].pgconn, data, len);

					if (ret < 0)
					{
						// Assert(PQstatus(conns[conn_id].pgconn) != CONNECTION_OK);
						conns[conn_id].state = Idle;
						DeleteWaitEvent(set, conns[conn_id].pos);

						elog(LOG,
							 "[DMQ] 1 broken connection to peer %d, idling: %s",
							 conn_id, PQerrorMessage(conns[i].pgconn));
					}
				}
				else
				{
					elog(LOG,
						 "[DMQ] dropping message to unconnected peer %d, '%s'",
						 conn_id, data);
				}

				wait = false;
			}
			else if (res == SHM_MQ_DETACHED)
			{
				shm_mq	   *mq = shm_mq_get_queue(mq_handles[i]);

				/*
				 * Overwrite old mq struct since mq api don't have a way to reattach
				 * detached queue.
				 */
				shm_mq_detach(mq_handles[i]);
				mq = shm_mq_create(mq, DMQ_MQ_SIZE);
				shm_mq_set_receiver(mq, MyProc);
				mq_handles[i] = shm_mq_attach(mq, seg, NULL);
				elog(LOG, "[DMQ] procno %d queue reattached", (int) i);
			}
		}

		/*
		 * Generate timeout or socket events.
		 */
		now_millisec = dmq_now();
		if (now_millisec - prev_timer_at > 250)
		{
			prev_timer_at = now_millisec;
			timer_event = true;
		}
		else
		{
			nevents = WaitEventSetWait(set, wait ? 250 : 0, &event,
									   1, PG_WAIT_EXTENSION);
		}

		/*
		 * Handle timer event: reconnect previously broken connection or
		 * send hearbeats.
		 */
		if (timer_event)
		{
			for (i = 0; i < DMQ_MAX_DESTINATIONS; i++)
			{
				/* Idle --> Connecting */
				if (conns[i].active && conns[i].state == Idle)
				{
					if (conns[i].pgconn)
						PQfinish(conns[i].pgconn);

					conns[i].pgconn = PQconnectStart(conns[i].connstr);

					if (PQstatus(conns[i].pgconn) == CONNECTION_BAD)
					{
						elog(WARNING, "[DMQ] connection to '%s': %s",
							 conns[i].connstr, PQerrorMessage(conns[i].pgconn));
						conns[i].state = Idle;
					}
					else
					{
						conns[i].pos = AddWaitEventToSet(set, WL_SOCKET_CONNECTED,
											PQsocket(conns[i].pgconn),
											NULL, (void *) i);
						conns[i].state = Connecting;
					}
				}
				/* Heatbeat */
				else if (conns[i].state == Active)
				{
					int ret = fe_send(conns[i].pgconn, "H", 2);
					if (ret < 0)
					{
						conns[i].state = Idle;
						DeleteWaitEvent(set, conns[i].pos);
						// Assert(PQstatus(conns[i].pgconn) != CONNECTION_OK);
						elog(LOG, "[DMQ] 1 broken connection to peer %d, idling: %s",
							 (int) i, PQerrorMessage(conns[i].pgconn));
					}
				}
			}
		}
		/*
		 * Handle all the connection machinery: consequently go through
		 * Connecting --> Negotiating --> Active states.
		 */
		else if (nevents > 0 && event.events & WL_SOCKET_MASK)
		{
			uint conn_id = (uint) event.user_data;

			switch (conns[conn_id].state)
			{
				case Idle:
					Assert(false);
					break;

				/* Await for connection establishment and call dmq_receiver_loop() */
				case Connecting:
				{
					PostgresPollingStatusType status = PQconnectPoll(conns[conn_id].pgconn);

					if (status == PGRES_POLLING_READING)
					{
						ModifyWaitEvent(set, event.pos, WL_SOCKET_READABLE, NULL);
					}
					else if (status == PGRES_POLLING_OK)
					{
						char *sender_name = conns[conn_id].sender_name;
						char *query = psprintf("select mtm.dmq_receiver_loop('%s')", sender_name);

						ModifyWaitEvent(set, event.pos, WL_SOCKET_READABLE, NULL);
						PQsendQuery(conns[conn_id].pgconn, query);
						conns[conn_id].state = Negotiating;
					}
					else if (status == PGRES_POLLING_FAILED)
					{
						conns[conn_id].state = Idle;
						DeleteWaitEvent(set, event.pos);
						elog(LOG, "[DMQ] [B1] failed to connect: %s",
							 PQerrorMessage(conns[conn_id].pgconn));
					}
					else
						Assert(status == PGRES_POLLING_WRITING);

					break;
				}

				/*
				 * Await for response to dmq_receiver_loop() call and switch to
				 * active state.
				 */
				case Negotiating:
					Assert(event.events & WL_SOCKET_READABLE);
					if (!PQconsumeInput(conns[conn_id].pgconn))
					{
						conns[conn_id].state = Idle;
						DeleteWaitEvent(set, event.pos);
						elog(LOG, "DMQ sender error: %s",
							 PQerrorMessage(conns[conn_id].pgconn));
					}
					if (!PQisBusy(conns[conn_id].pgconn))
					{
						conns[conn_id].state = Active;
						elog(LOG, "[DMQ] connection #%d established", conn_id);
					}
					break;

				/* Do nothing and check that connection is still alive */
				case Active:
					Assert(event.events & WL_SOCKET_READABLE);
					if (!PQconsumeInput(conns[conn_id].pgconn))
					{
						conns[conn_id].state = Idle;
						DeleteWaitEvent(set, event.pos);
						elog(LOG, "[DMQ] [B2] connection error: %s",
							 PQerrorMessage(conns[conn_id].pgconn));
					}
					break;
			}
		}
		else if (nevents > 0 && event.events & WL_LATCH_SET)
		{
			ResetLatch(MyLatch);
		}
		else if (nevents > 0 && event.events & WL_POSTMASTER_DEATH)
		{
			exit(1);
		}

		CHECK_FOR_INTERRUPTS();

		// XXX: handle WL_POSTMASTER_DEATH ?
	}
	FreeWaitEventSet(set);

}



/*****************************************************************************
 *
 * Receiver stuff
 *
 *****************************************************************************/

static void
dmq_handle_message(StringInfo msg, shm_mq_handle **mq_handles, dsm_segment *seg)
{
	const char *stream_name;
	const char *body;
	int 		body_len;
	bool		found;
	DmqStreamSubscription *sub;
	shm_mq_result res;
	shm_mq	   *mq;

	/*
	 * Consume stream_name packed as a string and interpret rest of the data
	 * as message body with unknown format that we are going to send down to
	 * the subscribed backend.
	 */
	stream_name = pq_getmsgrawstring(msg);
	body_len = msg->len - msg->cursor;
	body = pq_getmsgbytes(msg, body_len);
	pq_getmsgend(msg);

	/*
	 * Stream name "H" is reserved for simple heartbeats without body. So no
	 * need to send somewhere.
	 */
	if (strcmp(stream_name, "H") == 0)
	{
		Assert(body_len == 0);
		return;
	}

	/*
	 * Find subscriber.
	 * XXX: we can cache that and re-read shared memory upon a signal, but
	 * likely that won't show any measurable speedup.
	 */
	LWLockAcquire(dmq_state->lock, LW_SHARED);
	sub = (DmqStreamSubscription *) hash_search(dmq_state->subscriptions,
												stream_name, HASH_FIND,
												&found);
	LWLockRelease(dmq_state->lock);

	if (!found)
	{
		elog(WARNING,
			"dmq_receiver_loop: subscription %s is not found (body = %s)",
			stream_name, body);
		return;
	}

	/* select queue and reconnect it if needed */
	mq = shm_mq_get_queue(mq_handles[sub->procno]);
	if (shm_mq_get_sender(mq) == NULL)
	{
		shm_mq_set_sender(mq, MyProc);
		mq_handles[sub->procno] = shm_mq_attach(mq, seg, NULL);
	}

	elog(LOG, "got message %s.%s, passing to %d", stream_name, body, sub->procno);

	/* and send it */
	res = shm_mq_send(mq_handles[sub->procno], body_len, body, false);
	if (res != SHM_MQ_SUCCESS)
	{
		elog(WARNING, "dmq_receiver_loop: can't send to queue");
	}
}

#define DMQ_RECV_BUFFER 8192
static char recv_buffer[DMQ_RECV_BUFFER];
static int  recv_bytes;
static int  read_bytes;

/*
 * _pq_have_full_message.
 *
 * Check if our recv buffer has fully received message. Also left-justify
 * message in buffer if it doesn't fit in buffer starting from current
 * position.
 *
 * Return 1 and fill given StringInfo if there is message and return 0
 * otherwise.
 */
static int
_pq_have_full_message(StringInfo s)
{

	/* Have we got message length header? */
	if (recv_bytes - read_bytes >= 4)
	{
		int len = pg_ntoh32( * (int *) (recv_buffer + read_bytes) );

		Assert(len < DMQ_RECV_BUFFER);

		if (read_bytes + len <= recv_bytes)
		{
			/* Have full message, wrap it as StringInfo and return */
			s->data = recv_buffer + read_bytes;
			s->maxlen = s->len = len;
			s->cursor = 4;
			read_bytes += len;
			return 1;
		}
		else if (read_bytes + len >= DMQ_RECV_BUFFER)
		{
			memmove(recv_buffer, recv_buffer + read_bytes,
									recv_bytes - read_bytes);
			recv_bytes -= read_bytes;
			read_bytes = 0;
		}
	}

	return 0;
}

/*
 * _pq_getmessage_if_avalable()
 *
 * Get pq message in non-blocking mode. This uses it own recv buffer instead
 * of pqcomm one, since they are private to pqcomm.c.
 *
 * Returns 0 when no full message are available, 1 when we got message and EOF
 * in case of connection problems.
 *
 * Caller should not wait on latch after we've got message -- there can be
 * several of them in our buffer.
 */
static int
_pq_getmessage_if_avalable(StringInfo s)
{
	int rc;

	/* Check if we have full messages after previous call */
	if (_pq_have_full_message(s) > 0)
		return 1;

	if (read_bytes > 0)
	{
		if (recv_bytes == read_bytes)
		{
			/* no partially read messages, so just start over */
			read_bytes = recv_bytes = 0;
		}
		else
		{
			/*
			 * Move data to the left in case we are near the buffer end.
			 * Case when message starts earlier in the buffer and spans
			 * past it's end handled separately down the lines.
			 */
			Assert(recv_bytes > read_bytes);
			if (recv_bytes > (DMQ_RECV_BUFFER - 1024))
			{
				memmove(recv_buffer, recv_buffer + read_bytes,
										recv_bytes - read_bytes);
				recv_bytes -= read_bytes;
				read_bytes = 0;
			}
		}
	}

	/* defuse secure_read() from blocking */
	MyProcPort->noblock = true;

	rc = secure_read(MyProcPort, recv_buffer + recv_bytes,
						DMQ_RECV_BUFFER - recv_bytes);

	if (rc < 0)
	{
		if (errno != EAGAIN && errno != EWOULDBLOCK && errno != EINTR)
			return EOF;
	}
	else if (rc == 0)
	{
		return EOF;
	}
	else
	{
		recv_bytes += rc;
		Assert(recv_bytes >= read_bytes && recv_bytes < DMQ_RECV_BUFFER);

		/*
		 * Here we need to re-check for full message again, so the caller will know
		 * whether he should wait for event on socket.
		 */
		if (_pq_have_full_message(s) > 0)
			return 1;
	}

	return 0;
}


/*
 * _pq_getbyte_if_available.
 *
 * Same as pq_getbyte_if_available(), but works with our recv buffer.
 *
 * 1 on success, 0 if no data available, EOF on connection error.
 */
static int
_pq_getbyte_if_available(unsigned char *c)
{
	int rc;

	/*
	 * That is why we re-implementing this function: byte ccan be already in
	 * our recv buffer, so pqcomm version will miss it.
	 */
	if (recv_bytes > read_bytes)
	{
		*c = recv_buffer[read_bytes++];
		return 1;
	}

	/* defuse secure_read() from blocking */
	MyProcPort->noblock = true;

	/* read directly into *c skipping recv_buffer */
	rc = secure_read(MyProcPort, c, 1);

	if (rc < 0)
	{
		if (errno != EAGAIN && errno != EWOULDBLOCK && errno != EINTR)
			return EOF;
	}
	else if (rc == 0)
	{
		return EOF;
	}
	else
	{
		Assert(rc == 1);
		return rc;
	}

	return 0;

}

static void
dmq_receiver_at_exit(int status, Datum sender)
{
	int sender_id = DatumGetInt32(sender);

	LWLockAcquire(dmq_state->lock, LW_EXCLUSIVE);
	dmq_state->receiver[DatumGetInt32(sender_id)][0] = '\0';
	LWLockRelease(dmq_state->lock);

}


Datum
dmq_receiver_loop(PG_FUNCTION_ARGS)
{
	enum
	{
		NeedByte,
		NeedMessage
	} reader_state = NeedByte;

	dsm_segment		   *seg;
	shm_toc			   *toc;
	StringInfoData		s;
	shm_mq_handle	  **mq_handles;
	char			   *sender_name;
	int					i;
	int					receiver_id;
	double				last_message_at = dmq_now();

	sender_name = text_to_cstring(PG_GETARG_TEXT_PP(0));

	/* setup queues with backends */
	seg = dsm_create(dmq_toc_size(), 0);
	toc = shm_toc_create(DMQ_MQ_MAGIC, dsm_segment_address(seg),
						 dmq_toc_size());
	mq_handles = palloc(MaxBackends*sizeof(shm_mq_handle *));
	for (i = 0; i < MaxBackends; i++)
	{
		shm_mq	   *mq;
		mq = shm_mq_create(shm_toc_allocate(toc, DMQ_MQ_SIZE), DMQ_MQ_SIZE);
		shm_toc_insert(toc, i, mq);
		shm_mq_set_sender(mq, MyProc);
		mq_handles[i] = shm_mq_attach(mq, seg, NULL);
	}

	/* register ourself in dmq_state */
	LWLockAcquire(dmq_state->lock, LW_EXCLUSIVE);
	/* check for a conflicting receiver_name */
	for (i = 0; i < DMQ_MAX_RECEIVERS; i++)
	{
		if (strcmp(dmq_state->receiver[i], sender_name) == 0)
			elog(ERROR, "sender '%s' already connected", sender_name);
	}
	receiver_id = dmq_state->nreceivers;
	dmq_state->receiver_dsm[receiver_id] = dsm_segment_handle(seg);
	strncpy(dmq_state->receiver[receiver_id], sender_name, DMQ_NAME_MAXLEN);
	dmq_state->nreceivers++;
	LWLockRelease(dmq_state->lock);

	on_shmem_exit(dmq_receiver_at_exit, Int32GetDatum(receiver_id));

	/* okay, switch client to copyout state */
	pq_beginmessage(&s, 'W');
	pq_sendbyte(&s, 0);
	pq_sendint16(&s, 0);
	pq_endmessage(&s);
	pq_flush();

	pq_startmsgread();

	ModifyWaitEvent(FeBeWaitSet, 0, WL_SOCKET_READABLE, NULL);

	if (dmq_receiver_start_hook)
		dmq_receiver_start_hook(sender_name);

	for (;;)
	{
		unsigned char	qtype;
		WaitEvent		event;
		int				rc;
		int				nevents;

		if (reader_state == NeedByte)
		{
			rc = _pq_getbyte_if_available(&qtype);

			if (rc > 0)
			{
				if (qtype == 'd')
				{
					reader_state = NeedMessage;
				}
				else if (qtype == 'c')
				{
					break;
				}
				else
				{
					elog(ERROR, "dmq_recv_be: invalid message type %c, %s",
								qtype, s.data);
				}
			}
		}

		if (reader_state == NeedMessage)
		{
			rc = _pq_getmessage_if_avalable(&s);

			if (rc > 0)
			{
				last_message_at = dmq_now();
				dmq_handle_message(&s, mq_handles, seg);
				reader_state = NeedByte;
			}
		}

		if (rc == EOF)
		{
			ereport(COMMERROR,
				(errcode(ERRCODE_PROTOCOL_VIOLATION),
				 errmsg("dmq_recv_be: EOF on connection")));
			break;
		}

		nevents = WaitEventSetWait(FeBeWaitSet, 250, &event, 1,
								   WAIT_EVENT_CLIENT_READ);

		if (nevents > 0 && event.events & WL_LATCH_SET)
		{
			ResetLatch(MyLatch);
		}

		// XXX: is it enough?
		CHECK_FOR_INTERRUPTS();

		if (dmq_now() - last_message_at > 5000)
		{
			elog(ERROR, "[DMQ] exit receiver due to heatbeat timeout");
		}

	}
	pq_endmsgread();

	PG_RETURN_VOID();
}


/*****************************************************************************
 *
 * API stuff
 *
 *****************************************************************************/

static void
ensure_outq_handle()
{
	dsm_segment *seg;
	shm_toc    *toc;
	shm_mq	   *outq;
	MemoryContext oldctx;


	if (dmq_local.mq_outh != NULL)
		return;

	// Assert(dmq_state->handle != DSM_HANDLE_INVALID);
	seg = dsm_attach(dmq_state->out_dsm);
	if (seg == NULL)
		ereport(ERROR,
				(errcode(ERRCODE_OBJECT_NOT_IN_PREREQUISITE_STATE),
				 errmsg("unable to map dynamic shared memory segment")));

	dsm_pin_mapping(seg);

	toc = shm_toc_attach(DMQ_MQ_MAGIC, dsm_segment_address(seg));
	if (toc == NULL)
		ereport(ERROR,
				(errcode(ERRCODE_OBJECT_NOT_IN_PREREQUISITE_STATE),
				 errmsg("bad magic number in dynamic shared memory segment")));

	outq = shm_toc_lookup(toc, MyProc->pgprocno, false);
	shm_mq_set_sender(outq, MyProc);

	// elog(LOG, "XXXshm_mq_set_sender %d", MyProc->pgprocno);

	oldctx = MemoryContextSwitchTo(TopMemoryContext);
	dmq_local.mq_outh = shm_mq_attach(outq, seg, NULL);
	MemoryContextSwitchTo(oldctx);
}

void
dmq_push(DmqDestinationId dest_id, char *stream_name, char *msg)
{
	shm_mq_result res;
	StringInfoData buf;

	ensure_outq_handle();

	initStringInfo(&buf);
	pq_sendbyte(&buf, dest_id);
	pq_send_ascii_string(&buf, stream_name);
	pq_send_ascii_string(&buf, msg);

	// elog(LOG, "pushing l=%d '%.*s'", buf.len, buf.len, buf.data);

	res = shm_mq_send(dmq_local.mq_outh, buf.len, buf.data, false);
	if (res != SHM_MQ_SUCCESS)
		elog(ERROR, "dmq_push: can't send to queue");

	resetStringInfo(&buf);
}

void
dmq_stream_subscribe(char *sender_name, char *stream_name)
{
	bool found;
	DmqStreamSubscription *sub;
	int receiver_id = -1;

	dsm_segment	   *seg;
	shm_toc		   *toc;
	shm_mq		   *inq;
	MemoryContext oldctx;

	LWLockAcquire(dmq_state->lock, LW_EXCLUSIVE);
	sub = (DmqStreamSubscription *) hash_search(dmq_state->subscriptions, stream_name,
												HASH_ENTER, &found);
	if (found && sub->procno != MyProc->pgprocno)
	{
		elog(ERROR, "procno%d: %s: subscription is already active for procno %d / %s",
				MyProc->pgprocno, stream_name, sub->procno, sub->stream_name);
	}
	else
		sub->procno = MyProc->pgprocno;
	LWLockRelease(dmq_state->lock);

	/* await for sender to connect */
	LWLockAcquire(dmq_state->lock, LW_SHARED);
	for (;;)
	{
		int i;

		for (i = 0; i < DMQ_MAX_RECEIVERS; i++)
		{
			if (strcmp(dmq_state->receiver[i], sender_name) == 0)
			{
				receiver_id = i;
				break;
			}
		}

		if (receiver_id >= 0)
			break;

		LWLockRelease(dmq_state->lock);
		pg_usleep(100000L);
		CHECK_FOR_INTERRUPTS();
		LWLockAcquire(dmq_state->lock, LW_SHARED);
	}

	Assert(dmq_state->receiver_dsm[receiver_id] != DSM_HANDLE_INVALID);

	seg = dsm_attach(dmq_state->receiver_dsm[receiver_id]);
	if (seg == NULL)
		ereport(ERROR,
				(errcode(ERRCODE_OBJECT_NOT_IN_PREREQUISITE_STATE),
				 errmsg("unable to map dynamic shared memory segment")));

	LWLockRelease(dmq_state->lock);

	dsm_pin_mapping(seg);

	toc = shm_toc_attach(DMQ_MQ_MAGIC, dsm_segment_address(seg));
	if (toc == NULL)
		ereport(ERROR,
				(errcode(ERRCODE_OBJECT_NOT_IN_PREREQUISITE_STATE),
				 errmsg("bad magic number in dynamic shared memory segment")));

	inq = shm_toc_lookup(toc, MyProc->pgprocno, false);
	// xxx memleak
	inq = shm_mq_create(inq, DMQ_MQ_SIZE);
	shm_mq_set_receiver(inq, MyProc);

	oldctx = MemoryContextSwitchTo(TopMemoryContext);
	dmq_local.mq_inh[dmq_local.n_inhandles++] = shm_mq_attach(inq, seg, NULL);
	MemoryContextSwitchTo(oldctx);
}

void
dmq_pop(DmqSenderId *sender_id, StringInfo msg)
{
	shm_mq_result res;

	for (;;)
	{
		int i;

		for (i = 0; i < dmq_local.n_inhandles; i++)
		{
			Size	len;
			void   *data;

			res = shm_mq_receive(dmq_local.mq_inh[i], &len, &data, true);
			if (res == SHM_MQ_SUCCESS)
			{
				msg->data = data;
				msg->len = len;
				msg->maxlen = len;
				msg->cursor = 0;
				*sender_id = i;
				// elog(LOG, "pop message %s, from %d", (char *) *dataptr, i);
				return;
			}
			else if (res == SHM_MQ_DETACHED)
			{
				elog(ERROR, "dmq_pop: queue detached");
			}
		}

		// XXX cache that
		WaitLatch(MyLatch, WL_LATCH_SET, 10, WAIT_EVENT_MQ_RECEIVE);
		ResetLatch(MyLatch);
		CHECK_FOR_INTERRUPTS();
	}
}


DmqDestinationId
dmq_destination_add(char *connstr, char *sender_name, int ping_period)
{
	DmqDestinationId dest_id;
	pid_t sender_pid;

	LWLockAcquire(dmq_state->lock, LW_EXCLUSIVE);
	for (dest_id = 0; dest_id < DMQ_MAX_DESTINATIONS; dest_id++)
	{
		DmqDestination *dest = &(dmq_state->destinations[dest_id]);
		if (!dest->active)
		{
			strncpy(dest->sender_name, sender_name, DMQ_NAME_MAXLEN);
			strncpy(dest->connstr, connstr, DMQ_CONNSTR_MAX_LEN);
			dest->ping_period = ping_period;
			dest->active = true;
			break;
		}
	}
	sender_pid = dmq_state->sender_pid;
	LWLockRelease(dmq_state->lock);

	if (sender_pid)
		kill(sender_pid, SIGHUP);

	if (dest_id == DMQ_MAX_DESTINATIONS)
		elog(ERROR, "Can't add new destination. DMQ_MAX_DESTINATIONS reached.");
	else
		return dest_id;
}


void
dmq_push_buffer(DmqDestinationId dest_id, char *stream_name, const void *payload, size_t len)
{
	StringInfoData buf;
	shm_mq_result res;

	ensure_outq_handle();

	initStringInfo(&buf);
	pq_sendbyte(&buf, dest_id);
	pq_send_ascii_string(&buf, stream_name);
	pq_sendbytes(&buf, payload, len);

	// elog(LOG, "pushing l=%d '%.*s'", buf.len, buf.len, buf.data);

	res = shm_mq_send(dmq_local.mq_outh, buf.len, buf.data, false);
	if (res != SHM_MQ_SUCCESS)
		elog(ERROR, "dmq_push: can't send to queue");
}

