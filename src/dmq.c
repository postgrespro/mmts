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
 * XXX: needs better PQerror reporting logic -- perhaps once per given Idle
 * connection.
 *
 * XXX: is there max size for a connstr?
 *
 * Portions Copyright (c) 1996-2018, PostgreSQL Global Development Group
 * Portions Copyright (c) 1994, Regents of the University of California
 *
 *----------------------------------------------------------------------------
 */
#include "postgres.h"

#include "dmq.h"
#include "logger.h"

#include "access/transam.h"
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
#include "utils/ps_status.h"

#define DMQ_MQ_SIZE  ((Size) 65536)
#define DMQ_MQ_MAGIC 0x646d71

/*  XXX: move to common */
#define BIT_CLEAR(mask, bit) ((mask) &= ~((uint64)1 << (bit)))
#define BIT_CHECK(mask, bit) (((mask) & ((uint64)1 << (bit))) != 0)

/*
 * Shared data structures to hold current connections topology.
 * All that stuff can be moved to persistent tables to avoid hardcoded
 * size limits, but now it doesn't seems to be worth of troubles.
 */

#define DMQ_CONNSTR_MAX_LEN 150

#define DMQ_MAX_SUBS_PER_BACKEND 10
#define DMQ_MAX_DESTINATIONS 10
#define DMQ_MAX_RECEIVERS 10

typedef enum
{
	Idle,						/* upon init or falure */
	Connecting,					/* upon PQconnectStart */
	Negotiating,				/* upon PQconnectPoll == OK */
	Active,						/* upon dmq_receiver_loop() response */
} DmqConnState;

typedef struct
{
	bool		active;
	char		sender_name[DMQ_NAME_MAXLEN];
	char		receiver_name[DMQ_NAME_MAXLEN];
	char		connstr[DMQ_CONNSTR_MAX_LEN];
	int			recv_timeout;
	PGconn	   *pgconn;
	DmqConnState state;
	int			pos;
	int8		mask_pos;
} DmqDestination;

typedef struct
{
	char		stream_name[DMQ_NAME_MAXLEN];
	int			procno;
	uint64		procno_gen;
} DmqStreamSubscription;

/* receiver publishes this in shmem to let subscriber find his shm_mq */
typedef struct
{
	dsm_handle	h;
	uint64		procno_gen;
} ReceiverDSMHandle;

/* receiver state in shmem */
typedef struct
{
	char		name[DMQ_NAME_MAXLEN];
	pid_t		pid;
	ReceiverDSMHandle *dsm_handles; /* indexed by pgprocno */
} DmqReceiverSlot;

/* Global state for dmq */
struct DmqSharedState
{
	LWLock	   *lock;

	/* sender stuff */
	pid_t		sender_pid;
	dsm_handle	out_dsm;
	DmqDestination destinations[DMQ_MAX_DESTINATIONS];
	/*
	 * Stores counters incremented on each reconnect to destination, indexed
	 * by receiver mask_pos. This allows to detect conn failures to avoid
	 * infinite waiting for response when request could have been dropped,
	 * c.f. dmq_fill_sconn_cnt and dmq_purge_failed_participants.
	 *
	 * XXX This mechanism is unreliable and ugly.
	 * Unreliable, because though it saves from infinite waiting for reply, it
	 * doesn't save from potential deadlocks. Deadlocks may arise whenever a
	 * loop of nodes makes request-response sequences because request A->B and
	 * A's response to B's request go via the same TCP channel; thus, if all
	 * queues of loop in one direction are filled with requests, nobody will
	 * be able to answer.
	 *
	 * We could control the situation by ensuring 1) all possible requests
	 * node sends at time could fit in the output buffers 2) node never
	 * repeats the request until the previous one is delivered or dropped.
	 * However, to honor the 2), we must terminate send connection whenever
	 * receive conn failed (and thus we gonna to retry the requests) to flush
	 * previous possible undelivered requests, which we don't do currently.
	 *
	 * Ultimate non-deadlockable solution without such hacks would be to
	 * divide the job between different channels: A sends its requests to B
	 * and recevies responses from it via one TCP channel, and B sends its
	 * requests to A and receives responses via another one. Probably this is
	 * not worthwhile though as it would make dmq more complicated and
	 * increase number of shm_mqs.
	 *
	 * Besides, the counters are ugly because they require the external code
	 * to remember sender counters before request and check them while
	 * waiting for reply; moreover, this checking must be based on timouts
	 * as nobody would wake the clients on send conn failures.
	 *
	 * No locks are used because we don't care much about correct/up-to-date
	 * reads, though well aligned ints are atomic anyway.
	 */
	volatile int sconn_cnt[DMQ_MAX_DESTINATIONS];

	/*
	 * Receivers stuff. Some notes on shm_mq management: one of the subtle
	 * requirements here is avoid losing the message with live receiver, or we
	 * might fool the counterparty to wait for an answer while request has
	 * been dropped. OTOH, if receiver conn is dead, there is no point in
	 * waiting for messages as we obviously never know when they arrive. Hence
	 * naturally follows that mq should be created by receiver, not subscriber
	 * -- i.e. attached queue at subscriber side must mean receiver appeared
	 * (so we can wait for messages), and if he dies, he will detach from the
	 * queue so we'll get an error in shm_mq_receive.
	 *
	 * shm_mq doesn't allow reattachment to the queue, and even if it did, it
	 * would be cumbersome to maintain the failure detection logic as outlined
	 * above (see dmq_pop_nb for user-facing semantics). So each
	 * receiver-subscriber pair uses separate shm_mq (and underlying dsm
	 * segment). To distinguish between different processes with the same
	 * pgprocno, procno generations were invented. As additional nicety, this
	 * means subscriber can never get messages aimed to another backend (with
	 * the same procno). Also mqs are created on demand, backend who never
	 * subscribes doesn't waste memory on queue (though currently this is of
	 * little help as mqs for dead backends are not cleaned up until they are
	 * reused).
	 */

	/*
	 * using separate cv per receiver would be perceptibly more complicated
	 * and negligibly more performant
	 */
	ConditionVariable shm_mq_creation_cv;
	/*
	 * Indexed by pgprocno; each subscriber increments himself here so we
	 * could distinguish different processes with the same pgprocno.
	 */
	uint64 *procno_gens;
	DmqReceiverSlot receivers[DMQ_MAX_RECEIVERS];
}		   *dmq_state;

/* special value for sconn_cnt[] meaning the connection is dead */
#define DMQSCONN_DEAD 0

static HTAB *dmq_subscriptions;

/* Backend-local i/o queues. */
struct
{
	/* to send */
	shm_mq_handle *mq_outh;

	/* to receive */
	uint64	my_procno_gen;
	int			n_inhandles;
	struct
	{
		dsm_segment *dsm_seg;
		shm_mq_handle *mqh;
		char		name[DMQ_NAME_MAXLEN];
		int8		mask_pos;
	}			inhandles[DMQ_MAX_RECEIVERS];
}			dmq_local;

/* Flags set by signal handlers */
static volatile sig_atomic_t got_SIGHUP = false;

static shmem_startup_hook_type PreviousShmemStartupHook;

void *(*dmq_receiver_start_hook)(char *sender_name);
dmq_hook_type dmq_receiver_stop_hook;
dmq_hook_type dmq_sender_connect_hook;
dmq_hook_type dmq_sender_disconnect_hook;
void (*dmq_sender_heartbeat_hook)(char *receiver_name, StringInfo buf) = NULL;
void (*dmq_receiver_heartbeat_hook)(char *sender_name, StringInfo msg, void *extra) = NULL;

void		dmq_sender_main(Datum main_arg);

PG_FUNCTION_INFO_V1(dmq_receiver_loop);

/*****************************************************************************
 *
 * Helpers
 *
 *****************************************************************************/

static double
dmq_now(void)
{
	instr_time	cur_time;

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

/* per one receiver slot */
static Size
receiver_dsm_handles_size(void)
{
	return mul_size(sizeof(ReceiverDSMHandle), MaxBackends);
}

/*
 * Set pointer to dmq shared state in all backends.
 */
static void
dmq_shmem_startup_hook(void)
{
	bool		found;
	HASHCTL		hash_info;

	if (PreviousShmemStartupHook)
		PreviousShmemStartupHook();

	MemSet(&hash_info, 0, sizeof(hash_info));
	hash_info.keysize = DMQ_NAME_MAXLEN;
	hash_info.entrysize = sizeof(DmqStreamSubscription);

	LWLockAcquire(AddinShmemInitLock, LW_EXCLUSIVE);

	dmq_state = ShmemInitStruct("dmq",
								sizeof(struct DmqSharedState),
								&found);
	if (!found)
	{
		int			i;
		bool		procno_gens_found;

		dmq_state->lock = &(GetNamedLWLockTranche("dmq"))->lock;
		dmq_state->out_dsm = DSM_HANDLE_INVALID;
		memset(dmq_state->destinations, '\0', sizeof(DmqDestination) * DMQ_MAX_DESTINATIONS);

		dmq_state->sender_pid = 0;

		ConditionVariableInit(&dmq_state->shm_mq_creation_cv);
		dmq_state->procno_gens =
			ShmemInitStruct("dmq-procnogens",
							mul_size(sizeof(uint64), MaxBackends),
							&procno_gens_found);
		Assert(!procno_gens_found);
		MemSet(dmq_state->procno_gens, '\0', sizeof(uint64) * MaxBackends);

		for (i = 0; i < DMQ_MAX_RECEIVERS; i++)
		{
			bool dsm_handles_found;
			char dsm_handles_shmem_name[32];

			dmq_state->receivers[i].name[0] = '\0';
			dmq_state->receivers[i].pid = 0;
			snprintf(dsm_handles_shmem_name, 32, "dmq-%d", i);
			dmq_state->receivers[i].dsm_handles =
				ShmemInitStruct(dsm_handles_shmem_name,
								receiver_dsm_handles_size(),
								&dsm_handles_found);
			Assert(!dsm_handles_found);
			MemSet(dmq_state->receivers[i].dsm_handles, '\0',
				   receiver_dsm_handles_size());
		}
	}

	dmq_subscriptions = ShmemInitHash("dmq_stream_subscriptions",
									  DMQ_MAX_SUBS_PER_BACKEND * MaxBackends,
									  DMQ_MAX_SUBS_PER_BACKEND * MaxBackends,
									  &hash_info,
									  HASH_ELEM);

	LWLockRelease(AddinShmemInitLock);
}

static Size
dmq_shmem_size(void)
{
	Size		size = 0;

	size = add_size(size, sizeof(struct DmqSharedState));
	size = add_size(size, hash_estimate_size(DMQ_MAX_SUBS_PER_BACKEND * MaxBackends,
											 sizeof(DmqStreamSubscription)));
	return MAXALIGN(size);
}

void
dmq_init(int send_timeout)
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
	worker.bgw_main_arg = send_timeout;
	sprintf(worker.bgw_library_name, "multimaster");
	sprintf(worker.bgw_function_name, "dmq_sender_main");
	snprintf(worker.bgw_name, BGW_MAXLEN, "mtm-dmq-sender");
	snprintf(worker.bgw_type, BGW_MAXLEN, "mtm-dmq-sender");
	RegisterBackgroundWorker(&worker);

	/* Register shmem hooks */
	PreviousShmemStartupHook = shmem_startup_hook;
	shmem_startup_hook = dmq_shmem_startup_hook;

}

static Size
dmq_toc_size()
{
	int			i;
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

static int
fe_send(PGconn *conn, char *msg, size_t len)
{
	if (PQputCopyData(conn, msg, len) < 0)
		return -1;

	/* XXX: move PQflush out of the loop? */
	if (PQflush(conn) < 0)
		return -1;

	return 0;
}

static void
dmq_send(DmqDestination *conns, int conn_id, char *data, size_t len)
{
	int			ret = fe_send(conns[conn_id].pgconn, data, len);

	if (ret < 0)
	{
		conns[conn_id].state = Idle;
		dmq_state->sconn_cnt[conns[conn_id].mask_pos] = DMQSCONN_DEAD;

		mtm_log(DmqStateFinal,
				"[DMQ] failed to send message to %s: %s",
				conns[conn_id].receiver_name,
				PQerrorMessage(conns[conn_id].pgconn));

		dmq_sender_disconnect_hook(conns[conn_id].receiver_name);
	}
	else if (data[0] != 'H') /* skip logging heartbeats */
	{
		mtm_log(DmqTraceOutgoing,
				"[DMQ] sent message (l=%zu, m=%s) to %s",
				len, (char *) data, conns[conn_id].receiver_name);
	}
}

static void
dmq_sender_at_exit(int status, Datum arg)
{
	int			i;

	LWLockAcquire(dmq_state->lock, LW_SHARED);
	for (i = 0; i < DMQ_MAX_RECEIVERS; i++)
	{
		if (dmq_state->receivers[i].name[0] != '\0' &&
			dmq_state->receivers[i].pid > 0)
		{
			kill(dmq_state->receivers[i].pid, SIGTERM);
		}
	}
	LWLockRelease(dmq_state->lock);
}

void
dmq_sender_main(Datum main_arg)
{
	int			i;
	dsm_segment *seg;
	shm_toc    *toc;
	shm_mq_handle **mq_handles;
	WaitEventSet *set;
	DmqDestination conns[DMQ_MAX_DESTINATIONS];
	int			heartbeat_send_timeout = DatumGetInt32(main_arg);
	StringInfoData heartbeat_buf; /* heartbeat data is accumulated here */
	/*
	 * Seconds dmq_state->sconn_cnt to save the counter value when
	 * conn is dead.
	 */
	int			sconn_cnt[DMQ_MAX_DESTINATIONS];

	double		prev_timer_at = dmq_now();

	on_shmem_exit(dmq_sender_at_exit, (Datum) 0);
	initStringInfo(&heartbeat_buf);

	/* init this worker */
	pqsignal(SIGHUP, dmq_sighup_handler);
	pqsignal(SIGTERM, die);
	BackgroundWorkerUnblockSignals();

	/* setup queue receivers */
	seg = dsm_create(dmq_toc_size(), 0);
	dsm_pin_segment(seg);
	toc = shm_toc_create(DMQ_MQ_MAGIC, dsm_segment_address(seg),
						 dmq_toc_size());
	mq_handles = palloc(MaxBackends * sizeof(shm_mq_handle *));
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

				/* start connection for a freshly added destination */
				if (dest->active && !conns[i].active)
				{
					conns[i] = *dest;
					Assert(conns[i].pgconn == NULL);
					conns[i].state = Idle;
					sconn_cnt[dest->mask_pos] = 0;
					dmq_state->sconn_cnt[dest->mask_pos] = DMQSCONN_DEAD;
					prev_timer_at = 0;	/* do not wait for timer event */
				}
				/* close connection to deleted destination */
				else if (!dest->active && conns[i].active)
				{
					PQfinish(conns[i].pgconn);
					conns[i].active = false;
					conns[i].pgconn = NULL;
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
				int			conn_id;

				/* first byte is connection_id */
				conn_id = *(char *) data;
				data = (char *) data + 1;
				len -= 1;
				Assert(0 <= conn_id && conn_id < DMQ_MAX_DESTINATIONS);

				if (conns[conn_id].state == Active)
				{
					dmq_send(conns, conn_id, data, len);
				}
				else
				{
					mtm_log(WARNING,
							"[DMQ] dropping message (l=%zu, m=%s) to disconnected %s",
							len, (char *) data, conns[conn_id].receiver_name);
				}

				wait = false;
			}
			else if (res == SHM_MQ_DETACHED)
			{
				shm_mq	   *mq = shm_mq_get_queue(mq_handles[i]);

				/*
				 * Overwrite old mq struct since mq api don't have a way to
				 * reattach detached queue.
				 */
				shm_mq_detach(mq_handles[i]);
				mq = shm_mq_create(mq, DMQ_MQ_SIZE);
				shm_mq_set_receiver(mq, MyProc);
				mq_handles[i] = shm_mq_attach(mq, seg, NULL);

				mtm_log(DmqTraceShmMq,
						"[DMQ] sender reattached shm_mq to procno %d", i);
			}
		}

		/*
		 * Generate timeout or socket events.
		 *
		 *
		 * XXX: here we expect that whole cycle takes less then 250-100 ms.
		 * Otherwise we can stuck with timer_event forever.
		 */
		now_millisec = dmq_now();
		if (now_millisec - prev_timer_at > heartbeat_send_timeout)
		{
			prev_timer_at = now_millisec;
			timer_event = true;
		}
		else
		{
			nevents = WaitEventSetWait(set, wait ? 100 : 0, &event,
									   1, PG_WAIT_EXTENSION);
		}

		/*
		 * Handle timer event: reconnect previously broken connection or send
		 * hearbeats.
		 */
		if (timer_event)
		{
			uintptr_t	conn_id;

			for (conn_id = 0; conn_id < DMQ_MAX_DESTINATIONS; conn_id++)
			{
				if (!conns[conn_id].active)
					continue;

				/* Idle --> Connecting */
				if (conns[conn_id].state == Idle)
				{
					double		pqtime;

					if (conns[conn_id].pgconn)
						PQfinish(conns[conn_id].pgconn);

					pqtime = dmq_now();
					mtm_log(DmqStateIntermediate, "[DMQ] PQconnectStart to %s", conns[conn_id].connstr);
					conns[conn_id].pgconn = PQconnectStart(conns[conn_id].connstr);
					mtm_log(DmqPqTiming, "[DMQ] [TIMING] pqs = %f ms", dmq_now() - pqtime);

					if (PQstatus(conns[conn_id].pgconn) == CONNECTION_BAD)
					{
						conns[conn_id].state = Idle;

						mtm_log(DmqStateFinal,
								"[DMQ] failed to start connection with %s (%s): %s",
								conns[conn_id].receiver_name,
								conns[conn_id].connstr,
								PQerrorMessage(conns[conn_id].pgconn));
					}
					else
					{
						conns[conn_id].state = Connecting;
						conns[conn_id].pos = AddWaitEventToSet(set, WL_SOCKET_CONNECTED,
															   PQsocket(conns[conn_id].pgconn),
															   NULL, (void *) conn_id);

						mtm_log(DmqStateIntermediate,
								"[DMQ] switching %s from Idle to Connecting on '%s'",
								conns[conn_id].receiver_name,
								conns[conn_id].connstr);
					}
				}
				/* Heartbeat */
				else if (conns[conn_id].state == Active)
				{
					resetStringInfo(&heartbeat_buf);
					/* stream name is cstring by convention */
					appendStringInfoChar(&heartbeat_buf, 'H');
					appendStringInfoChar(&heartbeat_buf, '\0');
					/* Allow user to stuff some payload into the heartbeat */
					if (dmq_sender_heartbeat_hook != NULL)
						dmq_sender_heartbeat_hook(conns[conn_id].receiver_name,
												  &heartbeat_buf);
					dmq_send(conns, conn_id, heartbeat_buf.data, heartbeat_buf.len);
				}
			}
		}

		/*
		 * Handle all the connection machinery: consequently go through
		 * Connecting --> Negotiating --> Active states.
		 */
		else if (nevents > 0 && event.events & WL_SOCKET_MASK)
		{
			uintptr_t	conn_id = (uintptr_t) event.user_data;

			Assert(conns[conn_id].active);

			switch (conns[conn_id].state)
			{
				case Idle:
					Assert(false);
					break;

					/*
					 * Await for connection establishment and call
					 * dmq_receiver_loop()
					 */
				case Connecting:
					{
						double		pqtime;
						PostgresPollingStatusType status;
						int			pos = event.pos;

						pqtime = dmq_now();
						status = PQconnectPoll(conns[conn_id].pgconn);
						mtm_log(DmqPqTiming, "[DMQ] [TIMING] pqp = %f ms", dmq_now() - pqtime);

						mtm_log(DmqStateIntermediate,
								"[DMQ] Connecting: PostgresPollingStatusType = %d on %s",
								status,
								conns[conn_id].receiver_name);

						/*
						 * PQconnectPoll() can recreate socket behind the
						 * scene, so re-register it in WaitEventSet.
						 */
						if (status == PGRES_POLLING_READING || status == PGRES_POLLING_WRITING)
						{
							DeleteWaitEvent(set, pos);
							pos = AddWaitEventToSet(set, WL_SOCKET_CONNECTED,
													PQsocket(conns[conn_id].pgconn),
													NULL, (void *) conn_id);
							conns[conn_id].pos = pos;
						}

						if (status == PGRES_POLLING_READING)
						{
							ModifyWaitEvent(set, pos, WL_SOCKET_READABLE, NULL);
							mtm_log(DmqStateIntermediate,
									"[DMQ] Connecting: modify wait event to WL_SOCKET_READABLE on %s",
									conns[conn_id].receiver_name);
						}
						else if (status == PGRES_POLLING_WRITING)
						{
							ModifyWaitEvent(set, pos, WL_SOCKET_WRITEABLE, NULL);
							mtm_log(DmqStateIntermediate,
									"[DMQ] Connecting: modify wait event to WL_SOCKET_WRITEABLE on %s",
									conns[conn_id].receiver_name);
						}
						else if (status == PGRES_POLLING_OK)
						{
							char	   *sender_name = conns[conn_id].sender_name;
							char	   *query = psprintf("select mtm.dmq_receiver_loop('%s', %d)",
														 sender_name, conns[conn_id].recv_timeout);

							conns[conn_id].state = Negotiating;
							ModifyWaitEvent(set, pos, WL_SOCKET_READABLE, NULL);
							PQsendQuery(conns[conn_id].pgconn, query);

							mtm_log(DmqStateIntermediate,
									"[DMQ] switching %s from Connecting to Negotiating",
									conns[conn_id].receiver_name);
						}
						else if (status == PGRES_POLLING_FAILED)
						{
							conns[conn_id].state = Idle;
							DeleteWaitEvent(set, pos);

							mtm_log(DmqStateIntermediate,
									"[DMQ] failed to connect with %s (%s): %s",
									conns[conn_id].receiver_name,
									conns[conn_id].connstr,
									PQerrorMessage(conns[conn_id].pgconn));
						}
						else
							Assert(false);

						break;
					}

					/*
					 * Await for response to dmq_receiver_loop() call and
					 * switch to active state.
					 */
				case Negotiating:
					Assert(event.events & WL_SOCKET_READABLE);
					if (!PQconsumeInput(conns[conn_id].pgconn))
					{
						conns[conn_id].state = Idle;
						DeleteWaitEvent(set, event.pos);

						mtm_log(DmqStateIntermediate,
								"[DMQ] failed to get handshake from %s: %s",
								conns[conn_id].receiver_name,
								PQerrorMessage(conns[i].pgconn));
					}
					if (!PQisBusy(conns[conn_id].pgconn))
					{
						int8 mask_pos = conns[conn_id].mask_pos;

						conns[conn_id].state = Active;
						DeleteWaitEvent(set, event.pos);
						PQsetnonblocking(conns[conn_id].pgconn, 1);
						sconn_cnt[mask_pos]++;
						dmq_state->sconn_cnt[mask_pos] = sconn_cnt[mask_pos];

						mtm_log(DmqStateFinal,
								"[DMQ] Connected to %s",
								conns[conn_id].receiver_name);

						dmq_sender_connect_hook(conns[conn_id].receiver_name);
					}
					break;

					/* Do nothing and check that connection is still alive */
				case Active:
					Assert(event.events & WL_SOCKET_READABLE);
					if (!PQconsumeInput(conns[conn_id].pgconn))
					{
						conns[conn_id].state = Idle;
						dmq_state->sconn_cnt[conns[conn_id].mask_pos] = DMQSCONN_DEAD;

						mtm_log(DmqStateFinal,
								"[DMQ] connection error with %s: %s",
								conns[conn_id].receiver_name,
								PQerrorMessage(conns[conn_id].pgconn));

						dmq_sender_disconnect_hook(conns[conn_id].receiver_name);
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
			proc_exit(1);
		}

		CHECK_FOR_INTERRUPTS();
	}
	FreeWaitEventSet(set);

}



/*****************************************************************************
 *
 * Receiver stuff
 *
 *****************************************************************************/

/* recreate shm_mq to the given subscriber */
static void
dmq_receiver_recreate_mq(DmqReceiverSlot *my_slot,
						 int procno, uint64 procno_gen,
						 dsm_segment **seg_p, shm_mq_handle **mq_handle_p,
						 bool locked)
{
	shm_mq	*mq;

	/* release previous dsm, if any */
	/* shm_mq_detach is done automatically on dsm detach */
	if (*seg_p != NULL)
		dsm_detach(*seg_p);
	*seg_p = NULL;
	*mq_handle_p = NULL;

	/* allocate a new one, put shm_mq there */
	*seg_p = dsm_create(DMQ_MQ_SIZE, 0);
	dsm_pin_mapping(*seg_p);
	mq = shm_mq_create(dsm_segment_address(*seg_p), DMQ_MQ_SIZE);
	shm_mq_set_sender(mq, MyProc);
	*mq_handle_p = shm_mq_attach(mq, *seg_p, NULL);

	/* and publish it */
	if (!locked)
		LWLockAcquire(dmq_state->lock, LW_EXCLUSIVE);
	my_slot->dsm_handles[procno].h = dsm_segment_handle(*seg_p);
	my_slot->dsm_handles[procno].procno_gen = procno_gen;
	if (!locked)
		LWLockRelease(dmq_state->lock);

	mtm_log(DmqTraceShmMq, "[DMQ] created shm_mq to proc <%d, " UINT64_FORMAT ">, dsm handle %u",
			procno, procno_gen, dsm_segment_handle(*seg_p));

	/*
	 * This often duplicates cv broadcast, but not always: subscriber might
	 * not requested mq recreation directly but just subscribed to the stream
	 * and went to sleep on latch, and we got here through message
	 * receival. e.g. like mtm monitor with his permanent streams behaves like
	 * this. OTOH, removing cv altogether would require to wake all
	 * subscribers in dmq_receiver_at_exit, which is not too elegant as well.
	 */
	SetLatch(&ProcGlobal->allProcs[procno].procLatch);
}

/* hand over message to the subscriber */
static void
dmq_handle_message(StringInfo msg, DmqReceiverSlot *my_slot,
				   dsm_segment **segs, shm_mq_handle **mq_handles,
				   void *extra)
{
	const char *stream_name;
	const char *body;
	int			body_len;
	bool		found;
	DmqStreamSubscription sub;
	DmqStreamSubscription *psub;
	shm_mq_result res;

	/*
	 * Consume stream_name packed as a cstring and interpret rest of the data
	 * as message body with unknown format that we are going to send down to
	 * the subscribed backend.
	 */
	stream_name = pq_getmsgrawstring(msg);
	body_len = msg->len - msg->cursor;
	body = pq_getmsgbytes(msg, body_len);
	pq_getmsgend(msg);

	/*
	 * Stream name "H" is reserved for heartbeats.
	 */
	if (strcmp(stream_name, "H") == 0)
	{
		/*
		 * Allow user to read a payload potentially written by
		 * dmq_sender_heartbeat_hook
		 */
		if (dmq_receiver_heartbeat_hook != NULL)
		{
			StringInfoData body_s; /* skip stream name */

			body_s.data = (char *) body;
			body_s.len = body_len;
			body_s.maxlen = -1;
			body_s.cursor = 0;
			dmq_receiver_heartbeat_hook(my_slot->name, &body_s, extra);
		}
		return;
	}

	/*
	 * Find subscriber. XXX: we can cache that and re-read shared memory upon
	 * a signal, but likely that won't show any measurable speedup.
	 */
	LWLockAcquire(dmq_state->lock, LW_SHARED);
	psub = (DmqStreamSubscription *) hash_search(dmq_subscriptions,
												 stream_name, HASH_FIND,
												 &found);
	/*
	 * that's quite stupid, but gcc complains 'sub.procno etc might be used
	 * uninitialized' without this
	 */
	MemSet(&sub, '\0', sizeof(DmqStreamSubscription));
	if (found)
		sub = *psub;
	LWLockRelease(dmq_state->lock);

	if (!found)
	{
		/*
		 * Beware of using WARNING/NOTICEs in the receiver code; they will go
		 * to the sender, and evidently nobody cared to read them there.
		 * This ought to be fixed actually.
		 */
		mtm_log(COMMERROR,
				"[DMQ] subscription %s is not found (body = %s)",
				stream_name, body);
		return;
	}

	/*
	 * If we haven't created the queue to this backend, do this now without
	 * waiting for SIGHUP. This is needed to maintain the basic 'message
	 * arrived after dmq_stream_subscribe finished and connection is good =>
	 * must pass the message' idea. This is e.g. critical for permanent
	 * stream subscribers like monitor in mtm.
	 * procno_gen is ever updated by me, so it is safe to look at it without
	 * locks.
	 */
	if (mq_handles[sub.procno] == NULL ||
		my_slot->dsm_handles[sub.procno].procno_gen != sub.procno_gen)
	{
		dmq_receiver_recreate_mq(my_slot, sub.procno, sub.procno_gen,
								 &segs[sub.procno], &mq_handles[sub.procno],
								 false);
	}

	mtm_log(DmqTraceIncoming,
			"[DMQ] got message %s.%s (len=%d), passing to %d", stream_name, body,
			body_len, sub.procno);

	/* and send it */
	res = shm_mq_send(mq_handles[sub.procno], body_len, body, false);
	if (res != SHM_MQ_SUCCESS)
	{
		mtm_log(COMMERROR, "[DMQ] can't send to queue %d", sub.procno);
	}
}

#define DMQ_RECV_BUFFER 8192
static char recv_buffer[DMQ_RECV_BUFFER];
static int	recv_bytes;
static int	read_bytes;

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
		int			len = pg_ntoh32(*(int *) (recv_buffer + read_bytes));

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
	int			rc;

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
			 * Move data to the left in case we are near the buffer end. Case
			 * when message starts earlier in the buffer and spans past it's
			 * end handled separately down the lines.
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
		Assert(recv_bytes >= read_bytes && recv_bytes <= DMQ_RECV_BUFFER);

		mtm_log(DmqTraceIncoming, "dmq: got %d bytes", rc);

		/*
		 * Here we need to re-check for full message again, so the caller will
		 * know whether he should wait for event on socket.
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
	int			rc;

	/*
	 * That is why we re-implementing this function: byte can be already in
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

/*
 * For each subscription, create shm_mq to subscriber if we don't have it yet.
 */
static void
dmq_receiver_recreate_mqs(DmqReceiverSlot *my_slot, dsm_segment **segs,
						  shm_mq_handle **mq_handles)
{
	HASH_SEQ_STATUS hash_seq;
	DmqStreamSubscription *sub;

	/*
	 * can make separate lock for subs and create mqs after releasing the
	 * lock if this seems too heavy
	 */
	LWLockAcquire(dmq_state->lock, LW_EXCLUSIVE);

	hash_seq_init(&hash_seq, dmq_subscriptions);
	while ((sub = hash_seq_search(&hash_seq)) != NULL)
	{
		if (mq_handles[sub->procno] != NULL &&
			my_slot->dsm_handles[sub->procno].procno_gen == sub->procno_gen)
		{
			/* queue is already good */
			continue;
		}

		dmq_receiver_recreate_mq(my_slot, sub->procno, sub->procno_gen,
								 &segs[sub->procno], &mq_handles[sub->procno],
								 true);
	}

	LWLockRelease(dmq_state->lock);
	/* let subscribers know we are done */
	ConditionVariableBroadcast(&dmq_state->shm_mq_creation_cv);
}

static void
dmq_receiver_at_exit(int status, Datum receiver)
{
	int			receiver_id = DatumGetInt32(receiver);
	char		sender_name[DMQ_NAME_MAXLEN];

	/*
	 * We want the slot to be freed even if hook errors out, so order is
	 * important.
	 */
	LWLockAcquire(dmq_state->lock, LW_EXCLUSIVE);
	strncpy(sender_name, dmq_state->receivers[receiver_id].name,
			DMQ_NAME_MAXLEN);
	dmq_state->receivers[receiver_id].name[0] = '\0';
	LWLockRelease(dmq_state->lock);

	/* tell subscribers it is pointless to wait for shm_mq creation */
	ConditionVariableBroadcast(&dmq_state->shm_mq_creation_cv);

	if (dmq_receiver_stop_hook)
		dmq_receiver_stop_hook(sender_name);
}

/* xxx should wrap all this in try/catch to turn any ERROR into FATAL */
Datum
dmq_receiver_loop(PG_FUNCTION_ARGS)
{
	enum
	{
		NeedByte,
		NeedMessage
	}			reader_state = NeedByte;

	StringInfoData s;
	dsm_segment	  **segs;
	shm_mq_handle **mq_handles;
	char	   *sender_name;
	char	   *proc_name;
	int			i;
	int			j;
	int			receiver_id = -1;
	int			recv_timeout;
	double		last_message_at = dmq_now();
	void		*extra = NULL;

	sender_name = text_to_cstring(PG_GETARG_TEXT_PP(0));
	recv_timeout = PG_GETARG_INT32(1);

	proc_name = psprintf("mtm-dmq-receiver %s", sender_name);
	set_ps_display(proc_name, true);

	segs = palloc0(MaxBackends * sizeof(dsm_segment *));
	mq_handles = palloc0(MaxBackends * sizeof(shm_mq_handle *));

	/* register ourself in dmq_state */
	LWLockAcquire(dmq_state->lock, LW_EXCLUSIVE);

	for (i = 0; i < DMQ_MAX_RECEIVERS; i++)
	{
		if (dmq_state->receivers[i].name[0] == '\0') /* free slot */
		{
			receiver_id = i;
		}
		else if (strcmp(dmq_state->receivers[i].name, sender_name) == 0)
		{
			mtm_log(ERROR, "[DMQ] sender '%s' already connected", sender_name);
		}
	}

	if (receiver_id < 0)
		mtm_log(ERROR, "[DMQ] maximum number of dmq-receivers reached");

	strncpy(dmq_state->receivers[receiver_id].name, sender_name, DMQ_NAME_MAXLEN);
	dmq_state->receivers[receiver_id].pid = MyProcPid;
	for (j = 0; j < MaxBackends; j++)
		dmq_state->receivers[receiver_id].dsm_handles[j].h = DSM_HANDLE_INVALID;
	LWLockRelease(dmq_state->lock);

	/*
	 * This is not on_shmem_exit for a reason. We ought to check out from
	 * shmem *before* detaching dsm; otherwise, a subscriber who had attached
	 * previously might notice detached mq, find the same dsm and reattach to
	 * its end for the second time. Surely we could track dsm handles in
	 * subscribers local state and forbid attaching to the same handle twice,
	 * but it is better to avoid the race at all rather than fight it.
	 */
	before_shmem_exit(dmq_receiver_at_exit, Int32GetDatum(receiver_id));

	/* okay, switch client to copyout state */
	pq_beginmessage(&s, 'W');
	pq_sendbyte(&s, 0);			/* copy_is_binary */
	pq_sendint16(&s, 0);		/* numAttributes */
	pq_endmessage(&s);
	pq_flush();

	pq_startmsgread();

	ModifyWaitEvent(MyProcPort->pqcomm_waitset, 0, WL_SOCKET_READABLE, NULL);

	if (dmq_receiver_start_hook)
		extra = dmq_receiver_start_hook(sender_name);

	/* do not hold globalxmin. XXX: try to carefully release snaps */
	MyPgXact->xmin = InvalidTransactionId;

	for (;;)
	{
		unsigned char qtype;
		WaitEvent	event;
		int			rc;
		int			nevents;

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
					mtm_log(ERROR, "[DMQ] invalid message type %c, %s",
							qtype, s.data);
				}
			}
		}

		if (reader_state == NeedMessage)
		{
			rc = _pq_getmessage_if_avalable(&s);

			if (rc > 0)
			{
				dmq_handle_message(&s, &dmq_state->receivers[receiver_id],
								   segs, mq_handles, extra);
				last_message_at = dmq_now();
				reader_state = NeedByte;
			}
		}

		if (rc == EOF)
		{
			ereport(COMMERROR,
					(errcode(ERRCODE_PROTOCOL_VIOLATION),
					 errmsg("[DMQ] EOF on connection")));
			break;
		}

		nevents = WaitEventSetWait(MyProcPort->pqcomm_waitset, 250, &event, 1,
								   WAIT_EVENT_CLIENT_READ);

		if (nevents > 0 && event.events & WL_LATCH_SET)
		{
			ResetLatch(MyLatch);
		}

		if (nevents > 0 && event.events & WL_POSTMASTER_DEATH)
		{
			ereport(FATAL,
					(errcode(ERRCODE_ADMIN_SHUTDOWN),
					 errmsg("[DMQ] exit receiver due to unexpected postmaster exit")));
		}

		/* XXX: is it enough? */
		CHECK_FOR_INTERRUPTS();
		if (ConfigReloadPending)
		{
			/*
			 * note: we are not calling ProcessConfigFile here, so dmq
			 * receiver never rereads the actual conf. There are hardly any
			 * GUCs that meaningfully influence it though.
			 */
			dmq_receiver_recreate_mqs(&dmq_state->receivers[receiver_id],
									  segs, mq_handles);
			ConfigReloadPending = false;
		}

		if (dmq_now() - last_message_at > recv_timeout)
		{
			mtm_log(FATAL, "[DMQ] exit receiver due to heatbeat timeout");
		}

	}
	pq_endmsgread();

	PG_RETURN_VOID();
}

void
dmq_terminate_receiver(char *name)
{
	int			i;
	pid_t		pid = 0;

	LWLockAcquire(dmq_state->lock, LW_EXCLUSIVE);
	for (i = 0; i < DMQ_MAX_RECEIVERS; i++)
	{
		if (strncmp(dmq_state->receivers[i].name, name, DMQ_NAME_MAXLEN) == 0)
		{
			pid = dmq_state->receivers[i].pid;
			Assert(pid > 0);
			break;
		}
	}
	LWLockRelease(dmq_state->lock);

	if (pid != 0)
		kill(pid, SIGTERM);
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

	mtm_log(DmqTraceOutgoing, "[DMQ] pushing l=%d '%.*s'",
			buf.len, buf.len, buf.data);

	/* XXX: use sendv instead */
	res = shm_mq_send(dmq_local.mq_outh, buf.len, buf.data, false);
	pfree(buf.data);
	if (res != SHM_MQ_SUCCESS)
		mtm_log(ERROR, "[DMQ] dmq_push: can't send to queue");
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

	mtm_log(DmqTraceOutgoing, "[DMQ] pushing l=%d '%.*s'",
			buf.len, buf.len, buf.data);

	/* XXX: use sendv instead */
	res = shm_mq_send(dmq_local.mq_outh, buf.len, buf.data, false);
	pfree(buf.data);
	if (res != SHM_MQ_SUCCESS)
		mtm_log(WARNING, "[DMQ] dmq_push: can't send to queue");
}

/*
 * Called from the subscriber. (Re)attaches to shm_mq of receiver registered
 * at handle_id if it is alive. Caller must have subscription at this point --
 * it is a sign for receiver to create the queue.
 * Returns true on success, false otherwise, but who cares?
 */
static bool
dmq_reattach_shm_mq(int handle_id)
{
	/*
	 * Release the previous segment, if any. The placement of this here is not
	 * entirely random: in common sequence
	 * 1) connection lost, dmq_pop_nb calls dmq_reattach_shm_mq but fruitlessly
	 * 2) connection is on, and we do dmq_stream_subscribe
	 * this saves us from an excessive receive failure as NULL mqh forces
	 * a rettachment attempt in dmq_stream_subscribe.
	 */
	if (dmq_local.inhandles[handle_id].dsm_seg != NULL)
	{
		/* mq is detached automatically in dsm detach cb */
		dsm_detach(dmq_local.inhandles[handle_id].dsm_seg);
		dmq_local.inhandles[handle_id].dsm_seg = NULL;
		dmq_local.inhandles[handle_id].mqh = NULL;
	}

	/*
	 * Loop until receiver creates queue for us or we find it dead.
	 */
	for (;;)
	{
		int			i;
		pid_t		receiver_pid = 0;
		dsm_handle	receiver_dsm = DSM_HANDLE_INVALID;

		LWLockAcquire(dmq_state->lock, LW_SHARED);
		for (i = 0; i < DMQ_MAX_RECEIVERS; i++)
		{
			DmqReceiverSlot *rslot = &dmq_state->receivers[i];

			/* XXX: change to hash maybe */
			if (strcmp(rslot->name,
					   dmq_local.inhandles[handle_id].name) == 0)
			{
				receiver_pid = rslot->pid;

				if ((rslot->dsm_handles[MyProc->pgprocno].h != DSM_HANDLE_INVALID) &&
					(rslot->dsm_handles[MyProc->pgprocno].procno_gen ==
					 dmq_local.my_procno_gen))
				{
					/* good queue found */
					receiver_dsm = rslot->dsm_handles[MyProc->pgprocno].h;
				}
				break;
			}
		}
		LWLockRelease(dmq_state->lock);

		if (!receiver_pid)
		{
			/* receiver is dead, no good to wait */
			ConditionVariableCancelSleep();
			return false;
		}

		if (receiver_dsm != DSM_HANDLE_INVALID)
		{
			/* success, attach to the queue */
			MemoryContext oldctx;
			shm_mq	   *inq;

			ConditionVariableCancelSleep();
			mtm_log(DmqTraceShmMq, "[DMQ] attaching '%s', dsm handle %d",
					dmq_local.inhandles[handle_id].name,
					receiver_dsm);

			dmq_local.inhandles[handle_id].dsm_seg = dsm_attach(receiver_dsm);
			if (dmq_local.inhandles[handle_id].dsm_seg == NULL)
			{
				mtm_log(DmqTraceShmMq, "unable to map dynamic shared memory segment");
				/* looks like receiver just exited */
				return false;
			}
			dsm_pin_mapping(dmq_local.inhandles[handle_id].dsm_seg);

			inq = dsm_segment_address(dmq_local.inhandles[handle_id].dsm_seg);
			shm_mq_set_receiver(inq, MyProc);
			oldctx = MemoryContextSwitchTo(TopMemoryContext);
			dmq_local.inhandles[handle_id].mqh = shm_mq_attach(
				inq,
				dmq_local.inhandles[handle_id].dsm_seg,
				NULL);
			MemoryContextSwitchTo(oldctx);

			return true;
		}

		/*
		 * receiver seems to be live, but there is no queue yet;
		 * poke it to create one
		 */
		kill(receiver_pid, SIGHUP);
		ConditionVariableSleep(&dmq_state->shm_mq_creation_cv,
							   PG_WAIT_EXTENSION);
	}
}

/*
 * Declare that our process wishes to receive messages from 'sender_name'
 * counterparty (i.e. receiver speaking with it).
 * mask_pos also identifies the sender; dmq_pop_nb accepts bitmask saying
 * from which receivers caller wants to get message and filters inhandles
 * through it.
 */
void dmq_attach_receiver(char *sender_name, int8 mask_pos)
{
	int			i;
	int			handle_id = -1;

	for (i = 0; i < DMQ_MAX_RECEIVERS; i++)
	{
		if (dmq_local.inhandles[i].name[0] == '\0')
		{
			handle_id = i;
			/* assert that we don't add two receivers with the same mask_pos */
#ifndef USE_ASSERT_CHECKING
			break;
#endif
		}
		else
		{
			Assert(mask_pos != dmq_local.inhandles[i].mask_pos);
		}
	}

	if (handle_id == -1)
		mtm_log(ERROR, "[DMQ] dmq_attach_receiver: max receivers already attached");

	/* We could remove n_inhandles altogether as well... */
	if (dmq_local.n_inhandles < handle_id + 1)
		dmq_local.n_inhandles = handle_id + 1;

	dmq_local.inhandles[handle_id].mqh = NULL;
	dmq_local.inhandles[handle_id].mask_pos = mask_pos;
	strncpy(dmq_local.inhandles[handle_id].name, sender_name, DMQ_NAME_MAXLEN);
	mtm_log(DmqTraceIncoming, "[DMQ] dmq_attach_receiver from %s with mask_pos %d at handle %d done",
			sender_name, mask_pos, handle_id);

	/*
	 * TODO: idea to use subscriptions as trigger to mq creation was not so
	 * good: we might hang here if backend hasn't subscribed yet. Better to
	 * publish required procno_gens separately, then we could uncomment this.
	 */
	/* dmq_reattach_shm_mq(handle_id); */
}

void
dmq_detach_receiver(char *sender_name)
{
	int			i;
	int			handle_id = -1;

	for (i = 0; i < DMQ_MAX_RECEIVERS; i++)
	{
		if (strncmp(dmq_local.inhandles[i].name, sender_name, DMQ_NAME_MAXLEN) == 0)
		{
			handle_id = i;
			break;
		}
	}

	if (handle_id < 0)
		mtm_log(ERROR, "[DMQ] dmq_detach_receiver: receiver from %s not found",
				sender_name);

	if (dmq_local.inhandles[handle_id].mqh)
	{
		shm_mq_detach(dmq_local.inhandles[handle_id].mqh);
		dmq_local.inhandles[handle_id].mqh = NULL;
	}

	if (dmq_local.inhandles[handle_id].dsm_seg)
	{
		dsm_detach(dmq_local.inhandles[handle_id].dsm_seg);
		dmq_local.inhandles[handle_id].dsm_seg = NULL;
	}

	dmq_local.inhandles[handle_id].name[0] = '\0';
	mtm_log(DmqTraceIncoming, "[DMQ] dmq_detach_receiver %s from handle %d done",
			sender_name, handle_id);
}

/*
 * Subscribes caller to msgs from stream_name and attempts to reattach to
 * receivers.
 */
void
dmq_stream_subscribe(char *stream_name)
{
	bool		found;
	DmqStreamSubscription *sub;
	int			i;

	/*
	 * If our process subscribes for the first time obtain a procno gen.
	 */
	if (dmq_local.my_procno_gen == 0)
	{
		dmq_local.my_procno_gen = ++dmq_state->procno_gens[MyProc->pgprocno];
		mtm_log(DmqTraceShmMq, "my_procno_gen issued, my id <%d, " UINT64_FORMAT ">",
				MyProc->pgprocno, dmq_local.my_procno_gen);
	}

	LWLockAcquire(dmq_state->lock, LW_EXCLUSIVE);
	sub = (DmqStreamSubscription *) hash_search(dmq_subscriptions, stream_name,
												HASH_ENTER, &found);
	if (found && sub->procno != MyProc->pgprocno)
	{
		mtm_log(ERROR,
				"[DMQ] procno%d: %s: subscription is already active for procno %d / %s",
				MyProc->pgprocno, stream_name, sub->procno, sub->stream_name);
	}
	sub->procno = MyProc->pgprocno;
	sub->procno_gen = dmq_local.my_procno_gen;
	LWLockRelease(dmq_state->lock);

	/*
	 * Try to ensure we have live connections to receivers, if not yet. The
	 * typical usage is
	 * - subscribe to reply stream
	 * - send request
	 * - get reply (dmq_pop_nb)
	 * So this is the last convenient spot where we can try to reconnect
	 * without risk of missing connection failure after request was sent (thus
	 * possibly exposing client to infinite waiting for answer).
	 */
	for (i = 0; i < dmq_local.n_inhandles; i++)
	{
		if (dmq_local.inhandles[i].name[0] == '\0')
			continue; /* unused slot */

		/*
		 * TODO: it would be nice to attempt reattach if we have the handle
		 * but the queue is already broken (sender detached), however shm_mq
		 * doesn't have this in its API and hand-crafting it is a crutch.
		 */
		if (dmq_local.inhandles[i].mqh == NULL)
			dmq_reattach_shm_mq(i);
	}
}

void
dmq_stream_unsubscribe(char *stream_name)
{
	bool		found;

	LWLockAcquire(dmq_state->lock, LW_EXCLUSIVE);
	hash_search(dmq_subscriptions, stream_name, HASH_REMOVE, &found);
	LWLockRelease(dmq_state->lock);

	Assert(found);
}

/*
 * Fills (preallocated) sconn_cnt with current values of sender
 * connection counters to guys in participants mask (as registered in
 * dmq_destination_add), which allows to reveal connection failures, possibly
 * resulted in losing request -- and thus stop hopeless waiting for response.
 */
void
dmq_get_sendconn_cnt(uint64 participants, int *sconn_cnt)
{
	int i;

	for (i = 0; i < DMQ_N_MASK_POS; i++)
	{
		if (BIT_CHECK(participants, i))
			sconn_cnt[i] = dmq_state->sconn_cnt[i];
	}
}

/* XXX: this is never used, not well maintained and should be removed */
bool
dmq_pop(int8 *sender_mask_pos, StringInfo msg, uint64 mask)
{
	shm_mq_result res;

	for (;;)
	{
		int			i;
		int			rc;
		bool		nowait = false;

		CHECK_FOR_INTERRUPTS();

		for (i = 0; i < dmq_local.n_inhandles; i++)
		{
			Size		len;
			void	   *data;

			if (dmq_local.inhandles[i].name[0] == '\0' ||
				!BIT_CHECK(mask, dmq_local.inhandles[i].mask_pos))
				continue;

			if (dmq_local.inhandles[i].mqh)
				res = shm_mq_receive(dmq_local.inhandles[i].mqh, &len, &data, true);
			else
				res = SHM_MQ_DETACHED;

			if (res == SHM_MQ_SUCCESS)
			{
				msg->data = data;
				msg->len = len;
				msg->maxlen = -1;
				msg->cursor = 0;
				*sender_mask_pos = dmq_local.inhandles[i].mask_pos;

				mtm_log(DmqTraceIncoming,
						"[DMQ] dmq_pop: got message %s from %s",
						(char *) data, dmq_local.inhandles[i].name);
				return true;
			}
			else if (res == SHM_MQ_DETACHED)
			{
				mtm_log(DmqTraceShmMq,
						"[DMQ] dmq_pop: queue '%s' detached, trying to reattach",
						dmq_local.inhandles[i].name);

				if (dmq_reattach_shm_mq(i))
				{
					nowait = true;
				}
				else
				{
					*sender_mask_pos = dmq_local.inhandles[i].mask_pos;
					return false;
				}
			}
		}

		if (nowait)
			continue;

		/* XXX cache that */
		rc = WaitLatch(MyLatch, WL_LATCH_SET | WL_TIMEOUT, 10.0,
					   WAIT_EVENT_MQ_RECEIVE);

		if (rc & WL_POSTMASTER_DEATH)
			proc_exit(1);

		if (rc & WL_LATCH_SET)
			ResetLatch(MyLatch);
	}
}

/*
 * mask defines from which senders to attempt to get data, as registered with
 * mask_pos in dmq_attach_receiver.
 * sender_mask_pos returns the sender (again mask_pos) which returned message
 * or failed. -1 means all shm_mqs are SHM_MQ_WOULD_BLOCK.
 *
 * Failure detection semantics: if we don't have record of live connection to
 * receiver (attached shm_mq internally) in the moment of calling this, error
 * will be reported (even if immediate reconnect is successfull, we might
 * already missed the reply, so this is necessary behaviour). After such
 * failure and in dmq_stream_subscribe we try to find receiver ~ live TCP
 * connection again. If it is found, next dmq_pop_nb might be successfull. So
 * this reattach acts like an error flush (the error won't be reported again)
 * and at the same time a reconnection attempt.
 *
 * Returns true if successfully filled msg, false otherwise; in the latter
 * case, *wait is true if failed shmem_mq was successfully reestablished,
 * i.e. caller might not haste to exclude the failed sender (if any).
 * XXX I don't believe this flag has any useful applications.
 */
bool
dmq_pop_nb(int8 *sender_mask_pos, StringInfo msg, uint64 mask, bool *wait)
{
	shm_mq_result res;
	int			i;

	*wait = true;
	*sender_mask_pos = -1;

	for (i = 0; i < dmq_local.n_inhandles; i++)
	{
		Size		len;
		void	   *data;

		if (dmq_local.inhandles[i].name[0] == '\0' ||
			!BIT_CHECK(mask, dmq_local.inhandles[i].mask_pos))
			continue;

		if (dmq_local.inhandles[i].mqh)
			res = shm_mq_receive(dmq_local.inhandles[i].mqh, &len, &data, true);
		else
			res = SHM_MQ_DETACHED;

		if (res == SHM_MQ_SUCCESS)
		{
			msg->data = data;
			msg->len = len;
			msg->maxlen = -1;
			msg->cursor = 0;

			*sender_mask_pos = dmq_local.inhandles[i].mask_pos;
			*wait = false;

			mtm_log(DmqTraceIncoming,
					"[DMQ] dmq_pop_nb: got message %s (len=%zu) from %s",
					(char *) data, len, dmq_local.inhandles[i].name);
			return true;
		}
		else if (res == SHM_MQ_DETACHED)
		{
			*sender_mask_pos = dmq_local.inhandles[i].mask_pos;

			if (dmq_reattach_shm_mq(i))
				*wait = false;

			return false;
		}
	}

	return false;
}

/*
 * Accepts bitmask of participants and sconn_cnt counters with send
 * connections counters as passed to (and the latter filled by)
 * dmq_fill_sconn_cnt, returns this mask after unsetting bits for those
 * counterparties with whom we've lost the send connection since.
 */
uint64
dmq_purge_failed_participants(uint64 participants, int *sconn_cnt)
{
	int i;
	uint64 res = participants;

	for (i = 0; i < DMQ_N_MASK_POS; i++)
	{
		if (BIT_CHECK(participants, i) &&
			(sconn_cnt[i] == DMQSCONN_DEAD ||
			 sconn_cnt[i] != dmq_state->sconn_cnt[i]))
			BIT_CLEAR(res, i);
	}
	return res;
}

/*
 * recv_mask_pos is short (< DMQ_N_MASK_POS) variant of
 * receiver_name, used to track connection failures -- it must match mask_pos
 * in dmq_attach_receiver to work!
 */
DmqDestinationId
dmq_destination_add(char *connstr, char *sender_name, char *receiver_name,
					int8 recv_mask_pos, int recv_timeout)
{
	DmqDestinationId dest_id;
	pid_t		sender_pid;

	LWLockAcquire(dmq_state->lock, LW_EXCLUSIVE);
	for (dest_id = 0; dest_id < DMQ_MAX_DESTINATIONS; dest_id++)
	{
		DmqDestination *dest = &(dmq_state->destinations[dest_id]);

		if (!dest->active)
		{
			strncpy(dest->sender_name, sender_name, DMQ_NAME_MAXLEN);
			strncpy(dest->receiver_name, receiver_name, DMQ_NAME_MAXLEN);
			strncpy(dest->connstr, connstr, DMQ_CONNSTR_MAX_LEN);
			dest->recv_timeout = recv_timeout;
			dest->active = true;
			dest->mask_pos = recv_mask_pos;
			break;
		}
	}
	sender_pid = dmq_state->sender_pid;
	LWLockRelease(dmq_state->lock);

	if (sender_pid)
		kill(sender_pid, SIGHUP);

	if (dest_id == DMQ_MAX_DESTINATIONS)
		mtm_log(ERROR, "Can't add new destination. DMQ_MAX_DESTINATIONS reached.");
	else
		return dest_id;
}

void
dmq_destination_drop(char *receiver_name)
{
	DmqDestinationId dest_id;
	pid_t		sender_pid;

	LWLockAcquire(dmq_state->lock, LW_EXCLUSIVE);
	for (dest_id = 0; dest_id < DMQ_MAX_DESTINATIONS; dest_id++)
	{
		DmqDestination *dest = &(dmq_state->destinations[dest_id]);

		if (dest->active &&
			strncmp(dest->receiver_name, receiver_name, DMQ_NAME_MAXLEN) == 0)
		{
			dest->active = false;
			break;
		}
	}
	sender_pid = dmq_state->sender_pid;
	LWLockRelease(dmq_state->lock);

	if (sender_pid)
		kill(sender_pid, SIGHUP);
}
