/*-------------------------------------------------------------------------
 *
 * receiver_raw.c
 *		Receive and apply logical changes generated by decoder_raw. This
 *		creates some basics for a multi-master cluster using vanilla
 *		PostgreSQL without modifying its code.
 *
 * Copyright (c) 1996-2015, PostgreSQL Global Development Group
 *
 * IDENTIFICATION
 *		receiver_raw/receiver_raw.c
 *
 *-------------------------------------------------------------------------
 */

/* Some general headers for custom bgworker facility */

#include <unistd.h>
#include <sys/time.h>

#include "postgres.h"
#include "fmgr.h"
#include "miscadmin.h"
#include "pqexpbuffer.h"
#include "access/xact.h"
#include "access/clog.h"
#include "access/transam.h"
#include "lib/stringinfo.h"
#include "libpq/pqformat.h"
#include "pgstat.h"
#include "postmaster/bgworker.h"
#include "storage/ipc.h"
#include "storage/latch.h"
#include "storage/proc.h"
#include "utils/guc.h"
#include "utils/snapmgr.h"
#include "utils/memutils.h"
#include "executor/spi.h"
#include "replication/origin.h"
#include "replication/slot.h"
#include "utils/portal.h"
#include "tcop/pquery.h"
#include "tcop/tcopprot.h"
#include "utils/syscache.h"
#include "utils/inval.h"

#ifdef WITH_RSOCKET
#include "libpq-int.h"
#endif

#include "multimaster.h"
#include "bytebuf.h"
#include "spill.h"
#include "state.h"
#include "bgwpool.h"
#include "receiver.h"
#include "resolver.h"
#include "logger.h"
#include "compat.h"
#include "syncpoint.h"

#define ERRCODE_DUPLICATE_OBJECT_STR  "42710"
#define RECEIVER_SUSPEND_TIMEOUT (1*USECS_PER_SEC)

bool MtmIsReceiver;

typedef struct MtmFlushPosition
{
	dlist_node	node;
	int			node_id;
	XLogRecPtr	local_end;
	XLogRecPtr	remote_end;
} MtmFlushPosition;

char const* const MtmReplicationModeName[] =
{
	"recovery",    /* perform recorvery of the node by applying all data from theslot from specified point */
	"recovered"    /* recovery of node is completed so drop old slot and restart replication from the current position in WAL */
};

static dlist_head MtmLsnMapping = DLIST_STATIC_INIT(MtmLsnMapping);

MtmConfig	*receiver_mtm_cfg;
bool		receiver_mtm_cfg_valid;

/* Signal handling */
static volatile sig_atomic_t got_sighup = false;

/* GUC variables */
static bool receiver_sync_mode = true; /* We need sync mode to have up-to-date values of catalog_xmin in replication slots */

/* Worker name */
static char worker_proc[BGW_MAXLEN];

/* Lastly written positions */
static XLogRecPtr output_written_lsn = InvalidXLogRecPtr;

/* Stream functions */
static void fe_sendint64(int64 i, char *buf);
static int64 fe_recvint64(char *buf);

void pglogical_receiver_main(Datum main_arg);
static XLogRecPtr MtmGetFlushPosition(int nodeId);

static void
receiver_raw_sighup(SIGNAL_ARGS)
{
	int save_errno = errno;
	got_sighup = true;
	if (MyProc)
		SetLatch(&MyProc->procLatch);
	errno = save_errno;
}

/*
 * Send a Standby Status Update message to server.
 */
static bool
sendFeedback(PGconn *conn, int64 now, int node_id)
{
	char		replybuf[1 + 8 + 8 + 8 + 8 + 1];
	int		 len = 0;
	XLogRecPtr output_applied_lsn = output_written_lsn;
	XLogRecPtr output_flushed_lsn = MtmGetFlushPosition(node_id);

	replybuf[len] = 'r';
	len += 1;
	fe_sendint64(output_written_lsn, &replybuf[len]);	/* write */
	len += 8;
	fe_sendint64(output_flushed_lsn, &replybuf[len]);	/* flush */
	len += 8;
	fe_sendint64(output_applied_lsn, &replybuf[len]);	/* apply */
	len += 8;
	fe_sendint64(now, &replybuf[len]);	/* sendTime */
	len += 8;

	/* No reply requested from server */
	replybuf[len] = 0;
	len += 1;

	if (PQputCopyData(conn, replybuf, len) <= 0 || PQflush(conn))
	{
		ereport(LOG, (MTM_ERRMSG("%s: could not send feedback packet: %s",
							 worker_proc, PQerrorMessage(conn))));
		return false;
	}

	return true;
}

/*
 * Converts an int64 to network byte order.
 */
static void
fe_sendint64(int64 i, char *buf)
{
	uint32	  n32;

	/* High order half first, since we're doing MSB-first */
	n32 = (uint32) (i >> 32);
	n32 = pg_hton32(n32);
	memcpy(&buf[0], &n32, 4);

	/* Now the low order half */
	n32 = (uint32) i;
	n32 = pg_hton32(n32);
	memcpy(&buf[4], &n32, 4);
}

/*
 * Converts an int64 from network byte order to native format.
 */
static int64
fe_recvint64(char *buf)
{
	int64	   result;
	uint32	  h32;
	uint32	  l32;

	memcpy(&h32, buf, 4);
	memcpy(&l32, buf + 4, 4);
	h32 = pg_ntoh32(h32);
	l32 = pg_ntoh32(l32);

	result = h32;
	result <<= 32;
	result |= l32;

	return result;
}

static int64
feGetCurrentTimestamp(void)
{
	int64	   result;
	struct timeval tp;

	gettimeofday(&tp, NULL);

	result = (int64) tp.tv_sec -
		((POSTGRES_EPOCH_JDATE - UNIX_EPOCH_JDATE) * SECS_PER_DAY);

	result = (result * USECS_PER_SEC) + tp.tv_usec;

	return result;
}

static void
feTimestampDifference(int64 start_time, int64 stop_time,
					  long *secs, int *microsecs)
{
	int64	   diff = stop_time - start_time;

	if (diff <= 0)
	{
		*secs = 0;
		*microsecs = 0;
	}
	else
	{
		*secs = (long) (diff / USECS_PER_SEC);
		*microsecs = (int) (diff % USECS_PER_SEC);
	}
}

static XLogRecPtr
MtmGetFlushPosition(int node_id)
{
	dlist_mutable_iter iter;
	XLogRecPtr flush_lsn = InvalidXLogRecPtr;
	XLogRecPtr local_flush = GetFlushRecPtr();

	dlist_foreach_modify(iter, &MtmLsnMapping)
	{
		MtmFlushPosition *flushpos;
		flushpos = dlist_container(MtmFlushPosition, node, iter.cur);

		// XXX: probably it's better just not write this
		if (node_id != flushpos->node_id)
			continue;

		if (flushpos->local_end <= local_flush)
		{
			flush_lsn = flushpos->remote_end;
			dlist_delete(iter.cur);
			pfree(flushpos);
		}
		else
			break;
	}

	return flush_lsn;
}

/**
 * Keep track of progress of WAL writer.
 * We need to notify WAL senders at other nodes which logical records
 * are flushed to the disk and so can survive failure. In asynchronous commit mode
 * WAL is flushed by WAL writer. Current flush position can be obtained by GetFlushRecPtr().
 * So on applying new logical record we insert it in the MtmLsnMapping and compare
 * their poistions in local WAL log with current flush position.
 * The records which are flushed to the disk by WAL writer are removed from the list
 * and mapping ing mtm->nodes[].flushPos is updated for this node.
 */
void
MtmUpdateLsnMapping(int node_id, XLogRecPtr end_lsn)
{
	MtmFlushPosition* flushpos;
	MemoryContext old_context = MemoryContextSwitchTo(TopMemoryContext);

	Assert(MtmIsReceiver && !MtmIsPoolWorker);

	if (end_lsn != InvalidXLogRecPtr)
	{
		/* Track commit lsn */
		flushpos = (MtmFlushPosition *) palloc(sizeof(MtmFlushPosition));
		flushpos->node_id = node_id;
		flushpos->local_end = XactLastCommitEnd;
		flushpos->remote_end = end_lsn;
		dlist_push_tail(&MtmLsnMapping, &flushpos->node);
	}
	MemoryContextSwitchTo(old_context);
}

static void
MtmExecute(void* work, int size, MtmReceiverContext *receiver_ctx, bool no_pool)
{
	/* parallel_allowed should never be set during recovery */
	Assert( !(receiver_ctx->is_recovery && receiver_ctx->parallel_allowed) );

	if (receiver_ctx->is_recovery || !receiver_ctx->parallel_allowed || no_pool)
		MtmExecutor(work, size, receiver_ctx);
	else
		BgwPoolExecute(&Mtm->pools[MtmReplicationNodeId-1], work, size, receiver_ctx);

}

/*
 * Filter received transactions at destination side.
 * This function is executed by receiver,
 * so there are no race conditions and it is possible to update nodes[i].restartLSN without lock.
 * It is more efficient to filter records at senders size (done by MtmReplicationTxnFilterHook) to avoid sending useless data through network.
 * But asynchronous nature of logical replications makes it not possible to guarantee (at least I failed to do it)
 * that replica do not receive deteriorated data.
 */
static bool
MtmFilterTransaction(char *record, int size, Syncpoint *spvector, HTAB *filter_map)
{
	StringInfoData	s;
	uint8			event;
	XLogRecPtr		origin_lsn;
	XLogRecPtr		end_lsn;
	XLogRecPtr		tx_lsn;
	int				replication_node;
	int				origin_node;
	char const*		gid = "";
	char			msgtype PG_USED_FOR_ASSERTS_ONLY;

	s.data = record;
	s.len = size;
	s.maxlen = -1;
	s.cursor = 0;

	/* read fields */
	msgtype = pq_getmsgbyte(&s);
	Assert(msgtype == 'C');
	event = pq_getmsgbyte(&s); /* event */
	replication_node = pq_getmsgbyte(&s);
	pq_getmsgint64(&s); /* commit_lsn */
	end_lsn = pq_getmsgint64(&s); /* end_lsn */
	pq_getmsgint64(&s); /* commit_time */

	origin_node = pq_getmsgbyte(&s);
	origin_lsn = pq_getmsgint64(&s);

	/* Skip all transactions from our node */
	if (origin_node == Mtm->my_node_id)
		return true;

	switch (event)
	{
		case PGLOGICAL_PREPARE:
		case PGLOGICAL_PRECOMMIT_PREPARED:
		case PGLOGICAL_ABORT_PREPARED:
			gid = pq_getmsgstring(&s);
			break;
		case PGLOGICAL_COMMIT_PREPARED:
			pq_getmsgint64(&s); /* CSN */
			gid = pq_getmsgstring(&s);
			break;
		default:
			// XXX: notreached?
			break;
	}

	Assert(replication_node == MtmReplicationNodeId);
	tx_lsn = origin_node == MtmReplicationNodeId ? end_lsn : origin_lsn;

	if (tx_lsn <= spvector[origin_node-1].origin_lsn)
	{
		mtm_log(MtmReceiverFilter,
				"Filter transaction %s from node %d event=%x (restrt=%"INT64_MODIFIER"x, tx=%"INT64_MODIFIER"x)",
				gid, replication_node, event,
				spvector[origin_node-1].origin_lsn, tx_lsn);
		return true;
	}
	else
	{
		FilterEntry	entry;
		bool		found;

		memset(&entry, '\0', sizeof(FilterEntry));
		entry.node_id = origin_node;
		entry.origin_lsn = tx_lsn;

		hash_search(filter_map, &entry, HASH_FIND, &found);

		mtm_log(MtmReceiverFilter,
			"Filter (map) transaction %s from node %d event=%x (restrt=%"INT64_MODIFIER"x, tx=%d/%"INT64_MODIFIER"x) -> %d",
			gid, replication_node, event,
			spvector[origin_node-1].origin_lsn, origin_node, tx_lsn, found);

		return found;
	}
}

/*
 * Setup replication session origin to include origin location in WAL and
 * update slot position.
 * Sessions are not reetrant so we have to use exclusive lock here.
 */
void
MtmBeginSession(int nodeId)
{
	Assert(replorigin_session_origin == InvalidRepOriginId);
	replorigin_session_origin = MtmNodeById(receiver_mtm_cfg, nodeId)->origin_id;
	Assert(replorigin_session_origin != InvalidRepOriginId);

	// XXX: that is expensive! better to switch only in recovery
	replorigin_session_setup(replorigin_session_origin);
}

/*
 * Release replication session
 */
void
MtmEndSession(int nodeId, bool unlock)
{
	if (replorigin_session_origin != InvalidRepOriginId)
	{
		replorigin_session_origin = InvalidRepOriginId;
		replorigin_session_origin_lsn = InvalidXLogRecPtr;
		replorigin_session_origin_timestamp = 0;
		replorigin_session_reset();
	}
}

/* On failure bails out with ERROR. */
static PGconn *
receiver_connect(char *conninfo)
{
	PGconn			*conn;
	ConnStatusType	status;
	const char		*keys[] = {"dbname", "replication", NULL};
	const char		*vals[] = {conninfo, "database", NULL};

	conn = PQconnectdbParams(keys, vals, /* expand_dbname = */ true);
	status = PQstatus(conn);
	if (status != CONNECTION_OK)
	{
		char *err = PQerrorMessage(conn);
		PQfinish(conn);
		mtm_log(ERROR, "Could not establish connection to '%s': %s", conninfo, err);
	}

	return conn;
}

/*
 * Create slot on remote using logical replication protocol.
 */
void
MtmReceiverCreateSlot(char *conninfo, int my_node_id)
{
	StringInfoData	cmd;
	PGresult		*res;
	PGconn			*conn = receiver_connect(conninfo);

	if (!conn)
		mtm_log(ERROR, "Could not connect to '%s'", conninfo);

	initStringInfo(&cmd);
	appendStringInfo(&cmd, "CREATE_REPLICATION_SLOT " MULTIMASTER_SLOT_PATTERN
					 " LOGICAL multimaster", my_node_id);
	res = PQexec(conn, cmd.data);
	if (PQresultStatus(res) != PGRES_TUPLES_OK)
	{
		char *sqlstate = PQresultErrorField(res, PG_DIAG_SQLSTATE);
		if (!sqlstate || strcmp(sqlstate, ERRCODE_DUPLICATE_OBJECT_STR) != 0)
		{
			PQclear(res);
			pfree(cmd.data);
			PQfinish(conn);
			mtm_log(ERROR, "Could not create logical slot on '%s'", conninfo);
		}
	}
	PQclear(res);
	pfree(cmd.data);
	PQfinish(conn);
}

static void
subscription_change_cb(Datum arg, int cacheid, uint32 hashvalue)
{
	receiver_mtm_cfg_valid = false;
}

static void
pglogical_receiver_at_exit(int status, Datum arg)
{
	int node_id = DatumGetInt32(arg);

	LWLockAcquire(Mtm->lock, LW_EXCLUSIVE);
	Mtm->peers[node_id - 1].receiver_pid = InvalidPid;
	LWLockRelease(Mtm->lock);
}

void
pglogical_receiver_main(Datum main_arg)
{
	int					nodeId = DatumGetInt32(main_arg);
	/* Variables for replication connection */
	PQExpBuffer			query;
	PGconn				*conn = NULL;
	PGresult			*res;
	MtmReplicationMode	mode;
	MtmReceiverContext	receiver_ctx = {nodeId, false, false, 0};

	ByteBuffer			buf;
	/* Buffer for COPY data */
	char				*copybuf = NULL;
	int					spill_file = -1;
	StringInfoData		spill_info;
	static PortalData	fakePortal;

	Oid					db_id;
	Oid					user_id;

	on_shmem_exit(pglogical_receiver_at_exit, Int32GetDatum(nodeId));

	MtmIsReceiver = true;
	// XXX: get rid of that
	MtmBackgroundWorker = true;
	MtmIsLogicalReceiver = true;

	ByteBufferAlloc(&buf);

	initStringInfo(&spill_info);

	/* Register functions for SIGTERM/SIGHUP management */
	pqsignal(SIGHUP, receiver_raw_sighup);
	pqsignal(SIGTERM, die);

	MtmCreateSpillDirectory(nodeId);

	MtmReplicationNodeId = nodeId;

	/* We're now ready to receive signals */
	BackgroundWorkerUnblockSignals();

	memcpy(&db_id, MyBgworkerEntry->bgw_extra, sizeof(Oid));
	memcpy(&user_id, MyBgworkerEntry->bgw_extra + sizeof(Oid), sizeof(Oid));

	/* Connect to a database */
	BackgroundWorkerInitializeConnectionByOid(db_id, user_id, 0);
	ActivePortal = &fakePortal;
	ActivePortal->status = PORTAL_ACTIVE;
	ActivePortal->sourceText = "";

	receiver_mtm_cfg = MtmLoadConfig();

	/* Keep us informed about subscription changes. */
	CacheRegisterSyscacheCallback(SUBSCRIPTIONOID,
								  subscription_change_cb,
								  (Datum) 0);

	snprintf(worker_proc, BGW_MAXLEN, "mtm-logrep-receiver-%d-%d",
			 receiver_mtm_cfg->my_node_id, nodeId);
	BgwPoolStart(&Mtm->pools[nodeId-1], worker_proc, db_id, user_id);
	mtm_log(MtmReceiverStart, "Receiver %s has started.", worker_proc);

	/*
	 * This is the main loop of logical replication.
	 * In case of errors we simply cleanup and die, bgw will be restarted.
	 * Also reconnect is forced when node is switch to recovery mode.
	 * Finally, receiver customarily exits on Postmaster death.
	 */
	PG_TRY();
	{
		int			count,
					counterpart_disable_count;
		XLogRecPtr	remote_start = InvalidXLogRecPtr;
		Syncpoint	*spvector = NULL;
		HTAB		*filter_map = NULL;

		/*
		 * Determine when and how we should open replication slot.
		 * During recovery we need to open only one replication slot from which node should receive all transactions.
		 * Slots at other nodes should be removed
		 */
		mode = MtmGetReplicationMode(nodeId);

		/* Acquire recovery rep slot, so we can advance it without search */
		ReplicationSlotAcquire(psprintf(MULTIMASTER_RECOVERY_SLOT_PATTERN,
										nodeId),
							   true);

		LWLockAcquire(Mtm->lock, LW_EXCLUSIVE);
		Mtm->peers[nodeId - 1].receiver_pid = MyProcPid;
		Mtm->peers[nodeId - 1].receiver_mode = mode;
		LWLockRelease(Mtm->lock);

		count = MtmGetRecoveryCount();
		counterpart_disable_count = MtmGetNodeDisableCount(nodeId);

		/* Establish connection to remote server */
		conn = receiver_connect(MtmNodeById(receiver_mtm_cfg, nodeId)->conninfo);

		/* Create new slot if needed */
		query = createPQExpBuffer();

		/*
		 * Hand-made uuid. First byte is node_id, then rest is timestamp which
		 * is guaranteed to be uniq on this node.
		 */
		receiver_ctx.session_id = ((uint64) receiver_mtm_cfg->my_node_id << 56)
			+ MtmGetIncreasingTimestamp();

		receiver_ctx.is_recovery = mode == REPLMODE_RECOVERY;
		receiver_ctx.parallel_allowed = false;

		if (receiver_ctx.is_recovery)
		{
			if (receiver_mtm_cfg->backup_node_id > 0)
			{
				spvector = SyncpointGetAllLatest();
				filter_map = RecoveryFilterLoad(-1, spvector, receiver_mtm_cfg);
				remote_start = spvector[receiver_ctx.node_id - 1].origin_lsn;
				Assert(remote_start != InvalidXLogRecPtr);
			}
			else
			{
				spvector = SyncpointGetAllLatest();
				filter_map = RecoveryFilterLoad(-1, spvector, receiver_mtm_cfg);
				remote_start = QueryRecoveryHorizon(conn, receiver_ctx.node_id, spvector);
			}
		}
		else
		{
			spvector = palloc0(MTM_MAX_NODES * sizeof(Syncpoint));
			spvector[receiver_ctx.node_id - 1] = SyncpointGetLatest(receiver_ctx.node_id);
			filter_map = RecoveryFilterLoad(receiver_ctx.node_id, spvector, receiver_mtm_cfg);
			remote_start = spvector[receiver_ctx.node_id - 1].origin_lsn;
		}

		/* log our intentions */
		{
			int		i;
			StringInfo message = makeStringInfo();

			appendStringInfoString(message, "%s starting receiver:\n");
			appendStringInfo(message, "\t replication_node = %d\n", receiver_ctx.node_id);
			appendStringInfo(message, "\t mode = %s\n", MtmReplicationModeName[mode]);
			appendStringInfo(message, "\t remote_start = %"INT64_MODIFIER"x\n", remote_start);

			appendStringInfo(message, "\t syncpoint_vector (origin/local) = {");
			for (i = 0; i < MTM_MAX_NODES; i++)
			{
				if (spvector[i].origin_lsn != InvalidXLogRecPtr || spvector[i].local_lsn != InvalidXLogRecPtr)
				{
					appendStringInfo(message, "%d: " LSN_FMT "/" LSN_FMT ", ",
									 i + 1, spvector[i].origin_lsn, spvector[i].local_lsn);
				}
			}
			appendStringInfo(message, "}");

			elog(MtmReceiverStart, message->data, MTM_TAG);
		}

		Assert(filter_map && spvector);

		appendPQExpBuffer(query, "START_REPLICATION SLOT \"%s\" LOGICAL %x/%x ("
						  "\"startup_params_format\" '1',"
						  "\"max_proto_version\" '1',"
						  "\"min_proto_version\" '1',"
						  "\"forward_changesets\" '1',"
						  "\"mtm_replication_mode\" '%s',"
						  "\"mtm_session_id\" '"INT64_FORMAT"')",
						  psprintf(MULTIMASTER_SLOT_PATTERN, receiver_mtm_cfg->my_node_id),
						  (uint32) (remote_start >> 32),
						  (uint32) remote_start,
						  MtmReplicationModeName[mode],
						  receiver_ctx.session_id
			);
		res = PQexec(conn, query->data);
		if (PQresultStatus(res) != PGRES_COPY_BOTH)
		{
			elog(ERROR, "START_REPLICATION_SLOT to node%d failed, shutting down receiver: %s", nodeId, PQresultErrorMessage(res));
		}
		PQclear(res);
		resetPQExpBuffer(query);

		for(;;) /* main loop, jump out only with ERROR */
		{
			int rc, hdr_len;

			if (ProcDiePending)
			{
				BgwPoolShutdown(&Mtm->pools[nodeId-1]);
				proc_exit(0);
			}

			/* Wait necessary amount of time */
			rc = WaitLatchOrSocket(MyLatch,
								   WL_LATCH_SET | WL_SOCKET_READABLE |
								   WL_TIMEOUT | WL_POSTMASTER_DEATH,
								   PQsocket(conn), PQisRsocket(conn),
								   100.0, PG_WAIT_EXTENSION);
			ResetLatch(&MyProc->procLatch);

			/* Process signals */
			if (got_sighup)
			{
				/* Process config file */
				ProcessConfigFile(PGC_SIGHUP);
				got_sighup = false;
				ereport(ERROR, (MTM_ERRMSG("%s: processed SIGHUP", worker_proc)));
			}

			/* Emergency bailout if postmaster has died */
			if (rc & WL_POSTMASTER_DEATH)
			{
				BgwPoolCancel(&Mtm->pools[nodeId-1]);
				proc_exit(1);
			}

			CHECK_FOR_INTERRUPTS();
			AcceptInvalidationMessages();

			if (count != MtmGetRecoveryCount()) {
				ereport(ERROR, (MTM_ERRMSG("%s: restart WAL receiver because node was recovered", worker_proc)));
			}

			if (counterpart_disable_count != MtmGetNodeDisableCount(nodeId))
			{
				ereport(ERROR, (MTM_ERRMSG("%s: restart WAL receiver because counterpart was disabled", worker_proc)));
			}

			/*
			 * Receive data.
			 */
			while (true)
			{
				XLogRecPtr walEnd;
				char* stmt;

				/* Some cleanup */
				if (copybuf != NULL)
				{
					PQfreemem(copybuf);
					copybuf = NULL;
				}

				rc = PQgetCopyData(conn, &copybuf, 1);
				if (rc <= 0)
					break;

				/*
				 * Check message received from server:
				 * - 'k', keepalive message
				 * - 'w', check for streaming header
				 */
				if (copybuf[0] == 'k')
				{
					int			pos;
					bool		replyRequested;

					/*
					 * Parse the keepalive message, enclosed in the CopyData message.
					 * We just check if the server requested a reply, and ignore the
					 * rest.
					 */
					pos = 1;	/* skip msgtype 'k' */

					/*
					 * In this message is the latest WAL position that server has
					 * considered as sent to this receiver.
					 */
					walEnd = fe_recvint64(&copybuf[pos]);
					pos += 8;	/* read walEnd */
					pos += 8;	/* skip sendTime */
					if (rc < pos + 1)
					{
						ereport(ERROR, (MTM_ERRMSG("%s: streaming header too small: %d",
												 worker_proc, rc)));
					}
					replyRequested = copybuf[pos];

					/* Update written position */
					output_written_lsn = Max(walEnd, output_written_lsn);

					/*
					 * If the server requested an immediate reply, send one.
					 * If sync mode is sent reply in all cases to ensure that
					 * server knows how far replay has been done.
					 * In recovery mode also always send reply to provide master with more precise information
					 * about recovery progress
					 */
					if (replyRequested || receiver_sync_mode || MtmGetCurrentStatus() == MTM_RECOVERY)
					{
						int64 now = feGetCurrentTimestamp();

						/* Leave if feedback is not sent properly */
						sendFeedback(conn, now, nodeId);
					}
					continue;
				}
				else if (copybuf[0] != 'w')
				{
					ereport(ERROR, (MTM_ERRMSG("%s: Incorrect streaming header",
											   worker_proc)));
				}

				/* Now fetch the data */
				hdr_len = 1;		/* msgtype 'w' */
				fe_recvint64(&copybuf[hdr_len]);
				hdr_len += 8;		/* dataStart */
				walEnd = fe_recvint64(&copybuf[hdr_len]);
				hdr_len += 8;		/* WALEnd */
				hdr_len += 8;		/* sendTime */

				/*ereport(LOG, (MTM_ERRMSG("%s: receive message %c length %d", worker_proc, copybuf[hdr_len], rc - hdr_len)));*/

				Assert(rc >= hdr_len);

				if (rc > hdr_len)
				{
					int msg_len = rc - hdr_len;
					stmt = copybuf + hdr_len;
					if (buf.used + msg_len + 1 >= MtmTransSpillThreshold*1024L)
					{
						if (spill_file < 0)
						{
							int file_id;
							spill_file = MtmCreateSpillFile(nodeId, &file_id);
							pq_sendbyte(&spill_info, 'F');
							pq_sendint(&spill_info, nodeId, 4);
							pq_sendint(&spill_info, file_id, 4);
						}
						ByteBufferAppend(&buf, ")", 1);
						pq_sendbyte(&spill_info, '(');
						pq_sendint(&spill_info, buf.used, 4);
						MtmSpillToFile(spill_file, buf.data, buf.used);
						ByteBufferReset(&buf);
					}
					if (stmt[0] == 'Z' || (stmt[0] == 'M' && (stmt[1] == 'L' ||
							stmt[1] == 'P' || stmt[1] == 'C' || stmt[1] == 'S' )))
					{
						if (stmt[0] == 'M' && stmt[1] == 'C')
							/* concurrent DDL should be executed by parallel workers */
							MtmExecute(stmt, msg_len, &receiver_ctx, false);
						else
						{
							/* all other messages should be processed by receiver itself */
							MtmExecute(stmt, msg_len, &receiver_ctx, true);
						}
					}
					else
					{
						ByteBufferAppend(&buf, stmt, msg_len);
						if (stmt[0] == 'C') /* commit */
						{
							if (!MtmFilterTransaction(stmt, msg_len, spvector, filter_map))
							{
								if (spill_file >= 0)
								{
									ByteBufferAppend(&buf, ")", 1);
									pq_sendbyte(&spill_info, '(');
									pq_sendint(&spill_info, buf.used, 4);
									MtmSpillToFile(spill_file, buf.data, buf.used);
									MtmCloseSpillFile(spill_file);
									MtmExecute(spill_info.data, spill_info.len, &receiver_ctx, false);
									spill_file = -1;
									resetStringInfo(&spill_info);
								}
								else
									MtmExecute(buf.data, buf.used, &receiver_ctx, false);
							}
							else if (spill_file >= 0)
							{
								MtmCloseSpillFile(spill_file);
								resetStringInfo(&spill_info);
								spill_file = -1;
							}
							ByteBufferReset(&buf);
						}
					}
				}
				/* Update written position */
				output_written_lsn = Max(walEnd, output_written_lsn);
			}

			/* No data, move to next loop */
			if (rc == 0)
			{
				/*
				 * In async mode, and no data available. We block on reading but
				 * not more than the specified timeout, so that we can send a
				 * response back to the client.
				 */
				int				r;
				fd_set			input_mask;
				int64	   		message_target = 0;
				int64			fsync_target = 0;
				struct timeval	timeout;
				struct timeval	*timeoutptr = NULL;
				int64			targettime;
				long			secs;
				int				usecs;
				int64			now;

				FD_ZERO(&input_mask);
				FD_SET(PQsocket(conn), &input_mask);

				/* Now compute when to wakeup. */
				targettime = message_target;

				if (fsync_target > 0 && fsync_target < targettime)
					targettime = fsync_target;
				now = feGetCurrentTimestamp();
				feTimestampDifference(now, targettime, &secs, &usecs);
				if (secs <= 0)
					timeout.tv_sec = 1; /* Always sleep at least 1 sec */
				else
					timeout.tv_sec = secs;
				timeout.tv_usec = usecs;
				timeoutptr = &timeout;

				r = PQselect(PQsocket(conn) + 1, &input_mask, NULL, NULL, timeoutptr,
							 PQisRsocket(conn));
				if (r == 0)
				{
					int64 now = feGetCurrentTimestamp();

					MtmUpdateLsnMapping(nodeId, InvalidXLogRecPtr);
					sendFeedback(conn, now, nodeId);
				}
				else if (r < 0 && errno == EINTR)
					/*
					 * Got a timeout or signal. Continue the loop and either
					 * deliver a status packet to the server or just go back into
					 * blocking.
					 */
					continue;

				else if (r < 0)
				{
					ereport(ERROR, (MTM_ERRMSG("%s: Incorrect status received.",
											   worker_proc)));
				}

				/* Else there is actually data on the socket */
				if (PQconsumeInput(conn) == 0)
				{
					ereport(ERROR, (MTM_ERRMSG("%s: Data remaining on the socket.",
											   worker_proc)));
				}
				continue;
			}

			/* End of copy stream */
			if (rc == -1)
			{
				ereport(ERROR, (MTM_ERRMSG("%s: COPY Stream has abruptly ended...",
										   worker_proc)));
			}

			/* Failure when reading copy stream, leave */
			if (rc == -2)
			{
				ereport(ERROR, (MTM_ERRMSG("%s: Failure while receiving changes...",
										   worker_proc)));
			}
		}
	}
	PG_CATCH();
	{
		PQfinish(conn);
		ReplicationSlotRelease();

		/* cleanup shared state... */
		MtmStateProcessNeighborEvent(nodeId, MTM_NEIGHBOR_WAL_RECEIVER_ERROR, false);

		/*
		 * Some of the workers may stuck on lock and survive until node will come
		 * back and prepare stuck transaction when it was aborted long time ago.
		 * Force all workers to cancel stmt to ensure this will not happen.
		 */
		BgwPoolCancel(&Mtm->pools[nodeId - 1]);
		MtmSleep(RECEIVER_SUSPEND_TIMEOUT);
		mtm_log(MtmApplyError, "Receiver %s catch an error and will die", worker_proc);
		/* and die */
		PG_RE_THROW();
	}
	PG_END_TRY();

	Assert(false);
}


BackgroundWorkerHandle *
MtmStartReceiver(int nodeId, Oid db_id, Oid user_id, pid_t monitor_pid)
{
	BackgroundWorker		worker;
	BackgroundWorkerHandle	*handle;
	pid_t pid;
	BgwHandleStatus	status;

	MemSet(&worker, 0, sizeof(BackgroundWorker));
	worker.bgw_flags = BGWORKER_SHMEM_ACCESS |	BGWORKER_BACKEND_DATABASE_CONNECTION;
	worker.bgw_start_time = BgWorkerStart_ConsistentState;
	worker.bgw_restart_time = BGW_NEVER_RESTART;
	worker.bgw_main_arg = Int32GetDatum(nodeId);
	worker.bgw_notify_pid = monitor_pid;

	memcpy(worker.bgw_extra, &db_id, sizeof(Oid));
	memcpy(worker.bgw_extra + sizeof(Oid), &user_id, sizeof(Oid));

	sprintf(worker.bgw_library_name, "multimaster");
	sprintf(worker.bgw_function_name, "pglogical_receiver_main");
	snprintf(worker.bgw_name, BGW_MAXLEN, "mtm-logrep-receiver-%d-%d", Mtm->my_node_id, nodeId);

	if (!RegisterDynamicBackgroundWorker(&worker, &handle))
		elog(ERROR, "Failed to start receiver worker");

	status = WaitForBackgroundWorkerStartup(handle, &pid);
	Assert(status == BGWH_STARTED);
	return handle;
}
