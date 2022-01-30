/*-------------------------------------------------------------------------
 *
 * pglogical_receiver.c
 *
 * Portions Copyright (c) 2015-2021, Postgres Professional
 * Portions Copyright (c) 2015-2020, PostgreSQL Global Development Group
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
#include "postmaster/interrupt.h"
#include "storage/ipc.h"
#include "storage/latch.h"
#include "storage/proc.h"
#include "utils/guc.h"
#include "utils/snapmgr.h"
#include "utils/memutils.h"
#include "executor/spi.h"
#include "replication/origin.h"
#include "replication/walreceiver.h"
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
#include "global_tx.h"

#define ERRCODE_DUPLICATE_OBJECT_STR  "42710"

bool		MtmIsReceiver;

typedef struct
{
	MtmReceiverWorkerContext w;
	XLogRecPtr last_reported_flush;
	TimestampTz last_send_time;
	WalReceiverConn *wrconn;
} MtmReceiverContext;

MtmReplicationMode curr_replication_mode = REPLMODE_DISABLED;

char const *const MtmReplicationModeMnem[] =
{
	"disabled",
	"recovery",
	"normal"
};

/*
 * Unfortunately libpqwalreceiver doesn't allow to pass arbitrary
 * START_REPLICATION options, so expose its internals to exec it manually.
 */
typedef struct
{
	/* Current connection to the primary, if any */
	PGconn	   *streamConn;
	/* Used to remember if the connection is logical or physical */
	bool		logical;
	/* Buffer for currently read records */
	char	   *recvBuf;
} MyWalReceiverConn;

MtmConfig  *receiver_mtm_cfg;
bool		receiver_mtm_cfg_valid;

/* Stream functions */
static int64 fe_recvint64(char *buf);

static void MtmMaybeAdvanceSlot(MtmReceiverContext *rctx, char *conninfo);

void		pglogical_receiver_main(Datum main_arg);

/*
 * Send a Standby Status Update message to server.
 */
static void
sendFeedback(MtmReceiverContext *rctx, bool force)
{
	char		replybuf[1 + 8 + 8 + 8 + 8 + 1 + 1];
	StringInfoData str;
	XLogRecPtr	flush_pos;
	TimestampTz now;

	/*
	 * If the user doesn't want status to be reported to the publisher, be
	 * sure to exit before doing anything at all.
	 */
	if (!force && wal_receiver_status_interval <= 0)
		return;

	flush_pos = pg_atomic_read_u64(&Mtm->peers[rctx->w.sender_node_id - 1].horizon);

	now = GetCurrentTimestamp();
	/*
	 * if we've already reported everything and time for another ping
	 * hasn't come yet we're good
	 */
	if (!force &&
		flush_pos == rctx->last_reported_flush &&
		!TimestampDifferenceExceeds(rctx->last_send_time, now,
									wal_receiver_status_interval * 1000))
		return;
	rctx->last_send_time = now;

	/*
	 * In multimaster flush and apply are the same as we can't handle 2PC
	 * asynchronously: P and CP are always flushed before they are declared as
	 * applied. As for 'write', we could mimick vanilla LR behaviour,
	 * i.e. last applied record which is not necessarily commit (e.g. not yet
	 * committed insert is included into 'write'), but that requires
	 * additional work due to parallel apply -- declaring records which main
	 * receiver has just passed to workers as written would be, well, unfair.
	 */

	str.data = replybuf;
	resetStringInfo(&str);
	str.maxlen = sizeof(replybuf);

	pq_sendbyte(&str, 'r');
	pq_sendint64(&str, flush_pos);	/* write */
	pq_sendint64(&str, flush_pos);	/* flush */
	pq_sendint64(&str, flush_pos);	/* apply */
	pq_sendint64(&str, now);	/* sendTime */
	pq_sendbyte(&str, 0);	/* replyRequested */

	mtm_log(MtmReceiverFeedback, "acking flush of LSN %X/%X",
			(uint32) (flush_pos >> 32),
			(uint32) flush_pos);
	walrcv_send(rctx->wrconn, replybuf, str.len);
	rctx->last_reported_flush = flush_pos;
}

/*
 * pg_replication_slot_advance sender slot if we can do that further last
 * advancement. Note that decoding session startup is quite heavy operation as
 * we must read all unacked WAL + earlier chunk up to suitable snapshot
 * serialization point (which is created mostly each
 * LOG_SNAPSHOT_INTERVAL_MS).
 */
static void
MtmMaybeAdvanceSlot(MtmReceiverContext *rctx, char *conninfo)
{
	XLogRecPtr upto = GetRecoveryHorizon(rctx->w.sender_node_id);
	char *upto_text;
	char *sql;
	PGresult   *res;
	char	*err;

	/* already acked this */
	if (upto <= rctx->last_reported_flush)
		return;

	/*
	 * it would be nice for libpqwalreceiver to expose interruptable
	 * libpqrcv_PQexec and use it here
	 */
	rctx->wrconn = walrcv_connect(conninfo, true, MyBgworkerEntry->bgw_name,
								  &err);
	if (rctx->wrconn == NULL)
		ereport(ERROR,
				(errmsg("could not connect to the sender: %s", err)));

	upto_text = pg_lsn_out_c(upto);
	sql = psprintf("select pg_replication_slot_advance('" MULTIMASTER_SLOT_PATTERN	"', '%s');",
				   Mtm->my_node_id,
				   upto_text);

	res = PQexec(((MyWalReceiverConn *) rctx->wrconn)->streamConn, sql);
	if (PQresultStatus(res) != PGRES_TUPLES_OK)
	{
		mtm_log(ERROR, "%s at node %d failed: %s",
				sql, rctx->w.sender_node_id, PQresultErrorMessage(res));
	}
	rctx->last_reported_flush = upto;
	mtm_log(MtmReceiverFeedback, "advanced slot to %s", upto_text);

	pfree(upto_text);
	pfree(sql);
	walrcv_disconnect(rctx->wrconn);
	rctx->wrconn = NULL;

	/*
	 * libpqwalreceiver calls above have WaitLatch/ResetLatch inside so we
	 * could have swallowed SetLatch awaited by the outer code -- set it to
	 * avoid infinite sleeping.
	 */
	SetLatch(MyLatch);
}

/*
 * Converts an int64 from network byte order to native format.
 */
static int64
fe_recvint64(char *buf)
{
	int64		result;
	uint32		h32;
	uint32		l32;

	memcpy(&h32, buf, 4);
	memcpy(&l32, buf + 4, 4);
	h32 = pg_ntoh32(h32);
	l32 = pg_ntoh32(l32);

	result = h32;
	result <<= 32;
	result |= l32;

	return result;
}

static void
MtmExecute(void *work, int size, MtmReceiverWorkerContext *rwctx, bool no_pool)
{
	if (rwctx->mode == REPLMODE_RECOVERY || no_pool)
		MtmExecutor(work, size, rwctx);
	else
		BgwPoolExecute(BGW_POOL_BY_NODE_ID(rwctx->sender_node_id), work,
					   size, rwctx);

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
MtmFilterTransaction(char *record, int size, Syncpoint *spvector,
					 HTAB *filter_map, MtmReceiverContext *rctx)
{
	StringInfoData s;
	XLogRecPtr	origin_lsn;
	XLogRecPtr	end_lsn;
	XLogRecPtr	tx_lsn;
	int			origin_node;
	uint8		event = 0;
	char const *gid = "null";
	char		msgtype;

	s.data = record;
	s.len = size;
	s.maxlen = -1;
	s.cursor = 0;

	/* read fields */
	msgtype = pq_getmsgbyte(&s);

	if (msgtype == 'C')
	{
		int			sender_node_id PG_USED_FOR_ASSERTS_ONLY;

		event = pq_getmsgbyte(&s);	/* event */
		sender_node_id = pq_getmsgbyte(&s);
		Assert(sender_node_id == rctx->w.sender_node_id);
		pq_getmsgint64(&s);			/* commit_lsn */
		end_lsn = pq_getmsgint64(&s);	/* end_lsn */
		pq_getmsgint64(&s);			/* commit_time */

		origin_node = pq_getmsgbyte(&s);
		origin_lsn = pq_getmsgint64(&s);


		switch (event)
		{
			case PGLOGICAL_PREPARE:
			case PGLOGICAL_ABORT_PREPARED:
				gid = pq_getmsgstring(&s);
				break;
			case PGLOGICAL_PREPARE_PHASE2A:
				gid = pq_getmsgstring(&s);
				pq_getmsgstring(&s); /* state_3pc */
				break;
			case PGLOGICAL_COMMIT_PREPARED:
				pq_getmsgint64(&s); /* CSN */
				gid = pq_getmsgstring(&s);
				break;
			case PGLOGICAL_COMMIT:
				break;
			default:
				Assert(false);
		}
	}
	else if (msgtype == 'M')
	{
		char		action = pq_getmsgbyte(&s);
		int			messageSize;
		char const *messageBody;

		(void) action;		/* keep the compiler quiet when asserts are disabled*/

		end_lsn = pq_getmsgint64(&s);
		messageSize = pq_getmsgint(&s, 4);
		messageBody = pq_getmsgbytes(&s, messageSize);

		/* We hack origin info only into syncpoint messages */
		Assert(action == 'S');
		if (messageBody[0] == 'F') /* forwarded, c.f. process_syncpoint */
		{
			int rc PG_USED_FOR_ASSERTS_ONLY;

			rc = sscanf(messageBody, "F_%d_%" INT64_MODIFIER "X",
						&origin_node, &origin_lsn);
			Assert(rc == 2);
		}
		else
		{
			origin_node = rctx->w.sender_node_id;
			origin_lsn = InvalidXLogRecPtr;
		}
	}
	else {
		ereport(ERROR,
				(errmsg("invalid message type: %c", msgtype)));
	}

	tx_lsn = origin_node == rctx->w.sender_node_id ? end_lsn : origin_lsn;

	/*
	 * Skip all transaction from unknown nodes, i.e. dropped ones. This might
	 * lead to skipping dropped node xacts on some lagged node, but who ever
	 * said we support membership changes under load?
	 */
	if (origin_node == MtmInvalidNodeId)
	{
		mtm_log(MtmReceiverFilter,
				"skipping transaction gid=%s event=%x origin_node=%d origin_lsn=%X/%X as origin is unknown",
				gid, event, origin_node,
				(uint32) (tx_lsn >> 32),
				(uint32) tx_lsn);
		return true;
	}

	/*
	 * Similarly, if we don't know since which LSN to filter out changes for
	 * this origin it means we don't have filter slot and thus have no idea
	 * about such node -- ignore the change.  Again, such thing is possible
	 * after node drop: sp messages carry origin info in itself, so even if
	 * sender forgot about dropped node, origin_node will still be valid here,
	 * though we most probably have also dropped the node along with the
	 * filter slot.
	 *
	 * (note that origin_lsn can't be used here as 0 origin_lsn
	 * is normal situation immediately after start, when no syncpoint exist
	 * yet)
	 */
	if (spvector[origin_node - 1].local_lsn == InvalidXLogRecPtr)
	{
		mtm_log(MtmReceiverFilter,
				"skipping transaction gid=%s event=%x origin_node=%d origin_lsn=%X/%X as there is no filter slot",
				gid, event, origin_node,
				(uint32) (tx_lsn >> 32),
				(uint32) tx_lsn);
		return true;
	}

	/* Skip all transactions from our node */
	if (origin_node == Mtm->my_node_id)
	{
		mtm_log(MtmReceiverFilter,
				"skipping transaction gid=%s event=%x origin_node=%d origin_lsn=%X/%X as it is my own",
				gid, event, origin_node,
				(uint32) (tx_lsn >> 32),
				(uint32) tx_lsn);
		return true;
	}

	if (tx_lsn <= spvector[origin_node - 1].origin_lsn)
	{
		mtm_log(MtmReceiverFilter,
				"skipping transaction gid=%s event=%x origin_node=%d origin_lsn=%X/%X sp.origin_lsn=%X/%X as it is beyond syncpoint",
				gid, event, origin_node,
				(uint32) (tx_lsn >> 32),
				(uint32) tx_lsn,
				(uint32) (spvector[origin_node - 1].origin_lsn >> 32),
				(uint32) spvector[origin_node - 1].origin_lsn
			);
		return true;
	}
	else
	{
		FilterEntry entry;
		bool		found;

		memset(&entry, '\0', sizeof(FilterEntry));
		entry.node_id = origin_node;
		entry.origin_lsn = tx_lsn;

		hash_search(filter_map, &entry, HASH_FIND, &found);

		mtm_log(MtmReceiverFilter,
				"filter (map) transaction gid=%s event=%x origin_node=%d origin_lsn=%X/%X sp.origin_lsn=%X/%X: found=%d",
				gid, event, origin_node,
				(uint32) (tx_lsn >> 32),
				(uint32) tx_lsn,
				(uint32) (spvector[origin_node - 1].origin_lsn >> 32),
				(uint32) spvector[origin_node - 1].origin_lsn,
				found);
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

	/* XXX: that is expensive! better to switch only in recovery */
	replorigin_session_setup(replorigin_session_origin);
}

/*
 * Release replication session. XXX: remove args.
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

static void
subscription_change_cb(Datum arg, int cacheid, uint32 hashvalue)
{
	receiver_mtm_cfg_valid = false;
}

void
MtmWakeupReceivers(void)
{
	int i;

	LWLockAcquire(Mtm->lock, LW_SHARED);
	for (i = 0; i < MTM_MAX_NODES; i++)
	{
		PGPROC *proc = Mtm->peers[i].walreceiver_proc;

		if (proc)
			SetLatch(&proc->procLatch);
	}
	LWLockRelease(Mtm->lock);
}

static void
pglogical_receiver_at_exit(int status, Datum arg)
{
	MtmReceiverContext *rctx = (MtmReceiverContext *) DatumGetPointer(arg);

	/*
	 * We might've come here after siglongjmp to bgworker.c which had restored
	 * the signal mask -- and signals were blocked in StartBackgroundWorker as
	 * postmaster suprisingly spins up bgws in signal handler, so it blocks
	 * them beforehand.
	 *
	 * Code below will need to wait on latch, unblock them to prevent hanging.
	 * Surely we could've already missed some signals, but that's fine as long
	 * as we follow the standard convention of WaitLatch at the loop
	 * bottom. As for die signals, they don't matter anymore as we are already
	 * exiting (c.f. proc_exit_prepare) -- CHECK_FOR_INTERRUPTS is no-op at
	 * this point.
	 *
	 * Alternatively we could just have our own try/catch...
	 */
	BackgroundWorkerUnblockSignals();

	/* seems better to log this *before* we start killing workers */
	/* monitor will restart us immediately if our mode has changed */
	if (rctx->w.mode != MtmGetReceiverMode(rctx->w.sender_node_id))
		mtm_log(MtmReceiverState, "receiver %s is exiting to reconnect in another mode",
				MyBgworkerEntry->bgw_name);
	else
		/*
		 * TODO: it would be nice to distinguish exit on error here from
		 * normal one after SIGTERM (instance shutdown, for example).
		 */
		mtm_log(MtmReceiverState, "receiver %s is exiting",
				MyBgworkerEntry->bgw_name);

	/*
	 * Make sure all our workers die before checking out, we don't want to
	 * have orphaned workers prowling and applying around when we (or another
	 * receiver) (re)start.
	 */
	BgwPoolCancel(&Mtm->pools[rctx->w.sender_node_id - 1]);

	if (rctx->wrconn)
		walrcv_disconnect(rctx->wrconn);
	if (MyReplicationSlot != NULL)
		ReplicationSlotRelease();

	/*
	 * probably lost connection with this node, so initiate resolution of all
	 * xacts originated there
	 */
	GlobalTxMarkOrphaned(rctx->w.sender_node_id);
	ResolverWake();

	/* tell others we don't apply anymore */
	LWLockAcquire(Mtm->lock, LW_EXCLUSIVE);
	if (rctx->w.mode == REPLMODE_RECOVERY)
		Mtm->nreceivers_recovery--;
	else if (rctx->w.mode == REPLMODE_NORMAL)
		Mtm->nreceivers_normal--;
	Mtm->peers[rctx->w.sender_node_id - 1].walreceiver_proc = NULL;
	BIT_CLEAR(Mtm->walreceivers_mask, rctx->w.sender_node_id - 1);
	LWLockRelease(Mtm->lock);
	MtmWakeupReceivers();
}

void
pglogical_receiver_main(Datum main_arg)
{
	/* Variables for replication connection */
	char	   *conninfo;
	PQExpBuffer query;
	PGresult   *res;
	MtmReceiverContext *rctx;
	int sender;

	ByteBuffer	buf;

	int			spill_file = -1;
	StringInfoData spill_info;
	static PortalData fakePortal;

	Oid			db_id;
	Oid			user_id;

	rctx = MemoryContextAllocZero(TopMemoryContext, sizeof(MtmReceiverContext));
	rctx->w.sender_node_id = DatumGetInt32(main_arg);
	rctx->w.txlist_pos = -1;
	sender = rctx->w.sender_node_id; /* shorter lines */

	/*
	 * On any ERROR we simply cleanup in this hook and die, bgw will be
	 * restarted. We also reconnect through restart when needed.
	 */
	on_shmem_exit(pglogical_receiver_at_exit, PointerGetDatum(rctx));

	MtmIsReceiver = true;
	/* Run as replica session replication role. */
	SetConfigOption("session_replication_role", "replica",
									PGC_SUSET, PGC_S_OVERRIDE);
	/* XXX: get rid of that */
	MtmBackgroundWorker = true;
	MtmIsLogicalReceiver = true;

	ByteBufferAlloc(&buf);

	initStringInfo(&spill_info);

	/* Register functions for SIGTERM/SIGHUP management */
	pqsignal(SIGHUP, SignalHandlerForConfigReload);
	pqsignal(SIGTERM, die);

	MtmCreateSpillDirectory(sender);

	/* We're now ready to receive signals */
	BackgroundWorkerUnblockSignals();

	/*
	 * Load the libpq wrappers simplifying life a bit + making all (but
	 * sending) calls interruptable
	 */
	load_file("libpqwalreceiver", false);

	memcpy(&db_id, MyBgworkerEntry->bgw_extra, sizeof(Oid));
	memcpy(&user_id, MyBgworkerEntry->bgw_extra + sizeof(Oid), sizeof(Oid));

	/* Connect to a database */
	BackgroundWorkerInitializeConnectionByOid(db_id, user_id, 0);
	ActivePortal = &fakePortal;
	ActivePortal->status = PORTAL_ACTIVE;
	ActivePortal->sourceText = "";

	receiver_mtm_cfg = MtmLoadConfig(FATAL);
	if (MtmNodeById(receiver_mtm_cfg, sender) == NULL)
		proc_exit(0); /* our node was excluded */
	conninfo = MtmNodeById(receiver_mtm_cfg, sender)->conninfo;

	/* Keep us informed about subscription changes. */
	CacheRegisterSyscacheCallback(SUBSCRIPTIONOID,
								  subscription_change_cb,
								  (Datum) 0);

	BgwPoolStart(sender, MyBgworkerEntry->bgw_name, db_id, user_id);
	mtm_log(MtmReceiverState, "receiver %s started.", MyBgworkerEntry->bgw_name);

	/* TODO there used to be PG_TRY block, reindent it back */
	{
		XLogRecPtr	remote_start;
		Syncpoint  *spvector = NULL;
		HTAB	   *filter_map = NULL;
		nodemask_t	connected_mask;
		char	   *err;
		PGconn	   *conn;

		/* announce that we must be wakened in MtmWakeupReceivers */
		LWLockAcquire(Mtm->lock, LW_EXCLUSIVE);
		Mtm->peers[sender - 1].walreceiver_proc = MyProc;
		LWLockRelease(Mtm->lock);

		mtm_log(LOG, "-----> logical_recv_main: 2");
		/*
		 * Determine how we should pull and ensure we won't interfere with
		 * other receivers.
		 */
		for (;;)
		{
			int rc;
			MtmReplicationMode mode = MtmGetReceiverMode(sender);

			LWLockAcquire(Mtm->lock, LW_EXCLUSIVE);
			if ((mode == REPLMODE_RECOVERY &&
				 Mtm->nreceivers_recovery == 0 && Mtm->nreceivers_normal == 0) ||
				(mode == REPLMODE_NORMAL && Mtm->nreceivers_recovery == 0))
			{
				if (mode == REPLMODE_RECOVERY)
					Mtm->nreceivers_recovery++;
				else
					Mtm->nreceivers_normal++;
				Mtm->peers[sender - 1].receiver_mode = mode;
				BIT_SET(Mtm->walreceivers_mask, rctx->w.sender_node_id - 1);
				rctx->w.mode = mode;
				curr_replication_mode = rctx->w.mode;
			}
			LWLockRelease(Mtm->lock);

			if (rctx->w.mode != REPLMODE_DISABLED)
				break; /* success */

			/*
			 * So this receiver can't work which usually means we are in
			 * recovery and donor is not our sender. Attempt to advance our
			 * sender slot then -- this allows to trim WAL on non-donors
			 * during recovery which may be very long.
			 */
			MtmMaybeAdvanceSlot(rctx, conninfo);

			rc = WaitLatch(MyLatch,
						   WL_LATCH_SET | WL_EXIT_ON_PM_DEATH,
						   0, PG_WAIT_EXTENSION);

			if (rc & WL_LATCH_SET)
				ResetLatch(MyLatch);
		}
		mtm_log(MtmReceiverState, "registered as running in %s mode",
				MtmReplicationModeMnem[rctx->w.mode]);

		mtm_log(LOG, "-----> logical_recv_main: 7");
		/*
		 * do not start until dmq connection to the node is established,
		 * c.f. MtmOnDmqReceiverDisconnect
		 */
		connected_mask = MtmGetConnectedMask(false);
		if (!BIT_CHECK(connected_mask, rctx->w.sender_node_id - 1))
			elog(ERROR, "receiver %s exits as dmq connection to node %d is not yet established",
				 MyBgworkerEntry->bgw_name, rctx->w.sender_node_id);

		mtm_log(LOG, "-----> logical_recv_main: 8");
		/*
		 * Acquire filter rep slot, so we can advance it during normal work
		 * without search
		 */
		if (rctx->w.mode == REPLMODE_NORMAL)
		{
			ReplicationSlotAcquire(psprintf(MULTIMASTER_FILTER_SLOT_PATTERN,
											sender),
								   true);
		}

		/* Establish connection to the remote server */
		rctx->wrconn = walrcv_connect(conninfo, true, MyBgworkerEntry->bgw_name,
									  &err);
		mtm_log(LOG, "-----> logical_recv_main: 9");
		if (rctx->wrconn == NULL)
			ereport(ERROR,
					(errmsg("could not connect to the sender: %s", err)));


		/* Create new slot if needed */
		query = createPQExpBuffer();

		mtm_log(LOG, "-----> logical_recv_main: 10");
		/*
		 * Make sure config is up to date as we are going to check out
		 * backup_node_id; if it has been cleared we must be aware of that.
		 */
		AcceptInvalidationMessages();
		if (!receiver_mtm_cfg_valid)
		{
			MtmConfigFree(receiver_mtm_cfg);
			receiver_mtm_cfg = MtmLoadConfig(FATAL);
			if (MtmNodeById(receiver_mtm_cfg, rctx->w.sender_node_id) == NULL)
					proc_exit(0);

			receiver_mtm_cfg_valid = true;
		}

		mtm_log(LOG, "-----> logical_recv_main: 11");
		if (rctx->w.mode == REPLMODE_RECOVERY)
		{
			/*
			 * Immediately after add_node new node is forced to recover from
			 * donor as xact order there is the same, so syncpoints are
			 * irrelevant (it wouldn't be easy to correctly filter out xacts
			 * if we pull immediately from other nodes, e.g. donor xacts must
			 * be considered as having origin now).
			 */
			if (receiver_mtm_cfg->backup_node_id > 0)
			{
				int i;

				spvector = palloc0(MTM_MAX_NODES * sizeof(Syncpoint));
				/*
				 * Immediately after basebackup we don't have any
				 * syncpoints. We must fetch data since backup_end_lsn and
				 * filter since current position of filter slot (is is almost
				 * the same, but not necessarily equal: some wal could pass
				 * (and got recycled) between node startup and slot creation)
				 */
				spvector[sender - 1] = SyncpointGetLatest(sender);
				remote_start = receiver_mtm_cfg->backup_end_lsn;
				/*
				 * all other origins are also filtered from filter position
				 * to donor
				 */
				for (i = 0; i < receiver_mtm_cfg->n_nodes; i++)
				{
					spvector[receiver_mtm_cfg->nodes[i].node_id - 1].local_lsn =
						spvector[sender - 1].local_lsn;
				}
				/*
				 * we must filter out all xacts since sp to donor in this
				 * special add_node case, so MtmInvalidNodeId
				 * is important here
				 */
				filter_map = RecoveryFilterLoad(MtmInvalidNodeId,
												spvector, receiver_mtm_cfg);
				Assert(remote_start != InvalidXLogRecPtr);
			}
			else
			{
				spvector = SyncpointGetAllLatest(sender);
				filter_map = RecoveryFilterLoad(MtmInvalidNodeId,
												spvector, receiver_mtm_cfg);
				remote_start = GetRecoveryHorizon(sender);
			}
		}
		else
		{
			spvector = palloc0(MTM_MAX_NODES * sizeof(Syncpoint));
			spvector[sender - 1] = SyncpointGetLatest(sender);
			remote_start = spvector[sender - 1].origin_lsn;
			filter_map = RecoveryFilterLoad(sender, spvector, receiver_mtm_cfg);
		}

		/* log our intentions */
		{
			int			i;
			StringInfo	message = makeStringInfo();

			appendStringInfoString(message, "starting receiver: ");
			appendStringInfo(message, "replication_node = %d", sender);
			appendStringInfo(message, ", mode = %s",
							 MtmReplicationModeMnem[rctx->w.mode]);
			appendStringInfo(message, ", remote_start = %" INT64_MODIFIER "X",
							 remote_start);

			appendStringInfo(message, ", syncpoint_vector (origin/local) = {");
			for (i = 0; i < MTM_MAX_NODES; i++)
			{
				/*
				 * local_lsn must be always (even before first syncpoint)
				 * non-zero in used cell; it is filter slot position.
				 */
				if (spvector[i].local_lsn != InvalidXLogRecPtr)
				{
					appendStringInfo(message, "%d: " LSN_FMT "/" LSN_FMT ", ",
									 i + 1,
									 spvector[i].origin_lsn,
									 spvector[i].local_lsn);
				}
			}
			appendStringInfo(message, "}");

			mtm_log(MtmReceiverState, "%s", message->data);
		}

		Assert(filter_map && spvector);

		appendPQExpBuffer(query, "START_REPLICATION SLOT \"%s\" LOGICAL %x/%x ("
						  "\"startup_params_format\" '1',"
						  "\"max_proto_version\" '1',"
						  "\"min_proto_version\" '1',"
						  "\"forward_changesets\" '1',"
						  "\"binary.want_binary_basetypes\" '%d',"
						  "\"mtm_replication_mode\" '%s')",
						  psprintf(MULTIMASTER_SLOT_PATTERN, receiver_mtm_cfg->my_node_id),
						  (uint32) (remote_start >> 32),
						  (uint32) remote_start,
						  MtmBinaryBasetypes,
						  MtmReplicationModeMnem[rctx->w.mode]
			);
		conn = ((MyWalReceiverConn *) rctx->wrconn)->streamConn;
		res = PQexec(conn, query->data);
		if (PQresultStatus(res) != PGRES_COPY_BOTH)
		{
			elog(ERROR, "START_REPLICATION SLOT to node%d failed, shutting down receiver: %s",
				 sender, PQresultErrorMessage(res));
		}
		PQclear(res);
		resetPQExpBuffer(query);
		mtm_log(MtmReceiverStateDebug, "START_REPLICATION done");

		for (;;)				/* main loop, jump out only with ERROR */
		{
			pgsocket	fd = PGINVALID_SOCKET;
			long		wait_time;
			int			rc;

			CHECK_FOR_INTERRUPTS();

			sendFeedback(rctx, false);

			if (ConfigReloadPending)
			{
				ConfigReloadPending = false;
				ProcessConfigFile(PGC_SIGHUP);
			}

			/*
			 * this is currently trivially true as we always apply only after
			 * getting commit (or prepare)
			 */
			if (!IsTransactionState())
			{
				/*
				 * If we didn't get any transactions for a while there might
				 * be unconsumed invalidation messages in the queue, consume
				 * them now.
				 */
				AcceptInvalidationMessages();
			}

			for (;;) /* spin tightly while there is data */
			{
				char	   *copybuf = NULL;
				int			len;
				int			hdr_len;

				CHECK_FOR_INTERRUPTS();
				/*
				 * Do we need to reconnect in recovery?
				 * Note that parallel workers don't check this; we expect main
				 * receiver to notice the change and kill them.
				 */
				if (unlikely(rctx->w.mode != MtmGetReceiverMode(sender)))
				{
					proc_exit(0);
				}

				len = walrcv_receive(rctx->wrconn, &copybuf, &fd);

				if (len == 0)
				{
					break; /* WOULDBLOCK */
				}
				else if (len < 0)
				{
					ereport(LOG,
							(errmsg("data stream from sender has ended")));
					proc_exit(0);
				}

				/*
				 * Check message received from server: - 'k', keepalive
				 * message - 'w', check for streaming header
				 */
				if (copybuf[0] == 'k')
				{
					int			pos;
					bool		replyRequested;

					/*
					 * Parse the keepalive message, enclosed in the CopyData
					 * message. We just check if the server requested a reply,
					 * and ignore the rest.
					 */
					pos = 1;	/* skip msgtype 'k' */

					pos += 8;  /* skip so-called walEnd (actually the last LSN sent) */
					pos += 8;	/* skip sendTime */
					if (len < pos + 1)
					{
						ereport(ERROR, (MTM_ERRMSG("%s: streaming header too small: %d",
												   MyBgworkerEntry->bgw_name, len)));
					}
					replyRequested = copybuf[pos];

					sendFeedback(rctx, replyRequested);
					continue;
				}
				else if (copybuf[0] != 'w')
				{
					ereport(ERROR, (MTM_ERRMSG("%s: Incorrect streaming header",
											   MyBgworkerEntry->bgw_name)));
				}

				/* Now fetch the data */
				hdr_len = 1;	/* msgtype 'w' */
				fe_recvint64(&copybuf[hdr_len]);
				hdr_len += 8;	/* dataStart */
				hdr_len += 8;	/* WALEnd */
				hdr_len += 8;	/* sendTime */

				/* mtm_log(LOG, "receive message %c length %d", copybuf[hdr_len], len - hdr_len); */

				Assert(len >= hdr_len);

				if (len > hdr_len)
				{
					int			msg_len = len - hdr_len;
					char	   *stmt;

					stmt = copybuf + hdr_len;

					/*
					 * Non-tx logical messages are normally short and don't
					 * need spill support.
					 */
					if (stmt[0] == 'Z' || (stmt[0] == 'M' && (stmt[1] == 'L' ||
															  stmt[1] == 'P' ||
															  stmt[1] == 'C' ||
															  stmt[1] == 'V' ||
															  stmt[1] == 'S')))
					{
						/*
						 * Filter out already applied messages. Decoding API
						 * doesn't disclosure logical messages origin, so
						 * currently we have directly hacked it only into
						 * syncpoint messages (it is important to trim advance
						 * slots during recovery). It would be good to fix
						 * this for non-tx DDL as well -- currently it is
						 * never relogged and thus skipped in recovery from
						 * non-origin.
						 */
						if (stmt[0] == 'M' && stmt[1] == 'S' &&
							MtmFilterTransaction(stmt, msg_len, spvector,
												 filter_map, rctx))
						{
							continue;
						}
						if (stmt[0] == 'M' && stmt[1] == 'C')
						{
							/*
							 * non-tx DDL should be executed by parallel
							 * workers
							 */
							MtmExecute(stmt, msg_len, &rctx->w, false);
						}
						else
						{
							/*
							 * all other messages should be processed by
							 * receiver itself
							 */
							MtmExecute(stmt, msg_len, &rctx->w, true);
						}
						continue;
					}

					if (buf.used + msg_len + 1 >= MtmTransSpillThreshold * 1024L)
					{
						if (spill_file < 0)
						{
							int			file_id;

							spill_file = MtmCreateSpillFile(sender, &file_id);
							pq_sendbyte(&spill_info, 'F');
							pq_sendint(&spill_info, sender, 4);
							pq_sendint(&spill_info, file_id, 4);
						}
						ByteBufferAppend(&buf, ")", 1);
						pq_sendbyte(&spill_info, '(');
						pq_sendint(&spill_info, buf.used, 4);
						MtmSpillToFile(spill_file, buf.data, buf.used);
						ByteBufferReset(&buf);
					}

					ByteBufferAppend(&buf, stmt, msg_len);
					if (stmt[0] == 'C') /* commit */
					{
						/*
						 * Don't apply xact if our filter says we already
						 * did so. Drop it if this is ABORT as well --
						 * this means PREPARE at sender was aborted in the
						 * middle of decoding.
						 */
						if ((stmt[1] != PGLOGICAL_ABORT &&
							 !MtmFilterTransaction(stmt, msg_len, spvector,
												   filter_map, rctx)))
						{
							if (spill_file >= 0)
							{
								ByteBufferAppend(&buf, ")", 1);
								pq_sendbyte(&spill_info, '(');
								pq_sendint(&spill_info, buf.used, 4);
								MtmSpillToFile(spill_file, buf.data, buf.used);
								MtmCloseSpillFile(spill_file);
								MtmExecute(spill_info.data, spill_info.len,
										   &rctx->w, false);
								spill_file = -1;
								resetStringInfo(&spill_info);
							}
							else
								MtmExecute(buf.data, buf.used, &rctx->w,
										   /*
											* Force bdr-like transactions
											* ending with plain commit to
											* execute serially to avoid
											* reordering conflicts.
											*/
										   stmt[1] == PGLOGICAL_COMMIT);
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

			/* pedantically wake up when time for ping comes */
			wait_time = (long) ((rctx->last_send_time +
								wal_receiver_status_interval * USECS_PER_SEC -
								 GetCurrentTimestamp()) / 1000);
			if (wait_time <= 0)
				continue;
			/* no data, wait for it */
			rc = WaitLatchOrSocket(MyLatch,
								   WL_LATCH_SET | WL_SOCKET_READABLE |
								   WL_TIMEOUT | WL_EXIT_ON_PM_DEATH,
								   fd,
								   wait_time, PG_WAIT_EXTENSION);

			if (rc & WL_LATCH_SET)
			{
				ResetLatch(MyLatch);
			}
		}
	}

	Assert(false);
}
