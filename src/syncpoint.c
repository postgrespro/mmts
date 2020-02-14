/*----------------------------------------------------------------------------
 *
 * syncpoint.c
 *	  Track replication progress in case of parallelized apply process.
 *
 *	  This module solves the same problem that replication_session does, but
 * for the case when apply process is allowed to reorder transactions. Major
 * difference is that during recovery from other node we can't restart from
 * origin_lsn of latest record (as it is made in replication_session). Because
 * of reordering there is no guarantee that for a given latest replicated
 * record all previous were already committed, so trying to restart replication
 * from this lsn may result in lost transactions.
 *	  To deal with that we require that publication need to periodically log
 * special syncpoint message. Upon receival of that message applier is mandated
 * to await for finish of all previously received transactions and don't allow
 * any subsequent transaction to commit during this process. Hence for given
 * syncpoint we can be sure that all previous transaction are in our log and
 * none of next transaction did commit before syncpoint. So it is safe to start
 * recovery from this syncpoints.
 *
 * XXX: replication should start from syncpoint otherwise failure between
 * slot creation and first syncpoint would be mishandled. Postpone this till
 * we have function-based config with separate function for cluster init.
 *
 * Portions Copyright (c) 1996-2018, PostgreSQL Global Development Group
 * Portions Copyright (c) 1994, Regents of the University of California
 *
 *----------------------------------------------------------------------------
 */
#include "postgres.h"

#include "access/twophase.h"
#include "access/xlog.h"
#include "access/xlogutils.h"
#include "access/xlogreader.h"
#include "catalog/pg_type.h"
#include "replication/origin.h"
#include "executor/spi.h"
#include "lib/stringinfo.h"
#include "replication/slot.h"
#include "replication/message.h"
#include "access/xlog_internal.h"
#include "utils/builtins.h"
#include "utils/memutils.h"


#include "libpq-fe.h"

#include "multimaster.h"
#include "syncpoint.h"
#include "logger.h"

static void AdvanceRecoverySlot(int node_id, XLogRecPtr trim_lsn);

/* XXX: change to some receiver-local structures */
static int
origin_id_to_node_id(RepOriginId origin_id, MtmConfig *mtm_cfg)
{
	int			i;

	for (i = 0; i < mtm_cfg->n_nodes; i++)
	{
		if (mtm_cfg->nodes[i].origin_id == origin_id)
			return mtm_cfg->nodes[i].node_id;
	}

	/*
	 * Could happen if node was dropped. Might lead to skipping dropped node
	 * xacts on some lagged node, but who ever said we support membership
	 * changes under load?
	 */
	return -1;
}


/*
 * Periodically put in our log SyncPoint message that orders our followers
 * to await for finish of all transactions received before this message and
 * don't finish any transactions after this point.
 *
 * Also include our restart_lsn in message to allow followers to advance
 * their corresponding recovery slots.
 *
 * This is only function out of whole this file that is indended to be called
 * at publication side. It is called from user backend and also from receivers,
 * otherwise skewed node usage can result in needlessly long recovery.
 */
void
MaybeLogSyncpoint()
{
	XLogRecPtr	syncpoint_lsn;
	XLogRecPtr	min_confirmed_flush = InvalidXLogRecPtr;

	/* do unlocked check first */
	if (GetInsertRecPtr() - Mtm->latestSyncpoint < MULTIMASTER_SYNCPOINT_INTERVAL)
		return;

	LWLockAcquire(Mtm->syncpoint_lock, LW_EXCLUSIVE);
	if (GetInsertRecPtr() - Mtm->latestSyncpoint >= MULTIMASTER_SYNCPOINT_INTERVAL)
	{
		int			i;
		char	   *msg;

		/*
		 * Compute minimal restart lsn across our publications, but excluding
		 * recovery slots.
		 */
		Assert(max_replication_slots > 0);
		LWLockAcquire(ReplicationSlotControlLock, LW_SHARED);
		for (i = 0; i < max_replication_slots; i++)
		{
			ReplicationSlot *s;
			XLogRecPtr	confirmed_flush;
			int			node_id;

			s = &ReplicationSlotCtl->replication_slots[i];

			/* cannot change while ReplicationSlotCtlLock is held */
			if (!s->in_use)
				continue;

			/* we're only interested in logical slots */
			if (!SlotIsLogical(s))
				continue;

			/* skip non-mm slots and recovery slots */
			if (sscanf(s->data.name.data, MULTIMASTER_SLOT_PATTERN, &node_id) != 1)
				continue;

			/* read once, it's ok if it increases while we're checking */
			SpinLockAcquire(&s->mutex);
			confirmed_flush = s->data.confirmed_flush;
			SpinLockRelease(&s->mutex);

			if (min_confirmed_flush == InvalidXLogRecPtr ||
				confirmed_flush < min_confirmed_flush)
				min_confirmed_flush = confirmed_flush;
		}
		LWLockRelease(ReplicationSlotControlLock);

		/* And write it */
		msg = psprintf(UINT64_FORMAT, min_confirmed_flush);
		syncpoint_lsn = LogLogicalMessage("S", msg, strlen(msg) + 1, false);

		Mtm->latestSyncpoint = syncpoint_lsn;

		mtm_log(SyncpointCreated,
				"Syncpoint created (origin_lsn=%" INT64_MODIFIER "x, trim_lsn=%" INT64_MODIFIER "x)",
				syncpoint_lsn, min_confirmed_flush
			);
	}
	LWLockRelease(Mtm->syncpoint_lock);
}

static void
AdvanceRecoverySlot(int node_id, XLogRecPtr trim_lsn)
{
	char	   *sql;
	int			rc;
	XLogRecPtr	restart_lsn;
	Assert(trim_lsn != InvalidXLogRecPtr);

	/* Load latest checkpoint for given node */
	sql = psprintf("select restart_lsn from mtm.syncpoints "
				   "where node_id=%d and origin_lsn < " UINT64_FORMAT " "
				   "order by origin_lsn desc limit 1",
				   node_id, trim_lsn);
	rc = SPI_execute(sql, true, 0);

	if (rc == SPI_OK_SELECT && SPI_processed > 0)
	{
		TupleDesc	tupdesc = SPI_tuptable->tupdesc;
		HeapTuple	tup = SPI_tuptable->vals[0];
		bool		isnull;

		Assert(SPI_processed == 1);

		restart_lsn = DatumGetInt64(SPI_getbinval(tup, tupdesc, 1, &isnull));
		Assert(!isnull);
		Assert(TupleDescAttr(tupdesc, 0)->atttypid == INT8OID);
	}
	else if (rc == SPI_OK_SELECT && SPI_processed == 0)
	{
		mtm_log(SyncpointApply, "No saved restart_lsn found");
		return;
	}
	else
		mtm_log(ERROR, "Failed to load saved restart_lsn");

	/*
	 * XXX: simple delete of restart_lsn < $restart_lsn is not working
	 *
	 * Need to delete old syncpoints.
	 */

	/* Advance recovery slot if possibble */
	PhysicalConfirmReceivedLocation(restart_lsn);

	mtm_log(SyncpointApply,
			"Syncpoint processed: trim recovery slot to %" INT64_MODIFIER "x",
			restart_lsn);
}


/*
 * Register syncpoint and release old WAL.
 */
void
SyncpointRegister(int node_id, XLogRecPtr origin_lsn, XLogRecPtr local_lsn,
				  XLogRecPtr restart_lsn, XLogRecPtr trim_lsn)
{
	char	   *sql;
	int			rc;

	/* Start tx */
	StartTransactionCommand();
	if (SPI_connect() != SPI_OK_CONNECT)
		mtm_log(ERROR, "could not connect using SPI");
	PushActiveSnapshot(GetTransactionSnapshot());

	/* Save syncpoint */
	sql = psprintf("insert into mtm.syncpoints values "
				   "(%d, " UINT64_FORMAT ", " UINT64_FORMAT ", " UINT64_FORMAT ") "
				   "on conflict do nothing",
				   node_id,
				   origin_lsn,
				   local_lsn,
				   restart_lsn
		);
	rc = SPI_execute(sql, false, 0);

	if (rc < 0)
		mtm_log(ERROR, "Failed to save syncpoint");

	/* restart_lsn is invalid for forwarded messages */
	if (restart_lsn != InvalidXLogRecPtr && trim_lsn != InvalidXLogRecPtr)
		AdvanceRecoverySlot(node_id, trim_lsn);

	/* Finish transaction */
	SPI_finish();
	PopActiveSnapshot();
	CommitTransactionCommand();

	mtm_log(SyncpointApply,
			"Syncpoint processed (node_id=%d, origin_lsn=%" INT64_MODIFIER "x, local_lsn=%" INT64_MODIFIER "x, trim_lsn=%" INT64_MODIFIER "x, restart_lsn=%" INT64_MODIFIER "x)",
			node_id,
			origin_lsn,
			local_lsn,
			trim_lsn,
			restart_lsn
		);
}


/*
 * Get latest syncpoint for a given node.
 */
Syncpoint
SyncpointGetLatest(int node_id)
{
	int			rc;
	char	   *sql;
	Syncpoint	sp;
	MemoryContext oldcontext;

	Assert(node_id > 0 && node_id <= MTM_MAX_NODES);

	memset(&sp, '\0', sizeof(Syncpoint));
	sp.local_lsn = InvalidXLogRecPtr;
	sp.origin_lsn = InvalidXLogRecPtr;

	/* Init SPI */
	oldcontext = CurrentMemoryContext;
	StartTransactionCommand();
	if (SPI_connect() != SPI_OK_CONNECT)
		mtm_log(ERROR, "could not connect using SPI");
	PushActiveSnapshot(GetTransactionSnapshot());

	/* Load latest checkpoint for given node */
	sql = psprintf("select origin_lsn, local_lsn "
				   "from mtm.syncpoints where node_id=%d "
				   "order by origin_lsn desc limit 1",
				   node_id);
	rc = SPI_execute(sql, true, 0);

	if (rc == SPI_OK_SELECT && SPI_processed > 0)
	{
		TupleDesc	tupdesc = SPI_tuptable->tupdesc;
		HeapTuple	tup = SPI_tuptable->vals[0];
		bool		isnull;

		Assert(SPI_processed == 1);

		sp.origin_lsn = DatumGetInt64(SPI_getbinval(tup, tupdesc, 1, &isnull));
		Assert(!isnull);
		Assert(TupleDescAttr(tupdesc, 0)->atttypid == INT8OID);

		sp.local_lsn = DatumGetInt64(SPI_getbinval(tup, tupdesc, 2, &isnull));
		Assert(!isnull);
		Assert(TupleDescAttr(tupdesc, 1)->atttypid == INT8OID);
	}
	else if (rc == SPI_OK_SELECT && SPI_processed == 0)
	{
		/* no saved syncpoints found, proceed as is */
		mtm_log(MtmReceiverStart, "No saved syncpoints found");
	}
	else
	{
		mtm_log(ERROR, "Failed to load saved syncpoint");
	}

	/* Finish transaction */
	SPI_finish();
	PopActiveSnapshot();
	CommitTransactionCommand();
	/* transaction knocked down old ctx*/
	MemoryContextSwitchTo(oldcontext);

	return sp;
}


/*
 * Get array of latest syncpoints for all nodes.
 */
Syncpoint *
SyncpointGetAllLatest()
{
	int			rc;
	Syncpoint  *spvector;

	spvector = (Syncpoint *) palloc0(MTM_MAX_NODES * sizeof(Syncpoint));

	/* Init SPI */
	StartTransactionCommand();
	if (SPI_connect() != SPI_OK_CONNECT)
		mtm_log(ERROR, "could not connect using SPI");
	PushActiveSnapshot(GetTransactionSnapshot());

	/*
	 * Load latest checkpoints for all origins we have
	 */
	rc = SPI_execute("select * from mtm.latest_syncpoints",
					 true, 0);

	if (rc == SPI_OK_SELECT && SPI_processed > 0)
	{
		TupleDesc	tupdesc = SPI_tuptable->tupdesc;
		int			i;

		Assert(SPI_processed <= MTM_MAX_NODES);

		for (i = 0; i < SPI_processed; i++)
		{
			HeapTuple	tup = SPI_tuptable->vals[i];
			bool		isnull;
			int			node_id;
			XLogRecPtr	origin_lsn,
						local_lsn;

			node_id = DatumGetInt32(SPI_getbinval(tup, tupdesc, 1, &isnull));
			Assert(!isnull);
			Assert(TupleDescAttr(tupdesc, 0)->atttypid == INT4OID);
			Assert(node_id > 0 && node_id <= MTM_MAX_NODES);

			origin_lsn = DatumGetInt64(SPI_getbinval(tup, tupdesc, 2, &isnull));
			Assert(!isnull);
			Assert(TupleDescAttr(tupdesc, 1)->atttypid == INT8OID);

			local_lsn = DatumGetInt64(SPI_getbinval(tup, tupdesc, 3, &isnull));
			Assert(!isnull);
			Assert(TupleDescAttr(tupdesc, 2)->atttypid == INT8OID);

			spvector[node_id - 1].origin_lsn = origin_lsn;
			spvector[node_id - 1].local_lsn = local_lsn;
		}
	}
	else if (rc == SPI_OK_SELECT && SPI_processed == 0)
	{
		/* no saved syncpoints found, proceed as is */
		mtm_log(MtmReceiverFilter, "No saved syncpoints found");
	}
	else
	{
		mtm_log(ERROR, "Failed to load saved syncpoint");
	}

	/* Finish transaction */
	SPI_finish();
	PopActiveSnapshot();
	CommitTransactionCommand();

	return spvector;
}


/*
 * Now get minimal lsn among syncpoints on the replication node
 */
XLogRecPtr
QueryRecoveryHorizon(PGconn *conn, int node_id, Syncpoint *local_spvector)
{
	StringInfoData serialized_lsns;
	int			i;
	bool		nonzero = false;
	XLogRecPtr	local_horizon = local_spvector[node_id - 1].origin_lsn;
	XLogRecPtr	remote_horizon = InvalidXLogRecPtr;

	/* serialize filter_vector */
	initStringInfo(&serialized_lsns);
	for (i = 0; i < MTM_MAX_NODES; i++)
	{
		if (local_spvector[i].origin_lsn != InvalidXLogRecPtr &&
			i + 1 != node_id)
		{
			appendStringInfo(&serialized_lsns, "%d:" UINT64_FORMAT ",", i + 1,
							 local_spvector[i].origin_lsn);
			nonzero = true;
		}
	}

	if (nonzero)
	{
		char	   *sql;
		PGresult   *res;

		/* Find out minimal lsn of given syncpoints */
		sql = psprintf(
					   "select min(local_lsn) from regexp_split_to_table('%s', ',') as states "
					   "join (select node_id || ':' || origin_lsn as state, * from mtm.syncpoints) "
					   "local_states on states = local_states.state;", serialized_lsns.data);
		res = PQexec(conn, sql);
		pfree(sql);

		if (PQresultStatus(res) != PGRES_TUPLES_OK)
		{
			mtm_log(ERROR, "SyncpointGetRecoveryHorizon: %s",
					PQresultErrorMessage(res));
		}
		else if (PQnfields(res) != 1 ||
				 (PQntuples(res) != 0 && PQntuples(res) != 1))
		{
			mtm_log(ERROR, "SyncpointGetRecoveryHorizon: refusing unexpected result from replication node");
		}

		if (PQntuples(res) == 0)
		{
			mtm_log(MtmReceiverStart, "No saved syncpoints found on remote node");
		}
		else
		{
			remote_horizon = pg_strtouint64(PQgetvalue(res, 0, 0), NULL, 10);
		}
	}

	mtm_log(MtmReceiverStart,
			"QueryRecoveryHorizon (remote_horizon=%" INT64_MODIFIER "x, local_horizon=%" INT64_MODIFIER "x)",
			remote_horizon,
			local_horizon
		);

	return Min(remote_horizon, local_horizon);
}


/*
 * Load filter
 */
HTAB *
RecoveryFilterLoad(int filter_node_id, Syncpoint *spvector, MtmConfig *mtm_cfg)
{
	XLogReaderState *xlogreader;
	HASHCTL		hash_ctl;
	HTAB	   *filter_map;
	int			estimate_size;
	XLogRecPtr	start_lsn = PG_UINT64_MAX;
	XLogRecPtr	current_last_lsn = GetFlushRecPtr();
	int			i;

	Assert(current_last_lsn != InvalidXLogRecPtr);

	/* start from minimal among all of syncpoints */
	for (i = 0; i < MTM_MAX_NODES; i++)
	{
		if (start_lsn > spvector[i].local_lsn &&
			spvector[i].local_lsn != InvalidXLogRecPtr)
			start_lsn = spvector[i].local_lsn;
	}

	/* create hash */
	estimate_size = (GetFlushRecPtr() - start_lsn) / 100;
	estimate_size = Min(Max(estimate_size, 1000), 100000);
	MemSet(&hash_ctl, 0, sizeof(hash_ctl));
	hash_ctl.keysize = hash_ctl.entrysize = sizeof(FilterEntry);

	filter_map = hash_create("filter", estimate_size, &hash_ctl,
							 HASH_ELEM | HASH_BLOBS | HASH_CONTEXT);

	mtm_log(MtmReceiverStart,
			"load_filter_map from " LSN_FMT " node_id=%d current_last_lsn=" LSN_FMT,
			start_lsn, filter_node_id, current_last_lsn);

	Assert(start_lsn != InvalidXLogRecPtr);
	if (start_lsn == PG_UINT64_MAX)
		return filter_map;

	xlogreader = XLogReaderAllocate(wal_segment_size, &read_local_xlog_page, NULL);
	if (!xlogreader)
		ereport(ERROR,
				(errcode(ERRCODE_OUT_OF_MEMORY),
				 errmsg("out of memory"),
				 errdetail("Failed while allocating a WAL reading processor.")));

	/*
	 * The given start_lsn pointer points to the end of the syncpoint record,
	 * which is not necessarily the beginning of the next record, if the
	 * previous record happens to end at a page boundary. Skip over the page
	 * header in that case to find the next record.
	 */
	if (start_lsn % XLOG_BLCKSZ == 0)
	{
		if (XLogSegmentOffset(start_lsn, wal_segment_size) == 0)
			start_lsn += SizeOfXLogLongPHD;
		else
			start_lsn += SizeOfXLogShortPHD;
	}

	/* fill our filter */
	do
	{
		XLogRecord *record;
		char	   *errormsg = NULL;
		RepOriginId origin_id;
		int			node_id;
		char		*gid = ""; /* for debugging */

		record = XLogReadRecord(xlogreader, start_lsn, &errormsg);
		if (record == NULL)
		{
			if (errormsg)
				mtm_log(ERROR,
						"load_filter_map: got error: %s", errormsg);
			else
				mtm_log(MtmReceiverFilter,
						"load_filter_map: got NULL from XLogReadRecord, breaking");
			break;
		}

		/* continue reading on next iteration */
		start_lsn = InvalidXLogRecPtr;

		/* skip local records */
		origin_id = XLogRecGetOrigin(xlogreader);

		if (origin_id == InvalidRepOriginId)
			continue;

		if (XLogRecGetRmid(xlogreader) == RM_XACT_ID)
		{
			mtm_log(MtmReceiverFilter,
					"load_filter_map: process local=%" INT64_MODIFIER "x, origin=%d, node=%d",
					xlogreader->EndRecPtr, origin_id, origin_id_to_node_id(origin_id, mtm_cfg));
		}

		/* skip records from non-mm origins */
		node_id = origin_id_to_node_id(origin_id, mtm_cfg);
		if (node_id < 0)
			continue;

		/* if asked collect records only for a given node */
		if (filter_node_id != -1 && filter_node_id != node_id)
			continue;

		/* XXX: also cover standalone messages */

		if (XLogRecGetRmid(xlogreader) == RM_XACT_ID)
		{
			uint32		info = XLogRecGetInfo(xlogreader);
			FilterEntry entry;
			bool		found;

			memset(&entry, '\0', sizeof(FilterEntry));
			entry.node_id = node_id;
			entry.origin_lsn = InvalidXLogRecPtr;

			switch (info & XLOG_XACT_OPMASK)
			{
				case XLOG_XACT_PREPARE:
					{
						xl_xact_parsed_prepare parsed;

						ParsePrepareRecord(info, XLogRecGetData(xlogreader), &parsed);
						entry.origin_lsn = parsed.origin_lsn;
						gid = parsed.twophase_gid;
						break;
					}
				case XLOG_XACT_COMMIT_PREPARED:
					{
						xl_xact_parsed_commit parsed;

						ParseCommitRecord(info, (xl_xact_commit *) XLogRecGetData(xlogreader), &parsed);
						entry.origin_lsn = parsed.origin_lsn;
						gid = parsed.twophase_gid;
						break;
					}
				case XLOG_XACT_ABORT_PREPARED:
					{
						xl_xact_parsed_abort parsed;

						ParseAbortRecord(info, (xl_xact_abort *) XLogRecGetData(xlogreader), &parsed);
						entry.origin_lsn = parsed.origin_lsn;
						gid = parsed.twophase_gid;
						break;
					}
				default:
					continue;
			}

			/*
			 * Skip record before lsn of filter_vector as we anyway going to
			 * ignore them later.
			 */
			if (entry.origin_lsn <= spvector[node_id - 1].origin_lsn)
				continue;

			Assert(entry.origin_lsn != InvalidXLogRecPtr);
			mtm_log(MtmReceiverFilter, "load_filter_map: add {%d, %" INT64_MODIFIER "x } xact_opmask=%d local_lsn=%" INT64_MODIFIER "x, gid=%s",
					entry.node_id, entry.origin_lsn, info & XLOG_XACT_OPMASK, xlogreader->EndRecPtr, gid);
			hash_search(filter_map, &entry, HASH_ENTER, &found);
			Assert(!found);
		}

	} while (xlogreader->EndRecPtr < current_last_lsn);

	XLogReaderFree(xlogreader);
	return filter_map;
}
