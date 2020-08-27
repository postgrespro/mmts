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
#include "utils/pg_lsn.h"

#include "libpq-fe.h"

#include "multimaster.h"
#include "syncpoint.h"
#include "logger.h"

int			MtmSyncpointInterval;  /* in kilobytes */

PG_FUNCTION_INFO_V1(update_recovery_horizons);

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
 * Periodically put in our log SyncPoint message which hints our followers
 * that it would be nice to apply everything before this message and confirm
 * the receival to advance the replication slot.
 *
 * The receiver doesn't actually need anyting from the sender to create
 * syncpoint. It is logged only because sender knows the best when WAL becomes
 * bloated and it is time to trim it. In particular, is case of assymetric
 * node load, e.g. 1 node writes, 2 and 3 only apply, receiver 2->3 staying
 * idle won't know WAL is being generated on 2 without additional actions.
 *
 * This is only function out of whole this file that is indended to be called
 * at publication side. It is called from user backend and also from
 * receivers, otherwise skewed node usage can result in needlessly long
 * recovery.
 */
void
MaybeLogSyncpoint(void)
{
	/* do unlocked check first */
	if ((GetInsertRecPtr() < Mtm->latestSyncpoint) ||
		(GetInsertRecPtr() - Mtm->latestSyncpoint < MtmSyncpointInterval * 1024))
		return;

	LWLockAcquire(Mtm->syncpoint_lock, LW_EXCLUSIVE);
	/*
	 * GetInsertRecPtr returns slightly stale value, so the first check
	 * prevents too frequent syncpoint creation
	 */
	if ((GetInsertRecPtr() > Mtm->latestSyncpoint) &&
		(GetInsertRecPtr() - Mtm->latestSyncpoint >= MtmSyncpointInterval * 1024))
	{
		XLogRecPtr syncpoint_lsn;

		syncpoint_lsn = LogLogicalMessage("S", "", 1, false);
		Mtm->latestSyncpoint = syncpoint_lsn;

		mtm_log(SyncpointCreated,
				"syncpoint created, origin_lsn=%X/%X)",
				(uint32) (syncpoint_lsn >> 32), (uint32) syncpoint_lsn);
	}
	LWLockRelease(Mtm->syncpoint_lock);
}

/* copied from pg_lsn_out */
char*
pg_lsn_out_c(XLogRecPtr lsn)
{
	char		buf[32];
	uint32		id,
				off;

	/* Decode ID and offset */
	id = (uint32) (lsn >> 32);
	off = (uint32) lsn;

	snprintf(buf, sizeof buf, "%X/%X", id, off);
	return pstrdup(buf);
}

/*
 * Register that we have applied all origin_node_id changes with end_lsn <=
 * origin_lsn; all origin_node_id xacts with end_lsn > origin_lsn will have
 * start lsn >= receiver_lsn locally. Insertion of this info into
 * mtm.syncpoint broadcasts it to other nodes so they can advance their
 * progress reporting of origin changes to us.
 *
 */
void
SyncpointRegister(int origin_node_id, XLogRecPtr origin_lsn,
				  XLogRecPtr receiver_lsn)
{
	char	   *sql;
	int			rc;

	/* Start tx */
	StartTransactionCommand();
	PushActiveSnapshot(GetTransactionSnapshot());
	if (SPI_connect() != SPI_OK_CONNECT)
		mtm_log(ERROR, "could not connect using SPI");

	/*
	 * Log mark that this is bdr-like transaction: plain COMMIT will be
	 * performed instead of 3PC and so receiver is not allowed to fail
	 * applying this transaction and go ahead even in normal mode.
	 */
	LogLogicalMessage("B", "", 1, true);

	/* Save syncpoint */
	sql = psprintf("insert into mtm.syncpoints values "
				   "(%d, '%s', %d, '%s') "
				   "on conflict do nothing",
				   origin_node_id,
				   pg_lsn_out_c(origin_lsn),
				   Mtm->my_node_id,
				   pg_lsn_out_c(receiver_lsn)
		);
	rc = SPI_execute(sql, false, 0);
	if (rc < 0)
		mtm_log(ERROR, "failed to save syncpoint");

	/*
	 * Cleanup old entries. We'd better do that not only when we insert into
	 * syncpoints table but on any insertion, however that's not easy since
	 * deletion must be originated by me to broadcast it to others and avoid
	 * conflicts (c.f. cleanup_old_syncpoints)
	 */
	rc = SPI_execute("select mtm.cleanup_old_syncpoints()", false, 0);
	if (rc < 0)
		mtm_log(ERROR, "failed to cleanup syncpoints: %d", rc);

	/* Finish transaction */
	SPI_finish();

	UpdateRecoveryHorizons();

	PopActiveSnapshot();
	CommitTransactionCommand();

	/* outer code must have already acquired proper slot */
	PhysicalConfirmReceivedLocation(receiver_lsn);

	mtm_log(SyncpointApply,
			"syncpoint processed: node_id=%d, origin_lsn=%X/%X, receiver_lsn=%X/%X",
			origin_node_id,
			(uint32) (origin_lsn >> 32), (uint32) origin_lsn,
			(uint32) (receiver_lsn >> 32), (uint32) receiver_lsn);
}


/*
 * Get our latest syncpoint for a given node (origin_lsn is 0/0, local_lsn is
 * the curr position of the filter slot in case of absense).
 */
Syncpoint
SyncpointGetLatest(int origin_node_id)
{
	int			rc;
	char	   *sql;
	Syncpoint	sp;
	MemoryContext oldcontext;

	Assert(origin_node_id > 0 && origin_node_id <= MTM_MAX_NODES);

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
	sql = psprintf("select origin_lsn, fill_filter_since from mtm.latest_syncpoints "
				   "where origin_node_id = %d and receiver_node_id = %d",
				   origin_node_id, Mtm->my_node_id);
	rc = SPI_execute(sql, true, 0);

	if (rc == SPI_OK_SELECT && SPI_processed > 0)
	{
		TupleDesc	tupdesc = SPI_tuptable->tupdesc;
		HeapTuple	tup = SPI_tuptable->vals[0];
		bool		isnull;

		Assert(SPI_processed == 1);

		sp.origin_lsn = DatumGetLSN(SPI_getbinval(tup, tupdesc, 1, &isnull));
		Assert(!isnull);
		Assert(TupleDescAttr(tupdesc, 0)->atttypid == LSNOID);

		sp.local_lsn = DatumGetLSN(SPI_getbinval(tup, tupdesc, 2, &isnull));
		/* xxx c.f. similar in SyncpointGetAllLatest */
		if (isnull)
			mtm_log(ERROR, "fill_filter_since is null for node %d", origin_node_id);
		Assert(TupleDescAttr(tupdesc, 1)->atttypid == LSNOID);
	}
	else if (rc == SPI_OK_SELECT && SPI_processed == 0)
	{
		/* no saved syncpoints found, proceed as is */
		mtm_log(MtmReceiverStart, "no saved syncpoints found");
	}
	else
	{
		mtm_log(ERROR, "no to load saved syncpoint");
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
 * Get array of *our* latest syncpoints for all nodes (technically we are
 * interested only in local lsns to fill the filter).
 */
Syncpoint *
SyncpointGetAllLatest(int sender_node_id)
{
	int			rc;
	Syncpoint  *spvector;
	char *sql;

	spvector = (Syncpoint *) palloc0(MTM_MAX_NODES * sizeof(Syncpoint));

	/* Init SPI */
	StartTransactionCommand();
	if (SPI_connect() != SPI_OK_CONNECT)
		mtm_log(ERROR, "could not connect using SPI");
	PushActiveSnapshot(GetTransactionSnapshot());

	/*
	 * Load latest checkpoints for all origins we have
	 */
	sql = psprintf("select origin_node_id, origin_lsn, fill_filter_since "
				   "from mtm.latest_syncpoints "
				   "where receiver_node_id = %d",
				   Mtm->my_node_id);
	rc = SPI_execute(sql, true, 0);

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

			origin_lsn = DatumGetLSN(SPI_getbinval(tup, tupdesc, 2, &isnull));
			Assert(!isnull);
			Assert(TupleDescAttr(tupdesc, 1)->atttypid == LSNOID);
			spvector[node_id - 1].origin_lsn = origin_lsn;

			local_lsn = DatumGetLSN(SPI_getbinval(tup, tupdesc, 3, &isnull));
			Assert(TupleDescAttr(tupdesc, 2)->atttypid == LSNOID);
			/*
			 * XXX: there is no barrier between filter slot creation and
			 * receiver start, so in theory (really unlikely) fastest receiver
			 * may spin up before some slot is created by monitor and
			 * fill_filter_since would be null. Shout in this case.
			 */
			if (isnull)
				mtm_log(ERROR, "fill_filter_since is null for node %d", node_id);
			spvector[node_id - 1].local_lsn = local_lsn;
		}
	}
	else if (rc == SPI_OK_SELECT && SPI_processed == 0)
	{
		/* no saved syncpoints found, proceed as is */
		mtm_log(MtmReceiverFilter, "no saved syncpoints found");
	}
	else
	{
		mtm_log(ERROR, "failed to load saved syncpoint");
	}

	/* Finish transaction */
	SPI_finish();
	PopActiveSnapshot();
	CommitTransactionCommand();

	return spvector;
}

/*
 * Since which LSN we must request streaming from sender_node_id to get all
 * origin changes? This is the same LSN we ack as flushed.
 */
XLogRecPtr
GetRecoveryHorizon(int sender_node_id)
{
	int			rc;
	char *sql;
	bool inside_tx = IsTransactionOrTransactionBlock();
	HeapTuple	tup;
	bool		isnull;
	XLogRecPtr	horizon;

	/*
	 * Receiver calls this function when sending feedback, which may happen in
	 * the middle of transaction (between records) or outside it.
	 */
	if (!inside_tx)
		StartTransactionCommand();

	if (SPI_connect() != SPI_OK_CONNECT)
		mtm_log(ERROR, "could not connect using SPI");
	PushActiveSnapshot(GetTransactionSnapshot());

	sql = psprintf("select mtm.get_recovery_horizon(%d)", sender_node_id);
	rc = SPI_execute(sql, true, 0);

	if (rc != SPI_OK_SELECT && SPI_processed != 1)
		mtm_log(ERROR, "mtm.get_recovery_horizon failed: rc=%d, SPI_processed=%ld",
				rc, SPI_processed);

	tup = SPI_tuptable->vals[0];
	horizon = DatumGetLSN(SPI_getbinval(tup, SPI_tuptable->tupdesc, 1, &isnull));
	if (isnull)
		mtm_log(ERROR, "GetRecoveryHorizon: node %d is not found",
				sender_node_id);

	SPI_finish();
	PopActiveSnapshot();

	if (!inside_tx)
		CommitTransactionCommand();
	return horizon;
}

/*
 * receiver needs the horizon to report to walsender, however running SPI for
 * each report is too expensive, so put current positions in shmem for faster
 * retrieval.
 */
void
UpdateRecoveryHorizons(void)
{
	int			rc;

	if (SPI_connect() != SPI_OK_CONNECT)
		mtm_log(ERROR, "could not connect using SPI");
	PushActiveSnapshot(GetTransactionSnapshot());

	/* Load latest checkpoint for given node */
	rc = SPI_execute("select node_id, horizon from mtm.get_recovery_horizons();",
					 true, 0);

	if (rc == SPI_OK_SELECT)
	{
		TupleDesc	tupdesc = SPI_tuptable->tupdesc;
		int			i;

		Assert(SPI_processed <= MTM_MAX_NODES);
		for (i = 0; i < SPI_processed; i++)
		{
			HeapTuple	tup = SPI_tuptable->vals[i];
			bool		isnull;
			int			node_id;
			XLogRecPtr	horizon;

			node_id = DatumGetInt32(SPI_getbinval(tup, tupdesc, 1, &isnull));
			Assert(!isnull);
			Assert(TupleDescAttr(tupdesc, 0)->atttypid == INT4OID);
			Assert(node_id > 0 && node_id <= MTM_MAX_NODES);

			horizon = DatumGetLSN(SPI_getbinval(tup, tupdesc, 2, &isnull));
			Assert(!isnull);
			Assert(TupleDescAttr(tupdesc, 1)->atttypid == LSNOID);

			/*
			 * races around this might in theory set the value backwards, but
			 * there is nothing scary in that
			 */
			pg_atomic_write_u64(&Mtm->peers[node_id - 1].horizon, horizon);
		}
	}
	else
	{
		mtm_log(ERROR, "get_recovery_horizons failed, rc %d", rc);
	}

	SPI_finish();
}

Datum
update_recovery_horizons(PG_FUNCTION_ARGS)
{
	UpdateRecoveryHorizons();
	PG_RETURN_VOID();
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
	XLogRecPtr	current_last_lsn;
	int			i;

	/* ensure we will scan everything written up to this point, just in case */
	XLogFlush(GetXLogWriteRecPtr());
	current_last_lsn = GetFlushRecPtr();

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
		if (filter_node_id != MtmInvalidNodeId && filter_node_id != node_id)
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
 				case XLOG_XACT_COMMIT:
					{
						xl_xact_parsed_commit parsed;

						ParseCommitRecord(info, (xl_xact_commit *) XLogRecGetData(xlogreader), &parsed);
						entry.origin_lsn = parsed.origin_lsn;
						break;
					}
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
			/*
			 * Note: we used to have Assert(!found) here, but tests showed
			 * that empty (without changes) transaction commit does *not*
			 * advance GetFlushRecPtr, though physically written if xid was
			 * issued (and applier always acquired xid). This means such xact
			 * sometimes doesn't get into filter as we read WAL only up to
			 * GetFlushRecPtr, so later we get it second time. This is
			 * harmless as applying empty xact does nothing to the database,
			 * but the assertion would be violated. And empty xacts
			 * replication became quite common since plain commits streaming
			 * was enabled for syncpoints sake; e.g. vacuum creates one.
			 */
		}
	} while (xlogreader->EndRecPtr < current_last_lsn);

	XLogReaderFree(xlogreader);
	return filter_map;
}
