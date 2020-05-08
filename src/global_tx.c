/*-----------------------------------------------------------------------------
 *
 * global_tx.c
 *	  Persistent and in-memory state necessary for our E3PC-like atommic commit
 #	  protocol.
 *
 * Copyright (c) 2016-2019, Postgres Professional
 *
 *-----------------------------------------------------------------------------
 */

#include "postgres.h"

#include "access/twophase.h"
#include "catalog/pg_authid.h"
#include "catalog/pg_type.h"
#include "executor/spi.h"
#include "storage/ipc.h"
#include "utils/lsyscache.h"
#include "utils/pg_lsn.h"
#include "utils/snapmgr.h"
#include "utils/hsearch.h"
#include "miscadmin.h"

#include "global_tx.h"
#include "commit.h"
#include "logger.h"

/*
 * Definitions for the "mtm.gtx_proposals" table.
 */
#define MTM_GTX_PROPOSALS							"mtm.gtx_proposals"
#define Natts_mtm_gtx_proposals						3
#define Anum_mtm_gtx_proposals_gid					1	/* gid */
#define Anum_mtm_gtx_proposals_prepare_origin_lsn	2	/* origin_lsn */
#define Anum_mtm_gtx_proposals_state				3	/* state_3pc */

gtx_shared_data *gtx_shared;
static GlobalTx *my_locked_gtx;
static bool gtx_exit_registered;

char const *const GlobalTxStatusMnem[] =
{
	"GTXInvalid",
	"GTXPreCommitted",
	"GTXPreAborted",
	"GTXCommitted",
	"GTXAborted"
};

int
term_cmp(GlobalTxTerm t1, GlobalTxTerm t2)
{
	if (t1.ballot < t2.ballot)
	{
		return -1;
	}
	else if (t1.ballot == t2.ballot)
	{
		if (t1.node_id < t2.node_id)
			return -1;
		else if (t1.node_id == t2.node_id)
			return 0;
		else
			return 1;
	}
	else
		return 1;
}


char *
serialize_gtx_state(GlobalTxStatus status, GlobalTxTerm term_prop, GlobalTxTerm term_acc)
{
	char	   *state;
	char	   *status_abbr;

	if (status == GTXInvalid)
		status_abbr = "in";
	else if (status == GTXPreCommitted)
		status_abbr = "pc";
	else if (status == GTXPreAborted)
		status_abbr = "pa";
	else if (status == GTXCommitted)
		status_abbr = "cm";
	else
	{
		Assert(status == GTXAborted);
		status_abbr = "ab";
	}

	state = psprintf("%s-%d:%d-%d:%d",
					 status_abbr,
					 term_prop.ballot, term_prop.node_id,
					 term_acc.ballot, term_acc.node_id);
	return state;
}

void
parse_gtx_state(const char *state, GlobalTxStatus *status,
				GlobalTxTerm *term_prop, GlobalTxTerm *term_acc)
{
	int			n_parsed = 0;

	Assert(state);

	/*
	 * Might be immediately after PrepareTransaction. It would be better to
	 * also pass state_3pc to it, but...
	 */
	if (state[0] == '\0')
	{
		*status = GTXInvalid;
		*term_prop = (GlobalTxTerm) {1,0};
		*term_acc = (GlobalTxTerm) {0,0};
	}
	else
	{
		if (strncmp(state, "in-", 3) == 0)
			*status = GTXInvalid;
		else if (strncmp(state, "pc-", 3) == 0)
			*status = GTXPreCommitted;
		else if (strncmp(state, "pa-", 3) == 0)
			*status = GTXPreAborted;
		else if (strncmp(state, "cm-", 3) == 0)
			*status = GTXCommitted;
		else
		{
			Assert((strncmp(state, "ab-", 3) == 0));
			*status = GTXAborted;
		}

		n_parsed = sscanf(state + 3, "%d:%d-%d:%d",
						&term_prop->ballot, &term_prop->node_id,
						&term_acc->ballot, &term_acc->node_id);

		if (n_parsed != 4)
		{
			Assert(false);
			mtm_log(PANIC, "wrong state_3pc format: %s", state);
		}
	}
}

void
GlobalTxAtExit(int code, Datum arg)
{
	if (my_locked_gtx)
	{
		LWLockAcquire(gtx_shared->lock, LW_EXCLUSIVE);
		my_locked_gtx->acquired_by = InvalidBackendId;
		LWLockRelease(gtx_shared->lock);
		my_locked_gtx = NULL;
	}
}

void
MtmGlobalTxInit()
{
	Size		size = 0;

	size = add_size(size, sizeof(gtx_shared_data));
	size = add_size(size, hash_estimate_size(2*MaxConnections,
											 sizeof(GlobalTx)));
	size = MAXALIGN(size);

	RequestAddinShmemSpace(size);
	RequestNamedLWLockTranche("mtm-gtx-lock", 1);
}

void
MtmGlobalTxShmemStartup(void)
{
	HASHCTL		info;
	bool		found;

	memset(&info, 0, sizeof(info));
	info.keysize = GIDSIZE;
	info.entrysize = sizeof(GlobalTx);

	LWLockAcquire(AddinShmemInitLock, LW_EXCLUSIVE);

	gtx_shared = ShmemInitStruct("mtm-gtx",
								 sizeof(gtx_shared_data),
								 &found);

	if (!found)
		gtx_shared->lock = &(GetNamedLWLockTranche("mtm-gtx-lock"))->lock;

	gtx_shared->gid2gtx = ShmemInitHash("gid2gtx", 2*MaxConnections, 2*MaxConnections,
							&info, HASH_ELEM);

	LWLockRelease(AddinShmemInitLock);
}

/*
 * Obtain a global tx and lock it on calling backend.
 *
 * Several backend can try to access same gtx only during resolving
 * procedure and even then it's quite unlikely event. So just sleep a
 * bit.
 *
 * Here we allow for a global tx to be created again even when we saw
 * that tx as locked but later it was deleted from hash. That may happen
 * in two cases:
 *	  Prepare is racing with commit (abort) -- that should not happen as
 * would mean transaction duplication. (XXX is there any good way to assert
 * that without passing third argument?)
 *	  Status is racing with commit -- that is okay, we later will scan WAL
 * will read that commit (abort).
 */
GlobalTx *
GlobalTxAcquire(const char *gid, bool create, bool *is_new)
{
	GlobalTx   *gtx = NULL;
	bool		found;

	if (!gtx_exit_registered)
	{
		before_shmem_exit(GlobalTxAtExit, 0);
		gtx_exit_registered = true;
	}

	if (is_new != NULL)
		*is_new = false;

	LWLockAcquire(gtx_shared->lock, LW_EXCLUSIVE);

	/* Repeat attempts to acquire a global tx */
	while (true)
	{
		gtx = (GlobalTx *) hash_search(gtx_shared->gid2gtx, gid, HASH_FIND, &found);

		if (!found)
		{
			if (create)
			{
				gtx = (GlobalTx *) hash_search(gtx_shared->gid2gtx, gid, HASH_ENTER, &found);

				gtx->coordinator_end_lsn = InvalidXLogRecPtr;
				gtx->acquired_by = MyBackendId;
				gtx->state.status = GTXInvalid;
				gtx->state.proposal = InitialGTxTerm;
				gtx->state.accepted = InvalidGTxTerm;
				gtx->prepared = false;
				gtx->in_table = false;
				gtx->orphaned = false;
				gtx->resolver_stage = GTRS_AwaitStatus;
				memset(gtx->phase1_acks, 0, sizeof(gtx->phase1_acks));
				memset(gtx->phase2_acks, 0, sizeof(gtx->phase2_acks));

				if (is_new != NULL)
					*is_new = true;
			}
			else
			{
				gtx = NULL;
			}

			break;
		}

		if (gtx->acquired_by == InvalidBackendId)
		{
			gtx->acquired_by = MyBackendId;
			break;
		}

		LWLockRelease(gtx_shared->lock);
		MtmSleep(USECS_PER_SEC / 10);
		LWLockAcquire(gtx_shared->lock, LW_EXCLUSIVE);
	}

	LWLockRelease(gtx_shared->lock);
	my_locked_gtx = gtx;
	if (gtx)
		mtm_log(LOG, "acquired gtx %s, MyBackendId=%d", gtx->gid, MyBackendId);
	return gtx;
}

/*
 * needed for ugly hack in commit.c exit hook
 */
GlobalTx *GetMyGlobalTx(void)
{
	return my_locked_gtx;
}

/*
 * Release our lock on this transaction and remove it from hash. Xacts
 * finished via CP|AP are removed here; shmem mirrors of obsolete
 * gtx_proposals entries are cleaned up in GlobalTxGCInTableProposals.
 *
 */
void
GlobalTxRelease(GlobalTx *gtx)
{
	bool		found;

	Assert(gtx->acquired_by == MyBackendId);

	LWLockAcquire(gtx_shared->lock, LW_EXCLUSIVE);
	gtx->acquired_by = InvalidBackendId;

	/*
	 * Note that currently we don't register final states (CP|AP) for in table
	 * xacts; after in table -> prepare migration they will be finished by our
	 * resolver or neightbour's CP|AP record. This simplifies things a bit,
	 * allowing to have 'xact is committed|aborted => delete gtx on release'
	 * logic. However, resolving generally is quite expensive (as digging in
	 * WAL is long), so if testing would show this is a problem, we could add
	 * yet another flag 'finalized' to GlobalTx, set it after
	 * FinishPreparedTransaction and remove such finalized entries here.
	 *
	 * We also automatically remove shmem entry if gtx is not prepared and
	 * there is no voting state -- no xact and no data, essentially. This is
	 * used by
	 * - monitor 1a handling who after GlobalTxAcquire might decide it
	 *   won't vote (too old xact)
	 * - coordinator, if PREPARE on it failed.
	 */

	/* status GTXInvalid can be caused by an error during PREPARE */
	if ((gtx->state.status == GTXCommitted) ||
		(gtx->state.status == GTXAborted) ||
		(!gtx->prepared &&
		 (term_cmp(gtx->state.proposal, InitialGTxTerm) == 0) &&
		 gtx->state.status == GTXInvalid))
	{
		hash_search(gtx_shared->gid2gtx, gtx->gid, HASH_REMOVE, &found);
	}
	else if (gtx->orphaned && gtx->prepared)
	{
		mtm_log(ResolverTasks, "Transaction %s is orphaned", gtx->gid);
	}

	mtm_log(LOG, "released gtx %s by %d", gtx->gid, MyBackendId);
	LWLockRelease(gtx_shared->lock);

	my_locked_gtx = NULL;
}

/*
 * Scan and load in gid2gtx hashtable transactions from postgres gxacts and
 * from our private table.
 *
 * Called only upon multimaster startup and shmem restart from initialising
 * bgworker.
 */
void
GlobalTxLoadAll()
{
	PreparedTransaction pxacts;
	int			n_xacts,
				i,
				rc;

	LWLockAcquire(gtx_shared->lock, LW_EXCLUSIVE);

	/* Walk over postgres gxacts */
	n_xacts = GetPreparedTransactions(&pxacts);
	for (i = 0; i < n_xacts; i++)
	{
		GlobalTx   *gtx;
		bool		found;

		gtx = (GlobalTx *) hash_search(gtx_shared->gid2gtx, pxacts[i].gid, HASH_ENTER, &found);
		Assert(!found);

		gtx->coordinator_end_lsn = pxacts[i].origin_lsn == InvalidXLogRecPtr ?
			pxacts[i].prepare_end_lsn : pxacts[i].origin_lsn;
		gtx->acquired_by = InvalidBackendId;
		parse_gtx_state(pxacts[i].state_3pc, &gtx->state.status,
						&gtx->state.proposal, &gtx->state.accepted);
		gtx->prepared = true;
		gtx->in_table = false;
		gtx->orphaned = true;
		gtx->resolver_stage = GTRS_AwaitStatus;
		memset(gtx->phase1_acks, 0, sizeof(gtx->phase1_acks));
		memset(gtx->phase2_acks, 0, sizeof(gtx->phase2_acks));
	}

	/* Walk over proposals table */
	if (SPI_connect() != SPI_OK_CONNECT)
		mtm_log(ERROR, "could not connect using SPI");

	PushActiveSnapshot(GetTransactionSnapshot());

	rc = SPI_execute("select * from " MTM_GTX_PROPOSALS, true, 0);
	if (rc < 0 || rc != SPI_OK_SELECT)
		mtm_log(ERROR, "Failed to load saved global_tx proposals");

	for (i = 0; i < SPI_processed; i++)
	{
		TupleDesc	tupdesc = SPI_tuptable->tupdesc;
		HeapTuple	tup = SPI_tuptable->vals[i];
		char	   *gid;
		char	   *sstate;
		bool		found;
		GlobalTx   *gtx;

		gid = SPI_getvalue(tup, tupdesc, Anum_mtm_gtx_proposals_gid);
		Assert(TupleDescAttr(tupdesc, Anum_mtm_gtx_proposals_gid - 1)->atttypid == TEXTOID);

		sstate = SPI_getvalue(tup, tupdesc, Anum_mtm_gtx_proposals_state);
		Assert(TupleDescAttr(tupdesc, Anum_mtm_gtx_proposals_state - 1)->atttypid == TEXTOID);

		gtx = (GlobalTx *) hash_search(gtx_shared->gid2gtx, gid, HASH_ENTER, &found);

		/*
		 * Table entry is deleted upon commit/abort so state saved in table
		 * have lesser priority comparing to one in 3pc_state.
		 */
		if (!found)
		{
			bool isnull;

			gtx->coordinator_end_lsn = DatumGetLSN(
				SPI_getbinval(tup, tupdesc,
							  Anum_mtm_gtx_proposals_prepare_origin_lsn,
							  &isnull));
			Assert(!isnull);
			Assert(TupleDescAttr(tupdesc, Anum_mtm_gtx_proposals_prepare_origin_lsn - 1)->atttypid == LSNOID);
			parse_gtx_state(sstate, &gtx->state.status,
							&gtx->state.proposal, &gtx->state.accepted);
			gtx->prepared = false;
			gtx->in_table = true;
			gtx->orphaned = true;
			gtx->acquired_by = InvalidBackendId;
			gtx->resolver_stage = GTRS_AwaitStatus;
			memset(gtx->phase1_acks, 0, sizeof(gtx->phase1_acks));
			memset(gtx->phase2_acks, 0, sizeof(gtx->phase2_acks));
		}
		else
		{
			/*
			 * We have both in table entry and PREPARE record; it must mean we
			 * have unluckily crashed/errored after applying PREPARE but
			 * before deleting in table entry. Resolver periodically takes
			 * care to delete such entries.
			 */
			gtx->in_table = true;
		}
	}

	if (SPI_finish() != SPI_OK_FINISH)
		mtm_log(ERROR, "could not finish SPI connection");

	PopActiveSnapshot();

	LWLockRelease(gtx_shared->lock);
}

/*
 * Save (new or update current) transaction voting state in table. That
 * happens when we receive status request and participate in resolving before
 * prepare itself.
 *
 * Note: it performs a transaction itself.
 */
void
GlobalTxSaveInTable(const char *gid, XLogRecPtr coordinator_end_lsn,
					GlobalTxStatus status,
					GlobalTxTerm term_prop, GlobalTxTerm term_acc)
{
	int			rc;
	char	   *sql;
	char	   *slsn;
	char	   *sstate;
	MemoryContext oldcontext = CurrentMemoryContext;
	Oid typOutput;
	bool typIsVarlena;

	StartTransactionCommand();

	if (SPI_connect() != SPI_OK_CONNECT)
		mtm_log(ERROR, "could not connect using SPI");
	PushActiveSnapshot(GetTransactionSnapshot());

	getTypeOutputInfo(LSNOID, &typOutput, &typIsVarlena);
	slsn = OidOutputFunctionCall(typOutput, LSNGetDatum(coordinator_end_lsn));
	sstate = serialize_gtx_state(status, term_prop, term_acc);

	mtm_log(MtmTxTrace, "saving in table gid %s, olsn %s, state %s",
			gid, slsn, sstate);

	/* upsert overwriting whatever there was */
	sql = psprintf("insert into " MTM_GTX_PROPOSALS " values ('%s', '%s', '%s')"
				   "on conflict (gid) do update "
				   "set gid = excluded.gid, "
				   "prepare_origin_lsn = excluded.prepare_origin_lsn, "
				   "state = excluded.state",
				   gid, slsn, sstate);

	rc = SPI_execute(sql, false, 0);
	if (rc < 0 || rc != SPI_OK_INSERT)
		mtm_log(ERROR, "Failed to save global_tx proposal");

	if (SPI_finish() != SPI_OK_FINISH)
		mtm_log(ERROR, "could not finish SPI connection");
	PopActiveSnapshot();

	CommitTransactionCommand();
	/* transaction knocked down old ctx*/
	MemoryContextSwitchTo(oldcontext);
}

void
GlobalTxDeleteFromTable(const char *gid)
{
	int			rc;
	bool		inside_tx = IsTransactionState();
	Oid			save_userid;
	int			save_sec_context;

	if (!inside_tx)
		StartTransactionCommand();

	/*
	 * Escalate our privileges, as current user may not have rights to access
	 * mtm schema.
	 */
	GetUserIdAndSecContext(&save_userid, &save_sec_context);
	SetUserIdAndSecContext(BOOTSTRAP_SUPERUSERID,
						   save_sec_context | SECURITY_LOCAL_USERID_CHANGE);

	if (SPI_connect() != SPI_OK_CONNECT)
		mtm_log(ERROR, "could not connect using SPI");
	PushActiveSnapshot(GetTransactionSnapshot());

	rc = SPI_execute(psprintf("delete from " MTM_GTX_PROPOSALS " where gid='%s'",
							  gid),
					 false, 0);
	if (rc != SPI_OK_DELETE)
		mtm_log(ERROR, "Failed to delete saved gtx proposal for '%s'", gid);

	if (SPI_finish() != SPI_OK_FINISH)
		mtm_log(ERROR, "could not finish SPI");

	PopActiveSnapshot();

	/* restore back current user context */
	SetUserIdAndSecContext(save_userid, save_sec_context);

	if (!inside_tx)
		CommitTransactionCommand();
	mtm_log(MtmTxTrace, "transaction %s entry removed from gtx_proposals", gid);
}

typedef char pgid_t[GIDSIZE];

/*
 * This should be called sometimes as we might get resolve request before
 * PREPARE, save in table proposal and then, if xact is resolved to abort,
 * PREPARE might have never been received (aborted 2PCs are not decoded and it
 * might not apply anyway). Such hanged in table entries are vacuumed here.
 *
 * We can safely delete entry once syncpoint machinery says that PREPARE LSN
 * was eaten. If it was applied successfully, voting state migrated to
 * state_3pc and will live there until CP|AP. Otherwise, this is definitely
 * abort, and we will refuse to vote normally for the xact -- see monitor
 * actions.
 */
void
GlobalTxGCInTableProposals(void)
{
	int rc;
	int i;
	/* don't bother with dynamic memory, there should be very few entries ... */
	pgid_t gids[300];
	int n_gids = 0;

	StartTransactionCommand();
	if (SPI_connect() != SPI_OK_CONNECT)
		mtm_log(ERROR, "could not connect using SPI");
	PushActiveSnapshot(GetTransactionSnapshot());

	/*
	 * It seems that direct bulk DELETE here with the following repeated
	 * deletion from shmem would be simpler, but it has (incredibly unlikely)
	 * deadlock danger: while handling resolve requests 1) gtx is locked 2)
	 * upsert into gtx_proposals is done, but here it would be reversed. So go
	 * inefficient and ugly path of selecting and then removing entries
	 * one-by-one.
	 */
	rc = SPI_execute("select p.gid from " MTM_GTX_PROPOSALS " p, "
					 "mtm.latest_syncpoints s where "
					 "substring(p.gid from 'MTM-(\\d+)-')::int = s.node_id and "
					 "mtm.pg_lsn_to_bigint(p.prepare_origin_lsn) <= s.origin_lsn "
					 "limit 300",
					 false, 0);
	if (rc < 0 || rc != SPI_OK_SELECT)
		mtm_log(ERROR, "Failed to select stale global_tx proposals");

	/* and repeat the deletion for shmem */
	for (i = 0; i < SPI_processed; i++)
	{
		TupleDesc	tupdesc = SPI_tuptable->tupdesc;
		HeapTuple	tup = SPI_tuptable->vals[i];
		char		*gid = SPI_getvalue(tup, tupdesc, 1);

		strcpy(gids[n_gids], gid);
		n_gids++;
	}

	PopActiveSnapshot();
	if (SPI_finish() != SPI_OK_FINISH)
		mtm_log(ERROR, "could not finish SPI");
	CommitTransactionCommand();

	for (i = 0; i < n_gids; i++)
	{
		GlobalTx	*gtx;
		char *gid = gids[i];

		mtm_log(MtmTxTrace, "removing stale global_tx entry for xact %s", gid);
		GlobalTxDeleteFromTable(gid);

		gtx = GlobalTxAcquire(gid, false, NULL);
		if (!gtx)
			continue;

		gtx->in_table = false;
		LWLockAcquire(gtx_shared->lock, LW_EXCLUSIVE);
		/*
		 * Concurrent PREPARE will attempt to purge in table entry immediately
		 * after, well, apply, but in the unlikely scenario of it failing we
		 * should be careful not to delete prepared entry.
		 */
		if (!gtx->prepared)
		{
			hash_search(gtx_shared->gid2gtx, gid, HASH_REMOVE, NULL);
			LWLockRelease(gtx_shared->lock);
			my_locked_gtx = NULL;
		}
		else
		{
			LWLockRelease(gtx_shared->lock);
			GlobalTxRelease(gtx);
		}

	}
}

/*
 * Get maximux proposal among all transactions.
 */
GlobalTxTerm
GlobalTxGetMaxProposal()
{
	HASH_SEQ_STATUS hash_seq;
	GlobalTx   *gtx;
	GlobalTxTerm max_prop = (GlobalTxTerm) {0, 0};

	LWLockAcquire(gtx_shared->lock, LW_SHARED);
	hash_seq_init(&hash_seq, gtx_shared->gid2gtx);
	while ((gtx = hash_seq_search(&hash_seq)) != NULL)
	{
		if (term_cmp(max_prop, gtx->state.proposal) < 0)
			max_prop = gtx->state.proposal;
	}
	LWLockRelease(gtx_shared->lock);

	return max_prop;
}


void
GlobalTxMarkOrphaned(int node_id)
{
	HASH_SEQ_STATUS hash_seq;
	GlobalTx   *gtx;

	LWLockAcquire(gtx_shared->lock, LW_EXCLUSIVE);
	hash_seq_init(&hash_seq, gtx_shared->gid2gtx);
	while ((gtx = hash_seq_search(&hash_seq)) != NULL)
	{
		int			tx_node_id = MtmGidParseNodeId(gtx->gid);
		if (tx_node_id == node_id)
		{
			gtx->orphaned = true;
			mtm_log(MtmTxTrace, "%s is orphaned", gtx->gid);
		}
	}
	LWLockRelease(gtx_shared->lock);
}

static char *
GlobalTxStateToString(GTxState *gtx_state)
{
	StringInfoData	si;

	initStringInfo(&si);
	appendStringInfoString(&si, "{");
	appendStringInfo(&si, "\"status\": \"%s\"", GlobalTxStatusMnem[gtx_state->status]);
	appendStringInfo(&si, ", \"proposal\": [%d, %d]", gtx_state->proposal.ballot, gtx_state->proposal.node_id);
	appendStringInfo(&si, ", \"accepted\": [%d, %d]", gtx_state->accepted.ballot, gtx_state->accepted.node_id);
	appendStringInfoString(&si, "}");

	return si.data;
}

char *
GlobalTxToString(GlobalTx *gtx)
{
	StringInfoData si;
	int			i;
	bool		empty = true;

	initStringInfo(&si);
	appendStringInfoString(&si, "{");
	appendStringInfo(&si, "\"gid\": \"%s\"", gtx->gid);
	appendStringInfo(&si, ", \"coordinator_end_lsn\": \"%X/%X\"",
					 (uint32) (gtx->coordinator_end_lsn >> 32),
					 (uint32) gtx->coordinator_end_lsn);
	appendStringInfo(&si, ", \"state.status\": \"%s\"", GlobalTxStatusMnem[gtx->state.status]);
	appendStringInfo(&si, ", \"state.proposal\": [%d, %d]", gtx->state.proposal.ballot, gtx->state.proposal.node_id);
	appendStringInfo(&si, ", \"state.accepted\": [%d, %d]", gtx->state.accepted.ballot, gtx->state.accepted.node_id);
	appendStringInfo(&si, ", \"orphaned\": %s", gtx->orphaned ? "true" : "false");
	appendStringInfo(&si, ", \"prepared\": %s", gtx->prepared ? "true" : "false");
	appendStringInfo(&si, ", \"resolver_stage\": %d", gtx->resolver_stage);

	appendStringInfoString(&si, ", \"phase1_acks\": [");
	for (i = 0; i < MTM_MAX_NODES; i++)
	{
		if (gtx->phase1_acks[i].status == GTXInvalid)
			continue;

		if (!empty)
			appendStringInfoString(&si, ", ");
		appendStringInfoString(&si, GlobalTxStateToString(&gtx->phase1_acks[i]));
		empty = false;
	}
	appendStringInfoString(&si, "], ");

	empty = true;
	appendStringInfoString(&si, ", \"phase2_acks\": [");
	for (i = 0; i < MTM_MAX_NODES; i++)
	{
		if (gtx->phase2_acks[i].status == GTXInvalid)
			continue;

		if (!empty)
			appendStringInfoString(&si, ", ");
		appendStringInfoString(&si, GlobalTxStateToString(&gtx->phase2_acks[i]));
		empty = false;
	}
	appendStringInfoString(&si, "]}");

	return si.data;
}
