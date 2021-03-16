/*-----------------------------------------------------------------------------
 *
 * global_tx.c
 *	  Persistent and in-memory state necessary for our E3PC-like atomic commit
 #	  protocol.
 *
 *    Actually serves as a wrapper around vanilla 2PC (GlobalTransaction)
 *    which allows to atomically process transaction state. We could get away
 *    without it if LockGXact and friends would be public (with probably
 *    slight interface change). Previously global_tx also supported
 *    manipulating persistent xact state without PREPARE (in local table)
 *    which made possible to participate in normal voting before getting P,
 *    but there is no need for that anymore.
 *
 * Copyright (c) 2019-2020, Postgres Professional
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

#define XStateVersion 1

char *
serialize_xstate(XactInfo *xinfo, GTxState *gtx_state)
{
	char	   *state;
	char	   *status_abbr;

	if (gtx_state->status == GTXInvalid)
		status_abbr = "in";
	else if (gtx_state->status == GTXPreCommitted)
		status_abbr = "pc";
	else if (gtx_state->status == GTXPreAborted)
		status_abbr = "pa";
	else if (gtx_state->status == GTXCommitted)
		status_abbr = "cm";
	else
	{
		Assert(gtx_state->status == GTXAborted);
		status_abbr = "ab";
	}

	state = psprintf("%d-%d-" XID_FMT "-%" INT64_MODIFIER "X-%" INT64_MODIFIER "X-%s-%d:%d-%d:%d",
					 XStateVersion,
					 xinfo->coordinator,
					 xinfo->xid,
					 xinfo->gen_num,
					 xinfo->configured,
					 status_abbr,
					 gtx_state->proposal.ballot, gtx_state->proposal.node_id,
					 gtx_state->accepted.ballot, gtx_state->accepted.node_id);
	return state;
}

/* returns 0 on success */
int
deserialize_xstate(const char *state, XactInfo *xinfo, GTxState *gtx_state,
				   int elevel)
{
	int			n_parsed = 0;
	char		status_abbr[3]; /* must be big enough for '\0' */

	Assert(state);

	n_parsed = sscanf(state, "%*d-%d-" XID_FMT "-%" INT64_MODIFIER "X-%" INT64_MODIFIER "X-%2s-%d:%d-%d:%d",
					  &xinfo->coordinator,
					  &xinfo->xid,
					  &xinfo->gen_num,
					  &xinfo->configured,
					  status_abbr,
					  &gtx_state->proposal.ballot,
					  &gtx_state->proposal.node_id,
					  &gtx_state->accepted.ballot,
					  &gtx_state->accepted.node_id);
	if (n_parsed != 9)
	{
		mtm_log(elevel, "GlobalTxLoadAll: failed to deparse state_3pc %s, ignoring it (res=%d)",
				state, n_parsed);
		return n_parsed;
	}

	if (strncmp(status_abbr, "in", 2) == 0)
		gtx_state->status = GTXInvalid;
	else if (strncmp(status_abbr, "pc", 2) == 0)
		gtx_state->status = GTXPreCommitted;
	else if (strncmp(status_abbr, "pa", 2) == 0)
		gtx_state->status = GTXPreAborted;
	else if (strncmp(status_abbr, "cm", 3) == 0)
		gtx_state->status = GTXCommitted;
	else
	{
		Assert((strncmp(status_abbr, "ab", 2) == 0));
		gtx_state->status = GTXAborted;
	}
	return 0;
}

void
GlobalTxAtExit(int code, Datum arg)
{
	if (my_locked_gtx)
		GlobalTxRelease(my_locked_gtx);
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
 * needed for commit.c proper order of cleanup actions
 */
void
GlobalTxEnsureBeforeShmemExitHook(void)
{
	if (!gtx_exit_registered)
	{
		before_shmem_exit(GlobalTxAtExit, 0);
		gtx_exit_registered = true;
	}
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
 *
 * If nowait_own_live is true, gtx is already locked, I am the coordinator and
 * gtx is not orphaned, don't wait for release -- backend is still working on
 * xact, which may be very long. *busy (if provided) is set to true in this
 * case. coordinator must be passed for this to work.
 */
GlobalTx *
GlobalTxAcquire(const char *gid, bool create, bool nowait_own_live, bool *busy,
				int coordinator)
{
	GlobalTx   *gtx = NULL;
	bool		found;

	if (!gtx_exit_registered)
	{
		before_shmem_exit(GlobalTxAtExit, 0);
		gtx_exit_registered = true;
	}

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

				gtx->acquired_by = MyBackendId;
				gtx->state.status = GTXInvalid;
				gtx->state.proposal = InitialGTxTerm;
				gtx->state.accepted = InvalidGTxTerm;
				gtx->prepared = false;
				gtx->orphaned = false;
				gtx->resolver_stage = GTRS_AwaitStatus;
				memset(gtx->phase1_acks, 0, sizeof(gtx->phase1_acks));
				memset(gtx->phase2_acks, 0, sizeof(gtx->phase2_acks));
			}
			else
			{
				gtx = NULL;
				if (busy)
					*busy = false;
			}

			break;
		}

		if (gtx->acquired_by == InvalidBackendId)
		{
			gtx->acquired_by = MyBackendId;
			break;
		}

		if (nowait_own_live)
		{
			if (coordinator == Mtm->my_node_id && !gtx->orphaned)
			{
				if (busy)
					*busy = true;
				LWLockRelease(gtx_shared->lock);
				return NULL;
			}
		}

		LWLockRelease(gtx_shared->lock);
		MtmSleep(USECS_PER_SEC / 10);
		LWLockAcquire(gtx_shared->lock, LW_EXCLUSIVE);
	}

	LWLockRelease(gtx_shared->lock);
	my_locked_gtx = gtx;
	/* not prepared and finalized gtxes are purged immediately on release */
	if (found)
	{
		Assert(gtx->prepared);
		Assert(gtx->state.status != GTXCommitted);
		Assert(gtx->state.status != GTXAborted);
	}
	return gtx;
}

/*
 * Release our lock on this transaction and remove it from hash if it is
 * finished. We also remove shmem entry if gtx is not prepared: it is used
 * for cleanup after backend/apply error before PREPARE.
 *
 */
void
GlobalTxRelease(GlobalTx *gtx)
{
	bool		found;

	Assert(gtx->acquired_by == MyBackendId);

	LWLockAcquire(gtx_shared->lock, LW_EXCLUSIVE);
	gtx->acquired_by = InvalidBackendId;

	/* status GTXInvalid can be caused by an error during PREPARE */
	if ((gtx->state.status == GTXCommitted) ||
		(gtx->state.status == GTXAborted) ||
		(!gtx->prepared))
	{
		hash_search(gtx_shared->gid2gtx, gtx->gid, HASH_REMOVE, &found);
	}
	else if (gtx->orphaned)
	{
		mtm_log(ResolverTasks, "transaction %s is orphaned", gtx->gid);
	}

	LWLockRelease(gtx_shared->lock);

	my_locked_gtx = NULL;
}

/*
 * Scan and load in gid2gtx hashtable transactions from postgres gxacts and
 * from our private table.
 *
 * Called only upon multimaster startup.
 */
void
GlobalTxLoadAll()
{
	PreparedTransaction pxacts;
	int					n_xacts;
	int					i;
	HASH_SEQ_STATUS hash_seq;
	GlobalTx   *gtx;

	LWLockAcquire(gtx_shared->lock, LW_EXCLUSIVE);

	/*
	 * This is called without shmem reset if monitor restarts.
	 * XXX is there are a better way to rm all elements of hash?
	 */
	hash_seq_init(&hash_seq, gtx_shared->gid2gtx);
	while ((gtx = hash_seq_search(&hash_seq)) != NULL)
	{
		gtx = (GlobalTx *) hash_search(gtx_shared->gid2gtx, gtx->gid,
									   HASH_REMOVE, NULL);
	}

	/* Walk over postgres gxacts */
	n_xacts = GetPreparedTransactions(&pxacts);
	for (i = 0; i < n_xacts; i++)
	{
		GlobalTx   *gtx;
		bool		found;

		gtx = (GlobalTx *) hash_search(gtx_shared->gid2gtx, pxacts[i].gid, HASH_ENTER, &found);
		Assert(!found);

		gtx->acquired_by = InvalidBackendId;
		/*
		 * Allow instance to start even if we have problems parsing xstate...
		 */
		if (deserialize_xstate(pxacts[i].state_3pc, &gtx->xinfo, &gtx->state,
							   WARNING) != 0)
		{
			hash_search(gtx_shared->gid2gtx, pxacts[i].gid, HASH_REMOVE, &found);
			continue;
		}
		gtx->prepared = true;
		gtx->orphaned = true;
		gtx->resolver_stage = GTRS_AwaitStatus;
		memset(gtx->phase1_acks, 0, sizeof(gtx->phase1_acks));
		memset(gtx->phase2_acks, 0, sizeof(gtx->phase2_acks));
	}

	LWLockRelease(gtx_shared->lock);
}


/*
 * Get maximum proposal among all transactions.
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
		if (gtx->xinfo.coordinator == node_id)
		{
			gtx->orphaned = true;
			mtm_log(ResolverTasks, "%s is orphaned", gtx->gid);
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
