/*----------------------------------------------------------------------------
 *
 * global_tx.c
 *	  Persistent and in-memory state necessary for our E3PC-like atommic commit
 #	  protocol.
 *
 * Copyright (c) 2016-2019, Postgres Professional
 *
 *----------------------------------------------------------------------------
 */

#include "postgres.h"

#include "access/twophase.h"
#include "catalog/pg_authid.h"
#include "catalog/pg_type.h"
#include "executor/spi.h"
#include "utils/snapmgr.h"
#include "utils/hsearch.h"
#include "miscadmin.h"

#include "multimaster.h"
#include "logger.h"

typedef struct
{
	int		ballot;
	int		node_id;
} GlobalTxTerm;

typedef enum
{
	GTXInvalid = 0,
	GTXPrepared,
	GTXPreCommitted,
	GTXPreAborted,
	GTXCommitted,
	GTXAborted
} GlobalTxStatus;

typedef struct
{
	GlobalTxStatus	status;
	GlobalTxTerm	proposal;
	GlobalTxTerm	accepted;
} GTxState;

typedef struct 
{
	char		gid[GIDSIZE];
	int			acquired_by;
	GTxState	state;
	bool		orphaned;	/* Indication for resolver that current tx needs
							 * to be picked up. Comes from a failed backend or
							 * a disabled node. */
	GTxState	remote_states[MTM_MAX_NODES];
	bool		resolver_acks[MTM_MAX_NODES];
	bool		in_table;	/* True when gtx state was written in proposals
							 * table because we received status request before
							 * it was prepared on our node. */
} GlobalTx;

/*
 * Definitions for the "mtm.gtx_proposals" table.
 */
#define MTM_GTX_PROPOSALS				"mtm.gtx_proposals"
#define Natts_mtm_gtx_proposals			2
#define Anum_mtm_gtx_proposals_gid		1	/* node_id, same accross cluster */
#define Anum_mtm_gtx_proposals_state	2	/* connection string */

typedef struct
{
	LWLock	   *lock;
} gtx_shared_data;

static gtx_shared_data *gtx_shared;
static HTAB *gid2gtx;
static GlobalTx *my_locked_gtx;
static bool gtx_exit_registered;

static char *
serialize_gtx_state(GlobalTx *gtx)
{
	char	   *state;

	Assert(gtx->state.status == GTXPreCommitted ||
		   gtx->state.status == GTXPreAborted);
	state = psprintf("%s-%d:%d-%d:%d",
					 gtx->state.status == GTXPreCommitted ? "pc" : "pa",
					 gtx->state.proposal.ballot, gtx->state.proposal.node_id,
					 gtx->state.accepted.ballot, gtx->state.accepted.node_id);
	return state;
}

static void
parse_gtx_state(const char *state, GlobalTx *gtx)
{
	int			n_parsed = 0;

	Assert(gtx->gid[0] != '\0');

	if (state[0] != '\0')
	{
		gtx->state.status = GTXInvalid;
		if (strncmp(state, "pc-", 3) == 0)
			gtx->state.status = GTXPreCommitted;
		else if (strncmp(state, "pa-", 3) == 0)
			gtx->state.status = GTXPreAborted;

		n_parsed = sscanf(state + 3, "%d:%d-%d:%d",
						&gtx->state.proposal.ballot,
						&gtx->state.proposal.node_id,
						&gtx->state.accepted.ballot,
						&gtx->state.accepted.node_id);

		if (gtx->state.status == GTXInvalid || n_parsed != 4)
			mtm_log(ERROR, "Failed to parse 3pc state '%s' for gid '%s'",
					state, gtx->gid);
	}
	else
	{
		gtx->state.status = GTXPrepared;
		gtx->state.proposal = (GlobalTxTerm) {1, 0};
		gtx->state.accepted = (GlobalTxTerm) {0, 0};
	}
}

void
global_tx_at_abort()
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

	gid2gtx = ShmemInitHash("gid2gtx", 2*MaxConnections, 2*MaxConnections,
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
GlobalTxAcquire(const char *gid, bool create)
{
	GlobalTx   *gtx = NULL;
	bool		found;

	if (!gtx_exit_registered)
	{
		before_shmem_exit(global_tx_at_abort, 0);
		gtx_exit_registered = true;
	}

	LWLockAcquire(gtx_shared->lock, LW_EXCLUSIVE);

	/* Repeat attempts to acquire a global tx */
	while (true)
	{
		gtx = (GlobalTx *) hash_search(gid2gtx, gid, HASH_FIND, &found);

		if (!found)
		{
			if (create)
			{
				gtx = (GlobalTx *) hash_search(gid2gtx, gid, HASH_ENTER, &found);

				gtx->acquired_by = MyBackendId;
				gtx->state.status = GTXInvalid;
				gtx->state.proposal = (GlobalTxTerm) {1, 0};
				gtx->state.accepted = (GlobalTxTerm) {0, 0};
				gtx->orphaned = false;
				memset(gtx->remote_states, 0, sizeof(gtx->remote_states));
				memset(gtx->resolver_acks, 0, sizeof(gtx->resolver_acks));
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
	return gtx;
}

/*
 * Release our lock on this transaction and remove it from hash if it is in the
 * decided state.
 */
void
GlobalTxRelease(const char *gid)
{
	GlobalTx   *gtx;
	bool		found;

	LWLockAcquire(gtx_shared->lock, LW_EXCLUSIVE);

	gtx = (GlobalTx *) hash_search(gid2gtx, gid, HASH_FIND, &found);
	Assert(gtx->acquired_by == MyBackendId);
	gtx->acquired_by = InvalidBackendId;

	if (gtx->state.status == GTXCommitted || gtx->state.status == GTXAborted)
		hash_search(gid2gtx, gid, HASH_REMOVE, &found);

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

		gtx = (GlobalTx *) hash_search(gid2gtx, pxacts[i].gid, HASH_ENTER, &found);
		Assert(!found);

		gtx->orphaned = true;
		gtx->acquired_by = InvalidBackendId;
		gtx->in_table = false;
		memset(gtx->remote_states, 0, sizeof(gtx->remote_states));
		memset(gtx->resolver_acks, 0, sizeof(gtx->resolver_acks));
		parse_gtx_state(pxacts[i].state_3pc, gtx);
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
		bool		isnull;
		char	   *gid;
		char	   *state;
		bool		found;
		GlobalTx   *gtx;

		gid = SPI_getvalue(tup, tupdesc, Anum_mtm_gtx_proposals_gid);
		Assert(TupleDescAttr(tupdesc, Anum_mtm_gtx_proposals_gid - 1)->atttypid == TEXTOID);

		state = SPI_getvalue(tup, tupdesc, Anum_mtm_gtx_proposals_state);
		Assert(TupleDescAttr(tupdesc, Anum_mtm_gtx_proposals_state - 1)->atttypid == TEXTOID);

		gtx = (GlobalTx *) hash_search(gid2gtx, gid, HASH_ENTER, &found);
		Assert(!found);

		gtx->orphaned = true;
		gtx->acquired_by = InvalidBackendId;
		gtx->in_table = true;
		memset(gtx->remote_states, 0, sizeof(gtx->remote_states));
		memset(gtx->resolver_acks, 0, sizeof(gtx->resolver_acks));
		parse_gtx_state(state, gtx);
	}

	if (SPI_finish() != SPI_OK_FINISH)
		mtm_log(ERROR, "could not finish SPI connection");

	PopActiveSnapshot();

	LWLockRelease(gtx_shared->lock);
}

static void
delete_table_proposal(const char *gid)
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
}

