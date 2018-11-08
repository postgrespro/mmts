/*----------------------------------------------------------------------------
 *
 * resolver.c
 *	  Recovery procedures to resolve transactions that were left uncommited
 *	  because of detected failure.
 *
 * Portions Copyright (c) 1996-2018, PostgreSQL Global Development Group
 * Portions Copyright (c) 1994, Regents of the University of California
 *
 *----------------------------------------------------------------------------
 */

#include "postgres.h"
#include "resolver.h"
#include "multimaster.h"
#include "logger.h"

#include "access/twophase.h"
#include "postmaster/bgworker.h"
#include "storage/latch.h"
#include "storage/ipc.h"
#include "tcop/tcopprot.h"

#include "miscadmin.h"
#include "pgstat.h"

/*
 * Definition for data in shared memory. It's not publicly visible, but used
 * to create tasks for resolver from other backends.
 */

typedef struct
{
	LWLock		   *lock;
	pid_t			pid;
} resolver_state_data;

static resolver_state_data *resolver_state;

/*
 * Map of all unresolver transactions.
 */
typedef struct
{
	char			gid[GIDSIZE];
	MtmTxState		state[MTM_MAX_NODES];
} resolver_tx;

static HTAB *gid2tx = NULL;

/* sender_id to node_id mapping */
static int		sender_to_node[MTM_MAX_NODES];

/* Auxiliary stuff for bgworker lifecycle */
static shmem_startup_hook_type PreviousShmemStartupHook;


/*****************************************************************************
 *
 * Initialization
 *
 *****************************************************************************/

static Size
resolver_shmem_size(void)
{
	Size	size = 0;

	size = add_size(size, sizeof(resolver_state_data));
	size = add_size(size, MaxBackends*sizeof(resolver_tx));
	return MAXALIGN(size);
}

static void
resolver_shmem_startup_hook()
{
	HASHCTL	hash_info;
	bool	found;

	if (PreviousShmemStartupHook)
		PreviousShmemStartupHook();

	LWLockAcquire(AddinShmemInitLock, LW_EXCLUSIVE);

	resolver_state = ShmemInitStruct("resolver", sizeof(resolver_state_data),
									 &found);

	if (!found)
	{
		resolver_state->lock = &(GetNamedLWLockTranche("resolver")->lock);
		resolver_state->pid = 0;
	}

	/* init map with current unresolved transactions */
	hash_info.keysize = GIDSIZE;
	hash_info.entrysize = sizeof(resolver_tx);
	gid2tx = ShmemInitHash("gid2tx", MaxBackends, 2*MaxBackends, &hash_info,
						   HASH_ELEM);

	LWLockRelease(AddinShmemInitLock);
}

void
ResolverInit(void)
{
	BackgroundWorker worker;

	if (!process_shared_preload_libraries_in_progress)
		return;

	/* Reserve area for our shared state */
	RequestAddinShmemSpace(resolver_shmem_size());

	RequestNamedLWLockTranche("resolver", 1);

	/* Set up common data for all our workers */
	memset(&worker, 0, sizeof(worker));
	worker.bgw_flags = BGWORKER_SHMEM_ACCESS | BGWORKER_BACKEND_DATABASE_CONNECTION;
	worker.bgw_start_time = BgWorkerStart_ConsistentState;
	worker.bgw_restart_time = 1;
	worker.bgw_notify_pid = 0;
	worker.bgw_main_arg = 0;
	sprintf(worker.bgw_library_name, "multimaster");
	sprintf(worker.bgw_function_name, "ResolverMain");
	snprintf(worker.bgw_name, BGW_MAXLEN, "mtm-resolver");
	snprintf(worker.bgw_type, BGW_MAXLEN, "mtm-resolver");
	RegisterBackgroundWorker(&worker);

	/* Register shmem hooks */
	PreviousShmemStartupHook = shmem_startup_hook;
	shmem_startup_hook = resolver_shmem_startup_hook;
}

/*****************************************************************************
 *
 * Public API
 *
 *****************************************************************************/



static bool
load_tasks(int node_id)
{
	PreparedTransaction	pxacts;
	int					n_xacts,
						added_xacts = 0,
						i;

	Assert(LWLockHeldByMeInMode(resolver_state->lock, LW_EXCLUSIVE));
	Assert(node_id == -1 || node_id > 0);

	n_xacts = GetPreparedTransactions(&pxacts);

	for (i = 0; i < n_xacts; i++)
	{
		char const *gid = pxacts[i].gid;
		int			xact_node_id;

		xact_node_id = MtmGidParseNodeId(gid);

		if (xact_node_id > 0 &&
			 (node_id == -1 || node_id == xact_node_id))
		{
			int				j;
			resolver_tx	   *tx;

			tx = (resolver_tx *) hash_search(gid2tx, gid, HASH_ENTER, NULL);
			added_xacts++;

			for (j = 0; j < Mtm->nAllNodes; j++)
				tx->state[j] = MtmTxUnknown;

			if (strcmp(pxacts[i].state_3pc, MULTIMASTER_PRECOMMITTED) == 0)
				tx->state[MtmNodeId - 1] = MtmTxPreCommited;
			else
				tx->state[MtmNodeId - 1] = MtmTxPrepared;
		}
	}

	mtm_log(ResolverTasks, "[RESOLVER] got %d transactions to resolve",
			added_xacts);

	return true;
}


void
ResolveTransactionsForNode(int node_id)
{
	pid_t resolver_pid;

	LWLockAcquire(resolver_state->lock, LW_EXCLUSIVE);
	load_tasks(node_id);
	resolver_pid = resolver_state->pid;
	LWLockRelease(resolver_state->lock);

	if (resolver_pid)
		kill(resolver_pid, SIGHUP);
}


void
ResolveAllTransactions(void)
{
	ResolveTransactionsForNode(-1);
}

char *
MtmTxStateMnem(MtmTxState state)
{
	switch (state)
	{
		case MtmTxUnknown:
			return "MtmTxUnknown";
		case MtmTxNotFound:
			return "MtmTxNotFound";
		case MtmTxInProgress:
			return "MtmTxInProgress";
		case MtmTxPrepared:
			return "MtmTxPrepared";
		case MtmTxPreCommited:
			return "MtmTxPreCommited";
		case MtmTxPreAborted:
			return "MtmTxPreAborted";
		case MtmTxCommited:
			return "MtmTxCommited";
		case MtmTxAborted:
			return "MtmTxAborted";
	}

	/* silence compiler */
	Assert(false);
	return NULL;
}

/*****************************************************************************
 *
 * Transaction recovery logic, as prescribed by 3PC recovery protocol.
 *
 *****************************************************************************/

static bool
exists(resolver_tx *tx, MtmTxStateMask mask)
{
	int i;

	for (i = 0; i < Mtm->nAllNodes; i++)
	{
		if (tx->state[i] & mask)
			return true;
	}

	return false;
}

static bool
majority_in(resolver_tx *tx, MtmTxStateMask mask)
{
	int i;
	int hits = 0;

	for (i = 0; i < Mtm->nAllNodes; i++)
	{
		if (tx->state[i] & mask)
			hits++;
	}

	return (hits > Mtm->nAllNodes/2);
}

static void
resolve_tx(const char *gid, int node_id, MtmTxState state)
{
	bool found;
	resolver_tx *tx;

	Assert(LWLockHeldByMeInMode(resolver_state->lock, LW_EXCLUSIVE));
	Assert(state != MtmTxInProgress);

	tx = hash_search(gid2tx, gid, HASH_FIND, &found);
	if (!found)
		return;

	Assert(tx->state[MtmNodeId-1] == MtmTxPrepared ||
		   tx->state[MtmNodeId-1] == MtmTxPreCommited ||
		   tx->state[MtmNodeId-1] == MtmTxPreAborted);
	Assert(node_id != MtmNodeId);

	mtm_log(ResolverTraceTxMsg,
			"[RESOLVER] tx %s (%s) got state %s from node %d",
			gid, MtmTxStateMnem(tx->state[MtmNodeId-1]), MtmTxStateMnem(state),
			node_id);

	tx->state[node_id-1] = state;

	/* XXX: missing ok because we call this concurrently with logrep recovery */

	/* XXX: set replication session to avoid sending it everywhere */

	if (exists(tx, MtmTxAborted | MtmTxNotFound))
	{
		FinishPreparedTransaction(gid, false, true);
		mtm_log(ResolverTxFinish, "TXFINISH: %s aborted", gid);
		hash_search(gid2tx, gid, HASH_REMOVE, &found);
		Assert(found);
		return;
	}

	if (exists(tx, MtmTxCommited))
	{
		FinishPreparedTransaction(gid, true, true);
		mtm_log(ResolverTxFinish, "TXFINISH: %s committed", gid);
		hash_search(gid2tx, gid, HASH_REMOVE, &found);
		Assert(found);
		return;
	}

	if (exists(tx, MtmTxPreCommited) &&
		majority_in(tx, MtmTxPrepared | MtmTxPreCommited))
	{
		// XXX: do that through PreCommit
		// SetPreparedTransactionState(gid, MULTIMASTER_PRECOMMITTED);
		// tx->state[MtmNodeId-1] = MtmTxPreCommited;
		FinishPreparedTransaction(gid, true, true);
		mtm_log(ResolverTxFinish, "TXFINISH: %s committed", gid);
		hash_search(gid2tx, gid, HASH_REMOVE, &found);
		Assert(found);
		return;
	}

	if (majority_in(tx, MtmTxPrepared | MtmTxPreAborted))
	{
		// XXX: do that through PreAbort
		// SetPreparedTransactionState(gid, MULTIMASTER_PREABORTED);
		// tx->state[MtmNodeId-1] = MtmTxPreAborted;
		FinishPreparedTransaction(gid, false, true);
		mtm_log(ResolverTxFinish, "TXFINISH: %s aborted", gid);
		hash_search(gid2tx, gid, HASH_REMOVE, &found);
		Assert(found);
		return;
	}

}

/*****************************************************************************
 *
 * Main resolver loop.
 *
 *****************************************************************************/


static void
scatter_status_requests(void)
{
	HASH_SEQ_STATUS		hash_seq;
	resolver_tx		   *tx;

	LWLockAcquire(resolver_state->lock, LW_SHARED);

	hash_seq_init(&hash_seq, gid2tx);
	while ((tx = hash_seq_search(&hash_seq)) != NULL)
	{
		int i;

		for (i = 0; i < Mtm->nAllNodes; i++)
		{
			if (i + 1 != MtmNodeId && !BIT_CHECK(SELF_CONNECTIVITY_MASK, i))
			{
				MtmArbiterMessage msg;
				DmqDestinationId dest_id;

				memset(&msg, '\0', sizeof(MtmArbiterMessage));
				MtmInitMessage(&msg, MSG_POLL_REQUEST);
				strncpy(msg.gid, tx->gid, GIDSIZE);

				dest_id = Mtm->nodes[i].destination_id;

				mtm_log(ResolverTraceTxMsg,
						"[RESOLVER] sending request for %s to node%d",
						tx->gid, i+1);

				dmq_push_buffer(dest_id, "txreq", &msg,
								sizeof(MtmArbiterMessage));
			}
		}
	}

	LWLockRelease(resolver_state->lock);

}

static void
handle_responses(void)
{
	DmqSenderId sender_id;
	StringInfoData buffer;

	while(dmq_pop_nb(&sender_id, &buffer, ~SELF_CONNECTIVITY_MASK))
	{
		int node_id;
		MtmArbiterMessage *msg;

		node_id = sender_to_node[sender_id];
		msg = (MtmArbiterMessage *) buffer.data;

		Assert(msg->node == node_id);
		Assert(msg->code == MSG_POLL_STATUS);

		LWLockAcquire(resolver_state->lock, LW_EXCLUSIVE);
		resolve_tx(msg->gid, node_id, msg->state);
		LWLockRelease(resolver_state->lock);
	}
}

void
ResolverMain(void)
{
	int			i, sender_id = 0;
	bool		send_requests = true;

	/* init this worker */
	pqsignal(SIGHUP, PostgresSigHupHandler);
	pqsignal(SIGTERM, die);
	BackgroundWorkerUnblockSignals();

	MtmBackgroundWorker = true;

	BackgroundWorkerInitializeConnection(MtmDatabaseName, NULL, 0);

	MtmWaitForExtensionCreation();

	/* subscribe to status-responses channels from other nodes */
	for (i = 0; i < Mtm->nAllNodes; i++)
	{
		if (i + 1 != MtmNodeId)
		{
			dmq_attach_receiver(psprintf("node%d", i + 1), i);
			sender_to_node[sender_id++] = i + 1;
		}
	}

	dmq_stream_subscribe("txresp");

	LWLockAcquire(resolver_state->lock, LW_EXCLUSIVE);
	resolver_state->pid = MyProcPid;
	LWLockRelease(resolver_state->lock);

	for(;;)
	{
		int		rc;

		CHECK_FOR_INTERRUPTS();

		/* Scatter requests for unresolved transactions */
		if (send_requests)
		{
			scatter_status_requests();
			send_requests = false;
		}

		/* Gather responses */
		handle_responses();

		/* Sleep untl somebody wakes us */
		rc = WaitLatch(MyLatch,
					   WL_LATCH_SET | WL_TIMEOUT | WL_POSTMASTER_DEATH,
					   3000,
					   PG_WAIT_TIMEOUT);

		/* re-try to send requests if there are some unresolved transactions */
		if (rc & WL_TIMEOUT)
			send_requests = true;

		/* Emergency bailout if postmaster has died */
		if (rc & WL_POSTMASTER_DEATH)
			proc_exit(1);

		if (rc & WL_LATCH_SET)
			ResetLatch(MyLatch);
	}

}
