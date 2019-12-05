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
#include "replication/origin.h"
#include "storage/latch.h"
#include "storage/ipc.h"
#include "tcop/tcopprot.h"
#include "utils/syscache.h"
#include "utils/inval.h"
#include "libpq/pqformat.h"

#include "miscadmin.h"
#include "pgstat.h"
#include "state.h"
#include "commit.h"
#include "global_tx.h"
#include "messaging.h"

/*
 * Definition for data in shared memory. It's not publicly visible, but used
 * to create tasks for resolver from other backends.
 */

typedef struct
{
	// LWLock	   *lock;
	pid_t		pid;
} resolver_state_data;

static resolver_state_data *resolver_state;

// /*
//  * Map of all unresolver transactions.
//  */
// typedef struct
// {
// 	char		gid[GIDSIZE];
// 	MtmTxState	state[MTM_MAX_NODES];
// 	int			xact_node_id;
// 	int			n_participants;
// } resolver_tx;

// static HTAB *gid2tx = NULL;

/* sender_id to node_id mapping */
static int	sender_to_node[MTM_MAX_NODES];
static bool config_valid;

/* Auxiliary stuff for bgworker lifecycle */
static shmem_startup_hook_type PreviousShmemStartupHook;

static MtmConfig *mtm_cfg = NULL;

/*****************************************************************************
 *
 * Initialization
 *
 *****************************************************************************/

static Size
resolver_shmem_size(void)
{
	Size		size = 0;

	size = add_size(size, sizeof(resolver_state_data));
	// size = add_size(size, MaxBackends * sizeof(resolver_tx));
	return MAXALIGN(size);
}

static void
resolver_shmem_startup_hook()
{
	HASHCTL		hash_info;
	bool		found;

	if (PreviousShmemStartupHook)
		PreviousShmemStartupHook();

	LWLockAcquire(AddinShmemInitLock, LW_EXCLUSIVE);

	resolver_state = ShmemInitStruct("resolver", sizeof(resolver_state_data),
									 &found);

	if (!found)
	{
		// resolver_state->lock = &(GetNamedLWLockTranche("resolver")->lock);
		resolver_state->pid = 0;
	}

	// /* init map with current unresolved transactions */
	// hash_info.keysize = GIDSIZE;
	// hash_info.entrysize = sizeof(resolver_tx);
	// gid2tx = ShmemInitHash("gid2tx", MaxBackends, 2 * MaxBackends, &hash_info,
	// 					   HASH_ELEM);

	LWLockRelease(AddinShmemInitLock);
}

void
ResolverInit(void)
{
	if (!process_shared_preload_libraries_in_progress)
		return;

	/* Reserve area for our shared state */
	RequestAddinShmemSpace(resolver_shmem_size());

	RequestNamedLWLockTranche("resolver", 1);

	/* Register shmem hooks */
	PreviousShmemStartupHook = shmem_startup_hook;
	shmem_startup_hook = resolver_shmem_startup_hook;
}

BackgroundWorkerHandle *
ResolverStart(Oid db_id, Oid user_id)
{
	BackgroundWorker worker;
	BackgroundWorkerHandle *handle;

	MemSet(&worker, 0, sizeof(BackgroundWorker));
	worker.bgw_flags = BGWORKER_SHMEM_ACCESS | BGWORKER_BACKEND_DATABASE_CONNECTION;
	worker.bgw_start_time = BgWorkerStart_ConsistentState;
	worker.bgw_restart_time = BGW_NEVER_RESTART;

	memcpy(worker.bgw_extra, &db_id, sizeof(Oid));
	memcpy(worker.bgw_extra + sizeof(Oid), &user_id, sizeof(Oid));

	sprintf(worker.bgw_library_name, "multimaster");
	sprintf(worker.bgw_function_name, "ResolverMain");
	snprintf(worker.bgw_name, BGW_MAXLEN, "mtm-resolver");

	if (!RegisterDynamicBackgroundWorker(&worker, &handle))
		elog(ERROR, "Failed to start resolver worker");

	return handle;
}


/*****************************************************************************
 *
 * Public API
 *
 *****************************************************************************/



// static bool
// load_tasks(int node_id, int n_participants)
// {
// 	PreparedTransaction pxacts;
// 	int			n_xacts,
// 				added_xacts = 0,
// 				i;

// 	Assert(LWLockHeldByMeInMode(resolver_state->lock, LW_EXCLUSIVE));
// 	Assert(node_id == -1 || node_id > 0);

// 	n_xacts = GetPreparedTransactions(&pxacts);

// 	for (i = 0; i < n_xacts; i++)
// 	{
// 		char const *gid = pxacts[i].gid;
// 		int			xact_node_id;

// 		xact_node_id = MtmGidParseNodeId(gid);

// 		/* user-generated 2pc || resolve all || resolve only our txes */
// 		if (xact_node_id < 0 || node_id == -1 || node_id == xact_node_id)
// 		{
// 			int			j;
// 			resolver_tx *tx;

// 			tx = (resolver_tx *) hash_search(gid2tx, gid, HASH_ENTER, NULL);
// 			added_xacts++;

// 			tx->n_participants = n_participants;
// 			tx->xact_node_id = xact_node_id;

// 			for (j = 0; j < MTM_MAX_NODES; j++)
// 				tx->state[j] = MtmTxUnknown;

// 			if (strcmp(pxacts[i].state_3pc, MULTIMASTER_PRECOMMITTED) == 0)
// 				tx->state[Mtm->my_node_id - 1] = MtmTxPreCommited;
// 			else
// 				tx->state[Mtm->my_node_id - 1] = MtmTxPrepared;
// 		}
// 	}

// 	mtm_log(ResolverTasks, "[RESOLVER] got %d transactions to resolve",
// 			added_xacts);

// 	return true;
// }

void
ResolverWake()
{
	pid_t		resolver_pid;
	LWLockAcquire(resolver_state->lock, LW_SHARED);
	resolver_pid = resolver_state->pid;
	LWLockRelease(resolver_state->lock);
	if (resolver_pid)
		kill(resolver_pid, SIGUSR1);
}

void
ResolveTransactionsForNode(int node_id, int n_all_nodes)
{
	pid_t		resolver_pid;

	LWLockAcquire(resolver_state->lock, LW_EXCLUSIVE);
	load_tasks(node_id, n_all_nodes);
	resolver_pid = resolver_state->pid;
	LWLockRelease(resolver_state->lock);

	if (resolver_pid)
		kill(resolver_pid, SIGHUP);
}

void
ResolveAllTransactions(int n_all_nodes)
{
	ResolveTransactionsForNode(-1, n_all_nodes);
}

void
ResolveForRefereeWinner(int n_all_nodes)
{
	HASH_SEQ_STATUS hash_seq;
	resolver_tx *tx;

	mtm_log(LOG, "ResolveForRefereeWinner");

	LWLockAcquire(resolver_state->lock, LW_EXCLUSIVE);
	load_tasks(-1, n_all_nodes);

	hash_seq_init(&hash_seq, gid2tx);
	while ((tx = hash_seq_search(&hash_seq)) != NULL)
	{
		MtmTxState	state = tx->state[Mtm->my_node_id - 1];
		bool		found;

		if (state == MtmTxPrepared)
		{
			FinishPreparedTransaction(tx->gid, false, true);
			mtm_log(ResolverTxFinish, "TXFINISH: %s aborted", tx->gid);
			hash_search(gid2tx, tx->gid, HASH_REMOVE, &found);
			Assert(found);
		}
		else if (state == MtmTxPreCommited)
		{
			FinishPreparedTransaction(tx->gid, true, true);
			mtm_log(ResolverTxFinish, "TXFINISH: %s committed", tx->gid);
			hash_search(gid2tx, tx->gid, HASH_REMOVE, &found);
			Assert(found);
		}
		else
		{
			Assert(false);
		}
	}

	LWLockRelease(resolver_state->lock);
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
	int			i;

	for (i = 0; i < MTM_MAX_NODES; i++)
	{
		if (tx->state[i] & mask)
			return true;
	}

	return false;
}

static bool
majority_in(resolver_tx *tx, MtmTxStateMask mask)
{
	int			i;
	int			hits = 0;

	for (i = 0; i < MTM_MAX_NODES; i++)
	{
		if (tx->state[i] & mask)
			hits++;
	}

	return (hits > tx->n_participants / 2);
}


/*
 * resolve_tx
 *
 * This handles respenses with tx status and makes decision based on
 * 3PC resolving protocol.
 *
 * Here we can get an error when trying to commit transaction that
 * is already during commit by receiver (see gxact->locking_backend).
 * We can catch those, but better just let it happend, restart bgworker
 * and continue resolving.
 */
static void
resolve_tx(const char *gid, int node_id, MtmTxState state)
{
	bool		found;
	resolver_tx *tx;

	Assert(IsTransactionState());
	Assert(LWLockHeldByMeInMode(resolver_state->lock, LW_EXCLUSIVE));
	Assert(state != MtmTxInProgress);

	tx = hash_search(gid2tx, gid, HASH_FIND, &found);
	if (!found)
		return;

	Assert(tx->state[Mtm->my_node_id - 1] == MtmTxPrepared ||
		   tx->state[Mtm->my_node_id - 1] == MtmTxPreCommited ||
		   tx->state[Mtm->my_node_id - 1] == MtmTxPreAborted);
	Assert(node_id != Mtm->my_node_id);

	mtm_log(ResolverTraceTxMsg,
			"[RESOLVER] tx %s (%s) got state %s from node %d",
			gid, MtmTxStateMnem(tx->state[Mtm->my_node_id - 1]), MtmTxStateMnem(state),
			node_id);

	tx->state[node_id - 1] = state;

	/* XXX: missing ok because we call this concurrently with logrep recovery */

	/*
	 * Set origin replication session, so we don't send this abort to all
	 * peers.
	 *
	 * Otherwise we can scatter our abort to a different node (say node_A)
	 * before it actually recevies prepare from a node expiriencing failure
	 * (say node_B). If then failed node become online and also receives our
	 * abort before aborting tx itself, node_A will finally receive prepare,
	 * but won't receive abort from node_B since it was originated on other
	 * node. So this prepare on node_A will stuck indefinitely.
	 */
	Assert(replorigin_session_origin == InvalidRepOriginId);
	if (tx->xact_node_id != Mtm->my_node_id)
	{
		replorigin_session_origin = MtmNodeById(mtm_cfg, tx->xact_node_id)->origin_id;
		Assert(replorigin_session_origin != InvalidRepOriginId);
		replorigin_session_setup(replorigin_session_origin);
	}

	if (exists(tx, MtmTxAborted | MtmTxNotFound))
	{
		FinishPreparedTransaction(gid, false, true);
		mtm_log(ResolverTxFinish, "TXFINISH: %s aborted", gid);
		hash_search(gid2tx, gid, HASH_REMOVE, &found);
		Assert(found);
	}
	else if (exists(tx, MtmTxCommited))
	{
		FinishPreparedTransaction(gid, true, true);
		mtm_log(ResolverTxFinish, "TXFINISH: %s committed", gid);
		hash_search(gid2tx, gid, HASH_REMOVE, &found);
		Assert(found);
	}
	else if (exists(tx, MtmTxPreCommited) &&
			 majority_in(tx, MtmTxPrepared | MtmTxPreCommited))
	{
		/* XXX: do that through PreCommit */
		/* SetPreparedTransactionState(gid, MULTIMASTER_PRECOMMITTED); */
		/* tx->state[MtmNodeId-1] = MtmTxPreCommited; */
		FinishPreparedTransaction(gid, true, true);
		mtm_log(ResolverTxFinish, "TXFINISH: %s committed", gid);
		hash_search(gid2tx, gid, HASH_REMOVE, &found);
		Assert(found);
	}
	else if (majority_in(tx, MtmTxPrepared | MtmTxPreAborted))
	{
		/* XXX: do that through PreAbort */
		/* SetPreparedTransactionState(gid, MULTIMASTER_PREABORTED); */
		/* tx->state[MtmNodeId-1] = MtmTxPreAborted; */
		FinishPreparedTransaction(gid, false, true);
		mtm_log(ResolverTxFinish, "TXFINISH: %s aborted", gid);
		hash_search(gid2tx, gid, HASH_REMOVE, &found);
		Assert(found);
	}

	if (replorigin_session_origin != InvalidRepOriginId)
	{
		replorigin_session_origin = InvalidRepOriginId;
		replorigin_session_reset();
	}
}

/*****************************************************************************
 *
 * Main resolver loop.
 *
 *****************************************************************************/

static void
scatter(MtmConfig *mtm_cfg, nodemask_t cmask, char *stream_name, StringInfo msg)
{
	int			i;

	for (i = 0; i < mtm_cfg->n_nodes; i++)
	{
		int			node_id = mtm_cfg->nodes[i].node_id;
		DmqDestinationId dest_id;

		LWLockAcquire(Mtm->lock, LW_SHARED);
		dest_id = Mtm->peers[node_id - 1].dmq_dest_id;
		LWLockRelease(Mtm->lock);
		Assert(dest_id >= 0);

		if (BIT_CHECK(cmask, node_id - 1))
			dmq_push_buffer(dest_id, stream_name, msg->data, msg->len);
	}
}

static void
scatter_status_requests(MtmConfig *mtm_cfg)
{
	HASH_SEQ_STATUS hash_seq;
	GlobalTx   *gtx;
	bool		have_orphaned = false;
	GlobalTxTerm new_term;

	StartTransactionCommand();

	/* Is there any orphaned transactions? */
	LWLockAcquire(gtx_shared->lock, LW_SHARED);
	hash_seq_init(&hash_seq, gtx_shared->gid2gtx);
	while ((gtx = hash_seq_search(&hash_seq)) != NULL)
	{
		if (gtx->orphaned)
			have_orphaned = true;
	}
	LWLockRelease(gtx_shared->lock);

	/* Just rest if there no transactions to resolve */
	if (!have_orphaned)
		return;

	/* Generate next term */
	{
		MtmMessage	msg = {T_MtmLastTermRequest};
		uint64		connected;
		MtmLastTermResponse *acks[MTM_MAX_NODES];
		int			n_acks;
		int			i;

		/* local max proposal */
		new_term = GlobalTxGetMaxProposal();

		/* ask peers about their last term */
		connected = MtmGetConnectedNodeMask() &
						~((nodemask_t) 1 << (mtm_cfg->my_node_id - 1));
		scatter(mtm_cfg, connected, "txreq", MtmMesagePack(&msg));

		/* .. and get all responses */
		gather(connected, acks, &n_acks, true);

		for (i = 0; i < n_acks; i++)
		{
			if (term_cmp(new_term, gtx->state.proposal) < 0)
				new_term = gtx->state.proposal;
		}

		/* And generate next term */
		new_term.ballot += 1;
		new_term.node_id = mtm_cfg->my_node_id;
	}

	/*
	 * Stamp all orphaned transactions with a new proposal and send status
	 * requests.
	 */
	LWLockAcquire(gtx_shared->lock, LW_SHARED);
	hash_seq_init(&hash_seq, gtx_shared->gid2gtx);
	while ((gtx = hash_seq_search(&hash_seq)) != NULL)
	{
		/* skip acquired until next round */
		if (gtx->orphaned && gtx->acquired_by == 0)
		{
			uint64		connected;
			MtmTxRequest status_msg = {
				T_MtmTxRequest,
				MTReq_Status,
				new_term,
				gtx->gid
			};

			SetPreparedTransactionState(gtx->gid,
				serialize_gtx_state(
					gtx->state.status,
					new_term,
					gtx->state.accepted),
				false);
			gtx->state.proposal = new_term;

			connected = MtmGetConnectedNodeMask() &
						~((nodemask_t) 1 << (mtm_cfg->my_node_id - 1));
			scatter(mtm_cfg, connected, "txreq",  MtmMesagePack(&status_msg));
		}
	}
	LWLockRelease(gtx_shared->lock);

	CommitTransactionCommand();
}

static void
handle_responses(MtmConfig *mtm_cfg)
{
	DmqSenderId sender_id;
	StringInfoData msg;
	bool		wait;

	while (dmq_pop_nb(&sender_id, &msg, MtmGetConnectedNodeMask(), &wait))
	{
		MtmMessage *raw_msg;

		StartTransactionCommand();

		raw_msg = MtmMesageUnpack(&msg);
		Assert(raw_msg->tag == T_MtmTxStatusResponse ||
			   raw_msg->tag == T_MtmTxResponse);
		handle_status(mtm_cfg, raw_msg);

		CommitTransactionCommand();
	}
}

static bool
quorum(MtmConfig *mtm_cfg, GTxState * all_states)
{
	int i, n_states = 0;
	GTxState my_state = all_states[mtm_cfg->my_node_id - 1];

	for (i = 0; i < MTM_MAX_NODES; i++)
	{
		if (my_state.status == GTXInvalid)
			continue;

		if (term_cmp(my_state.proposal, all_states[i].proposal) == 0)
			n_states++;
	}

	return n_states >= mtm_cfg->n_nodes/2 + 1;
}

static void
handle_status(MtmConfig *mtm_cfg, MtmMessage *raw_msg)
{
	GlobalTx *gtx;

	if (gtx->resolver_stage == GTRS_AwaitStatus)
	{
		MtmTxStatusResponse *msg;

		Assert(raw_msg->tag == T_MtmTxStatusResponse);
		msg = (MtmTxStatusResponse *) raw_msg;

		gtx = GlobalTxAcquire(msg->gid, false);
		if (!gtx)
			return;

		gtx->remote_states[mtm_cfg->my_node_id-1] = gtx->state;
		gtx->remote_states[msg->node_id-1] = msg->state;

		if (msg->state.status == GTXCommitted)
		{
			gtx->state.status = GTXCommitted;
			FinishPreparedTransaction(gtx->gid, true, false);
		}
		else if (msg->state.status == GTXAborted)
		{
			gtx->state.status = GTXAborted;
			FinishPreparedTransaction(gtx->gid, false, false);
		}
		else if (quorum(mtm_cfg, gtx->remote_states))
		{
			int			i;
			char	   *sstate;
			bool		done;
			GlobalTxStatus decision = GTXInvalid;
			GlobalTxTerm max_accepted = gtx->state.accepted;

			for (i = 0; i < MTM_MAX_NODES; i++)
			{
				if (gtx->remote_states[i].status == GTXInvalid)
					continue;

				if (term_cmp(gtx->remote_states[i].accepted, max_accepted) > 0)
					max_accepted = gtx->remote_states[i].accepted;
			}

			for (i = 0; i < MTM_MAX_NODES; i++)
			{
				if (gtx->remote_states[i].status == GTXInvalid)
					continue;

				if (term_cmp(gtx->remote_states[i].accepted, max_accepted) == 0)
				{
					if (gtx->remote_states[i].status == GTXPreCommitted)
					{
						Assert(decision != GTXPreAborted);
						decision = GTXPreCommitted;
					}
					else
					{
						Assert(decision != GTXPreCommitted);
						decision = GTXPreAborted;
					}
				}
			}

			Assert(decision != GTXInvalid);

			sstate = serialize_gtx_state(decision, gtx->state.proposal,
										 gtx->state.proposal);
			done = SetPreparedTransactionState(gtx->gid, sstate, false);
			gtx->state.status = decision;
			gtx->state.accepted = gtx->state.proposal;
			gtx->resolver_stage = GTRS_AwaitAcks;
		}

		GlobalTxRelease(gtx);
	}
	else if (gtx->resolver_stage == GTRS_AwaitAcks)
	{
		MtmTxResponse *msg;

		Assert(raw_msg->tag == T_MtmTxResponse);
		msg = (MtmTxResponse *) raw_msg;
		Assert(msg->gid[0] != '\0');

		gtx = GlobalTxAcquire(msg->gid, false);
		if (!gtx)
			return;


		gtx->resolver_acks[mtm_cfg->my_node_id-1] = true;
		gtx->resolver_acks[msg->node_id-1] = true;


		GlobalTxRelease(gtx);
	}
	else
		Assert(false);

	
}

static void
subscription_change_cb(Datum arg, int cacheid, uint32 hashvalue)
{
	config_valid = false;
}

static void
attach_node(int node_id, MtmConfig *new_cfg, Datum arg)
{
	int			sender_id = dmq_attach_receiver(psprintf(MTM_DMQNAME_FMT, node_id),
												node_id - 1);

	sender_to_node[sender_id] = node_id;
}

static void
detach_node(int node_id, MtmConfig *new_cfg, Datum arg)
{
	/* detach incoming queues from this node */
	dmq_detach_receiver(psprintf(MTM_DMQNAME_FMT, node_id));
}

void
ResolverMain(Datum main_arg)
{
	bool		send_requests = true;
	Oid			db_id,
				user_id;

	/* init this worker */
	pqsignal(SIGHUP, PostgresSigHupHandler);
	pqsignal(SIGTERM, die);
	BackgroundWorkerUnblockSignals();

	MtmBackgroundWorker = true;

	memcpy(&db_id, MyBgworkerEntry->bgw_extra, sizeof(Oid));
	memcpy(&user_id, MyBgworkerEntry->bgw_extra + sizeof(Oid), sizeof(Oid));

	/* Connect to a database */
	BackgroundWorkerInitializeConnectionByOid(db_id, user_id, 0);

	/* Keep us informed about subscription changes. */
	CacheRegisterSyscacheCallback(SUBSCRIPTIONOID,
								  subscription_change_cb,
								  (Datum) 0);

	dmq_stream_subscribe("txresp");

	LWLockAcquire(resolver_state->lock, LW_EXCLUSIVE);
	resolver_state->pid = MyProcPid;
	LWLockRelease(resolver_state->lock);
	mtm_log(ResolverTraceTxMsg, "Resolver started");

	for (;;)
	{
		int			rc;

		CHECK_FOR_INTERRUPTS();

		/* XXX: add tx start/commit to free memory? */

		AcceptInvalidationMessages();
		if (!config_valid)
		{
			mtm_cfg = MtmReloadConfig(mtm_cfg, attach_node, detach_node, (Datum) NULL);

			if (mtm_cfg->my_node_id == 0)
				proc_exit(0);

			config_valid = true;
		}
		Assert(mtm_cfg);

		/* Scatter requests for unresolved transactions */
		if (send_requests)
		{
			scatter_status_requests(mtm_cfg);
			send_requests = false;
		}

		/* Gather responses */
		handle_responses(mtm_cfg);

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
