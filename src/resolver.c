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

/* sender_id to node_id mapping */
static int	sender_to_node[MTM_MAX_NODES];
static bool config_valid;
static MtmConfig *mtm_cfg = NULL;

static void
handle_status(MtmConfig *mtm_cfg, MtmMessage *raw_msg);

/*****************************************************************************
 *
 * Initialization
 *
 *****************************************************************************/

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

void
ResolverWake()
{
	pid_t		resolver_pid;
	LWLockAcquire(Mtm->lock, LW_SHARED);
	resolver_pid = Mtm->resolver_pid;
	LWLockRelease(Mtm->lock);
	if (resolver_pid)
		kill(resolver_pid, SIGUSR1);
}

void
ResolveForRefereeWinner(int n_all_nodes)
{
	HASH_SEQ_STATUS hash_seq;
	GlobalTx   *gtx;
	bool		try_again = true;

	mtm_log(LOG, "ResolveForRefereeWinner");

	while (try_again)
	{
		try_again = false;

		LWLockAcquire(gtx_shared->lock, LW_EXCLUSIVE);
		hash_seq_init(&hash_seq, gtx_shared->gid2gtx);
		while ((gtx = hash_seq_search(&hash_seq)) != NULL)
		{
			GlobalTxStatus state = gtx->state.status;
			bool		found;

			if (!gtx->orphaned || gtx->acquired_by != 0)
			{
				try_again = true;
				continue;
			}

			if (state == GTXPrepared)
			{
				FinishPreparedTransaction(gtx->gid, false, true);
				mtm_log(ResolverTxFinish, "TXFINISH: %s aborted", gtx->gid);
				hash_search(gtx_shared->gid2gtx, gtx->gid, HASH_REMOVE, &found);
				Assert(found);
			}
			else if (state == GTXPreCommitted)
			{
				FinishPreparedTransaction(gtx->gid, true, true);
				mtm_log(ResolverTxFinish, "TXFINISH: %s committed", gtx->gid);
				hash_search(gtx_shared->gid2gtx, gtx->gid, HASH_REMOVE, &found);
				Assert(found);
			}
			else
			{
				Assert(false);
			}
		}
		LWLockRelease(gtx_shared->lock);

		if (try_again)
			MtmSleep(USECS_PER_SEC / 10);
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
		scatter(mtm_cfg, connected, "txreq", MtmMesagePack((MtmMessage *) &msg));

		/* .. and get all responses */
		gather(connected, (MtmMessage **) acks, &n_acks, true);

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
	LWLockAcquire(gtx_shared->lock, LW_EXCLUSIVE);
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
			scatter(mtm_cfg, connected, "txreq", MtmMesagePack((MtmMessage *) &status_msg));
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
	GlobalTx   *gtx;
	const char *gid;

	if (raw_msg->tag == T_MtmTxStatusResponse)
		gid = ((MtmTxStatusResponse *) raw_msg)->gid;
	else if (raw_msg->tag == T_MtmTxResponse)
		gid = ((MtmTxResponse *) raw_msg)->gid;
	else
		Assert(false);

	gtx = GlobalTxAcquire(gid, false);
	if (!gtx)
		return;

	if (gtx->resolver_stage == GTRS_AwaitStatus)
	{
		MtmTxStatusResponse *msg;

		Assert(raw_msg->tag == T_MtmTxStatusResponse);
		msg = (MtmTxStatusResponse *) raw_msg;

		gtx->phase1_acks[mtm_cfg->my_node_id-1] = gtx->state;
		gtx->phase1_acks[msg->node_id-1] = msg->state;

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
		else if (quorum(mtm_cfg, gtx->phase1_acks))
		{
			int			i;
			char	   *sstate;
			bool		done;
			GlobalTxStatus decision = GTXInvalid;
			GlobalTxTerm max_accepted = gtx->state.accepted;
			MtmTxRequest request_msg;
			uint64		connected;

			for (i = 0; i < MTM_MAX_NODES; i++)
			{
				if (gtx->phase1_acks[i].status == GTXInvalid)
					continue;

				if (term_cmp(gtx->phase1_acks[i].accepted, max_accepted) > 0)
					max_accepted = gtx->phase1_acks[i].accepted;
			}

			for (i = 0; i < MTM_MAX_NODES; i++)
			{
				if (gtx->phase1_acks[i].status == GTXInvalid)
					continue;

				if (term_cmp(gtx->phase1_acks[i].accepted, max_accepted) == 0)
				{
					if (gtx->phase1_acks[i].status == GTXPreCommitted)
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

			request_msg = (MtmTxRequest) {
				T_MtmTxRequest,
				decision == GTXPreCommitted ? MTReq_Precommit : MTReq_Preabort,
				gtx->state.accepted,
				gtx->gid
			};
			connected = MtmGetConnectedNodeMask() &
						~((nodemask_t) 1 << (mtm_cfg->my_node_id - 1));
			scatter(mtm_cfg, connected, "txreq", MtmMesagePack((MtmMessage *) &request_msg));
		}

	}
	else if (gtx->resolver_stage == GTRS_AwaitAcks)
	{
		MtmTxResponse *msg;

		Assert(raw_msg->tag == T_MtmTxResponse);
		msg = (MtmTxResponse *) raw_msg;
		Assert(msg->gid[0] != '\0');
		Assert(msg->status == GTXPreAborted || msg->status == GTXPreCommitted);

		gtx->phase2_acks[mtm_cfg->my_node_id-1] = gtx->state;
		gtx->phase2_acks[msg->node_id-1] = (GTxState) {
			msg->status,
			msg->term,
			msg->term
		};

		if (quorum(mtm_cfg, gtx->phase2_acks))
		{
			MtmTxRequest request_msg;
			uint64		connected;

			Assert(gtx->state.status == msg->status);
			FinishPreparedTransaction(msg->gid, msg->status == GTXPreCommitted,
									  false);
			gtx->state.status = msg->status;

			request_msg = (MtmTxRequest) {
				T_MtmTxRequest,
				msg->status == GTXPreCommitted ? MTReq_Commit : MTReq_Abort,
				(GlobalTxTerm) {0,0},
				gtx->gid
			};
			connected = MtmGetConnectedNodeMask() &
						~((nodemask_t) 1 << (mtm_cfg->my_node_id - 1));
			scatter(mtm_cfg, connected, "txreq", MtmMesagePack((MtmMessage *) &request_msg));
		}
	}
	else
		Assert(false);

	GlobalTxRelease(gtx);
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

	LWLockAcquire(Mtm->lock, LW_EXCLUSIVE);
	Mtm->resolver_pid = MyProcPid;
	LWLockRelease(Mtm->lock);
	mtm_log(ResolverTraceTxMsg, "Resolver started");

	/* We use dmq to scatter tx state changes */
	replorigin_session_origin = DoNotReplicateId;

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
