/*----------------------------------------------------------------------------
 *
 * resolver.c
 *	  Recovery procedures to resolve transactions that were left uncommited
 *	  because of detected failure.
 *
 * Copyright (c) 2015-2019, Postgres Professional
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

static bool config_valid;
static MtmConfig *mtm_cfg = NULL;

static void handle_response(MtmConfig *mtm_cfg, MtmMessage *raw_msg);

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

			if (state == GTXInvalid)
			{
				FinishPreparedTransaction(gtx->gid, false, true);
				mtm_log(ResolverTx, "TXFINISH: %s aborted", gtx->gid);
				hash_search(gtx_shared->gid2gtx, gtx->gid, HASH_REMOVE, &found);
				Assert(found);
			}
			else if (state == GTXPreCommitted)
			{
				FinishPreparedTransaction(gtx->gid, true, true);
				mtm_log(ResolverTx, "TXFINISH: %s committed", gtx->gid);
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

typedef struct verdict
{
	char gid[GIDSIZE];
	bool commit;
} verdict;

/* gid_t is system type... */
typedef char pgid_t[GIDSIZE];

/*
 * Called periodically. Iterate over gtxes and
 * - finish xact immediately if we can (e.g. it is our xact which never got
 *   precommitted with backend gone dead)
 * - determine whether we still need to actually resolve something,
 *   returns true, if so
 * - remove gtx_proposals entry if we have both the entry and PREPARE record.
 *   This should be incredibly rare situation where applier wrote PREPARE but
 *   failed to cleaup the entry immeidately after; see PREPARE handling for
 *   the details.
 */
static bool
finish_ready(void)
{
	bool job_pending = false;
	HASH_SEQ_STATUS hash_seq;
	GlobalTx   *gtx;
	/*
	 * Calling FinishPreparedTransaction under lwlock is probably not a good
	 * idea, so let's collect xacts here and finish them after release.
	 */
	verdict *ready_xacts = palloc(sizeof(verdict) * max_prepared_xacts);
	int n_ready_xacts = 0;
	/* gids for which we must remove gtx_proposals entry */
	pgid_t *in_table_rm_pending = palloc(sizeof(pgid_t) * max_prepared_xacts);
	int n_in_table_rm_pending = 0;
	int i;
	int coordinator;

	LWLockAcquire(gtx_shared->lock, LW_SHARED);
	hash_seq_init(&hash_seq, gtx_shared->gid2gtx);
	while ((gtx = hash_seq_search(&hash_seq)) != NULL)
	{
		if (gtx->prepared && gtx->in_table)
		{
			Assert(n_in_table_rm_pending < max_prepared_xacts);
			strcpy(in_table_rm_pending[n_in_table_rm_pending], gtx->gid);
			n_in_table_rm_pending++;
			continue;
		}

		/* don't intervene if backend is still working on xact */
		if (!gtx->orphaned)
			continue;

		coordinator = MtmGidParseNodeId(gtx->gid);
		/*
		 * Actually, currently gtx can't live long in GTXCommitted or
		 * GTXAborted state; it is set only after finishing prepare, which
		 * makes GlobalTxRelease to rm shmem entry. However, this is easy and
		 * might be useful if we ever start recording AP in gtx_proposals for
		 * performance reasons as GlobalTxRelease explains.
		 */
		if (gtx->state.status == GTXCommitted ||
			gtx->state.status == GTXAborted ||
			/*
			 * Directly aborting own xacts which never got precommitted is not
			 * required (we could resolve them as usual as well), but this is
			 * a useful performance optimization as scanning WAL during
			 * resolution long. This is definitely safe as backend has already
			 * orphaned xact, and since it wasn't precommitted on the
			 * coordinator it can't be precommitted anywhere -- any resolution
			 * attempt will result in abort. However, note that once xact got
			 * precommitted this is no longer true and direct abort even of my
			 * own xacts is not safe.
			 */
			(gtx->prepared &&
			 coordinator == Mtm->my_node_id &&
			 gtx->state.status == GTXInvalid))
		{
			Assert(n_ready_xacts < max_prepared_xacts);
			strcpy(ready_xacts[n_ready_xacts].gid, gtx->gid);
			if (gtx->state.status == GTXCommitted)
				ready_xacts[n_ready_xacts].commit = true;
			else
				ready_xacts[n_ready_xacts].commit = false;
			n_ready_xacts++;
			if (gtx->state.status == GTXInvalid)
				mtm_log(MtmTxTrace, "my orphaned transaction %s will be directly aborted as it was never was precommitted", gtx->gid);
			continue;
		}

		/* so we have orphaned xact needing resolution */
		job_pending = true;
	}
	LWLockRelease(gtx_shared->lock);

	/* finish ready xacts */
	for (i = 0; i < n_ready_xacts; i++)
	{
		gtx = GlobalTxAcquire(ready_xacts[i].gid, false, NULL);
		if (!gtx)
			continue;

		StartTransactionCommand();
		FinishPreparedTransaction(gtx->gid, ready_xacts[i].commit, false);
		CommitTransactionCommand();
		gtx->state.status = ready_xacts[i].commit ? GTXCommitted : GTXAborted;
		mtm_log(MtmTxFinish, "TXFINISH: %s %s directly",
				gtx->gid, ready_xacts[i].commit ? "committed" : "aborted");
		GlobalTxRelease(gtx);
	}

	/* clear obsolete gtx_proposals entries */
	for (i = 0; i < n_in_table_rm_pending; i++)
	{
		gtx = GlobalTxAcquire(in_table_rm_pending[i], false, NULL);
		if (!gtx)
			continue;

		GlobalTxDeleteFromTable(gtx->gid);
		gtx->in_table = false;
		GlobalTxRelease(gtx);
	}

	pfree(ready_xacts);
	pfree(in_table_rm_pending);
	return job_pending;
}

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
		/*
		 * XXX ars: config could change after last MtmReloadConfig, this might be
		 * false if node was removed.
		 */
		Assert(dest_id >= 0);

		if (BIT_CHECK(cmask, node_id - 1))
			dmq_push_buffer(dest_id, stream_name, msg->data, msg->len);
	}
}

static bool
last_term_gather_hook(MtmMessage *msg, Datum arg)
{
	return msg->tag == T_MtmLastTermResponse;
}

static void
scatter_status_requests(MtmConfig *mtm_cfg)
{
	HASH_SEQ_STATUS hash_seq;
	GlobalTx   *gtx;
	GlobalTxTerm new_term;

	mtm_log(ResolverState, "orphaned transactions detected");

	/*
	 * Generate next term.
	 * Picking at least max local proposal term + 1 guarantees we never try
	 * the same term twice. A round of collecting others' max terms ought
	 * to help to conduct it successfully.
	 */
	{
		MtmMessage	msg = {T_MtmLastTermRequest};
		uint64		connected;
		MtmLastTermResponse *acks[MTM_MAX_NODES];
		int			n_acks;
		int			i;
		int		   sconn_cnt[DMQ_N_MASK_POS];

		/* local max proposal */
		new_term = GlobalTxGetMaxProposal();

		/* ask peers about their last term */
		connected = MtmGetConnectedMask(false);
		dmq_get_sendconn_cnt(connected, sconn_cnt);
		scatter(mtm_cfg, connected, "txreq", MtmMessagePack((MtmMessage *) &msg));

		/* .. and get all responses */
		gather(connected,
			   (MtmMessage **) acks, NULL, &n_acks,
			   last_term_gather_hook, 0,
			   sconn_cnt, MtmInvalidGenNum);

		for (i = 0; i < n_acks; i++)
		{
			Assert(acks[i]->tag == T_MtmLastTermResponse);
			if (term_cmp(new_term, acks[i]->term) < 0)
				new_term = acks[i]->term;
		}

		/* And generate next term */
		new_term.ballot += 1;
		new_term.node_id = mtm_cfg->my_node_id;
	}

	mtm_log(ResolverState, "New term is (%d,%d)", new_term.ballot, new_term.node_id);

	/*
	 * Stamp all orphaned transactions with a new proposal and send status
	 * requests.
	 */
	LWLockAcquire(gtx_shared->lock, LW_EXCLUSIVE);
	hash_seq_init(&hash_seq, gtx_shared->gid2gtx);
	while ((gtx = hash_seq_search(&hash_seq)) != NULL)
	{
		/* skip acquired until next round */
		if (gtx->orphaned && gtx->acquired_by == InvalidBackendId &&
			/* not much sense to resove xact if we don't have PREPARE */
			gtx->prepared &&
			!gtx->in_table &&
			gtx->state.status != GTXCommitted && gtx->state.status != GTXAborted &&
			/*
			 * monitor could already vote in the short gap after
			 * GlobalTxGetMaxProposal
			 */
			term_cmp(new_term, gtx->state.proposal) > 0)
		{
			uint64		connected;
			MtmTxRequest status_msg = {
				T_MtmTxRequest,
				MTReq_Status,
				new_term,
				gtx->gid,
				gtx->coordinator_end_lsn
			};

			SetPreparedTransactionState(gtx->gid,
				serialize_gtx_state(
					gtx->state.status,
					new_term,
					gtx->state.accepted),
				false);
			gtx->state.proposal = new_term;
			mtm_log(ResolverState, "proposal term (%d, %d) stamped to transaction %s",
					new_term.ballot, new_term.node_id, gtx->gid);
			/*
			 * We should set GTRS_AwaitStatus here, otherwise if one
			 * attempt to to resolve failed in GTRS_AwaitAcks, we would
			 * hang forever in it.
			 */
			gtx->resolver_stage = GTRS_AwaitStatus;

			connected = MtmGetConnectedMask(false);
			scatter(mtm_cfg, connected, "txreq", MtmMessagePack((MtmMessage *) &status_msg));
		}
	}
	LWLockRelease(gtx_shared->lock);
}

static void
handle_responses(MtmConfig *mtm_cfg)
{
	int8 sender_mask_pos;
	StringInfoData msg;
	bool		wait;

	/* ars: if we got failure here, not WOULDBLOCK, better continue spinning */
	while (dmq_pop_nb(&sender_mask_pos, &msg, MtmGetConnectedMask(false), &wait))
	{
		MtmMessage *raw_msg;

		/*
		 * SetPreparedTransactionState as well as FinishPreparedTransaction
		 * requires live xact. Yeah, you can't do many things in PG unless you
		 * have one.
		 */
		StartTransactionCommand();

		raw_msg = MtmMessageUnpack(&msg);
		if (raw_msg->tag == T_MtmTxStatusResponse ||
			raw_msg->tag == T_Mtm2AResponse)
		{
			handle_response(mtm_cfg, raw_msg);
		}

		CommitTransactionCommand();
	}
}

static bool
quorum(MtmConfig *mtm_cfg, GTxState *all_states)
{
	int i, n_states = 0;
	GTxState my_state = all_states[mtm_cfg->my_node_id - 1];

	/*
	 * Make sure it is actually our term we are trying to assemble majority
	 * for, not of some neighbour who invited us (via monitor) to its own
	 * voting. I'm not entirely sure removing this would make the algorithm
	 * incorrect, but better be safe.
	 */
	if (my_state.proposal.node_id != mtm_cfg->my_node_id)
		return false;

	for (i = 0; i < MTM_MAX_NODES; i++)
	{
		/*
		 * Zero proposal term means value doesn't exist (no node with this id)
		 * or node refused to vote. Note that .status perfectly can be
		 * GTXInvalid here -- e.g. if this is reply to 1a and node has never
		 * gave a vote yet.
		 */
		if (term_cmp(all_states[i].proposal, InvalidGTxTerm) == 0)
			continue;

		if (term_cmp(my_state.proposal, all_states[i].proposal) == 0)
			n_states++;
	}

	return MtmQuorum(mtm_cfg, n_states);
}

static void
handle_response(MtmConfig *mtm_cfg, MtmMessage *raw_msg)
{
	GlobalTx   *gtx;
	const char *gid;

	if (raw_msg->tag == T_MtmTxStatusResponse)
		gid = ((MtmTxStatusResponse *) raw_msg)->gid;
	else if (raw_msg->tag == T_Mtm2AResponse)
		gid = ((Mtm2AResponse *) raw_msg)->gid;
	else
		Assert(false);

	mtm_log(ResolverTx, "handle_response: got '%s'", MtmMesageToString(raw_msg));

	gtx = GlobalTxAcquire(gid, false, NULL);
	if (!gtx)
		return;
	if (gtx->state.status == GTXAborted || gtx->state.status == GTXCommitted)
	{
		GlobalTxRelease(gtx);
		return;
	}
	/* we resolve only prepared xacts */
	Assert(gtx->prepared);

	mtm_log(ResolverTx, "handle_response: processing gtx %s", GlobalTxToString(gtx));

	if (gtx->resolver_stage == GTRS_AwaitStatus &&
		raw_msg->tag == T_MtmTxStatusResponse)
	{
		MtmTxStatusResponse *msg;

		msg = (MtmTxStatusResponse *) raw_msg;

		gtx->phase1_acks[mtm_cfg->my_node_id-1] = gtx->state;
		gtx->phase1_acks[msg->node_id-1] = msg->state;

		if (msg->state.status == GTXCommitted)
		{
			FinishPreparedTransaction(gtx->gid, true, false);
			gtx->state.status = GTXCommitted;
			mtm_log(MtmTxFinish, "TXFINISH: %s committed", gtx->gid);
			GlobalTxRelease(gtx);
			return;
		}
		else if (msg->state.status == GTXAborted)
		{
			FinishPreparedTransaction(gtx->gid, false, false);
			gtx->state.status = GTXAborted;
			mtm_log(MtmTxFinish, "TXFINISH: %s aborted", gtx->gid);
			GlobalTxRelease(gtx);
			return;
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

			/*
			 * Determine the highest term of collected prevVote's
			 */
			for (i = 0; i < MTM_MAX_NODES; i++)
			{
				/* like in quorum(), skip empty/refused votes */
				if (term_cmp(gtx->phase1_acks[i].proposal, InvalidGTxTerm) == 0)
					continue;

				if (term_cmp(gtx->phase1_acks[i].accepted, max_accepted) > 0)
					max_accepted = gtx->phase1_acks[i].accepted;
			}

			/*
			 * And the decision is the decree of this highest term vote.
			 * Decrees of all the votes with this term must be equal, seize
			 * the moment to sanity check it.
			 */
			for (i = 0; i < MTM_MAX_NODES; i++)
			{
				/* like in quorum(), skip empty/refused votes */
				if (term_cmp(gtx->phase1_acks[i].proposal, InvalidGTxTerm) == 0)
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
						/*
						 * If choice is not forced, resolver always aborts because
						 * only coordinator knows when xact can be committed.
						 */
						Assert(decision != GTXPreCommitted);
						decision = GTXPreAborted;
					}
				}
			}

			Assert(decision != GTXInvalid);
			sstate = serialize_gtx_state(decision, gtx->state.proposal,
										 gtx->state.proposal);
			done = SetPreparedTransactionState(gtx->gid, sstate, false);
			/*
			 * If acquired gtx exists, it must not be finished yet, so state
			 * change ought to succeed.
			 *
			 * this if seems enough to make compilers believe var is used
			 * without asserts
			 */
			if (!done)
				Assert(false);
			gtx->state.status = decision;
			gtx->state.accepted = gtx->state.proposal;
			gtx->resolver_stage = GTRS_AwaitAcks;

			mtm_log(MtmTxTrace, "TXTRACE: set state %s", GlobalTxToString(gtx));

			request_msg = (MtmTxRequest) {
				T_MtmTxRequest,
				decision == GTXPreCommitted ? MTReq_Precommit : MTReq_Preabort,
				gtx->state.accepted,
				gtx->gid,
				InvalidXLogRecPtr
			};
			connected = MtmGetConnectedMask(false);
			scatter(mtm_cfg, connected, "txreq", MtmMessagePack((MtmMessage *) &request_msg));
		}
	}
	else if (gtx->resolver_stage == GTRS_AwaitAcks &&
			 raw_msg->tag == T_Mtm2AResponse)
	{
		Mtm2AResponse *msg;

		msg = (Mtm2AResponse *) raw_msg;
		Assert(msg->gid[0] != '\0');
		/* If GTXInvalid, node refused to accept the ballot */
		if (!(msg->status == GTXPreAborted || msg->status == GTXPreCommitted))
		{
			GlobalTxRelease(gtx);
			return;
		}

		gtx->phase2_acks[mtm_cfg->my_node_id-1] = gtx->state;
		/* abuse GTxState to reuse quorum() without fuss */
		gtx->phase2_acks[msg->node_id-1] = (GTxState) {
			msg->accepted_term,
			msg->accepted_term,
			msg->status
		};

		if (quorum(mtm_cfg, gtx->phase2_acks))
		{
			MtmTxRequest request_msg;
			uint64		connected;

			Assert(gtx->state.status == msg->status);
			FinishPreparedTransaction(msg->gid, msg->status == GTXPreCommitted,
									  false);
			mtm_log(MtmTxFinish, "TXFINISH: %s %s via quorum of 2a acks", msg->gid,
					msg->status == GTXPreCommitted ? "committed" : "aborted");
			gtx->state.status = msg->status == GTXPreCommitted ?
				GTXCommitted : GTXAborted;
			GlobalTxRelease(gtx);

			request_msg = (MtmTxRequest) {
				T_MtmTxRequest,
				msg->status == GTXPreCommitted ? MTReq_Commit : MTReq_Abort,
				InvalidGTxTerm,
				gid
			};
			connected = MtmGetConnectedMask(false);
			scatter(mtm_cfg, connected, "txreq", MtmMessagePack((MtmMessage *) &request_msg));
			return;
		}
	}

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
	dmq_attach_receiver(psprintf(MTM_DMQNAME_FMT, node_id), node_id - 1);
}

static void
detach_node(int node_id, MtmConfig *new_cfg, Datum arg)
{
	/* detach incoming queues from this node */
	dmq_detach_receiver(psprintf(MTM_DMQNAME_FMT, node_id));
}

static void
sigUsr1Handler(SIGNAL_ARGS)
{
	int			save_errno = errno;
	SetLatch(MyLatch);
	errno = save_errno;
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
	pqsignal(SIGUSR1, sigUsr1Handler);

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

	mtm_log(ResolverState, "Resolver started");

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
			bool job_pending;

			/* vacuum obsolete in table proposals periodically */
			GlobalTxGCInTableProposals();
			job_pending = finish_ready();

			if (job_pending)
			{
				StartTransactionCommand();
				scatter_status_requests(mtm_cfg);
				CommitTransactionCommand();
			}
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
		/* XXX ars: better to set it whenever backend wakes us */
		if (rc & WL_TIMEOUT)
			send_requests = true;

		/* Emergency bailout if postmaster has died */
		if (rc & WL_POSTMASTER_DEATH)
			proc_exit(1);

		if (rc & WL_LATCH_SET)
			ResetLatch(MyLatch);
	}

}
