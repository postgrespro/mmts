/*----------------------------------------------------------------------------
 *
 * resolver.c
 *	  Recovery procedures to resolve transactions that were left uncommited
 *	  because of detected failure.
 *
 * Copyright (c) 2015-2020, Postgres Professional
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

static MtmConfig *mtm_cfg = NULL;
static bool		send_requests;

static void handle_response(MtmConfig *mtm_cfg, MtmMessage *raw_msg);

/*****************************************************************************
 *
 * Initialization
 *
 *****************************************************************************/

void
ResolverWake()
{
	pid_t		resolver_pid;
	LWLockAcquire(Mtm->lock, LW_SHARED);
	resolver_pid = Mtm->resolver_pid;
	LWLockRelease(Mtm->lock);
	if (resolver_pid != InvalidPid)
		kill(resolver_pid, SIGHUP);
}

/* resolver never rereads PG config, but it currently doesn't need to */
static void
ResolverSigHupHandler(SIGNAL_ARGS)
{
	int			save_errno = errno;

	send_requests = true;
	SetLatch(MyLatch);

	errno = save_errno;
}

static void
resolver_at_exit(int status, Datum arg)
{
}

/*****************************************************************************
 *
 * Main resolver loop.
 *
 *****************************************************************************/

/* gid_t is system type... */
typedef char pgid_t[GIDSIZE];

static void
ResolveForRefereeWinner(void)
{
	MtmGeneration curr_gen;
	HASH_SEQ_STATUS hash_seq;
	GlobalTx   *gtx;
	/*
	 * Calling FinishPreparedTransaction under lwlock is probably not a good
	 * idea (as well as waiting inside GlobalTxAcquire), so let's collect
	 * xacts here and finish them after release.
	 */
	pgid_t *gids;
	int n_gids = 0;
	int i;

	/*
	 * Once both nodes switched into ONLINE in full (two nodes) generation
	 * direct resolution is not allowed anymore as grant might be cleared and
	 * consequently re-acquired by another node at any time. To enforce this,
	 * do the job under generation lock.
	 */
	AcquirePBByPreparer(true);

	curr_gen = MtmGetCurrentGen(true);
	/*
	 * can resolve directly only if I am in my referee granted generation
	 */
	if (!IS_REFEREE_GEN(curr_gen.members, curr_gen.configured) ||
		MtmGetCurrentStatusInGen() != MTM_GEN_ONLINE)
	{
		ReleasePB();
		return;
	}

	mtm_log(ResolverState, "ResolveForRefereeWinner");
	gids = palloc(sizeof(pgid_t) * max_prepared_xacts);

	LWLockAcquire(gtx_shared->lock, LW_SHARED);
	hash_seq_init(&hash_seq, gtx_shared->gid2gtx);
	while ((gtx = hash_seq_search(&hash_seq)) != NULL)
	{
		/* skip not orphaned xacts, will pick them up next time */
		if (!gtx->orphaned)
			continue;

		Assert(n_gids < max_prepared_xacts);
		strcpy(gids[n_gids], gtx->gid);
		n_gids++;
	}
	LWLockRelease(gtx_shared->lock);

	for (i = 0; i < n_gids; i++)
	{
		bool commit;

		gtx = GlobalTxAcquire(gids[i], false, false, NULL);
		if (!gtx)
			continue;

		/*
		 * - If referee winner doesn't have PC, it means other node might abort
		 *   (if it is coordinator) or do nothing (it can't go beyond PC|PA
		 *   without our vote), so we abort.
		 * - If referee winner has PC, other node might only commit (if it
		 *   got our PC) or do nothing, so we commit.
		 */
		commit = gtx->state.status == GTXPreCommitted;

		StartTransactionCommand();
		FinishPreparedTransaction(gtx->gid, commit, false);
		CommitTransactionCommand();
		gtx->state.status = commit ? GTXCommitted : GTXAborted;
		mtm_log(MtmTxFinish, "TXFINISH: %s %s as referee winner",
				gtx->gid, commit ? "committed" : "aborted");
		GlobalTxRelease(gtx);
	}

	ReleasePB();
	pfree(gids);
	return;
}

/*
 * Called periodically. Iterate over gtxes and
 * - finish xact immediately if we can (it is our xact which never got
 *   precommitted with backend gone dead)
 * - determine whether we still need to actually resolve something,
 *   returns true, if so
 */
static bool
finish_ready(void)
{
	bool job_pending = false;
	HASH_SEQ_STATUS hash_seq;
	GlobalTx   *gtx;
	/*
	 * Calling FinishPreparedTransaction under lwlock is probably not a good
	 * idea ((as well as waiting inside GlobalTxAcquire), so let's collect
	 * xacts here and finish them after release.
	 */
	pgid_t *agids = palloc(sizeof(pgid_t) * max_prepared_xacts);
	int n_agids = 0;
	int i;
	int coordinator;

	LWLockAcquire(gtx_shared->lock, LW_SHARED);
	hash_seq_init(&hash_seq, gtx_shared->gid2gtx);
	while ((gtx = hash_seq_search(&hash_seq)) != NULL)
	{
		/*
		 * don't intervene if backend is still working on xact or it is not
		 * prepared yet
		 */
		if (gtx->acquired_by != InvalidBackendId ||
			!gtx->orphaned || !gtx->prepared)
		{
			continue;
		}

		coordinator = MtmGidParseNodeId(gtx->gid);
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
		if (coordinator == Mtm->my_node_id &&
			gtx->state.status == GTXInvalid)
		{
			Assert(n_agids < max_prepared_xacts);
			strcpy(agids[n_agids], gtx->gid);
			n_agids++;
			continue;
		}

		/* so we have orphaned xact needing resolution */
		job_pending = true;
	}
	LWLockRelease(gtx_shared->lock);

	/* finish ready xacts */
	for (i = 0; i < n_agids; i++)
	{
		gtx = GlobalTxAcquire(agids[i], false, false, NULL);
		if (!gtx)
			continue;

		StartTransactionCommand();
		FinishPreparedTransaction(gtx->gid, false, false);
		CommitTransactionCommand();
		gtx->state.status = GTXAborted;
		mtm_log(MtmTxFinish, "TXFINISH: %s aborted as own orphaned not precommitted",
				gtx->gid);
		GlobalTxRelease(gtx);
	}

	pfree(agids);
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

		if (!BIT_CHECK(cmask, node_id - 1))
			continue;

		LWLockAcquire(Mtm->lock, LW_SHARED);
		dest_id = Mtm->peers[node_id - 1].dmq_dest_id;
		LWLockRelease(Mtm->lock);
		/*
		 * XXX ars: config could change after last MtmReloadConfig, this might be
		 * false if node was removed.
		 */
		Assert(dest_id >= 0);

		dmq_push_buffer(dest_id, stream_name, msg->data, msg->len);
	}
}

/*
 * TODO: it would be nice to retry xact resolution attempt only if we suspect
 * the previous one failed, not just blindly each 3 seconds.
 */
static void
scatter_status_requests(MtmConfig *mtm_cfg)
{
	HASH_SEQ_STATUS hash_seq;
	GlobalTx   *gtx;
	GlobalTxTerm new_term;

	/*
	 * It is almost pointless to resolve unless we see the majority, do not
	 * wind term numbers in waste. Yeah, we could get some finished xact
	 * statuses, but normal paxos resolution would surely fail.
	 */
	if (!MtmQuorum(mtm_cfg, popcount(MtmGetConnectedMaskWithMe(false))))
	{
		mtm_log(ResolverState, "not sending requests as there is no connected majority");
		return;
	}

	mtm_log(ResolverState, "orphaned transactions detected");

	/*
	 * Generate next term.
	 * Picking at least max local proposal term + 1 guarantees we never try
	 * the same term twice.
	 *
	 * We ignore knowledge about neighbours terms here, but, well, even if
	 * terms are radically different (and it is unobvious how this could
	 * happen) -- fine, than node with the highest term would succeed in xact
	 * resolution and tell us the outcome.
	 */
	new_term = GlobalTxGetMaxProposal();
	new_term.ballot += 1;
	new_term.node_id = mtm_cfg->my_node_id;
	mtm_log(ResolverState, "new term is (%d,%d)", new_term.ballot, new_term.node_id);

	/*
	 * Stamp all orphaned transactions with the new proposal and send status
	 * requests.
	 */
	LWLockAcquire(gtx_shared->lock, LW_EXCLUSIVE);
	hash_seq_init(&hash_seq, gtx_shared->gid2gtx);
	while ((gtx = hash_seq_search(&hash_seq)) != NULL)
	{
		/* skip acquired until next round */
		if (gtx->orphaned && gtx->acquired_by == InvalidBackendId &&
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
			scatter(mtm_cfg, connected, "reqresp",
					MtmMessagePack((MtmMessage *) &status_msg));
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
quorum(MtmConfig *mtm_cfg, GTxState *all_states, char *gid)
{
	int i, n_states = 0;
	GTxState my_state = all_states[mtm_cfg->my_node_id - 1];
	nodemask_t configured;

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

	/*
	 * only configured mask of xact generation participates in resolving --
	 * this guarantees two successfull votings with non-intersecting quorums
	 * are not possible
	 */
	configured = MtmGidParseConfigured(gid);
	return Quorum(popcount(configured), n_states);
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

	gtx = GlobalTxAcquire(gid, false, false, NULL);
	if (!gtx)
		return;

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
		else if (quorum(mtm_cfg, gtx->phase1_acks, gtx->gid))
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
			scatter(mtm_cfg, connected, "reqresp",
					MtmMessagePack((MtmMessage *) &request_msg));
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

		if (quorum(mtm_cfg, gtx->phase2_acks, gtx->gid))
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
			scatter(mtm_cfg, connected, "reqresp",
					MtmMessagePack((MtmMessage *) &request_msg));
			return;
		}
	}

	GlobalTxRelease(gtx);
}

void
ResolverMain(Datum main_arg)
{
	Oid			db_id,
				user_id;

	/* init this worker */
	pqsignal(SIGHUP, ResolverSigHupHandler);
	pqsignal(SIGTERM, die);

	BackgroundWorkerUnblockSignals();

	MtmBackgroundWorker = true;

	memcpy(&db_id, MyBgworkerEntry->bgw_extra, sizeof(Oid));
	memcpy(&user_id, MyBgworkerEntry->bgw_extra + sizeof(Oid), sizeof(Oid));

	/* Connect to a database */
	BackgroundWorkerInitializeConnectionByOid(db_id, user_id, 0);

	/* Keep us informed about subscription changes. */
	CacheRegisterSyscacheCallback(SUBSCRIPTIONOID,
								  mtm_pubsub_change_cb,
								  (Datum) 0);

	on_shmem_exit(resolver_at_exit, (Datum) 0);
	dmq_stream_subscribe("txresp");

	LWLockAcquire(Mtm->lock, LW_EXCLUSIVE);
	Mtm->resolver_pid = MyProcPid;
	LWLockRelease(Mtm->lock);

	mtm_log(ResolverState, "resolver started");

	send_requests = true;

	for (;;)
	{
		int			rc;

		CHECK_FOR_INTERRUPTS();

		AcceptInvalidationMessages();
		if (!mtm_config_valid)
		{
			mtm_cfg = MtmReloadConfig(mtm_cfg, mtm_attach_node, mtm_detach_node,
									  (Datum) NULL, FATAL);
			mtm_config_valid = true;
		}
		Assert(mtm_cfg);

		/* Scatter requests for unresolved transactions */
		if (send_requests)
		{
			bool job_pending;

			if (IS_REFEREE_ENABLED())
				ResolveForRefereeWinner();
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
					   WL_LATCH_SET | WL_TIMEOUT | WL_EXIT_ON_PM_DEATH,
					   3000,
					   PG_WAIT_TIMEOUT);

		/* re-try to send requests if there are some unresolved transactions */
		/* XXX ars: set it whenever any 3 seconds passed, not 3 idle seconds? */
		if (rc & WL_TIMEOUT)
			send_requests = true;

		if (rc & WL_LATCH_SET)
			ResetLatch(MyLatch);
	}

}
