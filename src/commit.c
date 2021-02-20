/*----------------------------------------------------------------------------
 *
 * commit.c
 *		Replace ordinary commit with 3PC.
 *
 * Copyright (c) 2019-2020, Postgres Professional
 *
 *----------------------------------------------------------------------------
 */

#include "postgres.h"
#include "access/twophase.h"
#include "access/xact.h"
#include "access/transam.h"
#include "access/xlog.h"
#include "storage/proc.h"
#include "storage/spin.h"
#include "utils/guc.h"
#include "utils/syscache.h"
#include "utils/snapmgr.h"
#include "utils/inval.h"
#include "miscadmin.h"
#include "commands/dbcommands.h"
#include "catalog/pg_subscription.h"
#include "tcop/tcopprot.h"
#include "postmaster/autovacuum.h"
#include "libpq/pqformat.h"
#include "pgstat.h"
#include "storage/ipc.h"

#include "multimaster.h"
#include "compat.h"
#include "logger.h"
#include "ddl.h"
#include "state.h"
#include "syncpoint.h"
#include "commit.h"
#include "global_tx.h"
#include "messaging.h"

static bool force_in_bgworker;
static bool init_done;
static bool config_valid;
static bool inside_mtm_begin;

static MtmConfig *mtm_cfg;

MtmCurrentTrans MtmTx;

/* holds state defining cleanup actions in case of failure during commit */
static struct MtmCommitState
{
	char gid[GIDSIZE];
	GlobalTx *gtx;
	bool	inside_commit_sequence;
	MemoryContext mctx;
} mtm_commit_state;

static void
pubsub_change_cb(Datum arg, int cacheid, uint32 hashvalue)
{
	config_valid = false;
}

static void
proc_change_cb(Datum arg, int cacheid, uint32 hashvalue)
{
	/* Force RemoteFunction reload */
	MtmSetRemoteFunction(NULL, NULL);
}

void
MtmXactCallback(XactEvent event, void *arg)
{
	/*
	 * Perform distributed commit only for transactions in ordinary backends
	 * with multimaster enabled.
	 */
	if (IsAnyAutoVacuumProcess() || !IsNormalProcessingMode() ||
		am_walsender || (IsBackgroundWorker && !force_in_bgworker))
	{
		return;
	}

	/*
	 * MtmTwoPhaseCommit does (Start|Commit)TransactionCommand, they shouldn't
	 * nest into our hooks again.
	 */
	if (mtm_commit_state.inside_commit_sequence)
		return;

	switch (event)
	{
		case XACT_EVENT_START:
			MtmBeginTransaction();
			break;

		case XACT_EVENT_COMMIT_COMMAND:
			/* Here we catching commit of single-statement transaction */
			if (IsTransactionOrTransactionBlock()
				&& !IsTransactionBlock()
				&& !IsSubTransaction())
			{
				MtmTwoPhaseCommit();
			}
			break;

		default:
			break;
	}

}

static void
mtm_commit_cleanup(int status, Datum arg)
{
	ReleasePB();
	dmq_stream_unsubscribe();

	if (mtm_commit_state.gtx != NULL)
	{
		/*
		 * If we managed to prepare the xact but failed to commit, try to
		 * abort it immediately if it is still possible (no precommit =>
		 * others nodes can't commit) or issue a warning about unclear xact
		 * status
		 */
		if (mtm_commit_state.gtx->prepared)
		{
			if ((term_cmp(mtm_commit_state.gtx->state.accepted,
						  InvalidGTxTerm) == 0) ||
				/* explicit prepare never does precommit and can be rolled back */
				IS_EXPLICIT_2PC_GID(mtm_commit_state.gid))
			{
				/* there was no precommit, we can abort */
				PG_TRY();
				{
					AbortOutOfAnyTransaction();
					StartTransactionCommand();
					FinishPreparedTransaction(mtm_commit_state.gid, false, false);
					mtm_commit_state.gtx->state.status = GTXAborted;
					mtm_log(MtmTxFinish, "TXFINISH: %s aborted as own orphaned not precomitted",
							mtm_commit_state.gid);
					CommitTransactionCommand();

				}
				/*
				 * this should be extremely unlikely, but if we fail, don't
				 * forget to release gtx
				 */
				PG_CATCH();
				{
					GlobalTxRelease(mtm_commit_state.gtx);
					mtm_commit_state.gtx = NULL;
					mtm_commit_state.inside_commit_sequence = false;
					PG_RE_THROW();
				}
				PG_END_TRY();
			}
			else
			{
				ResolverWake();
				if (!MtmVolksWagenMode)
				{
					ereport(WARNING,
							(errcode(ERRCODE_TRANSACTION_RESOLUTION_UNKNOWN),
							 errmsg("[multimaster] exiting commit sequence of transaction %s with unknown status",
									mtm_commit_state.gid),
							 errdetail("The transaction will be committed or aborted later.")));

				}
			}
		}
		GlobalTxRelease(mtm_commit_state.gtx);
		mtm_commit_state.gtx = NULL;
	}
	mtm_commit_state.inside_commit_sequence = false;
}

void
MtmBeginTransaction()
{
	MtmNodeStatus node_status;

	/* Set this on tx start, to avoid resetting in error handler */
	AllowTempIn2PC = false;

	/* XXX: clean MtmTx on commit and check on begin that it is clean. */
	/* That should unveil probable issues with subxacts. */

	if (!MtmIsEnabled())
	{
		MtmTx.distributed = false;
		return;
	}


	if (!init_done)
	{
		/* Keep us informed about subscription changes. */
		CacheRegisterSyscacheCallback(SUBSCRIPTIONOID,
									  pubsub_change_cb,
									  (Datum) 0);
		CacheRegisterSyscacheCallback(PUBLICATIONOID,
									  pubsub_change_cb,
									  (Datum) 0);
		CacheRegisterSyscacheCallback(PROCOID,
									  proc_change_cb,
									  (Datum) 0);
		/*
		 * mtm_commit_cleanup must do its job *before* gtx is released, so
		 * register gtx hook first (it will be called last)
		 */
		GlobalTxEnsureBeforeShmemExitHook();
		before_shmem_exit(mtm_commit_cleanup, Int32GetDatum(1));
		mtm_commit_state.mctx = AllocSetContextCreate(TopMemoryContext,
													  "MtmCommitContext",
													  ALLOCSET_DEFAULT_SIZES);
		init_done = true;
	}

	AcceptInvalidationMessages();
	if (!config_valid)
	{
		mtm_cfg = MtmReloadConfig(mtm_cfg, mtm_attach_node, mtm_detach_node,
								  (Datum) NULL, 0);
		if (mtm_cfg->my_node_id == MtmInvalidNodeId) /* mtm was dropped */
		{
			MtmTx.distributed = false;
			return;
		}
		config_valid = true;
	}

	/* Reset MtmTx */
	MtmTx.contains_ddl = false;
	MtmTx.contains_dml = false;
	MtmTx.distributed = true;

	MtmDDLResetStatement();

	node_status = MtmGetCurrentStatus(false, false);

	/* Application name can be changed using PGAPPNAME environment variable */
	if (node_status != MTM_ONLINE
		&& strcmp(application_name, MULTIMASTER_ADMIN) != 0
		&& strcmp(application_name, MULTIMASTER_BROADCAST_SERVICE) != 0)
	{
		/*
		 * Reject all user's transactions at offline cluster. Allow execution
		 * of transaction by bg-workers to makeit possible to perform
		 * recovery.
		 */
		if (!MtmBreakConnection)
		{
			mtm_log(ERROR,
					"Multimaster node is not online: current status %s",
					MtmNodeStatusMnem[node_status]);
		}
		else
		{
			mtm_log(FATAL,
					"Multimaster node is not online: current status %s",
					MtmNodeStatusMnem[node_status]);
		}
	}

	/*
	 * If during previous checks we acquired snapshot we'll prevent BEGIN
	 * TRANSACTION ISOLATION LEVEL REPEATABLE READ from happening. So
	 * commit/start transaction in this case.
	 */
	if (FirstSnapshotSet && !inside_mtm_begin)
	{
		inside_mtm_begin = true;
		CommitTransactionCommand();
		StartTransactionCommand();
		inside_mtm_begin = false;
	}
}

/*
 * Generate global transaction identifier for two-phase commit.
 * It should be unique for all nodes. This is basically the only requirement;
 * we have some important metadata associated with xact, but we put
 * everything into state_3pc for seamless integration with explicit 2PC.
 * (we still keep gen_num here for logging)
 */
void
MtmGenerateGid(char *gid, int node_id, TransactionId xid, uint64 gen_num)
{
	sprintf(gid, "MTM-%d-" XID_FMT "-" UINT64_FORMAT,
			node_id, xid, gen_num);
	return;
}

uint64
MtmGidParseGenNum(const char *gid)
{
	uint64 gen_num = MtmInvalidGenNum;
	TransactionId xid;

	sscanf(gid, "MTM-%*d-" XID_FMT "-%" INT64_MODIFIER "X", &xid, &gen_num);
	Assert(gen_num != MtmInvalidGenNum);
	return gen_num;
}

int
MtmGidParseNodeId(const char *gid)
{
	int			node_id = -1;

	sscanf(gid, "MTM-%d-%*d", &node_id);
	return node_id;
}

TransactionId
MtmGidParseXid(const char *gid)
{
	TransactionId xid = InvalidTransactionId;

	sscanf(gid, "MTM-%*d-" XID_FMT, &xid);
	Assert(xid != InvalidTransactionId);
	return xid;
}

/* ensure we get the right PREPARE ack */
static bool
PrepareGatherHook(MtmMessage *anymsg, Datum arg)
{
	MtmPrepareResponse *msg = (MtmPrepareResponse *) anymsg;
	TransactionId xid = DatumGetTransactionId(arg);

	if (anymsg->tag != T_MtmPrepareResponse)
		return false;
	return msg->xid == xid;
}

/* ensure we get the right 2A response */
static bool
Paxos2AGatherHook(MtmMessage *anymsg, Datum arg)
{
	Mtm2AResponse *msg = (Mtm2AResponse *) anymsg;
	char *gid = DatumGetPointer(arg);

	if (anymsg->tag != T_Mtm2AResponse)
		return false;
	return strcmp(msg->gid, gid) == 0;
}


/*
 * Returns false if mtm is not interested in this xact at all.
 */
bool
MtmTwoPhaseCommit(void)
{
	nodemask_t	cohort;
	bool		ret;
	TransactionId xid;
	MtmPrepareResponse *p_messages[MTM_MAX_NODES];
	Mtm2AResponse *twoa_messages[MTM_MAX_NODES]; /* wow, great name */
	int			n_messages;
	int			i;
	int 		nvotes;
	nodemask_t	pc_success_cohort;
	MtmGeneration xact_gen;
	char dmq_stream_name[DMQ_STREAM_NAME_MAXLEN];
	GTxState gtx_state;

	if (MtmNo3PC)
	{
		/*
		 * SET LOCAL which ensures GUC value is reset on xact commit is
		 * strongly recommended for this (internal) variable manipulations.
		 */
		return false;
	}

	if (!MtmTx.contains_ddl && !MtmTx.contains_dml)
		return false;

	if (!MtmTx.distributed)
		return false;

	/*
	 * If this is implicit single-query xact, wrap it in block to execute
	 * PREPARE.
	 */
	if (!IsTransactionBlock())
	{
		BeginTransactionBlockCompat();
		CommitTransactionCommand();
		StartTransactionCommand();
	}

	/* prepare for cleanup */
	mtm_commit_state.gtx = NULL;
	mtm_commit_state.inside_commit_sequence = true;
	/* used for allocations not inside tx, e.g. messages in gather() */
	MemoryContextReset(mtm_commit_state.mctx);

	/*
	 * Note that we do not HOLD_INTERRUPTS; user might cancel waiting whenever
	 * he wants. However, probably xact status would be unclear at that
	 * moment; we issue a warning in this case.
	 * (but to be fair, bail out with unclear xact status is currently possible
	 *  even without explicit cancellation; this ought to be fixed)
	 */
	PG_TRY();
	{
		/* Exclude concurrent gen switchers, c.f. AcquirePBByHolder call site */
		AcquirePBByPreparer(true);

		/*
		 * xact is allowed iff we are MTM_GEN_ONLINE in current gen, but
		 * MtmGetCurrentGenStatus is more useful for error reporting.
		 */
		if (MtmGetCurrentStatusInGen() != MTM_GEN_ONLINE)
		{
			/*
			 * XXX this ensures we are out of transaction block (no 'commands
			 * ignored until end of transaction block') after abort. This is
			 * fragile, as we must put it before each ERROR till prepare is
			 * done; same for MtmExplicitPrepare. Is there a better way?
			 */
			UserAbortTransactionBlockCompat(false);
			ereport(MtmBreakConnection ? FATAL : ERROR,
					(errcode(ERRCODE_INTERNAL_ERROR),
					 errmsg("multimaster node is not online: current status \"%s\"",
							MtmNodeStatusMnem[MtmGetCurrentStatus(true, false)])));
		}

		xact_gen = MtmGetCurrentGen(true);
		xid = GetTopTransactionId();
		MtmGenerateGid(mtm_commit_state.gid, mtm_cfg->my_node_id, xid,
					   xact_gen.num);
		sprintf(dmq_stream_name, "xid" XID_FMT, xid);
		dmq_stream_subscribe(dmq_stream_name);
		mtm_log(MtmTxTrace, "%s subscribed for %s", mtm_commit_state.gid,
				dmq_stream_name);

		/* prepare transaction on our node */
		mtm_commit_state.gtx = GlobalTxAcquire(mtm_commit_state.gid, true,
											   false, NULL, 0);
		/*
		 * it is simpler to mark gtx originated here as orphaned from the
		 * beginning rather than in error handler; resolver won't touch gtx
		 * while it is locked on us anyway
		 */
		mtm_commit_state.gtx->orphaned = true;
		mtm_commit_state.gtx->xinfo.coordinator = mtm_cfg->my_node_id;
		mtm_commit_state.gtx->xinfo.xid = xid;
		mtm_commit_state.gtx->xinfo.gen_num = xact_gen.num;
		mtm_commit_state.gtx->xinfo.configured = xact_gen.configured;
		Assert(mtm_commit_state.gtx->state.status == GTXInvalid);
		/*
		 * PREPARE doesn't happen here; ret 0 just means we were already in
		 * aborted transaction block and we expect the callee to handle this.
		 */
		ret = PrepareTransactionBlockWithState3PC(
			mtm_commit_state.gid,
			serialize_xstate(&mtm_commit_state.gtx->xinfo, &mtm_commit_state.gtx->state));
		if (!ret)
		{
			Assert(false);
			elog(PANIC, "unexpected PrepareTransactionBlock failure");
		}

		AllowTempIn2PC = true;
		CommitTransactionCommand(); /* here we actually PrepareTransaction */
		mtm_commit_state.gtx->prepared = true;
		ReleasePB(); /* don't hold generation switch anymore */
		/* end_lsn of PREPARE */
		mtm_log(MtmTxFinish, "TXFINISH: %s prepared at %X/%X",
				mtm_commit_state.gid,
				(uint32) (XactLastCommitEnd >> 32),
				(uint32) (XactLastCommitEnd));
		MemoryContextSwitchTo(mtm_commit_state.mctx);

		/*
		 * By definition of generations, we must collect PREPARE ack from
		 * *all* generation members.
		 * However, if generation switch has happened, we risk never getting
		 * response from some counterparties as e.g. they might get this
		 * PREPARE from some other node in recovery, so stop waiting and abort
		 * in this case. OTOH, if gen stays the same, we surely eventually
		 * will get the answer to apply attempt, regardless of transient
		 * problems with replication connection (if dmq connection broke
		 * though we abort as confirmation could have been lost).
		 *
		 * Probably we could act a bit gentler as generally not every gen
		 * switch requires abort of all currently preparing xacts. It is not
		 * clear whether related complications worth the benefits though.
		 */
		cohort = xact_gen.members;
		BIT_CLEAR(cohort, mtm_cfg->my_node_id - 1);
		ret = gather(cohort,
					 (MtmMessage **) p_messages, NULL, &n_messages,
					 PrepareGatherHook, TransactionIdGetDatum(xid),
					 NULL, xact_gen.num);

		/*
		 * The goal here is to check that every gen member applied the
		 * transaction; paxos doesn't demand 1a/1b roundtrip for correctness
		 * as coordinator uses first term {1, 0} and his decree choice
		 * (precommit, preabort) is surely free (unbounded).
		 */
		if (!ret)
		{
			MtmGeneration new_gen = MtmGetCurrentGen(false);
			ereport(ERROR,
					(errcode(ERRCODE_INTERNAL_ERROR),
					 errmsg("[multimaster] failed to collect prepare acks due to generation switch: was num=" UINT64_FORMAT ", members=%s, now num=" UINT64_FORMAT ", members=%s",
							xact_gen.num,
							maskToString(xact_gen.members),
							new_gen.num,
							maskToString(new_gen.members))));
		}
		if (n_messages != popcount(cohort))
		{
			nodemask_t failed_cohort = cohort;
			for (i = 0; i < n_messages; i++)
				BIT_CLEAR(failed_cohort, p_messages[i]->node_id - 1);

			ereport(ERROR,
					(errcode(ERRCODE_CONNECTION_FAILURE),
					 errmsg("[multimaster] failed to collect prepare acks from nodemask %s due to network error",
							maskToString(failed_cohort))));
		}
		for (i = 0; i < n_messages; i++)
		{
			if (!p_messages[i]->prepared)
			{
				/* don't print random gid, node id for regression tests output */
				if (MtmVolksWagenMode)
					ereport(ERROR,
							(errcode(p_messages[i]->errcode),
							 errmsg("[multimaster] failed to prepare transaction at peer node")));
				else
					ereport(ERROR,
							(errcode(p_messages[i]->errcode),
							 errmsg("[multimaster] failed to prepare transaction %s at node %d",
									mtm_commit_state.gid, p_messages[i]->node_id),
							 errdetail("sqlstate %s (%s)",
									   unpack_sql_state(p_messages[i]->errcode),
									   p_messages[i]->errmsg)));
			}
		}

		/* ok, we have all prepare responses, precommit */
		gtx_state.status = GTXPreCommitted;
		gtx_state.proposal = InitialGTxTerm;
		gtx_state.accepted = InitialGTxTerm;
		SetPreparedTransactionState(
			mtm_commit_state.gid,
			serialize_xstate(&mtm_commit_state.gtx->xinfo, &gtx_state),
			false);
		/*
		 * since this moment direct aborting is not allowed; others can
		 * receive our precommit and resolve xact to commit without us
		 */
		mtm_commit_state.gtx->state = gtx_state;
		mtm_log(MtmTxFinish, "TXFINISH: %s precommitted", mtm_commit_state.gid);

		/*
		 * Just skip precommit tour if I am online in my referee gen,
		 * i.e. working alone. We actually could do direct commits without 2PC
		 * as an optimization...
		 */
		pc_success_cohort = 0;
		if (IS_REFEREE_GEN(xact_gen.members, xact_gen.configured))
			goto precommit_tour_done;
		/*
		 * Here (paxos 2a/2b) we need only majority of acks, probably it'd be
		 * useful to teach gather return once quorum of good msgs collected.
		 */
		ret = gather(cohort,
					 (MtmMessage **) twoa_messages, NULL, &n_messages,
					 Paxos2AGatherHook, PointerGetDatum(mtm_commit_state.gid),
					 NULL, xact_gen.num);

		/* check ballots in answers */
		nvotes = 1; /* myself */
		for (i = 0; i < n_messages; i++)
		{
			if (term_cmp(twoa_messages[i]->accepted_term,
						 (GlobalTxTerm) {1, 0}) == 0 &&
				twoa_messages[i]->status == GTXPreCommitted)
			{
				nvotes++;
				BIT_SET(pc_success_cohort, twoa_messages[i]->node_id - 1);
				continue;
			}
			ereport(WARNING,
					(errcode(ERRCODE_INTERNAL_ERROR),
					 errmsg("[multimaster] failed to precommit transaction %s at node %d",
							mtm_commit_state.gid, twoa_messages[i]->node_id),
					 errdetail("status=%d, accepted term=<%d, %d>",
							   twoa_messages[i]->status,
							   twoa_messages[i]->accepted_term.ballot,
							   twoa_messages[i]->accepted_term.node_id)));
		}
		if (!Quorum(popcount(xact_gen.configured), nvotes))
		{
			nodemask_t failed_cohort;

			if (!ret)
			{
				MtmGeneration new_gen = MtmGetCurrentGen(false);
				ereport(ERROR,
						(errcode(ERRCODE_INTERNAL_ERROR),
						 errmsg("[multimaster] failed to collect precommit acks of transaction %s due to generation switch: was num=" UINT64_FORMAT ", members=%s, now num=" UINT64_FORMAT ", members=%s",
								mtm_commit_state.gid,
								xact_gen.num,
								maskToString(xact_gen.members),
								new_gen.num,
								maskToString(new_gen.members))));
			}

			failed_cohort = cohort;
			for (i = 0; i < MTM_MAX_NODES; i++)
			{
				if (BIT_CHECK(pc_success_cohort, i))
					BIT_CLEAR(failed_cohort, i);
			}
			ereport(ERROR,
					(errcode(ERRCODE_INTERNAL_ERROR),
					 errmsg("[multimaster] failed to collect precommit acks or precommit transaction %s at nodes %s due to network error or non-first term",
							mtm_commit_state.gid,
							maskToString(failed_cohort))));
		}

precommit_tour_done:
		/* we have majority precommits, commit */
		StartTransactionCommand();
		FinishPreparedTransaction(mtm_commit_state.gid, true, false);
		mtm_commit_state.gtx->state.status = GTXCommitted;
		mtm_log(MtmTxFinish, "TXFINISH: %s committed", mtm_commit_state.gid);
		GlobalTxRelease(mtm_commit_state.gtx);
		mtm_commit_state.gtx = NULL;

		/*
		 * Optionally wait for commit ack
		 */
		if (!MtmWaitPeerCommits)
			goto commit_tour_done;

		/* abusing both message type and gather hook is slightly dubious */
		ret = gather(pc_success_cohort,
					 (MtmMessage **) twoa_messages, NULL, &n_messages,
					 Paxos2AGatherHook, PointerGetDatum(mtm_commit_state.gid),
					 NULL, xact_gen.num);

		if (!ret)
		{
			MtmGeneration new_gen = MtmGetCurrentGen(false);
			ereport(WARNING,
					(errcode(ERRCODE_INTERNAL_ERROR),
					 errmsg("[multimaster] failed to collect commit acks of transaction %s due to generation switch: was num=" UINT64_FORMAT ", members=%s, now num=" UINT64_FORMAT ", members=%s",
							mtm_commit_state.gid,
							xact_gen.num,
							maskToString(xact_gen.members),
							new_gen.num,
							maskToString(new_gen.members))));
		}
		else if (n_messages != popcount(pc_success_cohort))
		{
			nodemask_t failed_cohort = pc_success_cohort;
			for (i = 0; i < n_messages; i++)
			{
				BIT_CLEAR(failed_cohort, twoa_messages[i]->node_id - 1);
			}
			ereport(WARNING,
					(errcode(ERRCODE_INTERNAL_ERROR),
					 errmsg("[multimaster] failed to collect commit acks of transaction %s at nodes %s due to network error",
							mtm_commit_state.gid,
							maskToString(failed_cohort))));
		}

commit_tour_done:
		dmq_stream_unsubscribe();
		mtm_log(MtmTxTrace, "%s unsubscribed for %s",
				mtm_commit_state.gid, dmq_stream_name);
		mtm_commit_state.inside_commit_sequence = false;
		/*
		 * If MtmTwoPhaseCommit happened in COMMIT's ProcessUtility hook,
		 * (explicit tblock), MtmTwoPhaseCommit will be again called later on
		 * postgres.c's CommitTransactionCommand without MtmBeginTransaction
		 * cleaning up things first as we prevent nested mm hooks entry. So
		 * command future MtmTwoPhaseCommit not perform 3PC: the last xact is
		 * empty, it is only needed to avoid confusing xact.c machinery --
		 * CommitTransactionCommand must find valid transaction.
		 */
		MtmTx.distributed = false;
	}
	PG_CATCH();
	{
		mtm_commit_cleanup(0, Int32GetDatum(0));

		PG_RE_THROW();
	}
	PG_END_TRY();

	MaybeLogSyncpoint();

	return true;
}

/*
 * The idea is to to pass through the generation barrier, i.e. prepare xact at
 * all generation members. Transaction will be finished in explicit
 * COMMIT|ROLLBACK PREPARED call.
 */
bool
MtmExplicitPrepare(char *gid)
{
	nodemask_t	cohort;
	bool		ret;
	TransactionId xid;
	char		stream[DMQ_STREAM_NAME_MAXLEN];
	int			i;
	MtmGeneration xact_gen;
	MtmPrepareResponse *p_messages[MTM_MAX_NODES];
	int			n_messages;

	/*
	 * GetTopTransactionId() will fail for aborted tx, but we still need to
	 * finish it, so handle that manually.
	 */
	if (IsAbortedTransactionBlockState())
	{
		ret = PrepareTransactionBlock(gid);
		Assert(!ret);
		return false;
	}

	if (!IS_EXPLICIT_2PC_GID(gid))
	{
		UserAbortTransactionBlockCompat(false); /* see same call above */
		ereport(ERROR,
				(errcode(ERRCODE_INVALID_PARAMETER_VALUE),
				 errmsg("invalid transaction identifier \"%s\": identifiers starting with \"MTM-\" are used by multimaster internally",
						gid)));
	}

	/* prepare for cleanup */
	mtm_commit_state.gtx = NULL;
	mtm_commit_state.inside_commit_sequence = true;

	/*
	 * Mostly subcopy of MtmTwoPhaseCommit; take care to maintain both of them
	 */
	PG_TRY();
	{
		/* Exclude concurrent gen switchers, c.f. AcquirePBByHolder call site */
		AcquirePBByPreparer(true);

		/*
		 * xact is allowed iff we are MTM_GEN_ONLINE in current gen, but
		 * MtmGetCurrentGenStatus is more useful for error reporting.
		 */
		if (MtmGetCurrentStatusInGen() != MTM_GEN_ONLINE)
		{
			UserAbortTransactionBlockCompat(false); /* see same call above */
			ereport(MtmBreakConnection ? FATAL : ERROR,
					(errcode(ERRCODE_INTERNAL_ERROR),
					 errmsg("multimaster node is not online: current status \"%s\"",
							MtmNodeStatusMnem[MtmGetCurrentStatus(true, false)])));
		}

		xact_gen = MtmGetCurrentGen(true);
		xid = GetTopTransactionId();
		strncpy(mtm_commit_state.gid, gid, GIDSIZE);

		/* prepare transaction on our node */
		mtm_commit_state.gtx = GlobalTxAcquire(mtm_commit_state.gid, true,
											   false, NULL, 0);
		if (mtm_commit_state.gtx->prepared)
		{
			/*
			 * Vups, gtx with such gid already prepared -- gid
			 * collision. Disarm mtm_commit_cleanup and bail out.
			 */
			GlobalTxRelease(mtm_commit_state.gtx);
			mtm_commit_state.gtx = NULL;
			UserAbortTransactionBlockCompat(false); /* see same call above */
			ereport(ERROR,
					(errcode(ERRCODE_DUPLICATE_OBJECT),
					 errmsg("transaction identifier \"%s\" is already in use",
							gid)));
		}
		/*
		 * it is simpler to mark gtx originated here as orphaned from the
		 * beginning rather than in error handler; resolver won't touch gtx
		 * while it is locked on us anyway
		 */
		mtm_commit_state.gtx->orphaned = true;
		mtm_commit_state.gtx->xinfo.coordinator = mtm_cfg->my_node_id;
		mtm_commit_state.gtx->xinfo.xid = xid;
		mtm_commit_state.gtx->xinfo.gen_num = xact_gen.num;
		mtm_commit_state.gtx->xinfo.configured = xact_gen.configured;
		Assert(mtm_commit_state.gtx->state.status == GTXInvalid);

		sprintf(stream, "xid" XID_FMT, xid);
		dmq_stream_subscribe(stream);
		mtm_log(MtmTxTrace, "%s subscribed for %s", gid, stream);


		ret = PrepareTransactionBlockWithState3PC(
			gid,
			serialize_xstate(&mtm_commit_state.gtx->xinfo,
							 &mtm_commit_state.gtx->state));
		if (!ret)
		{
			mtm_commit_cleanup(0, Int32GetDatum(0));
			return false;
		}
		CommitTransactionCommand(); /* prepared */
		mtm_commit_state.gtx->prepared = true;
		ReleasePB(); /* don't hold generation switch anymore */
		mtm_log(MtmTxFinish, "TXFINISH: %s prepared at %X/%X",
				mtm_commit_state.gid,
				(uint32) (XactLastCommitEnd >> 32),
				(uint32) (XactLastCommitEnd));

		/*
		 * Allocate gather() and other stuff in dummy xact which we need
		 * anyway to avoid confusing xact.c machinery after exit
		 */
		StartTransactionCommand();

		/* collect all gen members acks */
		cohort = xact_gen.members;
		BIT_CLEAR(cohort, mtm_cfg->my_node_id - 1);
		ret = gather(cohort,
					 (MtmMessage **) p_messages, NULL, &n_messages,
					 PrepareGatherHook, TransactionIdGetDatum(xid),
					 NULL, xact_gen.num);

		if (!ret)
		{
			MtmGeneration new_gen = MtmGetCurrentGen(false);
			ereport(ERROR,
					(errcode(ERRCODE_INTERNAL_ERROR),
					 errmsg("[multimaster] failed to collect prepare acks due to generation switch: was num=" UINT64_FORMAT ", members=%s, now num=" UINT64_FORMAT ", members=%s",
							xact_gen.num,
							maskToString(xact_gen.members),
							new_gen.num,
							maskToString(new_gen.members))));
		}
		if (n_messages != popcount(cohort))
		{
			nodemask_t failed_cohort = cohort;
			for (i = 0; i < n_messages; i++)
				BIT_CLEAR(failed_cohort, p_messages[i]->node_id - 1);

			ereport(ERROR,
					(errcode(ERRCODE_CONNECTION_FAILURE),
					 errmsg("[multimaster] failed to collect prepare acks from nodemask %s due to network error",
							maskToString(failed_cohort))));
		}
		for (i = 0; i < n_messages; i++)
		{
			if (!p_messages[i]->prepared)
			{
				ereport(ERROR,
						(errcode(p_messages[i]->errcode),
						 errmsg("[multimaster] failed to prepare transaction %s at node %d",
								mtm_commit_state.gid, p_messages[i]->node_id),
						 errdetail("sqlstate %s (%s)",
								   unpack_sql_state(p_messages[i]->errcode),
								   p_messages[i]->errmsg)));
			}
		}
		/* good, everyone prepared */
		GlobalTxRelease(mtm_commit_state.gtx);
		mtm_commit_state.gtx = NULL;
		dmq_stream_unsubscribe();
		mtm_log(MtmTxTrace, "%s unsubscribed for %s", gid, stream);
		mtm_commit_state.inside_commit_sequence = false;
	}
	PG_CATCH();
	{
		mtm_commit_cleanup(0, Int32GetDatum(0));

		PG_RE_THROW();
	}
	PG_END_TRY();

	return true;
}

void
MtmExplicitFinishPrepared(bool isTopLevel, char *gid, bool isCommit)
{
	Mtm2AResponse *twoa_messages[MTM_MAX_NODES]; /* wow, great name */
	int			n_messages;
	GlobalTx volatile *gtx;
	nodemask_t cohort;
	char	   stream[DMQ_STREAM_NAME_MAXLEN];
	MtmGeneration gen;
	bool		  ret;
	int			  i;

	PreventInTransactionBlock(isTopLevel,
							  isCommit ? "COMMIT PREPARED" : "ROLLBACK PREPARED");

	gtx = GlobalTxAcquire(gid, false, false, NULL, 0);
	if (gtx == NULL)
		ereport(ERROR,
				(errcode(ERRCODE_UNDEFINED_OBJECT),
				 errmsg("prepared transaction with identifier \"%s\" does not exist",
						gid)));
	PG_TRY();
	{
		/*
		 * With MtmWaitPeerCommits we try to collect acks from current gen
		 * members (supposedly live nodes), but not too hard: in particular,
		 * we don't check current generation liveness (node might be in
		 * recovery if check in MtmBeginTransaction was ignored with mtm_admin
		 * or failure happened later). Doing so doesn't seem to be too bad.
		 */
		if (MtmWaitPeerCommits)
		{
			sprintf(stream, "xid" XID_FMT, gtx->xinfo.xid);
			dmq_stream_subscribe(stream);
		}

		FinishPreparedTransaction(gid, isCommit, false);
		gtx->state.status = isCommit ? GTXCommitted : GTXAborted;
		mtm_log(MtmTxFinish, "TXFINISH: %s %s", gid,
				isCommit ? "committed" : "aborted");
		GlobalTxRelease((GlobalTx *) gtx);
	}
	PG_CATCH();
	{
		GlobalTxRelease((GlobalTx *) gtx);

		PG_RE_THROW();
	}
	PG_END_TRY();

	if (!MtmWaitPeerCommits)
		return;

	gen = MtmGetCurrentGen(false);
	cohort = gen.members;
	BIT_CLEAR(cohort, mtm_cfg->my_node_id - 1);

	ret = gather(cohort,
				 (MtmMessage **) twoa_messages, NULL, &n_messages,
				 Paxos2AGatherHook, PointerGetDatum(gid),
				 NULL, gen.num);
	if (!ret)
	{
		MtmGeneration new_gen = MtmGetCurrentGen(false);
		ereport(WARNING,
				(errcode(ERRCODE_INTERNAL_ERROR),
				 errmsg("[multimaster] failed to collect %s acks of transaction %s due to generation switch: was num=" UINT64_FORMAT ", members=%s, now num=" UINT64_FORMAT ", members=%s",
						isCommit ? "commit" : "abort",
						gid,
						gen.num,
						maskToString(gen.members),
						new_gen.num,
						maskToString(new_gen.members))));
	}
	else if (n_messages != popcount(cohort))
	{
		nodemask_t failed_cohort = cohort;
		for (i = 0; i < n_messages; i++)
		{
			BIT_CLEAR(failed_cohort, twoa_messages[i]->node_id - 1);
		}
		ereport(WARNING,
				(errcode(ERRCODE_INTERNAL_ERROR),
				 errmsg("[multimaster] failed to collect %s acks of transaction %s at nodes %s due to network error",
						isCommit ? "commit" : "abort",
						gid,
						maskToString(failed_cohort))));
	}

	dmq_stream_unsubscribe();
	mtm_log(MtmTxTrace, "%s unsubscribed for %s", gid, stream);
}

/*
 * Allow replication in bgworker.
 * Needed for scheduler.
 */
void
MtmToggleReplication(void)
{
	force_in_bgworker = true;
}
