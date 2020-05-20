/*----------------------------------------------------------------------------
 *
 * commit.c
 *		Replace ordinary commit with 3PC.
 *
 * Portions Copyright (c) 1996-2018, PostgreSQL Global Development Group
 * Portions Copyright (c) 1994, Regents of the University of California
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
	char stream_name[DMQ_NAME_MAXLEN];
	bool subscribed;
	char gid[GIDSIZE];
	GlobalTx *gtx;
	bool abort_prepare; /* whether cleanup can (should) abort xact immediately */
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
	/* 0 is ERROR, 1 is on exit hook */
	bool on_exit = DatumGetInt32(arg) == 1;

	ReleasePB();

	if (mtm_commit_state.subscribed)
	{
		dmq_stream_unsubscribe(mtm_commit_state.stream_name);
		mtm_commit_state.subscribed = false;
	}

	if (mtm_commit_state.gtx != NULL)
	{
		/*
		 * This crutchy dance matters if this is proc exit (FATAL in the
		 * middle of commit sequence): global_tx.c automatically releases gtx,
		 * and it would be really not nice to rely on the order in which the
		 * hooks are called. So reacquire the gtx if it was already released.
		 */
		Assert(GetMyGlobalTx() != NULL || on_exit);
		if (GetMyGlobalTx() == NULL)
			mtm_commit_state.gtx = GlobalTxAcquire(mtm_commit_state.gid,
												   false);
		/*
		 * If we managed to prepare the xact, tell resolver to deal with it
		 */
		if (mtm_commit_state.gtx != NULL && mtm_commit_state.gtx->prepared)
		{
			/*
			 * If there were no precommit, xact will definitely be aborted. We
			 * could abort it right here, but this requires very careful
			 * acting:
			 * 1) who would release gtx if we fail during
			 *    FinishPreparedTransaction?
			 *
			 * 2) Generally FinishPreparedTransaction and
			 *    SetPreparedTransactionState should run in xact, i.e.
			 *    surrounded by
			 *    StartTransactionCommand/CommitTransactionCommand.
			 *    e.g. LockGXact will fail if I am not the owner of
			 *    gxact. However, some hops are needed to avoid entering
			 *    MtmTwoPhaseCommit/MtmBeginTransaction twice. In particular,
			 *    erroring out from MtmBeginTransaction on attempt to abort
			 *    xact after we got network error because, well, we are
			 *    isolated after network error is really unpleasant. This
			 *    relates to all
			 *    StartTransactionCommand/CommitTransactionCommand in this
			 *    file.
			 *
			 * As a sort of compromise, we could instead add the 'this is my
			 * orphaned prepare => directly abort it' logic to resolver.
			 */
			if (term_cmp(mtm_commit_state.gtx->state.accepted,
						 InvalidGTxTerm) == 0)
			{
				ereport(WARNING,
						(errcode(ERRCODE_INTERNAL_ERROR),
						 errmsg("[multimaster] exiting commit sequence of transaction %s which will be aborted",
								mtm_commit_state.gid)));

			}
			else
			{
				ereport(WARNING,
						(errcode(ERRCODE_TRANSACTION_RESOLUTION_UNKNOWN),
						 errmsg("[multimaster] exiting commit sequence of transaction %s with unknown status",
								mtm_commit_state.gid),
						 errdetail("The transaction will be committed or aborted later.")));
			}
			mtm_commit_state.gtx->orphaned = true;
		}
		if (mtm_commit_state.gtx != NULL)
			GlobalTxRelease(mtm_commit_state.gtx);
		mtm_commit_state.gtx = NULL;
		ResolverWake();
	}
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
		on_shmem_exit(mtm_commit_cleanup, Int32GetDatum(1));
		init_done = true;
	}

	AcceptInvalidationMessages();
	if (!config_valid)
	{
		mtm_cfg = MtmReloadConfig(mtm_cfg, attach_node, detach_node, (Datum) NULL);
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
 * Genenerate global transaction identifier for two-phase commit.
 * It should be unique for all nodes.
 * MTM-node_id-xid is part ensuring uniqueness; the rest is necessary payload.
 * gen_num identifies the number of generation xact belongs to.
 * configured is mask of configured nodes of this generation; it is required
 * by resolver.
 * (in theory we could get rid of it if we remembered generation history for
 *  some time, but we don't currently)
 *
 * Beware that GlobalTxGCInTableProposals parses gid from SQL.
 *
 * TODO: add version and kinda backwards compatibility.
 */
void
MtmGenerateGid(char *gid, int node_id, TransactionId xid, uint64 gen_num,
			   nodemask_t configured)
{
	sprintf(gid, "MTM-%d-" XID_FMT "-%" INT64_MODIFIER "X-%" INT64_MODIFIER "X",
			node_id, xid, gen_num, configured);
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
		BeginTransactionBlock(false);
		CommitTransactionCommand();
		StartTransactionCommand();
	}

	/* prepare for cleanup */
	mtm_commit_state.subscribed = false;
	mtm_commit_state.gtx = NULL;
	mtm_commit_state.abort_prepare = false;
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
		AcquirePBByPreparer();

		/*
		 * xact is allowed iff we are MTM_GEN_ONLINE in current gen, but
		 * MtmGetCurrentGenStatus is more useful for error reporting.
		 */
		if (MtmGetCurrentStatusInGen() != MTM_GEN_ONLINE)
		{
			ereport(MtmBreakConnection ? FATAL : ERROR,
					(errcode(ERRCODE_INTERNAL_ERROR),
					 errmsg("multimaster node is not online: current status \"%s\"",
							MtmNodeStatusMnem[MtmGetCurrentStatus(true, false)])));
		}

		xact_gen = MtmGetCurrentGen(true);
		xid = GetTopTransactionId();
		MtmGenerateGid(mtm_commit_state.gid, mtm_cfg->my_node_id, xid,
					   xact_gen.num, xact_gen.configured);
		sprintf(mtm_commit_state.stream_name, "xid" XID_FMT, xid);
		dmq_stream_subscribe(mtm_commit_state.stream_name);
		mtm_commit_state.subscribed = true;
		mtm_log(MtmTxTrace, "%s subscribed for %s", mtm_commit_state.gid,
				mtm_commit_state.stream_name);

		/* prepare transaction on our node */

		mtm_commit_state.gtx = GlobalTxAcquire(mtm_commit_state.gid, true);
		Assert(mtm_commit_state.gtx->state.status == GTXInvalid);
		/*
		 * PREPARE doesn't happen here; ret 0 just means we were already in
		 * aborted transaction block and we expect the callee to handle this.
		 */
		ret = PrepareTransactionBlock(mtm_commit_state.gid);
		if (!ret)
		{
			Assert(false);
			elog(PANIC, "unexpected PrepareTransactionBlock failure");
		}

		AllowTempIn2PC = true;
		CommitTransactionCommand(); /* here we actually PrepareTransaction */
		ReleasePB(); /* don't hold generation switch anymore */
		mtm_commit_state.gtx->prepared = true;
		/* end_lsn of PREPARE */
		mtm_log(MtmTxFinish, "TXFINISH: %s prepared at %X/%X",
				mtm_commit_state.gid,
				(uint32) (XactLastCommitEnd >> 32),
				(uint32) (XactLastCommitEnd));

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
			mtm_commit_state.abort_prepare = true;
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
			mtm_commit_state.abort_prepare = true;

			ereport(ERROR,
					(errcode(ERRCODE_CONNECTION_FAILURE),
					 errmsg("[multimaster] failed to collect prepare acks from nodemask %s due to network error",
							maskToString(failed_cohort))));
		}
		for (i = 0; i < n_messages; i++)
		{
			if (!p_messages[i]->prepared)
			{
				mtm_commit_state.abort_prepare = true;
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
		SetPreparedTransactionState(mtm_commit_state.gid,
			serialize_gtx_state(GTXPreCommitted,
								InitialGTxTerm,
								InitialGTxTerm),
			false);
		/*
		 * since this moment direct aborting is not allowed; others can
		 * receive our precommit and resolve xact to commit without us
		 */
		mtm_commit_state.gtx->state.status = GTXPreCommitted;
		mtm_commit_state.gtx->state.accepted = (GlobalTxTerm) {1, 0};
		mtm_log(MtmTxFinish, "TXFINISH: %s precommitted", mtm_commit_state.gid);

		/*
		 * Just skip precommit tour if I am online in my referee gen,
		 * i.e. working alone. We actually could do direct commits without 2PC
		 * as an optimization...
		 */
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
		pc_success_cohort = 0;
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
		dmq_stream_unsubscribe(mtm_commit_state.stream_name);
		mtm_commit_state.subscribed = false;
		mtm_log(MtmTxTrace, "%s unsubscribed for %s",
				mtm_commit_state.gid, mtm_commit_state.stream_name);
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

/* TODO: explicit prepares are not adapted to generations yet, i.e. broken */
bool
MtmExplicitPrepare(char *gid)
{
	nodemask_t	participants;
	bool		ret;
	TransactionId xid;
	char		stream[DMQ_NAME_MAXLEN];
	MtmPrepareResponse *messages[MTM_MAX_NODES];
	int			n_messages;
	int			i;

	elog(ERROR, "mtm doesn't support user 2PC");

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

	xid = GetTopTransactionId();
	sprintf(stream, "xid" XID_FMT, xid);
	dmq_stream_subscribe(stream);
	mtm_log(MtmTxTrace, "%s subscribed for %s", gid, stream);

	participants = MtmGetEnabledNodeMask(false) &
		~((nodemask_t) 1 << (mtm_cfg->my_node_id - 1));

	ret = PrepareTransactionBlock(gid);
	if (!ret)
		return false;

	CommitTransactionCommand();

	mtm_log(MtmTxFinish, "TXFINISH: %s prepared", gid);

	gather(participants,
		   (MtmMessage **) messages, NULL, &n_messages,
		   NULL, 0,
		   NULL, 0);
	dmq_stream_unsubscribe(stream);

	for (i = 0; i < n_messages; i++)
	{
		/* Assert(messages[i]->status == GTXPrepared || messages[i]->status == GTXAborted); */

		if (!messages[i]->prepared)
		{
			StartTransactionCommand();
			FinishPreparedTransaction(gid, false, false);
			mtm_log(MtmTxFinish, "TXFINISH: %s aborted", gid);

			if (MtmVolksWagenMode)
				ereport(ERROR,
						(errcode(messages[i]->errcode),
						 errmsg("[multimaster] failed to prepare transaction at peer node")));
			else
				ereport(ERROR,
						(errcode(messages[i]->errcode),
						 errmsg("[multimaster] failed to prepare transaction %s at node %d",
								gid, messages[i]->node_id),
						 errdetail("sqlstate %s (%s)",
								   unpack_sql_state(messages[i]->errcode), messages[i]->errmsg)));
		}
	}

	StartTransactionCommand();

	return true;
}

void
MtmExplicitFinishPrepared(bool isTopLevel, char *gid, bool isCommit)
{
	nodemask_t	participants;
	Mtm2AResponse *messages[MTM_MAX_NODES];
	int			n_messages;

	PreventInTransactionBlock(isTopLevel,
							  isCommit ? "COMMIT PREPARED" : "ROLLBACK PREPARED");

	elog(ERROR, "mtm doesn't support user 2PC");

	if (isCommit)
	{
		dmq_stream_subscribe(gid);

		participants = MtmGetEnabledNodeMask(false) &
			~((nodemask_t) 1 << (mtm_cfg->my_node_id - 1));

		SetPreparedTransactionState(gid, MULTIMASTER_PRECOMMITTED, false);
		mtm_log(MtmTxFinish, "TXFINISH: %s precommitted", gid);
		gather(participants,
			   (MtmMessage **) messages, NULL, &n_messages,
			   NULL, 0,
			   NULL, 0);

		FinishPreparedTransaction(gid, true, false);

		/* XXX: make this conditional */
		mtm_log(MtmTxFinish, "TXFINISH: %s committed", gid);
		gather(participants,
			   (MtmMessage **) messages, NULL, &n_messages,
			   NULL, 0,
			   NULL, 0);

		dmq_stream_unsubscribe(gid);
	}
	else
	{
		FinishPreparedTransaction(gid, false, false);
		mtm_log(MtmTxFinish, "TXFINISH: %s abort", gid);
	}
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
