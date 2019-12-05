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
static bool committers_incremented;
static bool init_done;
static bool config_valid;
static bool inside_mtm_begin;

/*  XXX: change dmq api and avoid that */
static int	sender_to_node[MTM_MAX_NODES];
static MtmConfig *mtm_cfg;

MtmCurrentTrans MtmTx;

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

		case XACT_EVENT_ABORT:
			GlobalTxAtAbort(0,0);
			break;

		default:
			break;
	}

}

static void
backend_at_exit(int status, Datum arg)
{
	if (committers_incremented)
	{
		SpinLockAcquire(&Mtm->cb_lock);
		Mtm->n_committers -= 1;
		SpinLockRelease(&Mtm->cb_lock);
		committers_incremented = false;
		ConditionVariableBroadcast(&Mtm->commit_barrier_cv);
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
		on_shmem_exit(backend_at_exit, (Datum) 0);
		init_done = true;
	}

	AcceptInvalidationMessages();
	if (!config_valid)
	{
		mtm_cfg = MtmReloadConfig(mtm_cfg, attach_node, detach_node, (Datum) NULL);
		config_valid = true;
	}

	/* Reset MtmTx */
	MtmTx.contains_ddl = false;
	MtmTx.contains_dml = false;
	MtmTx.distributed = true;

	MtmDDLResetStatement();

	node_status = MtmGetCurrentStatus();

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
 * It should be unique for all nodes
 */
void
MtmGenerateGid(char *gid, TransactionId xid, int node_id)
{
	sprintf(gid, "MTM-%d-" XID_FMT, node_id, xid);
	return;
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
	return xid;
}

bool
MtmTwoPhaseCommit()
{
	uint64		participants;
	bool		ret;
	TransactionId xid;
	char		stream[DMQ_NAME_MAXLEN];
	char		gid[GIDSIZE];
	MtmTxResponse *messages[MTM_MAX_NODES];
	int			n_messages;
	int			i;
	GlobalTx   *gtx;

	if (!MtmTx.contains_ddl && !MtmTx.contains_dml)
		return false;

	if (!MtmTx.distributed)
		return false;

	if (!IsTransactionBlock())
	{
		BeginTransactionBlock(false);
		CommitTransactionCommand();
		StartTransactionCommand();
	}

	xid = GetTopTransactionId();
	MtmGenerateGid(gid, xid, mtm_cfg->my_node_id);
	sprintf(stream, "xid" XID_FMT, xid);
	dmq_stream_subscribe(stream);
	mtm_log(MtmTxTrace, "%s subscribed for %s", gid, stream);

	/*
	 * This lock is taken for a quite a long period of time but normally all
	 * callers lock it in shared mode, so it shouldn't be noticeable
	 * performance-wise.
	 *
	 * It is only used during startup of WalSender(node_id) in recovered mode
	 * to create a barrier after which all transactions doing our 3PC are
	 * guaranted to have seen participants with node_id enabled, so the
	 * receiver can apply them in parallel and be sure that precommit will not
	 * happens before node_id applies prepare.
	 *
	 * See also comments at the end of MtmReplicationStartupHook().
	 */
	PG_TRY();
	{
		/* Finish commit on a sudden disconnect */
		HOLD_INTERRUPTS();

		for (;;)
		{
			SpinLockAcquire(&Mtm->cb_lock);
			if (Mtm->n_commit_holders == 0)
			{
				Mtm->n_committers += 1;
				committers_incremented = true;
			}
			SpinLockRelease(&Mtm->cb_lock);

			if (committers_incremented)
				break;

			ConditionVariableSleep(&Mtm->commit_barrier_cv, PG_WAIT_EXTENSION);
		}
		ConditionVariableCancelSleep();

		participants = MtmGetEnabledNodeMask() &
			~((nodemask_t) 1 << (mtm_cfg->my_node_id - 1));

		/* prepare transaction on our node */
		gtx = GlobalTxAcquire(gid, true);
		ret = PrepareTransactionBlock(gid);
		if (!ret)
		{
			if (!MtmVolksWagenMode)
				mtm_log(WARNING, "Failed to prepare transaction %s", gid);

			Assert(committers_incremented);
			SpinLockAcquire(&Mtm->cb_lock);
			Mtm->n_committers -= 1;
			SpinLockRelease(&Mtm->cb_lock);
			committers_incremented = false;
			ConditionVariableBroadcast(&Mtm->commit_barrier_cv);

			// or just throw an error by ourselves?
			gtx->state.status = GTXAborted;
			GlobalTxRelease(gtx);
			return true;
		}
		mtm_log(MtmTxFinish, "TXFINISH: %s prepared", gid);
		AllowTempIn2PC = true;
		CommitTransactionCommand();
		gtx->state.status = GTXPrepared;
		gather(participants, messages, &n_messages);
		dmq_stream_unsubscribe(stream);

		/* check prepare responses */
		if (n_messages != popcount(participants))
		{
			ereport(ERROR,
					(errcode(messages[i]->errcode),
						errmsg("[multimaster] commit sequence interrupted due to a network failure")));
		}
		for (i = 0; i < n_messages; i++)
		{
			Assert(messages[i]->status == GTXPrepared || messages[i]->status == GTXAborted);
			Assert(term_cmp(messages[i]->term, (GlobalTxTerm) {1, 0}) == 0);

			if (messages[i]->status == GTXAborted)
			{
				FinishPreparedTransaction(gid, false, false);
				gtx->state.status = GTXAborted;
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

		/* ok, we have all prepare responses, precommit */
		dmq_stream_subscribe(gid);
		SetPreparedTransactionState(gid,
			serialize_gtx_state(GTXPreCommitted, (GlobalTxTerm) {1,0}, (GlobalTxTerm) {1,0}),
			false);
		gtx->state.status = GTXPreCommitted;
		gtx->state.accepted = (GlobalTxTerm) {1, 0};
		mtm_log(MtmTxFinish, "TXFINISH: %s precommitted", gid);
		gather(participants, messages, &n_messages);

		/* we have majority precommits, commit */
		StartTransactionCommand();
		FinishPreparedTransaction(gid, true, false);
		gtx->state.status = GTXCommitted;
		mtm_log(MtmTxFinish, "TXFINISH: %s committed", gid);
		/* XXX: make this conditional */
		gather(participants, messages, &n_messages);

		RESUME_INTERRUPTS();
	}
	PG_CATCH();
	{
		if (committers_incremented)
		{
			SpinLockAcquire(&Mtm->cb_lock);
			Mtm->n_committers -= 1;
			SpinLockRelease(&Mtm->cb_lock);
			committers_incremented = false;
			ConditionVariableBroadcast(&Mtm->commit_barrier_cv);
		}

		gtx->orphaned = true;
		GlobalTxRelease(gtx);
		PG_RE_THROW();
	}
	PG_END_TRY();

	Assert(committers_incremented);
	SpinLockAcquire(&Mtm->cb_lock);
	Mtm->n_committers -= 1;
	SpinLockRelease(&Mtm->cb_lock);
	committers_incremented = false;
	ConditionVariableBroadcast(&Mtm->commit_barrier_cv);

	GlobalTxRelease(gtx);
	dmq_stream_unsubscribe(gid);
	mtm_log(MtmTxTrace, "%s unsubscribed for %s", gid, gid);

	MaybeLogSyncpoint();

	return true;
}

void
gather(uint64 participants, MtmTxResponse **messages, int *msg_count, bool ignore_mtm_disabled)
{
	*msg_count = 0;
	while (participants != 0)
	{
		bool		ret;
		DmqSenderId sender_id;
		StringInfoData msg;
		int			rc;
		bool		wait;

		ret = dmq_pop_nb(&sender_id, &msg, participants, &wait);
		if (ret)
		{
			MtmMessage *raw_msg;

			raw_msg = MtmMesageUnpack(&msg);
			messages[*msg_count] = (MtmTxResponse *) raw_msg;
			// Assert(raw_msg->tag == T_MtmTxResponse);
			// Assert(messages[*msg_count]->node_id == sender_to_node[sender_id]);

			(*msg_count)++;
			BIT_CLEAR(participants, sender_to_node[sender_id] - 1);

			mtm_log(MtmTxTrace,
					"gather: got message from node%d",
					sender_to_node[sender_id]);
		}
		else
		{
			/*
			 * If queue is detached then the neignbour node is probably
			 * disconnected. Let's wait when it became disabled as we can
			 * became offline by this time.
			 */
			int			i;
			nodemask_t	enabled = MtmGetEnabledNodeMask(ignore_mtm_disabled);

			for (i = 0; i < MTM_MAX_NODES; i++)
			{
				if (BIT_CHECK(participants, i) && !BIT_CHECK(enabled, i))
				{
					BIT_CLEAR(participants, i);
					mtm_log(MtmTxTrace,
							"GatherPrecommit: dropping node%d from tx participants",
							i + 1);
				}
			}
		}

		if (wait)
		{
			/* XXX cache that */
			rc = WaitLatch(MyLatch, WL_LATCH_SET | WL_TIMEOUT, 100.0,
						   PG_WAIT_EXTENSION);

			if (rc & WL_POSTMASTER_DEATH)
				proc_exit(1);

			if (rc & WL_LATCH_SET)
				ResetLatch(MyLatch);
		}
	}
}

bool
MtmExplicitPrepare(char *gid)
{
	nodemask_t	participants;
	bool		ret;
	TransactionId xid;
	char		stream[DMQ_NAME_MAXLEN];
	MtmTxResponse messages[MTM_MAX_NODES];
	int			n_messages;
	int			i;

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

	participants = MtmGetEnabledNodeMask() &
		~((nodemask_t) 1 << (mtm_cfg->my_node_id - 1));

	ret = PrepareTransactionBlock(gid);
	if (!ret)
		return false;

	CommitTransactionCommand();

	mtm_log(MtmTxFinish, "TXFINISH: %s prepared", gid);

	gather(participants, messages, &n_messages);
	dmq_stream_unsubscribe(stream);

	for (i = 0; i < n_messages; i++)
	{
		Assert(messages[i].status == GTXPrepared || messages[i].status == GTXAborted);

		if (messages[i].status == GTXAborted)
		{
			StartTransactionCommand();
			FinishPreparedTransaction(gid, false, false);
			mtm_log(MtmTxFinish, "TXFINISH: %s aborted", gid);

			if (MtmVolksWagenMode)
				ereport(ERROR,
						(errcode(messages[i].errcode),
						 errmsg("[multimaster] failed to prepare transaction at peer node")));
			else
				ereport(ERROR,
						(errcode(messages[i].errcode),
						 errmsg("[multimaster] failed to prepare transaction %s at node %d",
								gid, messages[i].node_id),
						 errdetail("sqlstate %s (%s)",
								   unpack_sql_state(messages[i].errcode), messages[i].errmsg)));
		}
	}

	StartTransactionCommand();

	return true;
}

void
MtmExplicitFinishPrepared(bool isTopLevel, char *gid, bool isCommit)
{
	nodemask_t	participants;
	MtmTxResponse *messages[MTM_MAX_NODES];
	int			n_messages;

	PreventInTransactionBlock(isTopLevel,
							  isCommit ? "COMMIT PREPARED" : "ROLLBACK PREPARED");

	if (isCommit)
	{
		dmq_stream_subscribe(gid);

		participants = MtmGetEnabledNodeMask() &
			~((nodemask_t) 1 << (mtm_cfg->my_node_id - 1));

		SetPreparedTransactionState(gid, MULTIMASTER_PRECOMMITTED, false);
		mtm_log(MtmTxFinish, "TXFINISH: %s precommitted", gid);
		gather(participants, messages, &n_messages);

		FinishPreparedTransaction(gid, true, false);

		/* XXX: make this conditional */
		mtm_log(MtmTxFinish, "TXFINISH: %s committed", gid);
		gather(participants, messages, &n_messages);

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
