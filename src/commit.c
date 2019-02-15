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
#include "access/transam.h"
#include "storage/proc.h"
#include "utils/guc.h"
#include "utils/syscache.h"
#include "utils/inval.h"
#include "miscadmin.h"
#include "commands/dbcommands.h"
#include "catalog/pg_subscription.h"
#include "tcop/tcopprot.h"
#include "postmaster/autovacuum.h"

#include "multimaster.h"
#include "logger.h"
#include "ddl.h"
#include "state.h"
#include "syncpoint.h"

static bool	force_in_bgworker;

static bool	subchange_cb_registered;
static bool	config_valid;
// XXX: change dmq api and avoid that
static int	sender_to_node[MTM_MAX_NODES];
static MtmConfig *mtm_cfg;

MtmCurrentTrans MtmTx;

static bool GatherPrepares(TransactionId xid, nodemask_t participantsMask,
						   int *failed_at);
static void GatherPrecommits(TransactionId xid, nodemask_t participantsMask,
						     MtmMessageCode code);

static void
pubsub_change_cb(Datum arg, int cacheid, uint32 hashvalue)
{
	config_valid = false;
}

static void
attach_node(int node_id, MtmConfig *new_cfg, Datum arg)
{
	int sender_id = dmq_attach_receiver(psprintf(MTM_DMQNAME_FMT, node_id),
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
MtmXactCallback2(XactEvent event, void *arg)
{
	/*
	 * Perform distributed commit only for transactions in ordinary
	 * backends with multimaster enabled.
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

void
MtmBeginTransaction()
{
	MtmNodeStatus	node_status;

	// XXX: clean MtmTx on commit and check on begin that it is clean.
	// That should unveil probable issues with subxacts.

	if (!MtmIsEnabled())
	{
		MtmTx.distributed = false;
		return;
	}

	if (!subchange_cb_registered)
	{
		/* Keep us informed about subscription changes. */
		CacheRegisterSyscacheCallback(SUBSCRIPTIONOID,
								  pubsub_change_cb,
								  (Datum) 0);
		CacheRegisterSyscacheCallback(PUBLICATIONOID,
								  pubsub_change_cb,
								  (Datum) 0);
		subchange_cb_registered = true;
	}

	AcceptInvalidationMessages();
	if (!config_valid)
	{
		mtm_cfg = MtmReloadConfig(mtm_cfg, attach_node, detach_node, (Datum) NULL);
		config_valid = true;
	}

	/* Reset MtmTx */
	MtmTx.explicit_twophase = false;
	MtmTx.contains_ddl = false; // will be set by executor hook
	MtmTx.contains_dml = false;
	MtmTx.gid[0] = '\0';
	MtmTx.accessed_temp = false;
	MtmTx.distributed = true;

	MtmDDLResetStatement();

	node_status = MtmGetCurrentStatus();

	/* Application name can be changed using PGAPPNAME environment variable */
	if (node_status != MTM_ONLINE
		&& strcmp(application_name, MULTIMASTER_ADMIN) != 0
		&& strcmp(application_name, MULTIMASTER_BROADCAST_SERVICE) != 0)
	{
		/* Reject all user's transactions at offline cluster.
		 * Allow execution of transaction by bg-workers to makeit possible to perform recovery.
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
}

bool
MtmTwoPhaseCommit()
{
	nodemask_t participantsMask;
	bool	ret;
	int		failed_at = 0;
	TransactionId xid;
	char	stream[DMQ_NAME_MAXLEN];
	pgid_t  gid;

	if (!MtmTx.contains_ddl && !MtmTx.contains_dml)
		return false;

	if (!MtmTx.distributed)
		return false;

	if (MtmTx.accessed_temp)
	{
		if (MtmVolksWagenMode)
			return false;
		else
			mtm_log(ERROR, "Transaction accessed both temporary and replicated table, can't prepare");
	}

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
	 * This lock is taken for a quite a long period of time but normally
	 * all callers lock it in shared mode, so it shouldn't be noticeable
	 * performance-wise.
	 *
	 * It is only used during startup of WalSender(node_id) in recovered mode
	 * to create a barrier after which all transactions doing our 3PC are
	 * guaranted to have seen participantsMask with node_id enabled, so the
	 * receiver can apply them in parallel and be sure that precommit will
	 * not happens before node_id applies prepare.
	 *
	 * See also comments at the end of MtmReplicationStartupHook().
	 */
	while (Mtm->stop_new_commits)
		MtmSleep(USECS_PER_SEC);

	LWLockAcquire(MtmCommitBarrier, LW_SHARED);

	participantsMask = MtmGetEnabledNodeMask() &
						~((nodemask_t)1 << (mtm_cfg->my_node_id-1));

	ret = PrepareTransactionBlock(gid);
	if (!ret)
	{
		if (!MtmVolksWagenMode)
			mtm_log(WARNING, "Failed to prepare transaction %s", gid);
		return true;
	}
	mtm_log(MtmTxFinish, "TXFINISH: %s prepared", gid);
	CommitTransactionCommand();

	ret = GatherPrepares(xid, participantsMask, &failed_at);
	if (!ret)
	{
		dmq_stream_unsubscribe(stream);
		FinishPreparedTransaction(gid, false, false);
		mtm_log(MtmTxFinish, "TXFINISH: %s aborted", gid);
		mtm_log(ERROR, "Failed to prepare transaction %s at node %d",
						gid, failed_at);
	}

	SetPreparedTransactionState(gid, MULTIMASTER_PRECOMMITTED);
	mtm_log(MtmTxFinish, "TXFINISH: %s precommitted", gid);
	GatherPrecommits(xid, participantsMask, MSG_PRECOMMITTED);

	StartTransactionCommand();
	FinishPreparedTransaction(gid, true, false);
	mtm_log(MtmTxFinish, "TXFINISH: %s committed", gid);
	GatherPrecommits(xid, participantsMask, MSG_COMMITTED);

	LWLockRelease(MtmCommitBarrier);

	dmq_stream_unsubscribe(stream);
	mtm_log(MtmTxTrace, "%s unsubscribed for %s", gid, stream);

	MaybeLogSyncpoint(false);

	return true;
}

static bool
GatherPrepares(TransactionId xid, nodemask_t participantsMask, int *failed_at)
{
	bool prepared = true;

	while (participantsMask != 0)
	{
		bool ret;
		DmqSenderId sender_id;
		StringInfoData buffer;
		MtmArbiterMessage *msg;

		ret = dmq_pop(&sender_id, &buffer, participantsMask);

		if (ret)
		{
			msg = (MtmArbiterMessage *) buffer.data;

			Assert(msg->node == sender_to_node[sender_id]);
			Assert(msg->code == MSG_PREPARED || msg->code == MSG_ABORTED);
			Assert(msg->dxid == xid);
			Assert(BIT_CHECK(participantsMask, sender_to_node[sender_id] - 1));

			mtm_log(MtmTxTrace,
				"GatherPrepares: got '%s' for tx" XID_FMT " from node%d",
				msg->code == MSG_PREPARED ? "ok" : "failed",
				xid, sender_to_node[sender_id]);

			BIT_CLEAR(participantsMask, sender_to_node[sender_id] - 1);

			if (msg->code == MSG_ABORTED)
			{
				prepared = false;
				*failed_at = msg->node;
			}
		}
		else
		{
			/*
			 * If queue is detached then the neignbour node is probably
			 * disconnected. Let's wait when it became disabled as we can
			 * became offline by this time.
			 */
			if (!BIT_CHECK(MtmGetEnabledNodeMask(), sender_to_node[sender_id] - 1))
			{
				BIT_CLEAR(participantsMask, sender_to_node[sender_id] - 1);
				mtm_log(MtmTxTrace,
					"GatherPrepares: dropping node%d from participants of tx" XID_FMT,
					sender_to_node[sender_id], xid);
			}
		}
	}

	// XXX: assert that majority has responded

	return prepared;
}

static void
GatherPrecommits(TransactionId xid, nodemask_t participantsMask, MtmMessageCode code)
{
	while (participantsMask != 0)
	{
		bool ret;
		DmqSenderId sender_id;
		StringInfoData buffer;
		MtmArbiterMessage *msg;

		ret = dmq_pop(&sender_id, &buffer, participantsMask);

		if (ret)
		{
			msg = (MtmArbiterMessage *) buffer.data;

			Assert(msg->node == sender_to_node[sender_id]);
			Assert(msg->code == code);
			Assert(msg->dxid == xid);
			Assert(BIT_CHECK(participantsMask, sender_to_node[sender_id] - 1));

			mtm_log(MtmTxTrace,
				"GatherPrecommits: got 'ok' for tx" XID_FMT " from node%d",
				xid, sender_to_node[sender_id]);

			BIT_CLEAR(participantsMask, sender_to_node[sender_id] - 1);
		}
		else
		{
			/*
			 * If queue is detached then the neignbour node is probably
			 * disconnected. Let's wait when it became disabled as we can
			 * became offline by this time.
			 */
			if (!BIT_CHECK(MtmGetEnabledNodeMask(), sender_to_node[sender_id] - 1))
			{
				BIT_CLEAR(participantsMask, sender_to_node[sender_id] - 1);
				mtm_log(MtmTxTrace,
					"GatherPrecommit: dropping node%d from participants of tx" XID_FMT,
					sender_to_node[sender_id], xid);
			}
		}
	}

	// XXX: assert that majority has responded
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

