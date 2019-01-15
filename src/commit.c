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

static bool	backend_init_done;
static int	sender_to_node[MTM_MAX_NODES];
static MtmConfig *mtm_cfg;

MtmCurrentTrans MtmTx;

static bool GatherPrepares(TransactionId xid, nodemask_t participantsMask,
						   int *failed_at);
static void GatherPrecommits(TransactionId xid, nodemask_t participantsMask,
						     MtmMessageCode code);

static void
backend_init()
{
	int		i,
			sender_id = 0;

	/* load mm config */
	mtm_cfg = MtmLoadConfig();

	/* attach ourself to receiver queues and fill out sender_to_node[] */
	for (i = 0; i < mtm_cfg->n_nodes; i++)
	{
		if (i + 1 == mtm_cfg->my_node_id)
			continue;
		dmq_attach_receiver(psprintf("node%d", i + 1), i);
		sender_to_node[sender_id++] = i + 1;
	}
}

void
MtmXactCallback2(XactEvent event, void *arg)
{
	/*
	 * Perform distributed commit only for transactions in ordinary
	 * backends with multimaster enabled.
	 */
	if (IsAnyAutoVacuumProcess() || !IsNormalProcessingMode() ||
		am_walsender || IsBackgroundWorker)
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
	// XXX: clean MtmTx on commit and check on begin that it is clean.
	// That should unveil probable issues with subxacts.

	if (!MtmIsEnabled())
	{
		MtmTx.distributed = false;
		return;
	}

	// XXX: reload on new node. (origin inval callback?)
	if (!backend_init_done)
	{
		backend_init();
		backend_init_done = true;
	}

	/* Reset MtmTx */
	MtmTx.explicit_twophase = false;
	MtmTx.contains_ddl = false; // will be set by executor hook
	MtmTx.contains_dml = false;
	MtmTx.gid[0] = '\0';
	MtmTx.accessed_temp = false;
	MtmTx.distributed = true;

	MtmDDLResetStatement();


	/* Application name can be changed using PGAPPNAME environment variable */
	if (Mtm->status != MTM_ONLINE
		&& strcmp(application_name, MULTIMASTER_ADMIN) != 0
		&& strcmp(application_name, MULTIMASTER_BROADCAST_SERVICE) != 0)
	{
		/* Reject all user's transactions at offline cluster.
		 * Allow execution of transaction by bg-workers to makeit possible to perform recovery.
		 */
		mtm_log(ERROR,
				"Multimaster node is not online: current status %s",
				MtmNodeStatusMnem[Mtm->status]);
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

	MtmLock(LW_SHARED);
	participantsMask = (((nodemask_t)1 << mtm_cfg->n_nodes) - 1) &
								  ~Mtm->disabledNodeMask &
								  ~((nodemask_t)1 << (mtm_cfg->my_node_id-1));
	if (Mtm->status != MTM_ONLINE)
		mtm_log(ERROR, "This node became offline during current transaction");
	MtmUnlock();

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

	MaybeLogSyncpoint();

	return true;
}

static bool
GatherPrepares(TransactionId xid, nodemask_t participantsMask, int *failed_at)
{
	bool prepared = true;

	Assert(participantsMask != 0);

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
			MtmLock(LW_SHARED);
			if (BIT_CHECK(Mtm->disabledNodeMask, sender_to_node[sender_id] - 1))
			{
				if (Mtm->status != MTM_ONLINE)
				{
					elog(ERROR, "our node was disabled during transaction commit");
				}
				else
				{
					BIT_CLEAR(participantsMask, sender_to_node[sender_id] - 1);
					mtm_log(MtmTxTrace,
						"GatherPrepares: dropping node%d from participants of tx" XID_FMT,
						sender_to_node[sender_id], xid);
					prepared = false;
					*failed_at = sender_to_node[sender_id];
				}
			}
			MtmUnlock();
		}
	}

	// XXX: assert that majority has responded

	return prepared;
}

static void
GatherPrecommits(TransactionId xid, nodemask_t participantsMask, MtmMessageCode code)
{
	Assert(participantsMask != 0);

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
			MtmLock(LW_SHARED);
			if (BIT_CHECK(Mtm->disabledNodeMask, sender_to_node[sender_id] - 1))
			{
				if (Mtm->status != MTM_ONLINE)
				{
					elog(ERROR, "our node was disabled during transaction commit");
				}
				else
				{
					BIT_CLEAR(participantsMask, sender_to_node[sender_id] - 1);
					mtm_log(MtmTxTrace,
						"GatherPrecommit: dropping node%d from participants of tx" XID_FMT,
						sender_to_node[sender_id], xid);
				}
			}
			MtmUnlock();
		}
	}

	// XXX: assert that majority has responded
}
