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
#include "multimaster.h"
#include "storage/proc.h"
#include "utils/guc.h"
#include "miscadmin.h"
#include "commands/dbcommands.h"
#include "tcop/tcopprot.h"

#include "logger.h"

static Oid		MtmDatabaseId;
static bool		DmqSubscribed;
static int		sender_to_node[MTM_MAX_NODES];

MtmCurrentTrans MtmTx;

static void MtmBeginTransaction(MtmCurrentTrans* x);
static void MtmPrePrepareTransaction(MtmCurrentTrans* x);

static bool GatherPrepares(MtmCurrentTrans* x, nodemask_t participantsMask,
						   int *failed_at);
static void GatherPrecommits(MtmCurrentTrans* x, nodemask_t participantsMask);


void
MtmXactCallback2(XactEvent event, void *arg)
{
	if (!MtmIsLogicalReceiver)
	{
		switch (event)
		{
			case XACT_EVENT_START:
				MtmBeginTransaction(&MtmTx);
				break;
			case XACT_EVENT_PRE_PREPARE:
				MtmPrePrepareTransaction(&MtmTx);
				break;
			case XACT_EVENT_COMMIT_COMMAND:
				if (!MtmTx.isTransactionBlock && !IsSubTransaction())
					MtmTwoPhaseCommit(&MtmTx);
				break;
			default:
				break;
		}
	}
}

static void
MtmBeginTransaction(MtmCurrentTrans* x)
{
	// XXX: move it down the callbacks?
	x->isDistributed = MtmIsUserTransaction();
	x->containsDML = false; // will be set by executor hook
	x->isTransactionBlock = IsTransactionBlock();

	/* Application name can be changed using PGAPPNAME environment variable */
	if (x->isDistributed && Mtm->status != MTM_ONLINE
		&& strcmp(application_name, MULTIMASTER_ADMIN) != 0
		&& strcmp(application_name, MULTIMASTER_BROADCAST_SERVICE) != 0
		&& debug_query_string && pg_strcasecmp(debug_query_string, "create extension multimaster;") != 0)
	{
		/* Reject all user's transactions at offline cluster.
		 * Allow execution of transaction by bg-workers to makeit possible to perform recovery.
		 */
		MTM_ELOG(ERROR,
				"Multimaster node is not online: current status %s",
				MtmNodeStatusMnem[Mtm->status]);
	}
}

static void
MtmPrePrepareTransaction(MtmCurrentTrans* x)
{
	if (!x->isDistributed)
		return;

	if (!MtmDatabaseId)
		MtmDatabaseId = get_database_oid(MtmDatabaseName, false);

	if (MtmDatabaseId != MyDatabaseId)
		mtm_log(ERROR,
			"Refusing to work. Multimaster configured to work with database '%s'",
			MtmDatabaseName);

	Assert(TransactionIdIsValid(GetCurrentTransactionId()));
}

bool // XXX: do we need that bool?
MtmTwoPhaseCommit(MtmCurrentTrans* x)
{
	nodemask_t participantsMask;
	bool	ret;
	int		failed_at;
	TransactionId xid;
	char	stream[DMQ_NAME_MAXLEN];
	pgid_t  gid;

	if (!x->isDistributed || !x->containsDML || !Mtm->extension_created)
		return false;

	if (!DmqSubscribed)
	{
		int i, sender_id = 0;
		for (i = 0; i < Mtm->nAllNodes; i++)
		{
			if (i + 1 != MtmNodeId)
			{
				dmq_attach_receiver(psprintf("node%d", i + 1), i);
				sender_to_node[sender_id++] = i + 1;
			}
		}
		DmqSubscribed = true;
	}

	if (!x->isTransactionBlock)
	{
		BeginTransactionBlock();
		x->isTransactionBlock = true;
		CommitTransactionCommand();
		StartTransactionCommand();
	}

	xid = GetCurrentTransactionId();
	sprintf(stream, "xid" XID_FMT, xid);
	dmq_stream_subscribe(stream);
	x->xid = xid;

	MtmGenerateGid(gid, xid);
	participantsMask = (((nodemask_t)1 << Mtm->nAllNodes) - 1) &
								  ~Mtm->disabledNodeMask &
								  ~((nodemask_t)1 << (MtmNodeId-1));

	ret = PrepareTransactionBlock(gid);
	if (!ret)
	{
		mtm_log(MtmTxFinish, "%s prepared", gid);
		mtm_log(WARNING, "Failed to prepare transaction %s", gid);
		return false;
	}
	CommitTransactionCommand();

	ret = GatherPrepares(x, participantsMask, &failed_at);
	if (!ret)
	{
		dmq_stream_unsubscribe(stream);
		FinishPreparedTransaction(gid, false, false);
		mtm_log(MtmTxFinish, "%s aborted", gid);
		mtm_log(ERROR, "Failed to prepare transaction %s at node %d",
						gid, failed_at);
	}

	SetPreparedTransactionState(gid, MULTIMASTER_PRECOMMITTED);
	mtm_log(MtmTxFinish, "%s precommittted", gid);
	GatherPrecommits(x, participantsMask);

	StartTransactionCommand();
	FinishPreparedTransaction(gid, true, false);
	mtm_log(MtmTxFinish, "%s committted", gid);

	dmq_stream_unsubscribe(stream);

	return true;
}

static bool
GatherPrepares(MtmCurrentTrans* x, nodemask_t participantsMask, int *failed_at)
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
			Assert(msg->dxid == x->xid);
			Assert(BIT_CHECK(participantsMask, sender_to_node[sender_id] - 1));

			mtm_log(MtmTxTrace,
				"GatherPrepares: got '%s' for tx" XID_FMT " from node%d",
				msg->code == MSG_PREPARED ? "ok" : "failed",
				x->xid, sender_to_node[sender_id]);

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
						sender_to_node[sender_id], x->xid);
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
GatherPrecommits(MtmCurrentTrans* x, nodemask_t participantsMask)
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
			Assert(msg->code == MSG_PRECOMMITTED);
			Assert(msg->dxid == x->xid);
			Assert(BIT_CHECK(participantsMask, sender_to_node[sender_id] - 1));

			mtm_log(MtmTxTrace,
				"GatherPrecommits: got 'ok' for tx" XID_FMT " from node%d",
				x->xid, sender_to_node[sender_id]);

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
						sender_to_node[sender_id], x->xid);
				}
			}
			MtmUnlock();
		}
	}

	// XXX: assert that majority has responded
}
