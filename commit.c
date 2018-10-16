#include "postgres.h"
#include "multimaster.h"
#include "storage/proc.h"
#include "utils/guc.h"
#include "miscadmin.h"
#include "commands/dbcommands.h"

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

	// /* Application name can be changed using PGAPPNAME environment variable */
	// if (x->isDistributed && Mtm->status != MTM_ONLINE
	// 	&& strcmp(application_name, MULTIMASTER_ADMIN) != 0
	// 	&& strcmp(application_name, MULTIMASTER_BROADCAST_SERVICE) != 0)
	// {
	// 	/* Reject all user's transactions at offline cluster.
	// 	 * Allow execution of transaction by bg-workers to makeit possible to perform recovery.
	// 	 */
	// 	MTM_ELOG(ERROR,
	// 			"Multimaster node is not online: current status %s",
	// 			MtmNodeStatusMnem[Mtm->status]);
	// }
}

static void
MtmPrePrepareTransaction(MtmCurrentTrans* x)
{
	if (!x->isDistributed)
		return;

	if (!MtmDatabaseId)
		MtmDatabaseId = get_database_oid(MtmDatabaseName, false);

	if (MtmDatabaseId != MyDatabaseId)
		MTM_ELOG(ERROR,
			"Refusing to work. Multimaster configured to work with database '%s'",
			MtmDatabaseName);

	Assert(TransactionIdIsValid(GetCurrentTransactionId()));
	Assert(*x->gid != '\0');
}

bool // XXX: do we need that bool?
MtmTwoPhaseCommit(MtmCurrentTrans* x)
{
	nodemask_t participantsMask;
	bool	ret;
	int		failed_at;

	if (!x->isDistributed || !x->containsDML)
		return false;

	if (!DmqSubscribed)
	{
		int i, sender_id = 0;

		for (i = 0; i < Mtm->nAllNodes; i++)
		{
			if (i + 1 != MtmNodeId)
			{
				dmq_stream_subscribe(psprintf("node%d", i + 1),
								psprintf("be%d", MyProc->pgprocno));
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

	MtmGenerateGid(x->gid);
	participantsMask = (((nodemask_t)1 << Mtm->nAllNodes) - 1) &
								  ~Mtm->disabledNodeMask &
								  ~((nodemask_t)1 << (MtmNodeId-1));

	ret = PrepareTransactionBlock(x->gid);
	if (!ret)
	{
		MTM_ELOG(WARNING, "Failed to prepare transaction %s (%llu)", x->gid, (long64)x->xid);
		return false;
	}
	CommitTransactionCommand();

	ret = GatherPrepares(x, participantsMask, &failed_at);
	if (!ret)
	{
		FinishPreparedTransaction(x->gid, false, false);
		MTM_ELOG(ERROR, "Failed to prepare transaction %s at node %d",
						x->gid, failed_at);
	}

	SetPreparedTransactionState(x->gid, MULTIMASTER_PRECOMMITTED);
	GatherPrecommits(x, participantsMask);

	StartTransactionCommand();
	FinishPreparedTransaction(x->gid, true, false);

	return true;
}

static bool
GatherPrepares(MtmCurrentTrans* x, nodemask_t participantsMask, int *failed_at)
{
	bool prepared = true;

	Assert(participantsMask != 0);

	while (participantsMask != 0)
	{
		DmqSenderId sender_id;
		StringInfoData buffer;
		MtmArbiterMessage *msg;

		dmq_pop(&sender_id, &buffer);
		msg = (MtmArbiterMessage *) buffer.data;

		// elog(LOG, "GatherPrepares: got %s from node%d", msg->gid, sender_to_node[sender_id]);
		ereport(LOG,
					(errmsg("GatherPrepares: got %s from node%d",
							msg->gid, sender_to_node[sender_id]),
					 errhidestmt(true)));

		Assert(msg->node == sender_to_node[sender_id]);
		Assert(msg->code == MSG_PREPARED || msg->code == MSG_ABORTED);
		Assert(strcmp(msg->gid, x->gid) == 0);
		Assert(BIT_CHECK(participantsMask, sender_to_node[sender_id] - 1));

		BIT_CLEAR(participantsMask, sender_to_node[sender_id] - 1);

		if (msg->code == MSG_ABORTED)
		{
			prepared = false;
			*failed_at = msg->node;
		}
	}

	return prepared;
}

static void
GatherPrecommits(MtmCurrentTrans* x, nodemask_t participantsMask)
{
	Assert(participantsMask != 0);

	while (participantsMask != 0)
	{
		DmqSenderId sender_id;
		StringInfoData buffer;
		MtmArbiterMessage *msg;

		dmq_pop(&sender_id, &buffer);
		msg = (MtmArbiterMessage *) buffer.data;

		elog(LOG, "GatherPrecommits: got %s from node%d", msg->gid, sender_to_node[sender_id]);

		Assert(msg->node == sender_to_node[sender_id]);
		Assert(msg->code == MSG_PRECOMMITTED);
		Assert(strcmp(msg->gid, x->gid) == 0);
		Assert(BIT_CHECK(participantsMask, sender_to_node[sender_id] - 1));

		BIT_CLEAR(participantsMask, sender_to_node[sender_id] - 1);
	}
}
