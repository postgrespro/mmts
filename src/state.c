#include "postgres.h"
#include "access/twophase.h"
#include "executor/spi.h"
#include "utils/snapmgr.h"
#include "nodes/makefuncs.h"
#include "catalog/namespace.h"
#include "tcop/tcopprot.h"
#include "storage/ipc.h"
#include "miscadmin.h" /* PostmasterPid */
#include "utils/syscache.h"
#include "utils/inval.h"
#include "replication/slot.h"
#include "replication/origin.h"
#include "miscadmin.h"
#include "replication/logicalfuncs.h"
#include "utils/builtins.h"
#include "funcapi.h"

#include "multimaster.h"
#include "bkb.h"
#include "state.h"
#include "logger.h"

char const* const MtmNeighborEventMnem[] =
{
	"MTM_NEIGHBOR_CLIQUE_DISABLE",
	"MTM_NEIGHBOR_WAL_RECEIVER_START",
	"MTM_NEIGHBOR_WAL_RECEIVER_ERROR",
	"MTM_NEIGHBOR_WAL_SENDER_START_RECOVERY",
	"MTM_NEIGHBOR_WAL_SENDER_START_RECOVERED",
	"MTM_NEIGHBOR_RECOVERY_CAUGHTUP",
	"MTM_NEIGHBOR_WAL_SENDER_STOP"
};

char const* const MtmEventMnem[] =
{
	"MTM_REMOTE_DISABLE",
	"MTM_CLIQUE_DISABLE",
	"MTM_CLIQUE_MINORITY",
	"MTM_ARBITER_RECEIVER_START",
	"MTM_RECOVERY_START1",
	"MTM_RECOVERY_START2",
	"MTM_RECOVERY_FINISH1",
	"MTM_RECOVERY_FINISH2",
	"MTM_NONRECOVERABLE_ERROR"
};

char const* const MtmNodeStatusMnem[] =
{
	"disabled",
	"recovery",
	"recovered",
	"online"
};

struct MtmState
{
	nodemask_t	configured_mask;
	nodemask_t	connected_mask;
	nodemask_t	enabled_mask;
	nodemask_t	receivers_mask;
	nodemask_t	senders_mask;
	nodemask_t	clique;

	bool		referee_grant;
	int			referee_winner_id;

	bool		recovered;
	int			recovery_slot;

	int			recovery_count;

	MtmNodeStatus status;
} *mtm_state;

static int  MtmRefereeGetWinner(void);
static bool MtmRefereeClearWinner(void);
static int  MtmRefereeReadSaved(void);

static void MtmEnableNode(int node_id);
static void MtmDisableNode(int node_id);

PG_FUNCTION_INFO_V1(mtm_node_info);
PG_FUNCTION_INFO_V1(mtm_status);

static bool mtm_state_initialized;

static LWLock *MtmStateLock;

void
MtmStateInit()
{
	RequestAddinShmemSpace(sizeof(struct MtmState));
	RequestNamedLWLockTranche("mtm_state_lock", 1);
}

void
MtmStateShmemStartup()
{
	bool		found;

	LWLockAcquire(AddinShmemInitLock, LW_EXCLUSIVE);

	mtm_state = ShmemInitStruct("mtm_state", sizeof(struct MtmState), &found);

	if (!found)
		MemSet(mtm_state, '\0', sizeof(struct MtmState));

	MtmStateLock = &(GetNamedLWLockTranche("mtm_state_lock")->lock);

	LWLockRelease(AddinShmemInitLock);
}

// XXXX: allocate in context and clean it
static char *
maskToString(nodemask_t mask)
{
	char *strMask = palloc0(MTM_MAX_NODES + 1);
	int i, j = 0;

	for (i = 0; i < MTM_MAX_NODES; i++)
	{
		if (BIT_CHECK(mtm_state->configured_mask, i))
			strMask[j++] = BIT_CHECK(mask, i) ? '1' : '0';
	}

	return strMask;
}

static int
popcount(nodemask_t mask)
{
	int i, count = 0;
	for (i = 0; i < MTM_MAX_NODES; i++)
	{
		if (BIT_CHECK(mask, i))
			count++;
	}
	return count;
}

void
MtmStateFill(MtmConfig *cfg)
{
	int			i;

	Mtm->my_node_id = cfg->my_node_id;

	LWLockAcquire(MtmStateLock, LW_EXCLUSIVE);

	// XXX: that kind of dangerous. Move to mtm-monitor?
	if (cfg->backup_node_id != 0)
		mtm_state->recovery_slot = cfg->backup_node_id;

	BIT_SET(mtm_state->connected_mask, cfg->my_node_id - 1);

	/* re-create configured_mask */
	mtm_state->configured_mask = 0;
	BIT_SET(mtm_state->configured_mask, cfg->my_node_id - 1);
	for (i = 0; i < cfg->n_nodes; i++)
		BIT_SET(mtm_state->configured_mask, cfg->nodes[i].node_id - 1);

	LWLockRelease(MtmStateLock);
}

static void
MtmSetClusterStatus(MtmNodeStatus status)
{
	Assert(LWLockHeldByMeInMode(MtmStateLock, LW_EXCLUSIVE));

	if (mtm_state->status == status)
		return;

	mtm_log(MtmStateSwitch, "[STATE]   Switching status from %s to %s",
			 MtmNodeStatusMnem[mtm_state->status], MtmNodeStatusMnem[status]);

	/*
	 * Do some actions on specific status transitions.
	 * This will be executed only once because of preceeding if stmt.
	 */
	if (status == MTM_DISABLED)
	{
		mtm_state->recovered = false;
		mtm_state->recovery_slot = 0;
		mtm_state->receivers_mask = 0;
		mtm_state->senders_mask = 0;
		mtm_state->recovery_count++; /* this will restart replication connection */
	}

	/*
	 * Check saved referee decision and clean it
	 */
	if (status == MTM_ONLINE)
	{
		int saved_winner_node_id = MtmRefereeReadSaved();
		if (!mtm_state->referee_grant && saved_winner_node_id > 0)
		{
			/*
			 * We booted after being with refereeGrant,
			 * but now have ordinary majority.
			 */
			// MtmPollStatusOfPreparedTransactions(true);
			ResolveAllTransactions(popcount(mtm_state->configured_mask));
			mtm_state->referee_winner_id = saved_winner_node_id;
		}
		MtmEnableNode(Mtm->my_node_id);
	}

	mtm_state->status = status;
}

static void
MtmCheckState(void)
{
	bool isEnabledState;
	MtmNodeStatus old_status;
	int nEnabled    = popcount(mtm_state->enabled_mask);
	int nConnected  = popcount(mtm_state->connected_mask);
	int nReceivers  = popcount(mtm_state->receivers_mask);
	int nSenders    = popcount(mtm_state->senders_mask);
	int nConfigured = popcount(mtm_state->configured_mask);

	Assert(LWLockHeldByMeInMode(MtmStateLock, LW_EXCLUSIVE));

	old_status = mtm_state->status;

	mtm_log(MtmStateMessage,
		"[STATE]   Status = (enabled=%s, connected=%s, clique=%s, receivers=%s, senders=%s, total=%i, referee_grant=%d)",
		maskToString(mtm_state->enabled_mask),
		maskToString(mtm_state->connected_mask),
		maskToString(mtm_state->clique),
		maskToString(mtm_state->receivers_mask),
		maskToString(mtm_state->senders_mask),
		popcount(mtm_state->configured_mask),
		mtm_state->referee_grant);

	isEnabledState = (nConnected >= nConfigured/2 + 1) ||
					 (nConnected == nConfigured/2 && mtm_state->referee_grant);

	/* ANY -> MTM_DISABLED */
	if (!isEnabledState)
	{
		// BIT_SET(Mtm->disabledNodeMask, MtmNodeId-1);
		MtmSetClusterStatus(MTM_DISABLED);
		MtmDisableNode(Mtm->my_node_id);
		return;
	}

	switch (mtm_state->status)
	{
		case MTM_DISABLED:
			if (isEnabledState)
			{
				MtmSetClusterStatus(MTM_RECOVERY);

				if (old_status != mtm_state->status)
					MtmCheckState();
				return;
			}
			break;

		case MTM_RECOVERY:
			if (mtm_state->recovered)
			{
				MtmSetClusterStatus(MTM_RECOVERED);

				if (old_status != mtm_state->status)
					MtmCheckState();
				return;
			}
			break;

		/*
		 * Switching from MTM_RECOVERY to MTM_ONLINE requires two state
		 * re-checks. If by the time of MTM_RECOVERY -> MTM_RECOVERED all
		 * senders/receiveirs already atarted we can stuck in MTM_RECOVERED
		 * state. Hence call MtmCheckState() from periodic status check while
		 * in MTM_RECOVERED state.
		 */
		case MTM_RECOVERED:
			if (nReceivers == nEnabled && nSenders == nEnabled && nEnabled == nConnected - 1)
			{
				MtmSetClusterStatus(MTM_ONLINE);

				if (old_status != mtm_state->status)
					MtmCheckState();
				return;
			}
			break;

		case MTM_ONLINE:
			{
				int nEnabled = popcount(mtm_state->enabled_mask);
				int nConfigured = popcount(mtm_state->configured_mask);

				// XXX: re-check for nEnabled
				if ( !((nEnabled >= nConfigured/2+1) ||
						(nEnabled == nConfigured/2 && mtm_state->referee_grant)) )
				{
					mtm_log(MtmStateMessage, "[STATE] disable myself, nEnabled less then majority");
					MtmSetClusterStatus(MTM_DISABLED);
					MtmDisableNode(Mtm->my_node_id);
					/* do not recur */
					return;
				}
			}
			break;
	}

}


void
MtmStateProcessNeighborEvent(int node_id, MtmNeighborEvent ev, bool locked)
{
	mtm_log(MtmStateMessage, "[STATE] Node %i: %s", node_id, MtmNeighborEventMnem[ev]);

	Assert(node_id != Mtm->my_node_id);

	if (!locked)
		LWLockAcquire(MtmStateLock, LW_EXCLUSIVE);

	Assert(LWLockHeldByMeInMode(MtmStateLock, LW_EXCLUSIVE));

	switch(ev)
	{
		case MTM_NEIGHBOR_CLIQUE_DISABLE:
			MtmDisableNode(node_id);
			break;

		// XXX: now that means reception of parallel-safe message  through
		// replication. Need to be renamed.
		case MTM_NEIGHBOR_WAL_RECEIVER_START:
			BIT_SET(mtm_state->receivers_mask, node_id - 1);
			break;

		case MTM_NEIGHBOR_WAL_RECEIVER_ERROR:
			if (mtm_state->recovery_slot == node_id)
				mtm_state->recovery_slot = 0;
			break;

		case MTM_NEIGHBOR_WAL_SENDER_START_RECOVERY:
			/*
			 * With big heartbeat recv timeout it can heppend that other node will
			 * restart faster than we can detect that. Without disabledNodeMask bit set
			 * we will never send recovery_finish in such case. So set it now.
			 *
			 * It is also possible to change logic of recovery_finish but for
			 * now it is easier to do it here. MtmIsRecoveredNode deserves rewrite anyway.
			 */
			// XXX
			if (BIT_CHECK(mtm_state->enabled_mask, node_id - 1))
			{
				mtm_log(MtmStateMessage, "[WARN] node %d started recovery, but it wasn't disabled", node_id);
				MtmDisableNode(node_id);
			}
			break;

		case MTM_NEIGHBOR_WAL_SENDER_START_RECOVERED:
			BIT_SET(mtm_state->senders_mask, node_id - 1);
			MtmEnableNode(node_id);
			break;

		case MTM_NEIGHBOR_RECOVERY_CAUGHTUP:
			// MtmEnableNode(node_id);
			break;

		case MTM_NEIGHBOR_WAL_SENDER_STOP:
			BIT_CLEAR(mtm_state->senders_mask, node_id - 1);
			break;
	}

	MtmCheckState();

	if (!locked)
		LWLockRelease(MtmStateLock);
}


void
MtmStateProcessEvent(MtmEvent ev, bool locked)
{
	mtm_log(MtmStateMessage, "[STATE] %s", MtmEventMnem[ev]);

	if (!locked)
		LWLockAcquire(MtmStateLock, LW_EXCLUSIVE);

	Assert(LWLockHeldByMeInMode(MtmStateLock, LW_EXCLUSIVE));

	switch (ev)
	{
		case MTM_CLIQUE_DISABLE:
			BIT_CLEAR(mtm_state->enabled_mask, Mtm->my_node_id - 1);
			mtm_state->recovery_count++; /* this will restart replication connection */
			break;

		case MTM_REMOTE_DISABLE:
		case MTM_CLIQUE_MINORITY:
			break;

		case MTM_ARBITER_RECEIVER_START:
			// MtmOnNodeConnect(MtmNodeId);
			break;

		case MTM_RECOVERY_START1:
		case MTM_RECOVERY_START2:
			break;

		case MTM_RECOVERY_FINISH1:
		case MTM_RECOVERY_FINISH2:
			{
				mtm_state->recovered = true;
				mtm_state->recovery_count++; /* this will restart replication connection */
				mtm_state->recovery_slot = 0;
			}
			break;

		case MTM_NONRECOVERABLE_ERROR:
			// kill(PostmasterPid, SIGQUIT);
			break;
	}

	MtmCheckState();

	if (!locked)
		LWLockRelease(MtmStateLock);

}

/*
 * Node is disabled if it is not part of clique built using connectivity masks of all nodes.
 * There is no warranty that all nodes will make the same decision about clique, but as far as we want to avoid
 * some global coordinator (which will be SPOF), we have to rely on Bron–Kerbosch algorithm locating maximum clique in graph
 */
static void
MtmDisableNode(int node_id)
{
	if (!BIT_CHECK(mtm_state->enabled_mask, node_id - 1))
		return;

	mtm_log(MtmStateMessage, "[STATE] Node %i: disabled", node_id);

	BIT_CLEAR(mtm_state->enabled_mask, node_id - 1);

	// XXX my_node_id
	if (mtm_state->status == MTM_ONLINE)
	{
		ResolveTransactionsForNode(node_id, popcount(mtm_state->configured_mask));
	}
}

/*
 * Node is enabled when it's recovery is completed.
 * This why node is mostly marked as recovered when logical sender/receiver to this node is (re)started.
 */
static void
MtmEnableNode(int node_id)
{
	mtm_log(MtmStateMessage, "[STATE] Node %i: enabled", node_id);
	BIT_SET(mtm_state->enabled_mask, node_id - 1);
}

/*
 *
 */
void
MtmOnNodeDisconnect(char *node_name)
{
	int			node_id;

	sscanf(node_name, MTM_DMQNAME_FMT, &node_id);

	// XXX: assert?
	if (!BIT_CHECK(mtm_state->connected_mask, node_id - 1))
		return;

	mtm_log(MtmStateMessage, "[STATE] Node %i: disconnected", node_id);

	/*
	 * We should disable it, as clique detector will not necessarily
	 * do that. For example it will anyway find clique with one node.
	 */

	LWLockAcquire(MtmStateLock, LW_EXCLUSIVE);

	BIT_CLEAR(mtm_state->connected_mask, node_id - 1);
	MtmDisableNode(node_id);
	MtmCheckState();

	LWLockRelease(MtmStateLock);
}

// XXXX: make that event too
void MtmOnNodeConnect(char *node_name)
{
	int			node_id = -1;
	MtmConfig  *cfg = MtmLoadConfig();

	sscanf(node_name, MTM_DMQNAME_FMT, &node_id);

	if (MtmNodeById(cfg, node_id) == NULL)
		mtm_log(ERROR, "[STATE] Node %i not found.", node_id);
	else
		mtm_log(MtmStateMessage, "[STATE] Node %i: connected", node_id);

	/* do not hold lock for mtm.cluster_nodes */
	ResourceOwnerRelease(TopTransactionResourceOwner,
						RESOURCE_RELEASE_LOCKS,
						true, true);

	LWLockAcquire(MtmStateLock, LW_EXCLUSIVE);

	BIT_SET(mtm_state->connected_mask, node_id - 1);
	MtmCheckState();

	LWLockRelease(MtmStateLock);
}

/**
 * Build internode connectivity mask. 1 - means that node is disconnected.
 */
static void
MtmBuildConnectivityMatrix(nodemask_t* matrix)
{
	int i, j, n = MTM_MAX_NODES;

	// for (i = 0; i < n; i++)
	// 	matrix[i] = Mtm->nodes[i].connectivityMask;

	/* make matrix symmetric: required for Bron–Kerbosch algorithm */
	for (i = 0; i < n; i++) {
		for (j = 0; j < i; j++) {
			matrix[i] |= ((matrix[j] >> i) & 1) << j;
			matrix[j] |= ((matrix[i] >> j) & 1) << i;
		}
		matrix[i] &= ~((nodemask_t)1 << i);
	}
}


/*
 * Determine when and how we should open replication slot.
 * During recovery we need to open only one replication slot from which node should receive all transactions.
 * Slots at other nodes should be removed.
 */
MtmReplicationMode
MtmGetReplicationMode(int nodeId)
{
	LWLockAcquire(MtmStateLock, LW_EXCLUSIVE);

	/* Await until node is connected and both receiver and sender are in clique */
	while (!BIT_CHECK(mtm_state->connected_mask, nodeId - 1)) // ||
			// !BIT_CHECK(mtm_state->connected_mask, Mtm->my_node_id - 1))
	{
		LWLockRelease(MtmStateLock);
		MtmSleep(USECS_PER_SEC);
		LWLockAcquire(MtmStateLock, LW_EXCLUSIVE);
	}

	if (!mtm_state->recovered)
	{
		/* Ok, then start recovery by luckiest walreceiver (if there is no donor node).
		 * If this node was populated using basebackup, then donorNodeId is not zero and we should choose this node for recovery */
		if ((mtm_state->recovery_slot == 0 || mtm_state->recovery_slot == nodeId))
		{
			/* Lock on us */
			mtm_state->recovery_slot = nodeId;
			ResolveAllTransactions(popcount(mtm_state->configured_mask));
			LWLockRelease(MtmStateLock);
			return REPLMODE_RECOVERY;
		}

		/* And force less lucky walreceivers wait until recovery is completed */
		while (!mtm_state->recovered)
		{
			LWLockRelease(MtmStateLock);
			MtmSleep(USECS_PER_SEC);
			LWLockAcquire(MtmStateLock, LW_EXCLUSIVE);
		}
	}

	Assert(mtm_state->recovered);
	LWLockRelease(MtmStateLock);
	return REPLMODE_RECOVERED;
}

/**
 * Build connectivity graph, find clique in it and extend disabledNodeMask by nodes not included in clique.
 * This function is called by arbiter monitor process with period MtmHeartbeatSendTimeout
 */
void
MtmRefreshClusterStatus()
{
	nodemask_t newClique, oldClique;
	nodemask_t matrix[MTM_MAX_NODES];
	nodemask_t trivialClique = mtm_state->connected_mask;
	int cliqueSize;

	/*
	 * Periodical check that we are still in RECOVERED state.
	 * See comment to MTM_RECOVERED -> MTM_ONLINE transition in MtmCheckState()
	 */
	// MtmLock(LW_EXCLUSIVE);
	// MtmCheckState();
	// MtmUnlock();

	return;

	/*
	 * Check for referee decision when only half of nodes are visible.
	 * Do not hold lock here, but recheck later wheter mask changed.
	 */
	if (MtmRefereeConnStr && *MtmRefereeConnStr && !mtm_state->referee_winner_id &&
		popcount(mtm_state->connected_mask) == popcount(mtm_state->configured_mask)/2)
	{
		int winner_node_id = MtmRefereeGetWinner();

		/* We also can have old value. Do that only from single mtm-monitor process */
		if (winner_node_id <= 0 && !mtm_state_initialized)
		{
			winner_node_id = MtmRefereeReadSaved();
			mtm_state_initialized = true;
		}

		if (winner_node_id > 0)
		{
			mtm_state->referee_winner_id = winner_node_id;
			if (BIT_CHECK(mtm_state->connected_mask, winner_node_id - 1))
			{
				/*
				 * By the time we enter this block we can already see other nodes.
				 * So recheck old conditions under lock.
				 */
				LWLockAcquire(MtmStateLock, LW_EXCLUSIVE);
				if (popcount(mtm_state->connected_mask) == popcount(mtm_state->configured_mask)/2 &&
					BIT_CHECK(mtm_state->connected_mask, winner_node_id - 1))
				{
					mtm_log(MtmStateMessage, "[STATE] Referee allowed to proceed with half of the nodes (winner_id = %d)",
					winner_node_id);
					mtm_state->referee_grant = true;
					if (popcount(mtm_state->connected_mask) == 1)
					{
						// MtmPollStatusOfPreparedTransactions(true);
						ResolveAllTransactions(popcount(mtm_state->configured_mask));
					}
					MtmEnableNode(Mtm->my_node_id);
					MtmCheckState();
				}
				LWLockRelease(MtmStateLock);
			}
		}
	}

	/*
	 * Clear winner if we again have all nodes recovered.
	 * We should clean old value based on disabledNodeMask instead of SELF_CONNECTIVITY_MASK
	 * because we can clean old value before failed node starts it recovery and that node
	 * can get refereeGrant before start of walsender, so it start in recovered mode.
	 */
	if (MtmRefereeConnStr && *MtmRefereeConnStr && mtm_state->referee_winner_id &&
		popcount(mtm_state->connected_mask) == popcount(mtm_state->configured_mask) &&
		MtmGetCurrentStatus() == MTM_ONLINE) /* restrict this actions only to major -> online transition */
	{
		if (MtmRefereeClearWinner())
		{
			mtm_state->referee_winner_id = 0;
			mtm_state->referee_grant = false;
			mtm_log(MtmStateMessage, "[STATE] Cleaning old referee decision");
		}
	}

	// Mtm->clique = (((nodemask_t)1 << Mtm->nAllNodes) - 1);
	// return;

	/*
	 * Check for clique.
	 */
	MtmBuildConnectivityMatrix(matrix);
	newClique = MtmFindMaxClique(matrix, MTM_MAX_NODES, &cliqueSize);

	if (newClique == mtm_state->clique)
		return;

	mtm_log(MtmStateMessage, "[STATE] Old clique: %s", maskToString(mtm_state->clique));

	/*
	 * Otherwise make sure that all nodes have a chance to replicate their connectivity
	 * mask and we have the "consistent" picture. Obviously we can not get true consistent
	 * snapshot, but at least try to wait heartbeat send timeout is expired and
	 * connectivity graph is stabilized.
	 */
	do {
		oldClique = newClique;
		/*
		 * Double timeout to consider the worst case when heartbeat receive interval is added
		 * with refresh cluster status interval.
		 */
		MtmSleep(1000L*(MtmHeartbeatRecvTimeout)*2);
		MtmBuildConnectivityMatrix(matrix);
		newClique = MtmFindMaxClique(matrix, MTM_MAX_NODES, &cliqueSize);
	} while (newClique != oldClique);

	mtm_log(MtmStateMessage, "[STATE] New clique: %s", maskToString(oldClique));

	if (newClique != trivialClique)
	{
		mtm_log(MtmStateMessage, "[STATE] NONTRIVIAL CLIQUE! (trivial: %s)", maskToString(trivialClique)); // XXXX some false-positives, fixme
	}

	/*
	 * We are using clique only to disable nodes.
	 * So find out what node should be disabled and disable them.
	 */
	LWLockAcquire(MtmStateLock, LW_EXCLUSIVE);

	mtm_state->clique = newClique;

	/*
	 * Do not perform any action based on clique with referee grant,
	 * because we can disable ourself.
	 * But we also need to maintain actual clique not disable ourselves
	 * when neighbour node will come back and we erase refereeGrant.
	 */
	if (mtm_state->referee_grant)
	{
		LWLockRelease(MtmStateLock);
		return;
	}

	// for (i = 0; i < Mtm->nAllNodes; i++)
	// {
	// 	bool old_status = BIT_CHECK(Mtm->disabledNodeMask, i);
	// 	bool new_status = BIT_CHECK(~newClique, i);

	// 	if (new_status && new_status != old_status)
	// 	{
	// 		if ( i+1 == Mtm->my_node_id )
	// 			MtmStateProcessEvent(MTM_CLIQUE_DISABLE, true);
	// 		else
	// 			MtmStateProcessNeighborEvent(i+1, MTM_NEIGHBOR_CLIQUE_DISABLE, true);
	// 	}
	// }

	MtmCheckState();
	LWLockRelease(MtmStateLock);
}

/*
 * Referee caches decision in mtm.referee_decision
 */
static bool
MtmRefereeHasLocalTable()
{
	RangeVar   *rv;
	Oid			rel_oid;
	static bool _has_local_tables;
	bool		txstarted = false;

	/* memoized */
	if (_has_local_tables)
		return true;

	if (!IsTransactionState())
	{
		txstarted = true;
		StartTransactionCommand();
	}

	rv = makeRangeVar(MULTIMASTER_SCHEMA_NAME, "referee_decision", -1);
	rel_oid = RangeVarGetRelid(rv, NoLock, true);

	if (txstarted)
		CommitTransactionCommand();

	if (OidIsValid(rel_oid))
	{
		// MtmMakeRelationLocal(rel_oid);
		_has_local_tables = true;
		return true;
	}
	else
		return false;
}

static int
MtmRefereeReadSaved(void)
{
	int winner = -1;
	int rc;
	bool txstarted = false;

	if (!MtmRefereeHasLocalTable())
		return -1;

	/* Save result locally */
	if (!IsTransactionState())
	{
		txstarted = true;
		StartTransactionCommand();
	}
	SPI_connect();
	PushActiveSnapshot(GetTransactionSnapshot());
	rc = SPI_execute("select node_id from mtm.referee_decision where key = 'winner';", true, 0);
	if (rc != SPI_OK_SELECT)
	{
		mtm_log(WARNING, "Failed to load referee decision");
	}
	else if (SPI_processed > 0)
	{
		bool isnull;
		winner = DatumGetInt32(SPI_getbinval(SPI_tuptable->vals[0], SPI_tuptable->tupdesc, 1, &isnull));
		Assert(SPI_processed == 1);
		Assert(!isnull);
	}
	else
	{
		/* no saved decision found */
		Assert(SPI_processed == 0);
	}
	SPI_finish();
	PopActiveSnapshot();
	if (txstarted)
		CommitTransactionCommand();

	mtm_log(MtmStateMessage, "Read saved referee decision, winner=%d.", winner);
	return winner;
}

static int
MtmRefereeGetWinner(void)
{
	PGconn* conn;
	PGresult *res;
	char sql[128];
	int  winner_node_id;
	int  old_winner = -1;
	int  rc;

	conn = PQconnectdb(MtmRefereeConnStr);
	if (PQstatus(conn) != CONNECTION_OK)
	{
		mtm_log(WARNING, "Could not connect to referee");
		PQfinish(conn);
		return -1;
	}

	sprintf(sql, "select referee.get_winner(%d)", Mtm->my_node_id);
	res = PQexec(conn, sql);
	if (PQresultStatus(res) != PGRES_TUPLES_OK ||
		PQntuples(res) != 1 ||
		PQnfields(res) != 1)
	{
		mtm_log(WARNING, "Refusing unexpected result (r=%d, n=%d, w=%d, k=%s) from referee.get_winner()",
			PQresultStatus(res), PQntuples(res), PQnfields(res), PQgetvalue(res, 0, 0));
		PQclear(res);
		PQfinish(conn);
		return -1;
	}

	winner_node_id = atoi(PQgetvalue(res, 0, 0));

	if (winner_node_id < 1 || winner_node_id > MTM_MAX_NODES)
	{
		mtm_log(WARNING,
			"Referee responded with node_id=%d, it's out of our node range",
			winner_node_id);
		PQclear(res);
		PQfinish(conn);
		return -1;
	}

	/* Ok, we finally got it! */
	PQclear(res);
	PQfinish(conn);

	/* Save result locally */
	if (MtmRefereeHasLocalTable())
	{
		// MtmEnforceLocalTx = true;
		StartTransactionCommand();
		SPI_connect();
		PushActiveSnapshot(GetTransactionSnapshot());
		/* Check old value if any */
		rc = SPI_execute("select node_id from mtm.referee_decision where key = 'winner';", true, 0);
		if (rc != SPI_OK_SELECT)
		{
			mtm_log(WARNING, "Failed to load previous referee decision");
		}
		else if (SPI_processed > 0)
		{
			bool isnull;
			old_winner = DatumGetInt32(SPI_getbinval(SPI_tuptable->vals[0], SPI_tuptable->tupdesc, 1, &isnull));
			Assert(SPI_processed == 1);
			Assert(!isnull);
		}
		else
		{
			/* no saved decision found */
			Assert(SPI_processed == 0);
		}
		/* Update actual key */
		sprintf(sql,
			"insert into mtm.referee_decision values ('winner', %d) on conflict(key) do nothing;",
			winner_node_id);
		rc = SPI_execute(sql, false, 0);
		SPI_finish();
		if (rc < 0)
			mtm_log(WARNING, "Failed to save referee decision, but proceeding anyway");
		PopActiveSnapshot();
		CommitTransactionCommand();
		// MtmEnforceLocalTx = false;

		if (old_winner > 0 && old_winner != winner_node_id)
			mtm_log(MtmStateMessage, "WARNING Overriding old referee decision (%d) with new one (%d)", old_winner, winner_node_id);
	}

	mtm_log(MtmStateMessage, "Got referee response, winner node_id=%d.", winner_node_id);
	return winner_node_id;
}

static bool
MtmRefereeClearWinner(void)
{
	PGconn* conn;
	PGresult *res;
	char *response;
	int rc;

	/*
	 * Delete result locally first.
	 *
	 * If we delete decision from referee but fail to delete local cached
	 * that will be pretty bad -- on the next reboot we can read
	 * stale referee decision and on next failure end up with two masters.
	 * So just delete local cache first.
	 */
	if (MtmRefereeHasLocalTable())
	{
		StartTransactionCommand();
		SPI_connect();
		PushActiveSnapshot(GetTransactionSnapshot());
		rc = SPI_execute("delete from mtm.referee_decision where key = 'winner'", false, 0);
		SPI_finish();
		PopActiveSnapshot();
		CommitTransactionCommand();
		if (rc < 0)
		{
			mtm_log(WARNING, "Failed to clean referee decision");
			return false;
		}
	}


	conn = PQconnectdb(MtmRefereeConnStr);
	if (PQstatus(conn) != CONNECTION_OK)
	{
		mtm_log(WARNING, "Could not connect to referee");
		PQfinish(conn);
		return false;
	}

	res = PQexec(conn, "select referee.clean()");
	if (PQresultStatus(res) != PGRES_TUPLES_OK ||
		PQntuples(res) != 1 ||
		PQnfields(res) != 1)
	{
		mtm_log(WARNING, "Refusing unexpected result (r=%d, n=%d, w=%d, k=%s) from referee.clean().",
			PQresultStatus(res), PQntuples(res), PQnfields(res), PQgetvalue(res, 0, 0));
		PQclear(res);
		PQfinish(conn);
		return false;
	}

	response = PQgetvalue(res, 0, 0);

	if (strncmp(response, "t", 1) != 0)
	{
		mtm_log(WARNING, "Wrong response from referee.clean(): '%s'", response);
		PQclear(res);
		PQfinish(conn);
		return false;
	}

	/* Ok, we finally got it! */
	mtm_log(MtmStateMessage, "Got referee clear response '%s'", response);
	PQclear(res);
	PQfinish(conn);
	return true;
}

/*
 * Mtm current status accessor.
 */
MtmNodeStatus
MtmGetCurrentStatus()
{
	MtmNodeStatus status;
	LWLockAcquire(MtmStateLock, LW_SHARED);
	status = mtm_state->status;
	LWLockRelease(MtmStateLock);
	return status;
}

nodemask_t
MtmGetConnectedNodeMask()
{
	return mtm_state->connected_mask;
}

nodemask_t
MtmGetEnabledNodeMask()
{
	nodemask_t enabled;

	LWLockAcquire(MtmStateLock, LW_SHARED);
	enabled = mtm_state->enabled_mask;
	if (mtm_state->status != MTM_ONLINE)
		elog(ERROR, "our node was disabled");
	LWLockRelease(MtmStateLock);

	return enabled;
}

/* Compatibility with scheduler */
nodemask_t
MtmGetDisabledNodeMask()
{
	nodemask_t disabled;

	LWLockAcquire(MtmStateLock, LW_SHARED);
	disabled = ~mtm_state->enabled_mask;
	LWLockRelease(MtmStateLock);

	return disabled;
}

int
MtmGetRecoveryCount()
{
	return mtm_state->recovery_count;
}

// XXX: During evaluation of (mtm.node_info(id)).* this function called
// once each columnt for every row. So may be just rewrite to SRF.
Datum
mtm_node_info(PG_FUNCTION_ARGS)
{
	int			node_id = PG_GETARG_INT32(0);
	TupleDesc	desc;
	Datum		values[Natts_mtm_node_info];
	bool		nulls[Natts_mtm_node_info] = {false};

	LWLockAcquire(MtmStateLock, LW_EXCLUSIVE);

	values[Anum_mtm_node_info_enabled - 1] =
		BoolGetDatum(BIT_CHECK(mtm_state->enabled_mask, node_id - 1));
	values[Anum_mtm_node_info_connected - 1] =
		BoolGetDatum(BIT_CHECK(mtm_state->connected_mask, node_id - 1));

	if (Mtm->peers[node_id - 1].sender_pid != InvalidPid)
	{
		values[Anum_mtm_node_info_sender_pid - 1] =
			Int32GetDatum(Mtm->peers[node_id - 1].sender_pid);
	}
	else
	{
		nulls[Anum_mtm_node_info_sender_pid - 1] = true;
	}

	if (Mtm->peers[node_id - 1].receiver_pid != InvalidPid)
	{
		values[Anum_mtm_node_info_receiver_pid - 1] =
			Int32GetDatum(Mtm->peers[node_id - 1].receiver_pid);
		values[Anum_mtm_node_info_n_workers - 1] =
			Int32GetDatum(Mtm->pools[node_id - 1].nWorkers);
		values[Anum_mtm_node_info_receiver_status - 1] =
			CStringGetTextDatum(MtmReplicationModeName[Mtm->peers[node_id - 1].receiver_mode]);
	}
	else
	{
		nulls[Anum_mtm_node_info_receiver_pid - 1] = true;
		nulls[Anum_mtm_node_info_n_workers - 1] = true;
		nulls[Anum_mtm_node_info_receiver_status - 1] = true;
	}

	LWLockRelease(MtmStateLock);

	get_call_result_type(fcinfo, NULL, &desc);
	PG_RETURN_DATUM(HeapTupleGetDatum(heap_form_tuple(desc, values, nulls)));
}

Datum
mtm_status(PG_FUNCTION_ARGS)
{
	TupleDesc	desc;
	Datum		values[Natts_mtm_status];
	bool		nulls[Natts_mtm_status] = {false};

	LWLockAcquire(MtmStateLock, LW_SHARED);

	values[Anum_mtm_status_node_id - 1] = Int32GetDatum(Mtm->my_node_id);
	values[Anum_mtm_status_status - 1] =
		CStringGetTextDatum(MtmNodeStatusMnem[mtm_state->status]);
	values[Anum_mtm_status_n_nodes - 1] = Int32GetDatum(popcount(mtm_state->configured_mask));
	values[Anum_mtm_status_n_connected - 1] =
		Int32GetDatum(popcount(mtm_state->connected_mask));
	values[Anum_mtm_status_n_enabled - 1] =
		Int32GetDatum(popcount(mtm_state->enabled_mask));

	LWLockRelease(MtmStateLock);

	get_call_result_type(fcinfo, NULL, &desc);
	PG_RETURN_DATUM(HeapTupleGetDatum(heap_form_tuple(desc, values, nulls)));
}

/*****************************************************************************
 *
 * Mtm monitor
 *
 *****************************************************************************/


#include "storage/latch.h"
#include "postmaster/bgworker.h"
#include "utils/guc.h"
#include "pgstat.h"

void MtmMonitor(Datum arg);

// XXX: change dmq api and evict that
static int		sender_to_node[MTM_MAX_NODES];
static bool		config_valid = false;

bool			MtmIsMonitorWorker;

void
MtmMonitorStart(Oid db_id, Oid user_id)
{
	BackgroundWorker worker;
	BackgroundWorkerHandle *handle;

	MemSet(&worker, 0, sizeof(BackgroundWorker));
	worker.bgw_flags = BGWORKER_SHMEM_ACCESS |	BGWORKER_BACKEND_DATABASE_CONNECTION;
	worker.bgw_start_time = BgWorkerStart_ConsistentState;
	worker.bgw_restart_time = MULTIMASTER_BGW_RESTART_TIMEOUT;
	worker.bgw_main_arg = Int32GetDatum(0);

	memcpy(worker.bgw_extra, &db_id, sizeof(Oid));
	memcpy(worker.bgw_extra + sizeof(Oid), &user_id, sizeof(Oid));

	sprintf(worker.bgw_library_name, "multimaster");
	sprintf(worker.bgw_function_name, "MtmMonitor");
	snprintf(worker.bgw_name, BGW_MAXLEN, "mtm-monitor");

	if (!RegisterDynamicBackgroundWorker(&worker, &handle))
		elog(ERROR, "Failed to start monitor worker");
}

static void
check_status_requests(MtmConfig *mtm_cfg)
{
	DmqSenderId sender_id;
	StringInfoData buffer;

	while(dmq_pop_nb(&sender_id, &buffer, MtmGetConnectedNodeMask()))
	{
		int sender_node_id;
		MtmArbiterMessage *msg;
		DmqDestinationId dest_id;
		char *state_3pc;

		sender_node_id = sender_to_node[sender_id];
		msg = (MtmArbiterMessage *) buffer.data;

		Assert(msg->node == sender_node_id);
		Assert(msg->code == MSG_POLL_REQUEST);

		mtm_log(StatusRequest, "got status request for %s from %d",
				msg->gid, sender_node_id);

		state_3pc = GetLoggedPreparedXactState(msg->gid);

		// XXX: define this strings as constants like MULTIMASTER_PRECOMMITTED
		if (strncmp(state_3pc, "notfound", MAX_3PC_STATE_SIZE) == 0)
			msg->state = MtmTxNotFound;
		else if (strncmp(state_3pc, "prepared", MAX_3PC_STATE_SIZE) == 0)
			msg->state = MtmTxPrepared;
		else if (strncmp(state_3pc, "precommitted", MAX_3PC_STATE_SIZE) == 0)
			msg->state = MtmTxPreCommited;
		else if (strncmp(state_3pc, "preaborted", MAX_3PC_STATE_SIZE) == 0)
			msg->state = MtmTxPreAborted;
		else if (strncmp(state_3pc, "committed", MAX_3PC_STATE_SIZE) == 0)
			msg->state = MtmTxCommited;
		else if (strncmp(state_3pc, "aborted", MAX_3PC_STATE_SIZE) == 0)
			msg->state = MtmTxAborted;
		else
			Assert(false);

		mtm_log(StatusRequest, "responding to %d with %s -> %s",
				sender_node_id, msg->gid, MtmTxStateMnem(msg->state));

		pfree(state_3pc);

		msg->code = MSG_POLL_STATUS;
		msg->node = Mtm->my_node_id;

		LWLockAcquire(MtmLock, LW_SHARED);
		dest_id = Mtm->peers[sender_node_id - 1].dmq_dest_id;
		LWLockRelease(MtmLock);
		Assert(dest_id >= 0);

		// XXX: and define channels as strings too
		dmq_push_buffer(dest_id, "txresp", msg,
						sizeof(MtmArbiterMessage));

		mtm_log(StatusRequest, "responded to %d with %s -> %s, code = %d",
				sender_node_id, msg->gid, MtmTxStateMnem(msg->state), msg->code);
	}
}

static bool
slot_exists(char *name)
{
	int			i;
	bool		exists = false;

	LWLockAcquire(ReplicationSlotControlLock, LW_SHARED);
	for (i = 0; i < max_replication_slots; i++)
	{
		ReplicationSlot *s = &ReplicationSlotCtl->replication_slots[i];

		if (s->in_use && strcmp(name, NameStr(s->data.name)) == 0)
		{
			exists = true;
			break;
		}
	}
	LWLockRelease(ReplicationSlotControlLock);

	return exists;
}

static bool
is_basebackuped(MtmConfig *mtm_cfg)
{
	int			i;
	int			n_missing_slots = 0;

	StartTransactionCommand();
	for (i = 0; i < mtm_cfg->n_nodes; i++)
	{
		char	   *slot_name = psprintf(MULTIMASTER_SLOT_PATTERN,
										 mtm_cfg->nodes[i].node_id);

		if (mtm_cfg->nodes[i].init_done && !slot_exists(slot_name))
			n_missing_slots++;
	}
	CommitTransactionCommand();

	if (n_missing_slots == 0)
		return false;
	else if (n_missing_slots == mtm_cfg->n_nodes)
		return true;
	else
		mtm_log(ERROR, "Missing %d replication slots out of %d",
				n_missing_slots, mtm_cfg->n_nodes);
}

static void
start_node_workers(int node_id, MtmConfig *new_cfg, Datum arg)
{
	BackgroundWorkerHandle **receivers = (BackgroundWorkerHandle **) arg;
	LogicalDecodingContext *ctx;
	DmqDestinationId dest;
	int			sender_id;
	char	   *dmq_connstr,
			   *slot,
			   *recovery_slot,
			   *dmq_my_name,
			   *dmq_node_name;
	MemoryContext old_context;

	/*
	 * Transaction is needed for logical slot and replication origin creation.
	 * Also it clean ups psprintfs.
	 */
	StartTransactionCommand();

	dmq_connstr = psprintf("%s application_name=%s",
						   MtmNodeById(new_cfg, node_id)->conninfo,
						   MULTIMASTER_BROADCAST_SERVICE);
	slot = psprintf(MULTIMASTER_SLOT_PATTERN, node_id);
	recovery_slot = psprintf(MULTIMASTER_RECOVERY_SLOT_PATTERN, node_id);
	dmq_my_name = psprintf(MTM_DMQNAME_FMT, new_cfg->my_node_id);
	dmq_node_name = psprintf(MTM_DMQNAME_FMT, node_id);

	if (MtmNodeById(new_cfg, node_id)->init_done)
	{
		if (!slot_exists(recovery_slot))
			mtm_log(ERROR, "can't find recovery slot for node%d", node_id);

		if (!slot_exists(slot))
			mtm_log(ERROR, "can't find replication slot for node%d", node_id);
	}

	if (!MtmNodeById(new_cfg, node_id)->init_done)
	{
		/*
		 * Create recovery slot to hold WAL files that we may need during
		 * recovery.
		 */
		ReplicationSlotCreate(recovery_slot, false, RS_PERSISTENT);
		ReplicationSlotReserveWal();
		/* Write this slot to disk */
		ReplicationSlotMarkDirty();
		ReplicationSlotSave();
		ReplicationSlotRelease();
	}

	/* Add dmq destination */
	dest = dmq_destination_add(dmq_connstr, dmq_my_name, dmq_node_name,
							   MtmHeartbeatSendTimeout);

	LWLockAcquire(MtmLock, LW_EXCLUSIVE);
	Mtm->peers[node_id - 1].dmq_dest_id = dest;
	LWLockRelease(MtmLock);

	/* Attach receiver so we can collect tx requests */
	sender_id = dmq_attach_receiver(dmq_node_name, node_id - 1);
	sender_to_node[sender_id] = node_id;

	/*
	 * Finally start receiver.
	 * bgw handle should be allocated in TopMcxt.
	 *
	 * Start receiver before logical slot creation, as during start after a
	 * basebackup logical stot creation will wait for all in-progress
	 * transactions to finish (including prepared ones). And to finish them
	 * we need to start receiver.
	 */
	old_context = MemoryContextSwitchTo(TopMemoryContext);
	receivers[node_id - 1] = MtmStartReceiver(node_id, MyDatabaseId,
											  GetUserId(), MyProcPid);
	MemoryContextSwitchTo(old_context);

	if (!MtmNodeById(new_cfg, node_id)->init_done)
	{
		char	   *query;
		int			rc;

		/* Create logical slot for our publication to this neighbour */
		ReplicationSlotCreate(slot, true, RS_EPHEMERAL);
		ctx = CreateInitDecodingContext(MULTIMASTER_NAME, NIL,
										// XXX?
										false,  /* do not build snapshot */
										logical_read_local_xlog_page, NULL, NULL,
										NULL);
		DecodingContextFindStartpoint(ctx);
		FreeDecodingContext(ctx);
		ReplicationSlotPersist();
		ReplicationSlotRelease();

		/*
		 * Mark this node as init_done, so at next boot we won't try to create
		 * slots again.
		 */
		if (SPI_connect() != SPI_OK_CONNECT)
			mtm_log(ERROR, "could not connect using SPI");
		PushActiveSnapshot(GetTransactionSnapshot());

		query = psprintf("update " MTM_NODES " set init_done = 't' "
						 "where id = %d", node_id);
		rc = SPI_execute(query, false, 0);
		if (rc < 0 || rc != SPI_OK_UPDATE)
			mtm_log(ERROR, "Failed to set init_done to true for node%d", node_id);

		if (SPI_finish() != SPI_OK_FINISH)
			mtm_log(ERROR, "could not finish SPI");
		PopActiveSnapshot();
	}

	CommitTransactionCommand();

	mtm_log(NodeMgmt, "Attached node%d", node_id);
}

static void
stop_node_workers(int node_id, MtmConfig *new_cfg, Datum arg)
{
	BackgroundWorkerHandle **receivers = (BackgroundWorkerHandle **) arg;
	char	   *dmq_name;
	char	   *logical_slot;
	char	   *recovery_slot_name;

	Assert(!IsTransactionState());

	mtm_log(LOG, "dropping node %d", node_id);

	StartTransactionCommand();

	dmq_name = psprintf(MTM_DMQNAME_FMT, node_id);
	logical_slot = psprintf(MULTIMASTER_SLOT_PATTERN, node_id);
	recovery_slot_name = psprintf(MULTIMASTER_RECOVERY_SLOT_PATTERN, node_id);

	/* detach incoming queues from this node */
	dmq_detach_receiver(dmq_name);

	/*
	 * Disable this node by terminating receiver.
	 * It shouldn't came back online as dmq-receiver check node_id presense
	 * in mtm.cluster_nodes.
	 */
	dmq_terminate_receiver(dmq_name);

	/* do not try to connect this node by dmq */
	dmq_destination_drop(dmq_name);

	LWLockAcquire(MtmLock, LW_EXCLUSIVE);
	Mtm->peers[node_id - 1].dmq_dest_id = -1;
	LWLockRelease(MtmLock);

	/*
	 * Stop corresponding receiver.
	 * Also await for termination, so that we can drop slots and origins that
	 * were acquired by receiver.
	 */
	TerminateBackgroundWorker(receivers[node_id - 1]);
	WaitForBackgroundWorkerShutdown(receivers[node_id - 1]);
	pfree(receivers[node_id - 1]);
	receivers[node_id - 1] = NULL;

	/* delete recovery slot, was acquired by receiver */
	ReplicationSlotDrop(recovery_slot_name, true);

	/* delete replication origin, was acquired by receiver */
	replorigin_drop(replorigin_by_name(logical_slot, false), true);

	/*
	 * Delete logical slot. It is aquired by walsender, so call with
	 * nowait = false and wait for walsender exit.
	 */
	ReplicationSlotDrop(logical_slot, false);

	CommitTransactionCommand();

	mtm_log(NodeMgmt, "Detached node%d", node_id);
}

static void
pubsub_change_cb(Datum arg, int cacheid, uint32 hashvalue)
{
	config_valid = false;
}

void
MtmMonitor(Datum arg)
{
	Oid			db_id,
				user_id;
	MtmConfig  *mtm_cfg = NULL;
	BackgroundWorkerHandle *receivers[MTM_MAX_NODES];
	BackgroundWorkerHandle *resolver = NULL;

	memset(receivers, '\0', MTM_MAX_NODES * sizeof(BackgroundWorkerHandle *));

	pqsignal(SIGTERM, die);
	pqsignal(SIGHUP, PostgresSigHupHandler);
	
	MtmBackgroundWorker = true;
	MtmIsMonitorWorker = true;

	/* We're now ready to receive signals */
	BackgroundWorkerUnblockSignals();

	/* Connect to a database */
	memcpy(&db_id, MyBgworkerEntry->bgw_extra, sizeof(Oid));
	memcpy(&user_id, MyBgworkerEntry->bgw_extra + sizeof(Oid), sizeof(Oid));
	BackgroundWorkerInitializeConnectionByOid(db_id, user_id, 0);

	/*
	 * During init_node() our worker is started from transaction that created
	 * mtm config, so we can get here before this transaction is committed,
	 * so we won't see config yet. Just wait for it to became visible.
	 */
	while ((mtm_cfg = MtmLoadConfig()) && mtm_cfg->n_nodes == 0)
	{
		pfree(mtm_cfg);
		MtmSleep(USECS_PER_SEC);
	}

	/*
	 * Ok, we are starting from a basebackup. Delete neighbors from
	 * mtm.cluster_nodes so we don't start receivers using wrong my_node_id.
	 * mtm.join_cluster() should create proper info in mtm.cluster_nodes.
	 */
	if (is_basebackuped(mtm_cfg))
	{
		int			rc;

		mtm_log(LOG, "Basebackup detected");

		StartTransactionCommand();
		if (SPI_connect() != SPI_OK_CONNECT)
			mtm_log(ERROR, "could not connect using SPI");
		PushActiveSnapshot(GetTransactionSnapshot());

		rc = SPI_execute("select pg_replication_origin_drop(name) from "
						 "(select 'mtm_slot_' || id as name from " MTM_NODES " where is_self = 'f') names;",
							false, 0);
		if (rc < 0 || rc != SPI_OK_SELECT)
			mtm_log(ERROR, "Failed to clean up replication origins after a basebackup");

		rc = SPI_execute("delete from " MTM_NODES, false, 0);
		if (rc < 0 || rc != SPI_OK_DELETE)
			mtm_log(ERROR, "Failed to clean up nodes after a basebackup");

		rc = SPI_execute("delete from mtm.syncpoints", false, 0);
		if (rc < 0 || rc != SPI_OK_DELETE)
			mtm_log(ERROR, "Failed to clean up syncpoints after a basebackup");

		SPI_finish();
		PopActiveSnapshot();
		CommitTransactionCommand();

		proc_exit(0);
	}

	/*
	 * Reset mtm_cfg, as it need to be NULL during first call of MtmReloadConfig
	 * to properly fire on_node_create callbacks.
	 */
	pfree(mtm_cfg);
	mtm_cfg = NULL;

	/*
	 * Keep us informed about subscription changes, so we can react on node
	 * addition or deletion.
	 */
	CacheRegisterSyscacheCallback(SUBSCRIPTIONNAME,
								  pubsub_change_cb,
								  (Datum) 0);

	/*
	 * Keep us informed about publication changes. This is used to stop mtm
	 * after our node was dropped.
	 */
	CacheRegisterSyscacheCallback(PUBLICATIONNAME,
								  pubsub_change_cb,
								  (Datum) 0);

	dmq_stream_subscribe("txreq");

	for (;;)
	{
		int rc;

		CHECK_FOR_INTERRUPTS();

		if (ConfigReloadPending)
		{
			ConfigReloadPending = false;
			ProcessConfigFile(PGC_SIGHUP);
		}

		/* check wheter we need to update config */
		AcceptInvalidationMessages();
		if (!config_valid)
		{
			mtm_cfg = MtmReloadConfig(mtm_cfg, start_node_workers,
							stop_node_workers, (Datum) receivers);

			/* we were excluded from cluster */
			if (mtm_cfg->my_node_id == 0)
			{
				int			i;
				int			rc;

				for (i = 0; i < MTM_MAX_NODES; i++)
				{
					if (receivers[i] != NULL)
						stop_node_workers(i + 1, NULL, (Datum) receivers);
				}
				TerminateBackgroundWorker(resolver);

				StartTransactionCommand();
				if (SPI_connect() != SPI_OK_CONNECT)
					mtm_log(ERROR, "could not connect using SPI");
				PushActiveSnapshot(GetTransactionSnapshot());

				rc = SPI_execute("delete from " MTM_NODES, false, 0);
				if (rc < 0 || rc != SPI_OK_DELETE)
					mtm_log(ERROR, "Failed delete nodes");

				if (SPI_finish() != SPI_OK_FINISH)
					mtm_log(ERROR, "could not finish SPI");
				PopActiveSnapshot();
				CommitTransactionCommand();

				// XXX: kill myself somehow?
				proc_exit(0);
			}

			config_valid = true;
		}

		/* Launch resolver after we added dmq destinations */
		// XXX: that's because of current use of Mtm->peers[].dmq_dest_id
		if (resolver == NULL)
			resolver = ResolverStart(db_id, user_id);

		check_status_requests(mtm_cfg);

		rc = WaitLatch(MyLatch,
					   WL_LATCH_SET | WL_TIMEOUT | WL_POSTMASTER_DEATH,
					   1000, PG_WAIT_EXTENSION);

		/* Emergency bailout if postmaster has died */
		if (rc & WL_POSTMASTER_DEATH)
			proc_exit(1);

		if (rc & WL_LATCH_SET)
			ResetLatch(MyLatch);

	}
}
