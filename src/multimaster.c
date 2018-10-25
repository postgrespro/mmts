/*
 * multimaster.c
 *
 * Multimaster based on logical replication
 *
 */

#include <unistd.h>
#include <sys/time.h>
#include <time.h>

#include "postgres.h"
#include "funcapi.h"
#include "fmgr.h"
#include "miscadmin.h"
// #include "common/pg_socket.h"
#include "pgstat.h"
#include "utils/regproc.h"

#include "libpq-fe.h"
#include "lib/stringinfo.h"
#include "libpq/pqformat.h"
#include "common/username.h"

#include "postmaster/postmaster.h"
#include "postmaster/bgworker.h"
#include "storage/lwlock.h"
#include "storage/s_lock.h"
#include "storage/spin.h"
#include "storage/lmgr.h"
#include "storage/shmem.h"
#include "storage/ipc.h"
#include "storage/procarray.h"
#include "access/xlogdefs.h"
#include "access/xact.h"
#include "access/xtm.h"
#include "access/transam.h"
#include "access/subtrans.h"
#include "access/commit_ts.h"
#include "access/xlog.h"
#include "storage/proc.h"
#include "executor/executor.h"
#include "access/twophase.h"
#include "utils/guc.h"
#include "utils/guc_tables.h"
#include "utils/hsearch.h"
#include "utils/timeout.h"
#include "utils/tqual.h"
#include "utils/array.h"
#include "utils/builtins.h"
#include "utils/memutils.h"
#include "commands/dbcommands.h"
#include "commands/extension.h"
#include "commands/sequence.h"
#include "postmaster/autovacuum.h"
#include "storage/pmsignal.h"
#include "storage/proc.h"
#include "utils/syscache.h"
#include "utils/lsyscache.h"
#include "replication/walsender.h"
#include "replication/walsender_private.h"
#include "replication/slot.h"
#include "replication/message.h"
#include "port/atomics.h"
#include "tcop/utility.h"
#include "nodes/makefuncs.h"
#include "access/htup_details.h"
#include "catalog/indexing.h"
#include "catalog/namespace.h"
#include "catalog/pg_constraint.h"
#include "catalog/pg_proc.h"
#include "pglogical_output/hooks.h"
#include "parser/analyze.h"
#include "parser/parse_relation.h"
#include "parser/parse_type.h"
#include "parser/parse_func.h"
#include "catalog/pg_class.h"
#include "catalog/pg_type.h"
#include "tcop/pquery.h"
#include "lib/ilist.h"

#include "multimaster.h"
#include "ddd.h"
#include "state.h"

#include "compat.h"

typedef enum
{
	MTM_STATE_LOCK_ID
} MtmLockIds;

#define MTM_SHMEM_SIZE (8*1024*1024)
#define STATUS_POLL_DELAY USECS_PER_SEC

void _PG_init(void);
void _PG_fini(void);

PG_MODULE_MAGIC;

PG_FUNCTION_INFO_V1(mtm_stop_node);
PG_FUNCTION_INFO_V1(mtm_add_node);
PG_FUNCTION_INFO_V1(mtm_poll_node);
PG_FUNCTION_INFO_V1(mtm_recover_node);
PG_FUNCTION_INFO_V1(mtm_resume_node);
PG_FUNCTION_INFO_V1(mtm_get_nodes_state);
PG_FUNCTION_INFO_V1(mtm_get_cluster_state);
PG_FUNCTION_INFO_V1(mtm_collect_cluster_info);

PG_FUNCTION_INFO_V1(mtm_make_table_local);
PG_FUNCTION_INFO_V1(mtm_broadcast_table);
PG_FUNCTION_INFO_V1(mtm_copy_table);

static void MtmInitialize(void);
static void MtmBeginTransaction(MtmCurrentTrans* x);

static size_t MtmGetTransactionStateSize(void);
static void	  MtmSerializeTransactionState(void* ctx);
static void	  MtmDeserializeTransactionState(void* ctx);
static void	  MtmInitializeSequence(int64* start, int64* step);
static void*  MtmCreateSavepointContext(void);
static void	  MtmRestoreSavepointContext(void* ctx);
static void	  MtmReleaseSavepointContext(void* ctx);
static void   MtmSetRemoteFunction(char const* list, void* extra);
static void*  MtmSuspendTransaction(void);
static void   MtmResumeTransaction(void* ctx);

static void MtmShmemStartup(void);

static bool MtmRunUtilityStmt(PGconn* conn, char const* sql, char **errmsg);
static void MtmBroadcastUtilityStmt(char const* sql, bool ignoreError, int forceOnNode);
static void MtmProcessDDLCommand(char const* queryString, bool transactional);

MtmState* Mtm;

VacuumStmt* MtmVacuumStmt;
IndexStmt*	MtmIndexStmt;
DropStmt*	MtmDropStmt;
void*		MtmTablespaceStmt; /* CREATE/DELETE tablespace */
MemoryContext MtmApplyContext;
MtmConnectionInfo* MtmConnections;

static HTAB* MtmRemoteFunctions;
static HTAB* MtmLocalTables;

bool MtmIsRecoverySession;

static dlist_head MtmLsnMapping = DLIST_STATIC_INIT(MtmLsnMapping);

static TransactionManager MtmTM =
{
	MtmDetectGlobalDeadLock,
	MtmGetTransactionStateSize,
	MtmSerializeTransactionState,
	MtmDeserializeTransactionState,
	MtmInitializeSequence,
	MtmCreateSavepointContext,
	MtmRestoreSavepointContext,
	MtmReleaseSavepointContext,
	MtmSuspendTransaction,
	MtmResumeTransaction
};

char const* const MtmNodeStatusMnem[] =
{
	"Disabled",
	"Recovery",
	"Recovered",
	"Online"
};

char const* const MtmTxnStatusMnem[] =
{
	"InProgress",
	"Committed",
	"Aborted",
	"Unknown"
};

bool  MtmDoReplication;
char* MtmDatabaseName;
char* MtmDatabaseUser;
Oid	  MtmDatabaseId;
bool  MtmBackgroundWorker;

int	  MtmNodes;
int	  MtmNodeId;
int	  MtmReplicationNodeId;
int	  MtmArbiterPort;
int	  MtmNodeDisableDelay;
int	  MtmTransSpillThreshold;
int	  MtmMaxNodes;
int	  MtmHeartbeatSendTimeout;
int	  MtmHeartbeatRecvTimeout;

bool  MtmPreserveCommitOrder;
bool  MtmVolksWagenMode; /* Pretend to be normal postgres. This means skip some NOTICE's and use local sequences */
bool  MtmMajorNode;
char* MtmRefereeConnStr;
int	  MtmWorkers;

static char* MtmConnStrs;
static char* MtmRemoteFunctionsList;
static char* MtmClusterName;
static int	 MtmQueueSize;
static int	 MtmMinRecoveryLag;
static int	 MtmMaxRecoveryLag;
static bool	 MtmIgnoreTablesWithoutPk;
static int	 MtmLockCount;
static bool	 MtmBreakConnection;
static bool  MtmBypass;
static bool	 MtmInsideTransaction;
static bool  MtmMonotonicSequences;
static void const* MtmDDLStatement;

static ExecutorStart_hook_type PreviousExecutorStartHook;
static ExecutorFinish_hook_type PreviousExecutorFinishHook;
static ProcessUtility_hook_type PreviousProcessUtilityHook;
static shmem_startup_hook_type PreviousShmemStartupHook;
static seq_nextval_hook_t PreviousSeqNextvalHook;

static void MtmExecutorStart(QueryDesc *queryDesc, int eflags);
static void MtmExecutorFinish(QueryDesc *queryDesc);
static void MtmProcessUtility(PlannedStmt *pstmt,
										  const char *queryString, ProcessUtilityContext context,
										  ParamListInfo params,
										  QueryEnvironment *queryEnv,
										  DestReceiver *dest, char *completionTag);
static void MtmSeqNextvalHook(Oid seqid, int64 next);

static bool MtmAtExitHookRegistered = false;

/*
 * Release multimaster main lock if been hold.
 * This function is called when backend is terminated because of critical error or when error is catched
 * by FINALLY block
 */
void MtmReleaseLocks(void)
{
	MtmResetTransaction();
	if (MtmInsideTransaction)
	{
		MtmLock(LW_EXCLUSIVE);
		Assert(Mtm->nRunningTransactions > 0);
		Mtm->nRunningTransactions -= 1;
		MtmInsideTransaction = false;
		MtmUnlock();
	}
	// if (MtmClusterLocked) {
	// 	MtmUnlockCluster();
	// }
}

/*
 * -------------------------------------------
 * Synchronize access to MTM structures.
 * Using LWLock seems to be more efficient (at our benchmarks)
 * Multimaster uses trash of 2N+1 lwlocks, where N is number of nodes.
 * locks[0] is used to synchronize access to multimaster state,
 * locks[1..N] are used to provide exclusive access to replication session for each node
 * locks[N+1..2*N] are used to synchronize access to distributed lock graph at each node
 * -------------------------------------------
 */

// #define DEBUG_MTM_LOCK 1

#if DEBUG_MTM_LOCK
static timestamp_t MtmLockLastReportTime;
static timestamp_t MtmLockElapsedWaitTime;
static timestamp_t MtmLockMaxWaitTime;
static size_t      MtmLockHitCount;
#endif

void MtmLock(LWLockMode mode)
{
	if (!MtmAtExitHookRegistered) {
		atexit(MtmReleaseLocks);
		MtmAtExitHookRegistered = true;
	}
	if (MtmLockCount != 0 && Mtm->lastLockHolder == MyProcPid) {
		MtmLockCount += 1;
	}
	else
	{
#if DEBUG_MTM_LOCK
		timestamp_t start, stop;
		start = MtmGetSystemTime();
#endif
		if (MyProc == NULL) { /* Can not wait if have no PGPROC. It can happen at process exit. TODO: without lock we can get race condition and corrupt Mtm state */
			return;
		}
		LWLockAcquire((LWLockId)&Mtm->locks[MTM_STATE_LOCK_ID], mode);
#if DEBUG_MTM_LOCK
		stop = MtmGetSystemTime();
		MtmLockElapsedWaitTime += stop - start;
		if (stop - start > MtmLockMaxWaitTime) {
			MtmLockMaxWaitTime = stop - start;
		}
		MtmLockHitCount += 1;
		if (stop - MtmLockLastReportTime > USECS_PER_SEC) {
			MTM_LOG1("%d: average lock wait time %lld usec, maximal lock wait time: %lld usec",
					 MyProcPid, MtmLockElapsedWaitTime/MtmLockHitCount, MtmLockMaxWaitTime);
			MtmLockLastReportTime = stop;
			MtmLockMaxWaitTime = 0;
			MtmLockElapsedWaitTime = 0;
			MtmLockHitCount = 0;
		}
#endif
		if (mode == LW_EXCLUSIVE) {
			Assert(MyProcPid != 0);
			Mtm->lastLockHolder = MyProcPid;
			Assert(MyProcPid);
			MtmLockCount = 1;
		} else {
			MtmLockCount = 0;
		}
	}
}

void MtmUnlock(void)
{
	if (Mtm->lastLockHolder == MyProcPid)
	{
		if (MtmLockCount != 0 && --MtmLockCount != 0) {
			return;
		}
		Mtm->lastLockHolder = 0;
	}
	else
	{
		MtmLockCount = 0;
	}

	/* If we have no PGPROC, then lock was not obtained. */
	if (MyProc != NULL)
		LWLockRelease((LWLockId)&Mtm->locks[MTM_STATE_LOCK_ID]);
}

void MtmDeepUnlock(void)
{
	if (MtmLockCount == 0)
		return;

	Assert(Mtm->lastLockHolder == MyProcPid);

	MtmLockCount = 0;
	Mtm->lastLockHolder = 0;

	/* If we have no PGPROC, then lock was not obtained. */
	if (MyProc != NULL)
		LWLockRelease((LWLockId)&Mtm->locks[MTM_STATE_LOCK_ID]);
}

void MtmLockNode(int nodeId, LWLockMode mode)
{
	Assert(nodeId > 0 && nodeId <= MtmMaxNodes*2);
	LWLockAcquire((LWLockId)&Mtm->locks[nodeId], mode);
}

bool MtmTryLockNode(int nodeId, LWLockMode mode)
{
	return LWLockConditionalAcquire((LWLockId)&Mtm->locks[nodeId], mode);
}

void MtmUnlockNode(int nodeId)
{
	Assert(nodeId > 0 && nodeId <= MtmMaxNodes*2);
	LWLockRelease((LWLockId)&Mtm->locks[nodeId]);
}

/*
 * -------------------------------------------
 * System time manipulation functions
 * -------------------------------------------
 */


timestamp_t MtmGetSystemTime(void)
{
	struct timeval tv;
	gettimeofday(&tv, NULL);
	return (timestamp_t)tv.tv_sec*USECS_PER_SEC + tv.tv_usec;
}

void MtmSleep(timestamp_t usec)
{
	timestamp_t waketm = MtmGetSystemTime() + usec;

	for (;;)
	{
		int rc;
		timestamp_t sleepfor;

		CHECK_FOR_INTERRUPTS();

		sleepfor = waketm - MtmGetSystemTime();
		if (sleepfor < 0)
			break;

		rc = WaitLatch(MyLatch,
						WL_TIMEOUT | WL_POSTMASTER_DEATH,
						sleepfor/1000.0, WAIT_EVENT_BGWORKER_STARTUP);

		if (rc & WL_POSTMASTER_DEATH)
			proc_exit(1);
	}
}


/*
 * Distribute transaction manager functions
 */

static size_t
MtmGetTransactionStateSize(void)
{
	return sizeof(MtmTx);
}

static void
MtmSerializeTransactionState(void* ctx)
{
	memcpy(ctx, &MtmTx, sizeof(MtmTx));
}

static void
MtmDeserializeTransactionState(void* ctx)
{
	memcpy(&MtmTx, ctx, sizeof(MtmTx));
}


static void
MtmInitializeSequence(int64* start, int64* step)
{
	if (MtmVolksWagenMode)
	{
		*start = 1;
		*step  = 1;
	}
	else
	{
		*start = MtmNodeId;
		*step  = MtmMaxNodes;
	}
}

static void* MtmCreateSavepointContext(void)
{
	return (void*)(size_t)MtmTx.containsDML;
}

static void	 MtmRestoreSavepointContext(void* ctx)
{
	MtmTx.containsDML = ctx != NULL;
}

static void	 MtmReleaseSavepointContext(void* ctx)
{
}

static void* MtmSuspendTransaction(void)
{
	MtmCurrentTrans* ctx = malloc(sizeof(MtmCurrentTrans));
	*ctx = MtmTx;
	MtmResetTransaction();
	MtmBeginTransaction(&MtmTx);
	return ctx;
}

static void MtmResumeTransaction(void* ctx)
{
	MtmTx = *(MtmCurrentTrans*)ctx;
	MtmInsideTransaction = true;
	free(ctx);
}

/*
 * Check if this is "normal" user transaction which should be distributed to other nodes
 */
bool
MtmIsUserTransaction()
{
	return !IsAutoVacuumLauncherProcess() &&
		IsNormalProcessingMode() &&
		MtmDoReplication &&
		!am_walsender &&
		!MtmBackgroundWorker &&
		!IsAutoVacuumWorkerProcess();
}

void
MtmResetTransaction()
{
	MtmCurrentTrans* x = &MtmTx;
	x->snapshot = INVALID_CSN;
	x->xid = InvalidTransactionId;
	x->gtid.xid = InvalidTransactionId;
	x->isDistributed = false;
	x->isPrepared = false;
	x->isSuspended = false;
	x->isActive = false;
	x->isTwoPhase = false;
	x->csn = INVALID_CSN;
	x->status = TRANSACTION_STATUS_UNKNOWN;
	x->gid[0] = '\0';
	MtmDDLStatement = NULL;
}

static void
MtmBeginTransaction(MtmCurrentTrans* x)
{
	x->xid = GetCurrentTransactionIdIfAny();
	x->isReplicated = MtmIsLogicalReceiver;
	x->isDistributed = MtmIsUserTransaction();
	x->isPrepared = false;
	x->isSuspended = false;
	x->isTwoPhase = false;
	x->isTransactionBlock = IsTransactionBlock();
	x->containsDML = false;
	x->gtid.xid = InvalidTransactionId;
	x->gid[0] = '\0';
	x->status = TRANSACTION_STATUS_IN_PROGRESS;
	MtmInsideTransaction = true;
	MtmDDLStatement = NULL;
}


/*
 * Initialize message
 */
void MtmInitMessage(MtmArbiterMessage* msg, MtmMessageCode code)
{
	memset(msg, '\0', sizeof(MtmArbiterMessage));

	msg->code = code;
	msg->disabledNodeMask = Mtm->disabledNodeMask;
	msg->connectivityMask = SELF_CONNECTIVITY_MASK;
	msg->oldestSnapshot = Mtm->nodes[MtmNodeId-1].oldestSnapshot;
	msg->lockReq = Mtm->originLockNodeMask != 0;
	msg->locked = (Mtm->originLockNodeMask|Mtm->inducedLockNodeMask) != 0;
	msg->node = MtmNodeId;
}


static void MtmDropSlot(int nodeId)
{
	if (MtmTryLockNode(nodeId, LW_EXCLUSIVE))
	{
		MTM_ELOG(INFO, "Drop replication slot for node %d", nodeId);
		ReplicationSlotDrop(psprintf(MULTIMASTER_SLOT_PATTERN, nodeId), false);
		MtmUnlockNode(nodeId);
	} else {
		MTM_ELOG(WARNING, "Failed to drop replication slot for node %d", nodeId);
	}
	MtmLock(LW_EXCLUSIVE);
	BIT_SET(Mtm->stalledNodeMask, nodeId-1);
	BIT_SET(Mtm->stoppedNodeMask, nodeId-1); /* stalled node can not be automatically recovered */
	MtmUnlock();
}


void  MtmSetCurrentTransactionGID(char const* gid, int node_id)
{
	MTM_LOG3("Set current transaction xid="XID_FMT" GID %s", MtmTx.xid, gid);
	strncpy(MtmTx.gid, gid, GIDSIZE);
	MtmTx.isDistributed = true;
	MtmTx.isReplicated = true;
	MtmTx.gtid.node = node_id;
}

TransactionId MtmGetCurrentTransactionId(void)
{
	return MtmTx.xid;
}

void  MtmSetCurrentTransactionCSN(csn_t csn)
{
	MTM_LOG3("Set current transaction CSN %lld", csn);
	MtmTx.csn = csn;
	MtmTx.isDistributed = true;
	MtmTx.isReplicated = true;
}


/*
 * -------------------------------------------
 * HA functions
 * -------------------------------------------
 */

/*
 * Handle critical errors while applying transaction at replica.
 * Such errors should cause shutdown of this cluster node to allow other nodes to continue serving client requests.
 * Other error will be just reported and ignored
 */
void MtmHandleApplyError(void)
{
	ErrorData *edata = CopyErrorData();
	MtmLockCount = 0; /* LWLocks will be released by AbortTransaction, we just need to clear owr MtmLockCount */
	switch (edata->sqlerrcode) {
		case ERRCODE_DISK_FULL:
		case ERRCODE_INSUFFICIENT_RESOURCES:
		case ERRCODE_IO_ERROR:
		case ERRCODE_DATA_CORRUPTED:
		case ERRCODE_INDEX_CORRUPTED:
		  /* Should we really treate this errors as fatal?
		case ERRCODE_SYSTEM_ERROR:
		case ERRCODE_INTERNAL_ERROR:
		case ERRCODE_OUT_OF_MEMORY:
		  */
			MtmStateProcessEvent(MTM_NONRECOVERABLE_ERROR);
	}
	FreeErrorData(edata);
}



/**
 * Check state of replication slots. If some of them are too much lag behind wal, then drop this slots to avoid
 * WAL overflow
 */
void
MtmCheckSlots()
{
	if (MtmMaxRecoveryLag != 0 && Mtm->disabledNodeMask != 0)
	{
		int i;
		for (i = 0; i < max_replication_slots; i++) {
			ReplicationSlot* slot = &ReplicationSlotCtl->replication_slots[i];
			int nodeId;
			if (slot->in_use
				&& sscanf(slot->data.name.data, MULTIMASTER_SLOT_PATTERN, &nodeId) == 1
				&& BIT_CHECK(Mtm->disabledNodeMask, nodeId-1)
				&& slot->data.confirmed_flush + (long64) MtmMaxRecoveryLag * 1024 < GetXLogInsertRecPtr()
				&& slot->data.confirmed_flush != 0)
			{
				MTM_ELOG(WARNING, "Drop slot for node %d which lag %lld B is larger than threshold %d kB",
					 nodeId,
					 (long64)(GetXLogInsertRecPtr() - slot->data.restart_lsn),
					 MtmMaxRecoveryLag);
				MtmDropSlot(nodeId);
			}
		}
	}
}

/*
 * Get lag between replication slot position (dsata proceeded by WAL sender) and current position in WAL
 */
static int64 MtmGetSlotLag(int nodeId)
{
	int i;
	for (i = 0; i < max_replication_slots; i++) {
		ReplicationSlot* slot = &ReplicationSlotCtl->replication_slots[i];
		int node;
		if (slot->in_use
			&& sscanf(slot->data.name.data, MULTIMASTER_SLOT_PATTERN, &node) == 1
			&& node == nodeId)
		{
			return GetXLogInsertRecPtr() - slot->data.confirmed_flush;
		}
	}
	return -1;
}


/*
 * This function is called by WAL sender when start sending new transaction.
 * It returns true if specified node is in recovery mode. In this case we should send to it all transactions from WAL,
 * not only coordinated by self node as in normal mode.
 */
bool MtmIsRecoveredNode(int nodeId)
{
	if (BIT_CHECK(Mtm->disabledNodeMask, nodeId-1)) {
		if (!MtmIsRecoverySession) {
			MtmDeepUnlock();
			MTM_ELOG(ERROR, "Node %d is marked as disabled but is not in recovery mode", nodeId);
		}
		return true;
	} else {
		return false;
	}
}

/*
 * Check if wal sender replayed all transactions from WAL log.
 * It can never happen if there are many active transactions.
 * In this case we wait until gap between sent and current position in the
 * WAL becomes smaller than threshold value MtmMinRecoveryLag and
 * after it prohibit start of new transactions until WAL is completely replayed.
 */
void MtmCheckRecoveryCaughtUp(int nodeId, lsn_t slotLSN)
{
	MtmLock(LW_EXCLUSIVE);
	if (MtmIsRecoveredNode(nodeId)) {
		lsn_t walLSN = GetXLogInsertRecPtr();
		if (!BIT_CHECK(Mtm->originLockNodeMask, nodeId-1)
			&& slotLSN + (long64) MtmMinRecoveryLag * 1024 > walLSN)
		{
			/*
			 * Wal sender almost caught up.
			 * Lock cluster preventing new transaction to start until wal is completely replayed.
			 * We have to maintain two bitmasks: one is marking wal sender, another - correspondent nodes.
			 * Is there some better way to establish mapping between nodes ad WAL-seconder?
			 */
			MTM_LOG1("Node %d is almost caught-up: slot position %llx, WAL position %llx, active transactions %d",
				 nodeId, slotLSN, walLSN, Mtm->nActiveTransactions);

			MTM_LOG1("[LOCK] set lock on MtmCheckRecoveryCaughtUp");
			BIT_SET(Mtm->originLockNodeMask, nodeId-1); // XXXX: log that
		} else {
			MTM_LOG2("Continue recovery of node %d, slot position %llx, WAL position %llx,"
					 " WAL sender position %llx, lockers %llx, active transactions %d",
					 nodeId, (long long unsigned int) slotLSN,
					 (long long unsigned int) walLSN,
					 (long long unsigned int) MyWalSnd->sentPtr,
					 Mtm->originLockNodeMask, Mtm->nActiveTransactions);
		}
	}
	MtmUnlock();
}

/*
 * Notification about node recovery completion.
 * If recovery is in progress and WAL sender replays all records in WAL,
 * then enable recovered node and send notification to it about end of recovery.
 */
bool MtmRecoveryCaughtUp(int nodeId, lsn_t walEndPtr)
{
	bool caughtUp = false;
	MtmLock(LW_EXCLUSIVE);
	if (MtmIsRecoveredNode(nodeId))
	{
		MtmStateProcessNeighborEvent(nodeId, MTM_NEIGHBOR_RECOVERY_CAUGHTUP);
		caughtUp = true;
		MtmIsRecoverySession = false;
	}
	MtmUnlock();
	return caughtUp;
}


/*
 * -------------------------------------------
 * Node initialization
 * -------------------------------------------
 */

/*
 * Initialize hash table used to mark local (not distributed) tables
 */
static HTAB*
MtmCreateLocalTableMap(void)
{
	HASHCTL info;
	HTAB* htab;
	memset(&info, 0, sizeof(info));
	info.entrysize = info.keysize = sizeof(Oid);
	htab = ShmemInitHash(
		"MtmLocalTables",
		MULTIMASTER_MAX_LOCAL_TABLES, MULTIMASTER_MAX_LOCAL_TABLES,
		&info,
		HASH_ELEM | HASH_BLOBS
	);
	return htab;
}

void MtmMakeRelationLocal(Oid relid)
{
	if (OidIsValid(relid)) {
		MtmLock(LW_EXCLUSIVE);
		hash_search(MtmLocalTables, &relid, HASH_ENTER, NULL);
		MtmUnlock();
	}
}


void MtmMakeTableLocal(char const* schema, char const* name)
{
	RangeVar* rv = makeRangeVar((char*)schema, (char*)name, -1);
	Oid relid = RangeVarGetRelid(rv, NoLock, true);
	MtmMakeRelationLocal(relid);
}


typedef struct {
	NameData schema;
	NameData name;
} MtmLocalTablesTuple;

static void MtmLoadLocalTables(void)
{
	RangeVar	   *rv;
	Relation		rel;
	SysScanDesc		scan;
	HeapTuple		tuple;

	Assert(IsTransactionState());

	rv = makeRangeVar(MULTIMASTER_SCHEMA_NAME, MULTIMASTER_LOCAL_TABLES_TABLE, -1);
	rel = heap_openrv_extended(rv, RowExclusiveLock, true);
	if (rel != NULL) {
		scan = systable_beginscan(rel, 0, true, NULL, 0, NULL);

		while (HeapTupleIsValid(tuple = systable_getnext(scan)))
		{
			MtmLocalTablesTuple	*t = (MtmLocalTablesTuple*) GETSTRUCT(tuple);
			MtmMakeTableLocal(NameStr(t->schema), NameStr(t->name));
		}

		systable_endscan(scan);
		heap_close(rel, RowExclusiveLock);
	}
}

/*
 * Multimaster control file is used to prevent erroneous inclusion of node in the cluster.
 * It contains cluster name (any user defined identifier) and node id.
 * In case of creating new cluster node using pg_basebackup this file is copied together will
 * all other PostgreSQL files and so new node will know ID of the cluster node from which it
 * is cloned. It is necessary to complete synchronization of new node with the rest of the cluster.
 */
static void MtmCheckControlFile(void)
{
	char controlFilePath[MAXPGPATH];
	char buf[MULTIMASTER_MAX_CTL_STR_SIZE];
	FILE* f;
	snprintf(controlFilePath, MAXPGPATH, "%s/global/mmts_control", DataDir);
	f = fopen(controlFilePath, "r");
	if (f != NULL && fgets(buf, sizeof buf, f)) {
		char* sep = strchr(buf, ':');
		if (sep == NULL) {
			MTM_ELOG(FATAL, "File mmts_control doesn't contain cluster name");
		}
		*sep = '\0';
		if (strcmp(buf, MtmClusterName) != 0) {
			MTM_ELOG(FATAL, "Database belongs to some other cluster %s rather than %s", buf, MtmClusterName);
		}
		if (sscanf(sep+1, "%d", &Mtm->donorNodeId) != 1) {
			MTM_ELOG(FATAL, "File mmts_control doesn't contain node id");
		}
		fclose(f);
	} else {
		if (f != NULL) {
			fclose(f);
		}
		f = fopen(controlFilePath, "w");
		if (f == NULL) {
			MTM_ELOG(FATAL, "Failed to create mmts_control file: %m");
		}
		Mtm->donorNodeId = MtmNodeId;
		fprintf(f, "%s:%d\n", MtmClusterName, Mtm->donorNodeId);
		fclose(f);
	}
}

/*
 * Update control file and donor node id to enable recovery from any node
 * after syncing new node with its donor
 */
void MtmUpdateControlFile()
{
	if (Mtm->donorNodeId != MtmNodeId)
	{
		char controlFilePath[MAXPGPATH];
		FILE* f;

		Mtm->donorNodeId = MtmNodeId;
		snprintf(controlFilePath, MAXPGPATH, "%s/global/mmts_control", DataDir);
		f = fopen(controlFilePath, "w");
		if (f == NULL) {
			MTM_ELOG(FATAL, "Failed to create mmts_control file: %m");
		}
		fprintf(f, "%s:%d\n", MtmClusterName, Mtm->donorNodeId);
		fclose(f);
	}
}
/*
 * Perform initialization of multimaster state.
 * This function is called from shared memory startup hook (after completion of initialization of shared memory)
 */
static void MtmInitialize()
{
	bool found;
	int i;

	MtmDeadlockDetectorShmemStartup(MtmMaxNodes);

	LWLockAcquire(AddinShmemInitLock, LW_EXCLUSIVE);
	Mtm = (MtmState*)ShmemInitStruct(MULTIMASTER_NAME, sizeof(MtmState) + sizeof(MtmNodeInfo)*(MtmMaxNodes-1), &found);
	if (!found)
	{
		MemSet(Mtm, 0, sizeof(MtmState) + sizeof(MtmNodeInfo)*(MtmMaxNodes-1));
		Mtm->extension_created = false;
		Mtm->status = MTM_DISABLED; //MTM_INITIALIZATION;
		Mtm->recoverySlot = 0;
		Mtm->locks = GetNamedLWLockTranche(MULTIMASTER_NAME);
		Mtm->csn = 42;
		Mtm->lastCsn = INVALID_CSN;
		Mtm->oldestXid = FirstNormalTransactionId;
		Mtm->nLiveNodes = 0; //MtmNodes;
		Mtm->nAllNodes = MtmNodes;
		Mtm->disabledNodeMask =  (((nodemask_t)1 << MtmNodes) - 1);
		Mtm->clique = (((nodemask_t)1 << Mtm->nAllNodes) - 1); //0;
		Mtm->refereeGrant = false;
		Mtm->refereeWinnerId = 0;
		Mtm->stalledNodeMask = 0;
		Mtm->stoppedNodeMask = 0;
		Mtm->deadNodeMask = 0;
		Mtm->recoveredNodeMask = 0;
		Mtm->pglogicalReceiverMask = 0;
		Mtm->pglogicalSenderMask = 0;
		Mtm->inducedLockNodeMask = 0;
		Mtm->currentLockNodeMask = 0;
		Mtm->originLockNodeMask = 0;
		Mtm->reconnectMask = 0;
		Mtm->recoveredLSN = INVALID_LSN;
		Mtm->nActiveTransactions = 0;
		Mtm->nRunningTransactions = 0;
		Mtm->votingTransactions = NULL;
		Mtm->transListHead = NULL;
		Mtm->transListTail = &Mtm->transListHead;
		Mtm->activeTransList.next = Mtm->activeTransList.prev = &Mtm->activeTransList;
		Mtm->nReceivers = 0;
		Mtm->nSenders = 0;
		Mtm->timeShift = 0;
		Mtm->transCount = 0;
		Mtm->gcCount = 0;
		Mtm->nConfigChanges = 0;
		Mtm->recoveryCount = 0;
		Mtm->localTablesHashLoaded = false;
		Mtm->preparedTransactionsLoaded = false;
		Mtm->inject2PCError = 0;
		Mtm->sendQueue = NULL;
		Mtm->freeQueue = NULL;
		for (i = 0; i < MtmMaxNodes; i++)
		{
			Mtm->nodes[i].oldestSnapshot = 0;
			Mtm->nodes[i].disabledNodeMask = 0;
			Mtm->nodes[i].connectivityMask = (((nodemask_t)1 << MtmNodes) - 1) & ~((nodemask_t)1 << (MtmNodeId-1));
			Mtm->nodes[i].lockGraphUsed = 0;
			Mtm->nodes[i].lockGraphAllocated = 0;
			Mtm->nodes[i].lockGraphData = NULL;
			Mtm->nodes[i].transDelay = 0;
			Mtm->nodes[i].lastStatusChangeTime = MtmGetSystemTime();
			Mtm->nodes[i].con = MtmConnections[i];
			Mtm->nodes[i].flushPos = 0;
			Mtm->nodes[i].lastHeartbeat = 0;
			Mtm->nodes[i].restartLSN = INVALID_LSN;
			Mtm->nodes[i].originId = InvalidRepOriginId;
			Mtm->nodes[i].timeline = 0;
			Mtm->nodes[i].nHeartbeats = 0;
			Mtm->nodes[i].manualRecovery = false;
			Mtm->nodes[i].slotDeleted = false;
			BgwPoolInit(&Mtm->nodes[i].pool, MtmExecutor, MtmDatabaseName, MtmDatabaseUser, MtmQueueSize, 0);
		}
		Mtm->nodes[MtmNodeId-1].originId = DoNotReplicateId;
		/* All transaction originated from the current node should be ignored during recovery */
		Mtm->nodes[MtmNodeId-1].restartLSN = (lsn_t)PG_UINT64_MAX;
		Mtm->sendSemaphore = PGSemaphoreCreate();
		PGSemaphoreReset(Mtm->sendSemaphore);
		SpinLockInit(&Mtm->queueSpinlock);
		MtmTx.snapshot = INVALID_CSN;
		MtmTx.xid = InvalidTransactionId;
	}

	RegisterXactCallback(MtmXactCallback2, NULL);

	MtmLocalTables = MtmCreateLocalTableMap();
	MtmDoReplication = true;
	TM = &MtmTM;
	LWLockRelease(AddinShmemInitLock);

	MtmCheckControlFile();

}

static void
MtmShmemStartup(void)
{
	if (PreviousShmemStartupHook) {
		PreviousShmemStartupHook();
	}
	MtmInitialize();
}

static void MtmSetRemoteFunction(char const* list, void* extra)
{
	if (MtmRemoteFunctions) {
		hash_destroy(MtmRemoteFunctions);
		MtmRemoteFunctions = NULL;
	}
}

static void MtmInitializeRemoteFunctionsMap()
{
	HASHCTL info;
	char* p, *q;
	int n_funcs = 1;
	FuncCandidateList clist;

	for (p = MtmRemoteFunctionsList; (q = strchr(p, ',')) != NULL; p = q + 1, n_funcs++);

	Assert(MtmRemoteFunctions == NULL);

	memset(&info, 0, sizeof(info));
	info.entrysize = info.keysize = sizeof(Oid);
	info.hcxt = TopMemoryContext;
	MtmRemoteFunctions = hash_create("MtmRemoteFunctions", n_funcs, &info,
									 HASH_ELEM | HASH_BLOBS | HASH_CONTEXT);

	p = pstrdup(MtmRemoteFunctionsList);
	do {
		q = strchr(p, ',');
		if (q != NULL) {
			*q++ = '\0';
		}
		clist = FuncnameGetCandidates(stringToQualifiedNameList(p), -1, NIL, false, false, true);
		if (clist == NULL) {
			MTM_ELOG(WARNING, "Failed to lookup function %s", p);
		} else if (clist->next != NULL) {
			MTM_ELOG(ERROR, "Ambigious function %s", p);
		} else {
			hash_search(MtmRemoteFunctions, &clist->oid, HASH_ENTER, NULL);
		}
		p = q;
	} while (p != NULL);

	clist = FuncnameGetCandidates(stringToQualifiedNameList("mtm.alter_sequences"), -1, NIL, false, false, true);
	if (clist != NULL) {
		hash_search(MtmRemoteFunctions, &clist->oid, HASH_ENTER, NULL);
	}
}

/*
 * Parse node connection string.
 * This function is called at cluster startup and while adding new cluster node
 */
void MtmUpdateNodeConnectionInfo(MtmConnectionInfo* conn, char const* connStr)
{
	char const* host;
	char const* end;
	int			hostLen;
	char const* port;
	int			connStrLen = (int)strlen(connStr);

	if (connStrLen >= MULTIMASTER_MAX_CONN_STR_SIZE) {
		MTM_ELOG(ERROR, "Too long (%d) connection string '%s': limit is %d",
			 connStrLen, connStr, MULTIMASTER_MAX_CONN_STR_SIZE-1);
	}

	while(isspace(*connStr))
		connStr++;

	strncpy(conn->connStr, connStr, MULTIMASTER_MAX_CONN_STR_SIZE);

	host = strstr(connStr, "host=");
	if (host == NULL) {
		MTM_ELOG(ERROR, "Host not specified in connection string: '%s'", connStr);
	}
	host += 5;
	for (end = host; *end != ' ' && *end != '\0'; end++);
	hostLen = end - host;
	if (hostLen >= MULTIMASTER_MAX_HOST_NAME_SIZE) {
		MTM_ELOG(ERROR, "Too long (%d) host name '%.*s': limit is %d",
			 hostLen, hostLen, host, MULTIMASTER_MAX_HOST_NAME_SIZE-1);
	}
	memcpy(conn->hostName, host, hostLen);
	conn->hostName[hostLen] = '\0';

	port = strstr(connStr, "arbiter_port=");
	if (port != NULL) {
		if (sscanf(port+13, "%d", &conn->arbiterPort) != 1) {
			MTM_ELOG(ERROR, "Invalid arbiter port: %s", port+13);
		}
	} else {
		conn->arbiterPort = MULTIMASTER_DEFAULT_ARBITER_PORT;
	}
	MTM_ELOG(INFO, "Using arbiter port: %d", conn->arbiterPort);

	port = strstr(connStr, " port=");
	if (port == NULL && strncmp(connStr, "port=", 5) == 0) {
		port = connStr-1;
	}
	if (port != NULL) {
		if (sscanf(port+6, "%d", &conn->postmasterPort) != 1) {
			MTM_ELOG(ERROR, "Invalid postmaster port: %s", port+6);
		}
	} else {
		conn->postmasterPort = DEF_PGPORT;
	}
}

/*
 * Parse "multimaster.conn_strings" configuration parameter and
 * set connection string for each node using MtmUpdateNodeConnectionInfo
 */
static void MtmSplitConnStrs(void)
{
	int i;
	FILE* f = NULL;
	char buf[MULTIMASTER_MAX_CTL_STR_SIZE];
	MemoryContext old_context = MemoryContextSwitchTo(TopMemoryContext);

	if (*MtmConnStrs == '@') {
		f = fopen(MtmConnStrs+1, "r");
		for (i = 0; fgets(buf, sizeof buf, f) != NULL; i++) {
			if (strlen(buf) <= 1) {
				MTM_ELOG(ERROR, "Empty lines are not allowed in %s file", MtmConnStrs+1);
			}
		}
	} else {
		char* p = MtmConnStrs;
		for (i = 0; *p != '\0'; p++, i++) {
			if ((p = strchr(p, ',')) == NULL) {
				i += 1;
				break;
			}
		}
	}

	if (i > MAX_NODES) {
		MTM_ELOG(ERROR, "Multimaster with more than %d nodes is not currently supported", MAX_NODES);
	}
	if (i < 2) {
		MTM_ELOG(ERROR, "Multimaster should have at least two nodes");
	}
	if (MtmMaxNodes == 0) {
		MtmMaxNodes = i;
	} else if (MtmMaxNodes < i) {
		MTM_ELOG(ERROR, "More than %d nodes are specified", MtmMaxNodes);
	}
	MtmNodes = i;
	MtmConnections = (MtmConnectionInfo*)palloc0(MtmMaxNodes*sizeof(MtmConnectionInfo));

	if (f != NULL) {
		fseek(f, SEEK_SET, 0);
		for (i = 0; fgets(buf, sizeof buf, f) != NULL; i++) {
			size_t len = strlen(buf);
			if (buf[len-1] == '\n') {
				buf[len-1] = '\0';
			}
			MtmUpdateNodeConnectionInfo(&MtmConnections[i], buf);
		}
		fclose(f);
	} else {
		char* copy = pstrdup(MtmConnStrs);
		char* connStr = copy;
		char* connStrEnd = connStr + strlen(connStr);

		for (i = 0; connStr < connStrEnd; i++) {
			char* p = strchr(connStr, ',');
			if (p == NULL) {
				p = connStrEnd;
			}
			*p = '\0';
			MtmUpdateNodeConnectionInfo(&MtmConnections[i], connStr);
			connStr = p + 1;
		}
		pfree(copy);
	}

		if (MtmNodeId == INT_MAX) {
			if (gethostname(buf, sizeof buf) != 0) {
				MTM_ELOG(ERROR, "Failed to get host name: %m");
			}
			for (i = 0; i < MtmNodes; i++) {
				MTM_LOG3("Node %d, host %s, port=%d, my port %d", i, MtmConnections[i].hostName, MtmConnections[i].postmasterPort, PostPortNumber);
				if ((strcmp(MtmConnections[i].hostName, buf) == 0 || strcmp(MtmConnections[i].hostName, "localhost") == 0 || strcmp(MtmConnections[i].hostName, "127.0.0.1") == 0)
					&& MtmConnections[i].postmasterPort == PostPortNumber)
				{
					if (MtmNodeId == INT_MAX) {
						MtmNodeId = i+1;
					} else {
						MTM_ELOG(ERROR, "multimaster.node_id is not explicitly specified and more than one nodes are configured for host %s port %d", buf, PostPortNumber);
					}
				}
			}
			if (MtmNodeId == INT_MAX) {
				MTM_ELOG(ERROR, "multimaster.node_id is not specified and host name %s can not be located in connection strings list", buf);
			}
		} else if (MtmNodeId > i) {
			MTM_ELOG(ERROR, "Multimaster node id %d is out of range [%d..%d]", MtmNodeId, 1, MtmNodes);
		}
		{
			char* connStr = MtmConnections[MtmNodeId-1].connStr;
			char* dbName = strstr(connStr, "dbname="); // XXX: shoud we care about string 'itisnotdbname=xxx'?
			char* dbUser = strstr(connStr, "user=");
			char* end;
			size_t len;

			if (dbName == NULL)
				MTM_ELOG(ERROR, "Database is not specified in connection string: '%s'", connStr);

			if (dbUser == NULL)
			{
				char *errstr;
				const char *username = get_user_name(&errstr);
				if (!username)
					MTM_ELOG(FATAL, "Database user is not specified in connection string '%s', fallback failed: %s", connStr, errstr);
				else
					MTM_ELOG(WARNING, "Database user is not specified in connection string '%s', fallback to '%s'", connStr, username);
				MtmDatabaseUser = pstrdup(username);
			}
			else
			{
				dbUser += 5;
				end = strchr(dbUser, ' ');
				if (!end) end = strchr(dbUser, '\0');
				Assert(end != NULL);
				len = end - dbUser;
				MtmDatabaseUser = pnstrdup(dbUser, len);
			}

			dbName += 7;
			end = strchr(dbName, ' ');
			if (!end) end = strchr(dbName, '\0');
			Assert(end != NULL);
			len = end - dbName;
			MtmDatabaseName = pnstrdup(dbName, len);
		}
	MemoryContextSwitchTo(old_context);
}

/*
 * Check correctness of multimaster configuration
 */
static bool ConfigIsSane(void)
{
	bool ok = true;

#if 0
	if (DefaultXactIsoLevel != XACT_REPEATABLE_READ)
	{
		MTM_ELOG(WARNING, "multimaster requires default_transaction_isolation = 'repeatable read'");
		ok = false;
	}
#endif

	if (MtmMaxNodes < 1)
	{
		MTM_ELOG(WARNING, "multimaster requires multimaster.max_nodes > 0");
		ok = false;
	}

	if (max_prepared_xacts < 1)
	{
		MTM_ELOG(WARNING,
			 "multimaster requires max_prepared_transactions > 0, "
			 "because all transactions are implicitly two-phase");
		ok = false;
	}

	{
		int workers_required = 2 * MtmMaxNodes + 1;
		if (max_worker_processes < workers_required)
		{
			MTM_ELOG(WARNING,
				 "multimaster requires max_worker_processes >= %d",
				 workers_required);
			ok = false;
		}
	}

	if (wal_level != WAL_LEVEL_LOGICAL)
	{
		MTM_ELOG(WARNING,
			 "multimaster requires wal_level = 'logical', "
			 "because it is build on top of logical replication");
		ok = false;
	}

	if (max_wal_senders < MtmMaxNodes)
	{
		MTM_ELOG(WARNING,
			 "multimaster requires max_wal_senders >= %d (multimaster.max_nodes), ",
			 MtmMaxNodes);
		ok = false;
	}

	if (max_replication_slots < MtmMaxNodes)
	{
		MTM_ELOG(WARNING,
			 "multimaster requires max_replication_slots >= %d (multimaster.max_nodes), ",
			 MtmMaxNodes);
		ok = false;
	}

	return ok;
}

void
_PG_init(void)
{
	/*
	 * In order to create our shared memory area, we have to be loaded via
	 * shared_preload_libraries.  If not, fall out without hooking into any of
	 * the main system.	 (We don't throw error here because it seems useful to
	 * allow the cs_* functions to be created even when the
	 * module isn't active.	 The functions must protect themselves against
	 * being called then, however.)
	 */
	if (!process_shared_preload_libraries_in_progress)
		return;

	DefineCustomIntVariable(
		"multimaster.heartbeat_send_timeout",
		"Timeout in milliseconds of sending heartbeat messages",
		"Period of broadcasting heartbeat messages by arbiter to all nodes",
		&MtmHeartbeatSendTimeout,
		200,
		1,
		INT_MAX,
		PGC_BACKEND,
		0,
		NULL,
		NULL,
		NULL
	);

	DefineCustomIntVariable(
		"multimaster.heartbeat_recv_timeout",
		"Timeout in milliseconds of receiving heartbeat messages",
		"If no heartbeat message is received from node within this period, it assumed to be dead",
		&MtmHeartbeatRecvTimeout,
		1000,
		1,
		INT_MAX,
		PGC_BACKEND,
		0,
		NULL,
		NULL,
		NULL
	);

	DefineCustomIntVariable(
		"multimaster.max_nodes",
		"Maximal number of cluster nodes",
		"This parameters allows to add new nodes to the cluster, default value 0 restricts number of nodes to one specified in multimaster.conn_strings",
		&MtmMaxNodes,
		0,
		0,
		MAX_NODES,
		PGC_POSTMASTER,
		0,
		NULL,
		NULL,
		NULL
	);
	DefineCustomIntVariable(
		"multimaster.trans_spill_threshold",
		"Maximal size of transaction after which transaction is written to the disk",
		NULL,
		&MtmTransSpillThreshold,
		100 * 1024, /* 100Mb */
		0,
		MaxAllocSize/1024,
		PGC_SIGHUP,
		GUC_UNIT_KB,
		NULL,
		NULL,
		NULL
	);

	DefineCustomIntVariable(
		"multimaster.min_recovery_lag",
		"Minimal lag of WAL-sender performing recovery after which cluster is locked until recovery is completed",
		"When wal-sender almost catch-up WAL current position we need to stop 'Achilles tortile competition' and "
		"temporary stop commit of new transactions until node will be completely repared",
		&MtmMinRecoveryLag,
		10, /* 10 kB */
		0,
		INT_MAX,
		PGC_SIGHUP,
		GUC_UNIT_KB,
		NULL,
		NULL,
		NULL
	);

	DefineCustomIntVariable(
		"multimaster.max_recovery_lag",
		"Maximal lag of replication slot of failed node after which this slot is dropped to avoid transaction log overflow",
		"Dropping slot makes it not possible to recover node using logical replication mechanism, it will be ncessary to completely copy content of some other nodes "
		"using basebackup or similar tool. Zero value of parameter disable dropping slot.",
		&MtmMaxRecoveryLag,
		1 * 1024 * 1024, /* 1 GB */
		0,
		INT_MAX,
		PGC_SIGHUP,
		GUC_UNIT_KB,
		NULL,
		NULL,
		NULL
	);

	DefineCustomBoolVariable(
		"multimaster.break_connection",
		"Break connection with client when node is no online",
		NULL,
		&MtmBreakConnection,
		false,
		PGC_BACKEND,
		0,
		NULL,
		NULL,
		NULL
	);

	DefineCustomBoolVariable(
		"multimaster.bypass",
		"Allow access to offline multimaster node",
		NULL,
		&MtmBypass,
		false,
		PGC_USERSET, /* context */
		0,
		NULL,
		NULL,
		NULL
	);

	DefineCustomBoolVariable(
		"multimaster.major_node",
		"Node which forms a majority in case of partitioning in cliques with equal number of nodes",
		NULL,
		&MtmMajorNode,
		false,
		PGC_SUSET,
		0,
		NULL,
		NULL,
		NULL
	);

	DefineCustomBoolVariable(
		"multimaster.monotonic_sequences",
		"Enforce monotinic behaviour of sequence values obtained from different nodes",
		NULL,
		&MtmMonotonicSequences,
		false,
		PGC_BACKEND,
		0,
		NULL,
		NULL,
		NULL
	);

	DefineCustomBoolVariable(
		"multimaster.ignore_tables_without_pk",
		"Do not replicate tables without primary key",
		NULL,
		&MtmIgnoreTablesWithoutPk,
		false,
		PGC_BACKEND,
		0,
		NULL,
		NULL,
		NULL
	);

	DefineCustomStringVariable(
		"multimaster.referee_connstring",
		"Referee connection string",
		NULL,
		&MtmRefereeConnStr,
		"",
		PGC_POSTMASTER,
		0,
		NULL,
		NULL,
		NULL
	);

	DefineCustomBoolVariable(
		"multimaster.preserve_commit_order",
		"Transactions from one node will be committed in same order on all nodes",
		NULL,
		&MtmPreserveCommitOrder,
		false,
		PGC_BACKEND,
		GUC_NO_SHOW_ALL,
		NULL,
		NULL,
		NULL
	);

	DefineCustomBoolVariable(
		"multimaster.volkswagen_mode",
		"Pretend to be normal postgres. This means skip some NOTICE's and use local sequences. Default false.",
		NULL,
		&MtmVolksWagenMode,
		false,
		PGC_BACKEND,
		GUC_NO_SHOW_ALL,
		NULL,
		NULL,
		NULL
	);

	DefineCustomIntVariable(
		"multimaster.max_workers",
		"Maximal number of multimaster dynamic executor workers",
		NULL,
		&MtmMaxWorkers,
		100,
		0,
		INT_MAX,
		PGC_BACKEND,
		0,
		NULL,
		NULL,
		NULL
	);

	DefineCustomIntVariable(
		"multimaster.queue_size",
		"Multimaster queue size",
		NULL,
		&MtmQueueSize,
		10*1024*1024,
		1024*1024,
		INT_MAX,
		PGC_BACKEND,
		GUC_NO_SHOW_ALL,
		NULL,
		NULL,
		NULL
	);

	DefineCustomStringVariable(
		"multimaster.conn_strings",
		"Multimaster node connection strings separated by commas, i.e. 'replication=database dbname=postgres host=localhost port=5001,replication=database dbname=postgres host=localhost port=5002'",
		NULL,
		&MtmConnStrs,
		"",
		PGC_BACKEND, /* context */
		0,			 /* flags */
		NULL,		 /* GucStringCheckHook check_hook */
		NULL,		 /* GucStringAssignHook assign_hook */
		NULL		 /* GucShowHook show_hook */
	);

	DefineCustomStringVariable(
		"multimaster.remote_functions",
		"List of function names which should be executed remotely at all multimaster nodes instead of executing them at master and replicating result of their work",
		NULL,
		&MtmRemoteFunctionsList,
		"lo_create,lo_unlink",
		PGC_USERSET, /* context */
		GUC_LIST_INPUT, /* flags */
		NULL,		 /* GucStringCheckHook check_hook */
		MtmSetRemoteFunction,		 /* GucStringAssignHook assign_hook */
		NULL		 /* GucShowHook show_hook */
	);

	DefineCustomStringVariable(
		"multimaster.cluster_name",
		"Name of the cluster",
		NULL,
		&MtmClusterName,
		"mmts",
		PGC_BACKEND, /* context */
		0,			 /* flags */
		NULL,		 /* GucStringCheckHook check_hook */
		NULL,		 /* GucStringAssignHook assign_hook */
		NULL		 /* GucShowHook show_hook */
	);

	DefineCustomIntVariable(
		"multimaster.node_id",
		"Multimaster node ID",
		NULL,
		&MtmNodeId,
		INT_MAX,
		1,
		INT_MAX,
		PGC_BACKEND,
		0,
		NULL,
		NULL,
		NULL
	);

	/* This will also perform some checks on connection strings */
	MtmSplitConnStrs();

	if (!ConfigIsSane()) {
		MTM_ELOG(ERROR, "Multimaster config is insane, refusing to work");
	}

	MtmDeadlockDetectorInit(MtmMaxNodes);

	MtmStartReceivers();

	/*
	 * Request additional shared resources.	 (These are no-ops if we're not in
	 * the postmaster process.)	 We'll allocate or attach to the shared
	 * resources in mtm_shmem_startup().
	 */
	RequestAddinShmemSpace(MTM_SHMEM_SIZE + MtmMaxNodes*MtmQueueSize);
	RequestNamedLWLockTranche(MULTIMASTER_NAME, 1 + MtmMaxNodes*2);

	MtmMonitorInitialize();

	dmq_init();
	dmq_receiver_start_hook = MtmOnNodeConnect;
	dmq_receiver_stop_hook = MtmOnNodeDisconnect;

	ResolverInit();

	/*
	 * Install hooks.
	 */
	PreviousShmemStartupHook = shmem_startup_hook;
	shmem_startup_hook = MtmShmemStartup;

	PreviousExecutorStartHook = ExecutorStart_hook;
	ExecutorStart_hook = MtmExecutorStart;

	PreviousExecutorFinishHook = ExecutorFinish_hook;
	ExecutorFinish_hook = MtmExecutorFinish;

	PreviousProcessUtilityHook = ProcessUtility_hook;
	ProcessUtility_hook = MtmProcessUtility;

	PreviousSeqNextvalHook = SeqNextvalHook;
	SeqNextvalHook = MtmSeqNextvalHook;
}

/*
 * Module unload callback
 */
void
_PG_fini(void)
{
	shmem_startup_hook = PreviousShmemStartupHook;
	ExecutorFinish_hook = PreviousExecutorFinishHook;
	ProcessUtility_hook = PreviousProcessUtilityHook;
	SeqNextvalHook = PreviousSeqNextvalHook;
}


/*
 * Recovery slot is node ID from which new or crash node is performing recovery.
 * This function is called in case of logical receiver error to make it possible to try to perform
 * recovery from some other node
 */
void MtmReleaseRecoverySlot(int nodeId)
{
	if (Mtm->recoverySlot == nodeId) {
		Mtm->recoverySlot = 0;
	}
}

/*
 * Determine when and how we should open replication slot.
 * During recovery we need to open only one replication slot from which node should receive all transactions.
 * Slots at other nodes should be removed.
 */
MtmReplicationMode MtmGetReplicationMode(int nodeId)
{
	MtmLock(LW_EXCLUSIVE);

	/* Await until node is connected and both receiver and sender are in clique */
	while (BIT_CHECK(EFFECTIVE_CONNECTIVITY_MASK, nodeId - 1) ||
			BIT_CHECK(EFFECTIVE_CONNECTIVITY_MASK, MtmNodeId - 1))
	{
		MtmUnlock();
		MtmSleep(STATUS_POLL_DELAY);
		MtmLock(LW_EXCLUSIVE);
	}

	if (BIT_CHECK(Mtm->disabledNodeMask, MtmNodeId - 1))
	{
		/* Ok, then start recovery by luckiest walreceiver (if there is no donor node).
		 * If this node was populated using basebackup, then donorNodeId is not zero and we should choose this node for recovery */
		if ((Mtm->recoverySlot == 0 || Mtm->recoverySlot == nodeId)
			&& (Mtm->donorNodeId == MtmNodeId || Mtm->donorNodeId == nodeId))
		{
			/* Lock on us */
			Mtm->recoverySlot = nodeId;
			// MtmPollStatusOfPreparedTransactions(false);
			ResolveAllTransactions();
			MtmUnlock();
			return REPLMODE_RECOVERY;
		}

		/* And force less lucky walreceivers wait until recovery is completed */
		while (BIT_CHECK(Mtm->disabledNodeMask, MtmNodeId - 1))
		{
			MtmUnlock();
			MtmSleep(STATUS_POLL_DELAY);
			MtmLock(LW_EXCLUSIVE);
		}
	}

	MtmUnlock();
	return REPLMODE_RECOVERED;
}

static bool MtmIsBroadcast()
{
	return application_name != NULL && strcmp(application_name, MULTIMASTER_BROADCAST_SERVICE) == 0;
}

/*
 * Recover node is needed to return stopped and newly added node to the cluster.
 * This function creates logical replication slot for the node which will collect
 * all changes which should be sent to this node from this moment.
 */
void MtmRecoverNode(int nodeId)
{
	if (nodeId <= 0 || nodeId > Mtm->nAllNodes)
	{
		MTM_ELOG(ERROR, "NodeID %d is out of range [1,%d]", nodeId, Mtm->nAllNodes);
	}
	MtmLock(LW_EXCLUSIVE);
	Mtm->nodes[nodeId-1].manualRecovery = true;
	if (BIT_CHECK(Mtm->stoppedNodeMask, nodeId-1))
	{
		Assert(BIT_CHECK(Mtm->disabledNodeMask, nodeId-1));
		BIT_CLEAR(Mtm->stoppedNodeMask, nodeId-1);
		BIT_CLEAR(Mtm->stalledNodeMask, nodeId-1);
	}
	MtmUnlock();

	if (!MtmIsBroadcast())
	{
		MtmBroadcastUtilityStmt(psprintf("select pg_create_logical_replication_slot('" MULTIMASTER_SLOT_PATTERN "', '" MULTIMASTER_NAME "')", nodeId), true, 0);
		MtmBroadcastUtilityStmt(psprintf("select mtm.recover_node(%d)", nodeId), true, 0);
	}
}

/*
 * Resume previosly stopped node.
 * This function creates logical replication slot for the node which will collect
 * all changes which should be sent to this node from this moment.
 */
void MtmResumeNode(int nodeId)
{
	if (nodeId <= 0 || nodeId > Mtm->nAllNodes)
	{
		MTM_ELOG(ERROR, "NodeID %d is out of range [1,%d]", nodeId, Mtm->nAllNodes);
	}
	MtmLock(LW_EXCLUSIVE);
	if (BIT_CHECK(Mtm->stalledNodeMask, nodeId-1))
	{
		MtmUnlock();
		MTM_ELOG(ERROR, "Node %d can not be resumed because it's replication slot is dropped", nodeId);
	}
	if (BIT_CHECK(Mtm->stoppedNodeMask, nodeId-1))
	{
		Assert(BIT_CHECK(Mtm->disabledNodeMask, nodeId-1));
		BIT_CLEAR(Mtm->stoppedNodeMask, nodeId-1);
	}
	MtmUnlock();

	if (!MtmIsBroadcast())
	{
		MtmBroadcastUtilityStmt(psprintf("select mtm.resume_node(%d)", nodeId), true, nodeId);
	}
}

/*
 * Permanently exclude node from the cluster. Node will not participate in voting and can not be automatically recovered
 * until MtmRecoverNode is invoked.
 */
void MtmStopNode(int nodeId, bool dropSlot)
{
	if (nodeId <= 0 || nodeId > Mtm->nAllNodes)
	{
		MTM_ELOG(ERROR, "NodeID %d is out of range [1,%d]", nodeId, Mtm->nAllNodes);
	}

	if (!MtmIsBroadcast())
	{
		MtmBroadcastUtilityStmt(psprintf("select mtm.stop_node(%d,%s)", nodeId, dropSlot ? "true" : "false"), true, nodeId);
	}

	MtmLock(LW_EXCLUSIVE);
	BIT_SET(Mtm->stoppedNodeMask, nodeId-1);
	if (!BIT_CHECK(Mtm->disabledNodeMask, nodeId-1))
	{
		MtmDisableNode(nodeId);
	}
	MtmUnlock();

	if (dropSlot)
	{
		MtmDropSlot(nodeId);
	}
}

static void
MtmOnProcExit(int code, Datum arg)
{
	if (MtmReplicationNodeId > 0) {
		Mtm->nodes[MtmReplicationNodeId-1].senderPid = -1;
		MTM_LOG1("WAL-sender to %d is terminated", MtmReplicationNodeId);
		/* MtmOnNodeDisconnect(MtmReplicationNodeId); */
	}
}

static void
MtmReplicationStartupHook(struct PGLogicalStartupHookArgs* args)
{
	ListCell *param;
	bool recoveryCompleted = false;
	ulong64 recoveryStartPos = INVALID_LSN;
	int i;

	MtmIsRecoverySession = false;
	Mtm->nodes[MtmReplicationNodeId-1].senderPid = MyProcPid;
	Mtm->nodes[MtmReplicationNodeId-1].senderStartTime = MtmGetSystemTime();
	foreach(param, args->in_params)
	{
		DefElem	   *elem = lfirst(param);
		if (strcmp("mtm_replication_mode", elem->defname) == 0) {
			if (elem->arg != NULL && strVal(elem->arg) != NULL) {
				if (strcmp(strVal(elem->arg), "recovery") == 0) {
					MtmIsRecoverySession = true;
				} else if (strcmp(strVal(elem->arg), "recovered") == 0) {
					recoveryCompleted = true;
				} else if (strcmp(strVal(elem->arg), "open_existed") != 0 && strcmp(strVal(elem->arg), "create_new") != 0) {
					MTM_ELOG(ERROR, "Illegal recovery mode %s", strVal(elem->arg));
				}
			} else {
				MTM_ELOG(ERROR, "Replication mode is not specified");
			}
		} else if (strcmp("mtm_restart_pos", elem->defname) == 0) {
			if (elem->arg != NULL && strVal(elem->arg) != NULL) {
				sscanf(strVal(elem->arg), "%llx", &recoveryStartPos);
			} else {
				MTM_ELOG(ERROR, "Restart position is not specified");
			}
		} else if (strcmp("mtm_recovered_pos", elem->defname) == 0) {
			if (elem->arg != NULL && strVal(elem->arg) != NULL) {
				ulong64 recoveredLSN;
				sscanf(strVal(elem->arg), "%llx", &recoveredLSN);
				MTM_LOG1("Recovered position of node %d is %llx", MtmReplicationNodeId, recoveredLSN);
				// if (Mtm->nodes[MtmReplicationNodeId-1].restartLSN < recoveredLSN) {
				// 	MTM_LOG1("Advance restartLSN for node %d from %llx to %llx (MtmReplicationStartupHook)",
				// 			 MtmReplicationNodeId, Mtm->nodes[MtmReplicationNodeId-1].restartLSN, recoveredLSN);
				// 	// Assert(Mtm->nodes[MtmReplicationNodeId-1].restartLSN == INVALID_LSN
				// 	// 	   || recoveredLSN < Mtm->nodes[MtmReplicationNodeId-1].restartLSN + MtmMaxRecoveryLag);
				// 	Mtm->nodes[MtmReplicationNodeId-1].restartLSN = recoveredLSN;
				// }
			} else {
				MTM_ELOG(ERROR, "Recovered position is not specified");
			}
		}
	}
	MTM_LOG1("Startup of logical replication to node %d", MtmReplicationNodeId);
	MtmLock(LW_EXCLUSIVE);

	/*
	 * Set proper originId mappings.
	 *
	 * This is copypasted from receiver. Better to have normal init method
	 * to setup all stuff in shared memory. But seems that there is no such
	 * callback in vanilla pg and adding one will require some carefull thoughts.
	 */
	for (i = 0; i < Mtm->nAllNodes; i++)
	{
		char	   *originName;
		RepOriginId originId;

		originName = psprintf(MULTIMASTER_SLOT_PATTERN, i + 1);
		originId = replorigin_by_name(originName, true);
		if (originId == InvalidRepOriginId) {
			originId = replorigin_create(originName);
		}
		CommitTransactionCommand();
		StartTransactionCommand();
		Mtm->nodes[i].originId = originId;
	}

	if (BIT_CHECK(Mtm->stalledNodeMask, MtmReplicationNodeId-1)) {
		MtmUnlock();
		MTM_ELOG(ERROR, "Stalled node %d tries to initiate recovery", MtmReplicationNodeId);
	}

	if (BIT_CHECK(Mtm->stoppedNodeMask, MtmReplicationNodeId-1)) {
		MtmUnlock();
		MTM_ELOG(ERROR, "Stopped node %d tries to connect", MtmReplicationNodeId);
	}

	if (!BIT_CHECK(Mtm->clique, MtmReplicationNodeId-1)) {
		MtmUnlock();
		MTM_ELOG(ERROR, "Out-of-clique node %d tries to connect", MtmReplicationNodeId);
	}

	if (MtmIsRecoverySession) {
		MTM_LOG1("%d: Node %d start recovery of node %d at position %llx", MyProcPid, MtmNodeId, MtmReplicationNodeId, recoveryStartPos);
		Assert(MyReplicationSlot != NULL);
		if (recoveryStartPos < MyReplicationSlot->data.restart_lsn) {
			MTM_ELOG(WARNING, "Specified recovery start position %llx is beyond restart lsn %llx", recoveryStartPos, (long64)MyReplicationSlot->data.restart_lsn);
		}
		MtmStateProcessNeighborEvent(MtmReplicationNodeId, MTM_NEIGHBOR_WAL_SENDER_START_RECOVERY);
	} else { //if (BIT_CHECK(Mtm->disabledNodeMask,	 MtmReplicationNodeId-1)) {
		if (recoveryCompleted) {
			MTM_LOG1("Node %d consider that recovery of node %d is completed: start normal replication", MtmNodeId, MtmReplicationNodeId);
			MtmStateProcessNeighborEvent(MtmReplicationNodeId, MTM_NEIGHBOR_WAL_SENDER_START_RECOVERED);
		} else {
			/* Force arbiter to reestablish connection with this node, send heartbeat to inform this node that it was disabled and should perform recovery */
			BIT_SET(Mtm->reconnectMask, MtmReplicationNodeId-1);
			MtmUnlock();
			MTM_ELOG(ERROR, "Disabled node %d tries to reconnect without recovery", MtmReplicationNodeId);
		}
	}
	// else {
	// 	// MTM_LOG1("Node %d start logical replication to node %d in normal mode", MtmNodeId, MtmReplicationNodeId);
	// 	MtmStateProcessNeighborEvent(MtmReplicationNodeId, MTM_NEIGHBOR_WAL_SENDER_START_NORMAL);
	// }

	BIT_SET(Mtm->reconnectMask, MtmReplicationNodeId-1); /* arbiter should try to reestablish connection with this node */
	MtmUnlock();
	on_shmem_exit(MtmOnProcExit, 0);
}

lsn_t MtmGetFlushPosition(int nodeId)
{
	return Mtm->nodes[nodeId-1].flushPos;
}

/**
 * Keep track of progress of WAL writer.
 * We need to notify WAL senders at other nodes which logical records
 * are flushed to the disk and so can survive failure. In asynchronous commit mode
 * WAL is flushed by WAL writer. Current flush position can be obtained by GetFlushRecPtr().
 * So on applying new logical record we insert it in the MtmLsnMapping and compare
 * their poistions in local WAL log with current flush position.
 * The records which are flushed to the disk by WAL writer are removed from the list
 * and mapping ing mtm->nodes[].flushPos is updated for this node.
 */
void  MtmUpdateLsnMapping(int node_id, lsn_t end_lsn)
{
	dlist_mutable_iter iter;
	MtmFlushPosition* flushpos;
	lsn_t local_flush = GetFlushRecPtr();
	MemoryContext old_context = MemoryContextSwitchTo(TopMemoryContext);

	if (end_lsn != INVALID_LSN) {
		/* Track commit lsn */
		flushpos = (MtmFlushPosition *) palloc(sizeof(MtmFlushPosition));
		flushpos->node_id = node_id;
		flushpos->local_end = XactLastCommitEnd;
		flushpos->remote_end = end_lsn;
		dlist_push_tail(&MtmLsnMapping, &flushpos->node);
	}
	MtmLock(LW_EXCLUSIVE);
	dlist_foreach_modify(iter, &MtmLsnMapping)
	{
		flushpos = dlist_container(MtmFlushPosition, node, iter.cur);
		if (flushpos->local_end <= local_flush)
		{
			if (Mtm->nodes[node_id-1].flushPos < flushpos->remote_end) {
				Mtm->nodes[node_id-1].flushPos = flushpos->remote_end;
			}
			dlist_delete(iter.cur);
			pfree(flushpos);
		} else {
			break;
		}
	}
	MtmUnlock();
	MemoryContextSwitchTo(old_context);
}


static void
MtmReplicationShutdownHook(struct PGLogicalShutdownHookArgs* args)
{
	MtmLock(LW_EXCLUSIVE);
	if (MtmReplicationNodeId >= 0 && BIT_CHECK(Mtm->pglogicalSenderMask, MtmReplicationNodeId-1)) {
		BIT_CLEAR(Mtm->pglogicalSenderMask, MtmReplicationNodeId-1);
		Mtm->nSenders -= 1;
		MTM_LOG1("Logical replication to node %d is stopped", MtmReplicationNodeId);
		/* MtmOnNodeDisconnect(MtmReplicationNodeId); */
		MtmReplicationNodeId = -1; /* defuse MtmOnProcExit hook */
	}
	MtmUnlock();
}

/*
 * Filter transactions which should be replicated to other nodes.
 * This filter is applied at sender side (WAL sender).
 * Final filtering is also done at destination side by MtmFilterTransaction function.
 */
static bool
MtmReplicationTxnFilterHook(struct PGLogicalTxnFilterArgs* args)
{
	/* Do not replicate any transactions in recovery mode (because we should apply
	 * changes sent to us rather than send our own pending changes)
	 * and transactions received from other nodes
	 * (originId should be non-zero in this case)
	 * unless we are performing recovery of disabled node
	 * (in this case all transactions should be sent)
	 */
	/*
	 * I removed (Mtm->status != MTM_RECOVERY) here since in major
	 * mode we need to recover from offline node too. Also it seems
	 * that with amount of nodes >= 3 we also need that. --sk
	 *
	 * On a first look this works fine.
	 */
	bool res = (args->origin_id == InvalidRepOriginId
			|| MtmIsRecoveredNode(MtmReplicationNodeId));
	if (!res) {
		MTM_LOG2("Filter transaction with origin_id=%d", args->origin_id);
	}
	return res;
}

/*
 * Filter record corresponding to local (non-distributed) tables
 */
static bool
MtmReplicationRowFilterHook(struct PGLogicalRowFilterArgs* args)
{
	bool isDistributed;

	/*
	 * We have several built-in local tables that shouldn't be replicated.
	 * It is hard to insert them into MtmLocalTables properly on extension
	 * creation so we just list them here.
	 */
	if (strcmp(args->changed_rel->rd_rel->relname.data, "referee_decision") == 0)
		return false;

	/*
	 * Check in shared hash of local tables.
	 */
	MtmLock(LW_SHARED);
	if (!Mtm->localTablesHashLoaded) {
		MtmUnlock();
		MtmLock(LW_EXCLUSIVE);
		if (!Mtm->localTablesHashLoaded) {
			MtmLoadLocalTables();
			Mtm->localTablesHashLoaded = true;
		}
	}
	isDistributed = hash_search(MtmLocalTables, &RelationGetRelid(args->changed_rel), HASH_FIND, NULL) == NULL;
	MtmUnlock();

	return isDistributed;
}

/*
 * Filter received transactions at destination side.
 * This function is executed by receiver,
 * so there are no race conditions and it is possible to update nodes[i].restartLSN without lock.
 * It is more efficient to filter records at senders size (done by MtmReplicationTxnFilterHook) to avoid sending useless data through network.
 * But asynchronous nature of logical replications makes it not possible to guarantee (at least I failed to do it)
 * that replica do not receive deteriorated data.
 */
bool MtmFilterTransaction(char* record, int size)
{
	StringInfoData s;
	uint8		event;
	lsn_t		origin_lsn;
	lsn_t		end_lsn;
	lsn_t		restart_lsn;
	int			replication_node;
	int			origin_node;
	char const* gid = "";
	char		msgtype PG_USED_FOR_ASSERTS_ONLY;
	bool		duplicate = false;

	s.data = record;
	s.len = size;
	s.maxlen = -1;
	s.cursor = 0;

	msgtype = pq_getmsgbyte(&s);
	Assert(msgtype == 'C');
	event = pq_getmsgbyte(&s); /* event */
	replication_node = pq_getmsgbyte(&s);

	/* read fields */
	pq_getmsgint64(&s); /* commit_lsn */
	end_lsn = pq_getmsgint64(&s); /* end_lsn */
	pq_getmsgint64(&s); /* commit_time */

	origin_node = pq_getmsgbyte(&s);
	origin_lsn = pq_getmsgint64(&s);

	Assert(replication_node == MtmReplicationNodeId);
	if (!(origin_node != 0 &&
		  (Mtm->status == MTM_RECOVERY || origin_node == replication_node)))
	{
		MTM_ELOG(WARNING, "Receive redirected commit event %d from node %d origin node %d origin LSN %llx in %s mode",
			 event, replication_node, origin_node, origin_lsn, MtmNodeStatusMnem[Mtm->status]);
	}

	switch (event)
	{
	  case PGLOGICAL_PREPARE:
	  case PGLOGICAL_PRECOMMIT_PREPARED:
	  case PGLOGICAL_ABORT_PREPARED:
		gid = pq_getmsgstring(&s);
		break;
	  case PGLOGICAL_COMMIT_PREPARED:
		pq_getmsgint64(&s); /* CSN */
		gid = pq_getmsgstring(&s);
		break;
	  default:
		break;
	}
	restart_lsn = origin_node == MtmReplicationNodeId ? end_lsn : origin_lsn;
	if (Mtm->nodes[origin_node-1].restartLSN < restart_lsn) {
		MTM_LOG2("[restartlsn] node %d: %llx -> %llx (MtmFilterTransaction)", MtmReplicationNodeId, Mtm->nodes[MtmReplicationNodeId-1].restartLSN, restart_lsn);
		// if (event != PGLOGICAL_PREPARE) {
		// 	/* Transactions can be prepared in different order, so to avoid loosing transactions we should not update restartLsn for them */
		// 	Mtm->nodes[origin_node-1].restartLSN = restart_lsn;
		// }
	} else {
		duplicate = true;
	}

	if (duplicate) {
		MTM_LOG1("Ignore transaction %s from node %d event=%x because our LSN position %llx for origin node %d is greater or equal than LSN %llx of this transaction (end_lsn=%llx, origin_lsn=%llx) mode %s",
				 gid, replication_node, event, Mtm->nodes[origin_node-1].restartLSN, origin_node, restart_lsn, end_lsn, origin_lsn, MtmNodeStatusMnem[Mtm->status]);
	} else {
		MTM_LOG2("Apply transaction %s from node %d lsn %llx, event=%x, origin node %d, original lsn=%llx, current lsn=%llx",
				 gid, replication_node, end_lsn, event, origin_node, origin_lsn, restart_lsn);
	}

	return duplicate;
}

void MtmSetupReplicationHooks(struct PGLogicalHooks* hooks)
{
	hooks->startup_hook = MtmReplicationStartupHook;
	hooks->shutdown_hook = MtmReplicationShutdownHook;
	hooks->txn_filter_hook = MtmReplicationTxnFilterHook;
	hooks->row_filter_hook = MtmReplicationRowFilterHook;
}

/*
 * Setup replication session origin to include origin location in WAL and
 * update slot position.
 * Sessions are not reetrant so we have to use exclusive lock here.
 */
void MtmBeginSession(int nodeId)
{
	// MtmLockNode(nodeId, LW_EXCLUSIVE);
	Assert(replorigin_session_origin == InvalidRepOriginId);
	replorigin_session_origin = Mtm->nodes[nodeId-1].originId;
	Assert(replorigin_session_origin != InvalidRepOriginId);
	MTM_LOG3("%d: Begin setup replorigin session: %d", MyProcPid, replorigin_session_origin);
	replorigin_session_setup(replorigin_session_origin);
	MTM_LOG3("%d: End setup replorigin session: %d", MyProcPid, replorigin_session_origin);
}

/*
 * Release replication session
 */
void MtmEndSession(int nodeId, bool unlock)
{
	if (replorigin_session_origin != InvalidRepOriginId) {
		MTM_LOG2("%d: Begin reset replorigin session for node %d: %d, progress %llx",
				 MyProcPid, nodeId, replorigin_session_origin,
				 (long long unsigned int) replorigin_session_get_progress(false));
		replorigin_session_origin = InvalidRepOriginId;
		replorigin_session_origin_lsn = INVALID_LSN;
		replorigin_session_origin_timestamp = 0;
		replorigin_session_reset();
		// if (unlock) {
		// 	MtmUnlockNode(nodeId);
		// }
		MTM_LOG3("%d: End reset replorigin session: %d", MyProcPid, replorigin_session_origin);
	}
}


/*
 * -------------------------------------------
 * SQL API functions
 * -------------------------------------------
 */

Datum
mtm_stop_node(PG_FUNCTION_ARGS)
{
	int nodeId = PG_GETARG_INT32(0);
	bool dropSlot = PG_GETARG_BOOL(1);
	MtmStopNode(nodeId, dropSlot);
	PG_RETURN_VOID();
}

Datum
mtm_add_node(PG_FUNCTION_ARGS)
{
	char *connStr = text_to_cstring(PG_GETARG_TEXT_PP(0));

	if (Mtm->nAllNodes == MtmMaxNodes) {
		MTM_ELOG(ERROR, "Maximal number of nodes %d is reached", MtmMaxNodes);
	}
	if (!MtmIsBroadcast())
	{
		MtmBroadcastUtilityStmt(psprintf("select pg_create_logical_replication_slot('" MULTIMASTER_SLOT_PATTERN "', '" MULTIMASTER_NAME "')", Mtm->nAllNodes+1), true, 0);
		MtmBroadcastUtilityStmt(psprintf("select mtm.add_node('%s')", connStr), true, 0);
	}
	else
	{
		int nodeId;
		MtmLock(LW_EXCLUSIVE);
		nodeId = Mtm->nAllNodes;
		MTM_ELOG(NOTICE, "Add node %d: '%s'", nodeId+1, connStr);

		MtmUpdateNodeConnectionInfo(&Mtm->nodes[nodeId].con, connStr);

		if (*MtmConnStrs == '@') {
			FILE* f = fopen(MtmConnStrs+1, "a");
			fprintf(f, "%s\n", connStr);
			fclose(f);
		}

		Mtm->nodes[nodeId].transDelay = 0;
		Mtm->nodes[nodeId].lastStatusChangeTime = MtmGetSystemTime();
		Mtm->nodes[nodeId].flushPos = 0;
		Mtm->nodes[nodeId].oldestSnapshot = 0;

		BIT_SET(Mtm->disabledNodeMask, nodeId);
		Mtm->nConfigChanges += 1;
		Mtm->nAllNodes += 1;
		MtmUnlock();

		MtmStartReceiver(nodeId+1, true);
	}
	PG_RETURN_VOID();
}

Datum
mtm_poll_node(PG_FUNCTION_ARGS)
{
	int nodeId = PG_GETARG_INT32(0);
	bool nowait = PG_GETARG_BOOL(1);
	bool online = true;
	while ((nodeId == MtmNodeId && Mtm->status != MTM_ONLINE)
		   || (nodeId != MtmNodeId && BIT_CHECK(Mtm->disabledNodeMask, nodeId-1)))
	{
		if (nowait) {
			online = false;
			break;
		} else {
			MtmSleep(STATUS_POLL_DELAY);
		}
	}
	PG_RETURN_BOOL(online);
}

Datum
mtm_recover_node(PG_FUNCTION_ARGS)
{
	int nodeId = PG_GETARG_INT32(0);
	MtmRecoverNode(nodeId);
	PG_RETURN_VOID();
}

Datum
mtm_resume_node(PG_FUNCTION_ARGS)
{
	int nodeId = PG_GETARG_INT32(0);
	MtmResumeNode(nodeId);
	PG_RETURN_VOID();
}

typedef struct
{
	int		  nodeId;
	TupleDesc desc;
	Datum	  values[Natts_mtm_nodes_state];
	bool	  nulls[Natts_mtm_nodes_state];
} MtmGetNodeStateCtx;

Datum
mtm_get_nodes_state(PG_FUNCTION_ARGS)
{
	FuncCallContext* funcctx;
	MtmGetNodeStateCtx* usrfctx;
	MemoryContext oldcontext;
	int64 lag;
	bool is_first_call = SRF_IS_FIRSTCALL();

	if (is_first_call) {
		funcctx = SRF_FIRSTCALL_INIT();
		oldcontext = MemoryContextSwitchTo(funcctx->multi_call_memory_ctx);
		usrfctx = (MtmGetNodeStateCtx*)palloc(sizeof(MtmGetNodeStateCtx));
		get_call_result_type(fcinfo, NULL, &usrfctx->desc);
		usrfctx->nodeId = 1;
		memset(usrfctx->nulls, false, sizeof(usrfctx->nulls));
		funcctx->user_fctx = usrfctx;
		MemoryContextSwitchTo(oldcontext);
	}
	funcctx = SRF_PERCALL_SETUP();
	usrfctx = (MtmGetNodeStateCtx*)funcctx->user_fctx;
	if (usrfctx->nodeId > Mtm->nAllNodes) {
		SRF_RETURN_DONE(funcctx);
	}
	usrfctx->values[0] = Int32GetDatum(usrfctx->nodeId);
	usrfctx->values[1] = BoolGetDatum(!BIT_CHECK(Mtm->disabledNodeMask, usrfctx->nodeId-1));
	usrfctx->values[2] = BoolGetDatum(!BIT_CHECK(SELF_CONNECTIVITY_MASK, usrfctx->nodeId-1));
	usrfctx->values[3] = BoolGetDatum(BIT_CHECK(Mtm->stalledNodeMask, usrfctx->nodeId-1));
	usrfctx->values[4] = BoolGetDatum(BIT_CHECK(Mtm->stoppedNodeMask, usrfctx->nodeId-1));

	usrfctx->values[5] = BoolGetDatum(BIT_CHECK(Mtm->originLockNodeMask, usrfctx->nodeId-1));
	lag = MtmGetSlotLag(usrfctx->nodeId);
	usrfctx->values[6] = Int64GetDatum(lag);
	usrfctx->nulls[6] = lag < 0;

	usrfctx->values[7] = Int64GetDatum(Mtm->transCount ? Mtm->nodes[usrfctx->nodeId-1].transDelay/Mtm->transCount : 0);
	usrfctx->values[8] = TimestampTzGetDatum(time_t_to_timestamptz(Mtm->nodes[usrfctx->nodeId-1].lastStatusChangeTime/USECS_PER_SEC));
	usrfctx->values[9] = Int64GetDatum(Mtm->nodes[usrfctx->nodeId-1].oldestSnapshot);

	usrfctx->values[10] = Int32GetDatum(Mtm->nodes[usrfctx->nodeId-1].senderPid);
	usrfctx->values[11] = TimestampTzGetDatum(time_t_to_timestamptz(Mtm->nodes[usrfctx->nodeId-1].senderStartTime/USECS_PER_SEC));
	usrfctx->values[12] = Int32GetDatum(Mtm->nodes[usrfctx->nodeId-1].receiverPid);
	usrfctx->values[13] = TimestampTzGetDatum(time_t_to_timestamptz(Mtm->nodes[usrfctx->nodeId-1].receiverStartTime/USECS_PER_SEC));

	if (usrfctx->nodeId == MtmNodeId)
	{
		usrfctx->nulls[10] = true;
		usrfctx->nulls[11] = true;
		usrfctx->nulls[12] = true;
		usrfctx->nulls[13] = true;
	}

	usrfctx->values[14] = CStringGetTextDatum(Mtm->nodes[usrfctx->nodeId-1].con.connStr);
	usrfctx->values[15] = Int64GetDatum(Mtm->nodes[usrfctx->nodeId-1].connectivityMask);
	usrfctx->values[16] = Int64GetDatum(Mtm->nodes[usrfctx->nodeId-1].nHeartbeats);
	usrfctx->nodeId += 1;

	SRF_RETURN_NEXT(funcctx, HeapTupleGetDatum(heap_form_tuple(usrfctx->desc, usrfctx->values, usrfctx->nulls)));
}

Datum
mtm_get_cluster_state(PG_FUNCTION_ARGS)
{
	TupleDesc desc;
	Datum	  values[Natts_mtm_cluster_state];
	bool	  nulls[Natts_mtm_cluster_state] = {false};
	int		  i,
			  pool_active = 0,
			  pool_pending = 0,
			  pool_queue_size = 0;

	get_call_result_type(fcinfo, NULL, &desc);

	for (i = 0; i < Mtm->nAllNodes; i++)
	{
		pool_active += (int) Mtm->nodes[i].pool.active;
		pool_pending += (int) Mtm->nodes[i].pool.pending;
		pool_queue_size += (int) BgwPoolGetQueueSize(&Mtm->nodes[i].pool);
	}

	values[0] = Int32GetDatum(MtmNodeId);
	values[1] = CStringGetTextDatum(MtmNodeStatusMnem[Mtm->status]);
	values[2] = Int64GetDatum(Mtm->disabledNodeMask);
	values[3] = Int64GetDatum(SELF_CONNECTIVITY_MASK);
	values[4] = Int64GetDatum(Mtm->originLockNodeMask);
	values[5] = Int32GetDatum(Mtm->nAllNodes);
	values[6] = Int32GetDatum(Mtm->nAllNodes);
	values[7] = Int32GetDatum(pool_active);
	values[8] = Int32GetDatum(pool_pending);
	values[9] = Int64GetDatum(pool_queue_size);
	values[10] = Int64GetDatum(Mtm->transCount);
	values[11] = Int64GetDatum(Mtm->timeShift);
	values[12] = Int32GetDatum(Mtm->recoverySlot);
	values[13] = Int64GetDatum(0);
	values[14] = Int64GetDatum(0);
	values[15] = Int64GetDatum(Mtm->oldestXid);
	values[16] = Int32GetDatum(Mtm->nConfigChanges);
	values[17] = Int64GetDatum(Mtm->stalledNodeMask);
	values[18] = Int64GetDatum(Mtm->stoppedNodeMask);
	values[19] = TimestampTzGetDatum(time_t_to_timestamptz(Mtm->nodes[MtmNodeId-1].lastStatusChangeTime/USECS_PER_SEC));

	PG_RETURN_DATUM(HeapTupleGetDatum(heap_form_tuple(desc, values, nulls)));
}


typedef struct
{
	int		  nodeId;
} MtmGetClusterInfoCtx;

void
erase_option_from_connstr(const char *option, char *connstr)
{
	char *needle = psprintf("%s=", option);
	while (1) {
		char *found = strstr(connstr, needle);
		if (found == NULL) break;
		while (*found != '\0' && *found != ' ') {
			*found = ' ';
			found++;
		}
	}
	pfree(needle);
}

PGconn *
PQconnectdb_safe(const char *conninfo, int timeout)
{
	PGconn *conn;
	struct timeval tv = { timeout, 0 };
	char *safe_connstr = pstrdup(conninfo);

	/* XXXX add timeout to connstring if set */

	erase_option_from_connstr("arbiter_port", safe_connstr);
	conn = PQconnectdb(safe_connstr);

	if (PQstatus(conn) != CONNECTION_OK)
	{
		MTM_ELOG(WARNING, "Could not connect to '%s': %s",
			safe_connstr, PQerrorMessage(conn));
		return conn;
	}

	pfree(safe_connstr);

	if (timeout != 0)
	{
		int socket_fd = PQsocket(conn);

		if (socket_fd < 0)
		{
			MTM_ELOG(WARNING, "Referee socket is invalid");
			return conn;
		}

		if (pg_setsockopt(socket_fd, SOL_SOCKET, SO_RCVTIMEO,
									(char *)&tv, sizeof(tv), MtmUseRDMA) < 0)
		{
			MTM_ELOG(WARNING, "Could not set referee socket timeout: %s",
						strerror(errno));
			return conn;
		}
	}

	return conn;
}

Datum
mtm_collect_cluster_info(PG_FUNCTION_ARGS)
{

	FuncCallContext* funcctx;
	MtmGetClusterInfoCtx* usrfctx;
	MemoryContext oldcontext;
	TupleDesc desc;
	bool is_first_call = SRF_IS_FIRSTCALL();
	int i;
	PGconn* conn;
	PGresult *result;
	char* values[Natts_mtm_cluster_state];
	HeapTuple tuple;

	if (is_first_call) {
		funcctx = SRF_FIRSTCALL_INIT();
		oldcontext = MemoryContextSwitchTo(funcctx->multi_call_memory_ctx);
		usrfctx = (MtmGetClusterInfoCtx*)palloc(sizeof(MtmGetNodeStateCtx));
		get_call_result_type(fcinfo, NULL, &desc);
		funcctx->attinmeta = TupleDescGetAttInMetadata(desc);
		usrfctx->nodeId = 0;
		funcctx->user_fctx = usrfctx;
		MemoryContextSwitchTo(oldcontext);
	}
	funcctx = SRF_PERCALL_SETUP();
	usrfctx = (MtmGetClusterInfoCtx*)funcctx->user_fctx;
	while (++usrfctx->nodeId <= Mtm->nAllNodes && BIT_CHECK(Mtm->disabledNodeMask, usrfctx->nodeId-1));
	if (usrfctx->nodeId > Mtm->nAllNodes) {
		SRF_RETURN_DONE(funcctx);
	}

	conn = PQconnectdb_safe(Mtm->nodes[usrfctx->nodeId-1].con.connStr, 0);
	if (PQstatus(conn) != CONNECTION_OK)
	{
		MTM_ELOG(WARNING, "Failed to establish connection '%s' to node %d: error = %s", Mtm->nodes[usrfctx->nodeId-1].con.connStr, usrfctx->nodeId, PQerrorMessage(conn));
		PQfinish(conn);
		SRF_RETURN_NEXT_NULL(funcctx);
	}
	else
	{
		result = PQexec(conn, "select * from mtm.get_cluster_state()");

		if (PQresultStatus(result) != PGRES_TUPLES_OK || PQntuples(result) != 1) {
			MTM_ELOG(WARNING, "Failed to receive data from %d", usrfctx->nodeId);
			PQclear(result);
			PQfinish(conn);
			SRF_RETURN_NEXT_NULL(funcctx);
		}
		else
		{
			for (i = 0; i < Natts_mtm_cluster_state; i++)
				values[i] = PQgetvalue(result, 0, i);
			tuple = BuildTupleFromCStrings(funcctx->attinmeta, values);
			PQclear(result);
			PQfinish(conn);
			SRF_RETURN_NEXT(funcctx, HeapTupleGetDatum(tuple));
		}
	}
}

Datum mtm_broadcast_table(PG_FUNCTION_ARGS)
{
	MtmCopyRequest copy;
	copy.sourceTable = PG_GETARG_OID(0);
	copy.targetNodes = ~Mtm->disabledNodeMask;
	LogLogicalMessage("B", (char*)&copy, sizeof(copy), true);
	MtmTx.containsDML = true;
	PG_RETURN_VOID();
}

Datum mtm_copy_table(PG_FUNCTION_ARGS)
{
	MtmCopyRequest copy;
	copy.sourceTable = PG_GETARG_OID(0);
	copy.targetNodes = (nodemask_t)1 << (PG_GETARG_INT32(1) - 1);
	LogLogicalMessage("B", (char*)&copy, sizeof(copy), true);
	MtmTx.containsDML = true;
	PG_RETURN_VOID();
}


Datum mtm_make_table_local(PG_FUNCTION_ARGS)
{
	Oid	reloid = PG_GETARG_OID(0);
	RangeVar   *rv;
	Relation	rel;
	TupleDesc	tupDesc;
	HeapTuple	tup;
	Datum		values[Natts_mtm_local_tables];
	bool		nulls[Natts_mtm_local_tables];

	MtmMakeRelationLocal(reloid);

	rv = makeRangeVar(MULTIMASTER_SCHEMA_NAME, MULTIMASTER_LOCAL_TABLES_TABLE, -1);
	rel = heap_openrv(rv, RowExclusiveLock);
	if (rel != NULL) {
		char* tableName = get_rel_name(reloid);
		Oid	  schemaid = get_rel_namespace(reloid);
		char* schemaName = get_namespace_name(schemaid);

		tupDesc = RelationGetDescr(rel);

		/* Form a tuple. */
		memset(nulls, false, sizeof(nulls));

		values[Anum_mtm_local_tables_rel_schema - 1] = CStringGetDatum(schemaName);
		values[Anum_mtm_local_tables_rel_name - 1] = CStringGetDatum(tableName);

		tup = heap_form_tuple(tupDesc, values, nulls);

		/* Insert the tuple to the catalog and update the indexes. */
		CatalogTupleInsert(rel, tup);

		/* Cleanup. */
		heap_freetuple(tup);
		heap_close(rel, RowExclusiveLock);

		MtmTx.containsDML = true;
	}
	return false;
}

/*
 * -------------------------------------------
 * Broadcast utulity statements
 * -------------------------------------------
 */

/*
 * Execute statement with specified parameters and check its result
 */
static bool MtmRunUtilityStmt(PGconn* conn, char const* sql, char **errmsg)
{
	PGresult *result = PQexec(conn, sql);
	int status = PQresultStatus(result);

	bool ret = status == PGRES_COMMAND_OK || status == PGRES_TUPLES_OK;

	if (!ret) {
		char *errstr = PQresultErrorMessage(result);
		int errlen = strlen(errstr);
		if (errlen > 9) {
			*errmsg = palloc0(errlen);

			/* Strip "ERROR:  " from beginning and "\n" from end of error string */
			strncpy(*errmsg, errstr + 8, errlen - 1 - 8);
		}
	}

	PQclear(result);
	return ret;
}

static void
MtmNoticeReceiver(void *i, const PGresult *res)
{
	char *notice = PQresultErrorMessage(res);
	char *stripped_notice;
	int len = strlen(notice);

	/* Skip notices from other nodes */
	if ( (*(int *)i) != MtmNodeId - 1)
		return;

	stripped_notice = palloc0(len + 1);

	if (*notice == 'N')
	{
		/* Strip "NOTICE:  " from beginning and "\n" from end of error string */
		strncpy(stripped_notice, notice + 9, len - 1 - 9);
		MTM_ELOG(NOTICE, "%s", stripped_notice);
	}
	else if (*notice == 'W')
	{
		/* Strip "WARNING:	" from beginning and "\n" from end of error string */
		strncpy(stripped_notice, notice + 10, len - 1 - 10);
		MTM_ELOG(WARNING, "%s", stripped_notice);
	}
	else
	{
		strncpy(stripped_notice, notice, len + 1);
		MTM_ELOG(WARNING, "%s", stripped_notice);
	}

	MTM_LOG1("%s", stripped_notice);
	pfree(stripped_notice);
}

static void MtmBroadcastUtilityStmt(char const* sql, bool ignoreError, int forceOnNode)
{
	int i = 0;
	nodemask_t disabledNodeMask = Mtm->disabledNodeMask;
	int failedNode = -1;
	char const* errorMsg = NULL;
	PGconn **conns = palloc0(sizeof(PGconn*)*Mtm->nAllNodes);
	char* utility_errmsg;
	int nNodes = Mtm->nAllNodes;

	for (i = 0; i < nNodes; i++)
	{
		if (!BIT_CHECK(disabledNodeMask, i) || (i + 1 == forceOnNode))
		{
			conns[i] = PQconnectdb_safe(psprintf("%s application_name=%s", Mtm->nodes[i].con.connStr, MULTIMASTER_BROADCAST_SERVICE), 0);
			if (PQstatus(conns[i]) != CONNECTION_OK)
			{
				if (ignoreError)
				{
					PQfinish(conns[i]);
					conns[i] = NULL;
				} else {
					failedNode = i;
					do {
						PQfinish(conns[i]);
					} while (--i >= 0);
					MTM_ELOG(ERROR, "Failed to establish connection '%s' to node %d, error = %s", Mtm->nodes[failedNode].con.connStr, failedNode+1, PQerrorMessage(conns[i]));
				}
			}
			PQsetNoticeReceiver(conns[i], MtmNoticeReceiver, &i);
		}
	}
	Assert(i == nNodes);

	for (i = 0; i < nNodes; i++)
	{
		if (conns[i])
		{
			if (!MtmRunUtilityStmt(conns[i], "BEGIN TRANSACTION", &utility_errmsg) && !ignoreError)
			{
				errorMsg = MTM_TAG "Failed to start transaction at node %d";
				failedNode = i;
				break;
			}
			if (!MtmRunUtilityStmt(conns[i], sql, &utility_errmsg) && !ignoreError)
			{
				if (i + 1 == MtmNodeId)
					errorMsg = psprintf(MTM_TAG "%s", utility_errmsg);
				else
				{
					MTM_ELOG(ERROR, "%s", utility_errmsg);
					errorMsg = MTM_TAG "Failed to run command at node %d";
				}

				failedNode = i;
				break;
			}
		}
	}
	if (failedNode >= 0 && !ignoreError)
	{
		for (i = 0; i < nNodes; i++)
		{
			if (conns[i])
			{
				MtmRunUtilityStmt(conns[i], "ROLLBACK TRANSACTION", &utility_errmsg);
			}
		}
	} else {
		for (i = 0; i < nNodes; i++)
		{
			if (conns[i] && !MtmRunUtilityStmt(conns[i], "COMMIT TRANSACTION", &utility_errmsg) && !ignoreError)
			{
				errorMsg = MTM_TAG "Commit failed at node %d";
				failedNode = i;
			}
		}
	}
	for (i = 0; i < nNodes; i++)
	{
		if (conns[i])
		{
			PQfinish(conns[i]);
		}
	}
	if (!ignoreError && failedNode >= 0)
	{
		elog(ERROR, errorMsg, failedNode+1);
	}
}

/*
 * Genenerate global transaction identifier for two-pahse commit.
 * It should be unique for all nodes
 */
void
MtmGenerateGid(char *gid, TransactionId xid)
{
	sprintf(gid, "MTM-%d-" XID_FMT, MtmNodeId, xid);
	return;
}

int
MtmGidParseNodeId(const char* gid)
{
	int MtmNodeId = -1;
	sscanf(gid, "MTM-%d-%*d", &MtmNodeId);
	return MtmNodeId;
}

TransactionId
MtmGidParseXid(const char* gid)
{
	TransactionId xid = InvalidTransactionId;
	sscanf(gid, "MTM-%*d-" XID_FMT, &xid);
	Assert(TransactionIdIsValid(xid));
	return xid;
}


/*
 * -------------------------------------------
 * GUC Context Handling
 * -------------------------------------------
 */

// XXX: is it defined somewhere?
#define GUC_KEY_MAXLEN 255
#define MTM_GUC_HASHSIZE 20

typedef struct MtmGucEntry
{
	char	key[GUC_KEY_MAXLEN];
	dlist_node	list_node;
	char   *value;
} MtmGucEntry;

static HTAB *MtmGucHash = NULL;
static dlist_head MtmGucList = DLIST_STATIC_INIT(MtmGucList);
static inline void MtmGucUpdate(const char *key, char *value);

static void MtmGucInit(void)
{
	HASHCTL		hash_ctl;
	char	   *current_role;
	MemoryContext oldcontext;

	MemSet(&hash_ctl, 0, sizeof(hash_ctl));
	hash_ctl.keysize = GUC_KEY_MAXLEN;
	hash_ctl.entrysize = sizeof(MtmGucEntry);
	hash_ctl.hcxt = TopMemoryContext;
	MtmGucHash = hash_create("MtmGucHash",
						MTM_GUC_HASHSIZE,
						&hash_ctl,
						HASH_ELEM | HASH_CONTEXT);

	/*
	 * If current role is not equal to MtmDatabaseUser, than set it bofore
	 * any other GUC vars.
	 */
	oldcontext = MemoryContextSwitchTo(TopMemoryContext);
	current_role = GetConfigOptionByName("session_authorization", NULL, false);
	if (current_role && *current_role && strcmp(MtmDatabaseUser, current_role) != 0)
		MtmGucUpdate("session_authorization", current_role);
	MemoryContextSwitchTo(oldcontext);
}

static void MtmGucDiscard()
{
	dlist_iter iter;

	if (dlist_is_empty(&MtmGucList))
		return;

	dlist_foreach(iter, &MtmGucList)
	{
		MtmGucEntry *cur_entry = dlist_container(MtmGucEntry, list_node, iter.cur);
		pfree(cur_entry->value);
	}
	dlist_init(&MtmGucList);

	hash_destroy(MtmGucHash);
	MtmGucHash = NULL;
}

static inline void MtmGucUpdate(const char *key, char *value)
{
	MtmGucEntry *hentry;
	bool found;

	if (!MtmGucHash)
		MtmGucInit();

	hentry = (MtmGucEntry*)hash_search(MtmGucHash, key, HASH_ENTER, &found);
	if (found)
	{
		pfree(hentry->value);
		dlist_delete(&hentry->list_node);
	}
	hentry->value = value;
	dlist_push_tail(&MtmGucList, &hentry->list_node);
}

static inline void MtmGucRemove(const char *key)
{
	MtmGucEntry *hentry;
	bool found;

	if (!MtmGucHash)
		MtmGucInit();

	hentry = (MtmGucEntry*)hash_search(MtmGucHash, key, HASH_FIND, &found);
	if (found)
	{
		pfree(hentry->value);
		dlist_delete(&hentry->list_node);
		hash_search(MtmGucHash, key, HASH_REMOVE, NULL);
	}
}

static void MtmGucSet(VariableSetStmt *stmt, const char *queryStr)
{
	MemoryContext oldcontext;

	if (!MtmGucHash)
		MtmGucInit();

	oldcontext = MemoryContextSwitchTo(TopMemoryContext);

	switch (stmt->kind)
	{
		case VAR_SET_VALUE:
			MtmGucUpdate(stmt->name, ExtractSetVariableArgs(stmt));
			break;

		case VAR_SET_DEFAULT:
			MtmGucRemove(stmt->name);
			break;

		case VAR_RESET:
			if (strcmp(stmt->name, "session_authorization") == 0)
				MtmGucRemove("role");
			MtmGucRemove(stmt->name);
			break;

		case VAR_RESET_ALL:
			/* XXX: shouldn't we keep auth/role here? */
			MtmGucDiscard();
			break;

		case VAR_SET_CURRENT:
		case VAR_SET_MULTI:
			break;
	}

	MemoryContextSwitchTo(oldcontext);
}

static int
_var_name_cmp(const void *a, const void *b)
{
	const struct config_generic *confa = *(struct config_generic * const *) a;
	const struct config_generic *confb = *(struct config_generic * const *) b;

	return strcmp(confa->name, confb->name);
}

static struct config_generic *
fing_guc_conf(const char *name)
{
	int num;
	struct config_generic **vars;
	const char **key = &name;
	struct config_generic **res;

	num = GetNumConfigOptions();
	vars = get_guc_variables();

	res = (struct config_generic **) bsearch((void *) &key,
										(void *) vars,
										num, sizeof(struct config_generic *),
										_var_name_cmp);

	return res ? *res : NULL;
}

char* MtmGucSerialize(void)
{
	StringInfo serialized_gucs;
	dlist_iter iter;
	const char *search_path;

	if (!MtmGucHash)
		MtmGucInit();

	serialized_gucs = makeStringInfo();

	dlist_foreach(iter, &MtmGucList)
	{
		MtmGucEntry *cur_entry = dlist_container(MtmGucEntry, list_node, iter.cur);
		struct config_generic *gconf;

		if (strcmp(cur_entry->key, "search_path") == 0)
			continue;

		appendStringInfoString(serialized_gucs, "SET ");
		appendStringInfoString(serialized_gucs, cur_entry->key);
		appendStringInfoString(serialized_gucs, " TO ");

		gconf = fing_guc_conf(cur_entry->key);
		if (gconf && (gconf->vartype == PGC_STRING || gconf->vartype == PGC_ENUM || (gconf->flags & (GUC_UNIT_MEMORY | GUC_UNIT_TIME))))
		{
			appendStringInfoString(serialized_gucs, "'");
			appendStringInfoString(serialized_gucs, cur_entry->value);
			appendStringInfoString(serialized_gucs, "'");
		}
		else
		{
			appendStringInfoString(serialized_gucs, cur_entry->value);
		}
		appendStringInfoString(serialized_gucs, "; ");
	}

	/*
	 * Crutch for scheduler. It sets search_path through SetConfigOption()
	 * so our callback do not react on that.
	 */
	search_path = GetConfigOption("search_path", false, true);
	appendStringInfo(serialized_gucs, "SET search_path TO %s; ", search_path);

	return serialized_gucs->data;
}

/*
 * -------------------------------------------
 * DDL Handling
 * -------------------------------------------
 */

static void MtmProcessDDLCommand(char const* queryString, bool transactional)
{
	if (transactional)
	{
		char *gucCtx = MtmGucSerialize();
		queryString = psprintf("RESET SESSION AUTHORIZATION; reset all; %s %s", gucCtx, queryString);

		/* Transactional DDL */
		MTM_LOG3("Sending DDL: %s", queryString);
		LogLogicalMessage("D", queryString, strlen(queryString) + 1, true);
		MtmTx.containsDML = true;
	}
	else
	{
		/* Concurrent DDL */
		MTM_LOG1("Sending concurrent DDL: %s", queryString);
		XLogFlush(LogLogicalMessage("C", queryString, strlen(queryString) + 1, false));
	}
}

static void MtmFinishDDLCommand()
{
	LogLogicalMessage("E", "", 1, true);
}


static bool MtmIsTempType(TypeName* typeName)
{
	bool isTemp = false;

	if (typeName != NULL)
	{
		Type typeTuple = LookupTypeName(NULL, typeName, NULL, false);
		if (typeTuple != NULL)
		{
			Form_pg_type typeStruct = (Form_pg_type) GETSTRUCT(typeTuple);
		    Oid relid = typeStruct->typrelid;
		    ReleaseSysCache(typeTuple);

			if (relid != InvalidOid)
			{
				HeapTuple classTuple = SearchSysCache1(RELOID, relid);
				Form_pg_class classStruct = (Form_pg_class) GETSTRUCT(classTuple);
				if (classStruct->relpersistence == 't')
					isTemp = true;
				ReleaseSysCache(classTuple);
			}
		}
	}
	return isTemp;
}

static bool MtmFunctionProfileDependsOnTempTable(CreateFunctionStmt* func)
{
	ListCell* elem;

	if (MtmIsTempType(func->returnType))
	{
		return true;
	}
	foreach (elem, func->parameters)
	{
		FunctionParameter* param = (FunctionParameter*) lfirst(elem);
		if (MtmIsTempType(param->argType))
		{
			return true;
		}
	}
	return false;
}

static void
AdjustCreateSequence(List *options)
{
	bool has_increment = false, has_start = false;
	ListCell   *option;

	foreach(option, options)
	{
		DefElem    *defel = (DefElem *) lfirst(option);
		if (strcmp(defel->defname, "increment") == 0)
			has_increment = true;
		else if (strcmp(defel->defname, "start") == 0)
			has_start = true;
	}

	if (!has_increment)
	{
		DefElem *defel = makeDefElem("increment", (Node *) makeInteger(MtmMaxNodes), -1);
		options = lappend(options, defel);
	}

	if (!has_start)
	{
		DefElem *defel = makeDefElem("start", (Node *) makeInteger(MtmNodeId), -1);
		options = lappend(options, defel);
	}
}

static void MtmProcessUtility(PlannedStmt *pstmt,
							  const char *queryString, ProcessUtilityContext context,
							  ParamListInfo params,
							  QueryEnvironment *queryEnv,
							  DestReceiver *dest, char *completionTag)
{
	bool skipCommand = false;
	bool executed = false;
	bool prevMyXactAccessedTempRel;
	Node *parsetree = pstmt->utilityStmt;
	int stmt_start = pstmt->stmt_location > 0 ? pstmt->stmt_location : 0;
	int stmt_len = pstmt->stmt_len > 0 ? pstmt->stmt_len : strlen(queryString + stmt_start);
	char *stmt_string = palloc(stmt_len + 1);

	strncpy(stmt_string, queryString + stmt_start, stmt_len);
	stmt_string[stmt_len] = 0;

	MTM_LOG2("%d: Process utility statement tag=%d, context=%d, issubtrans=%d, creating_extension=%d, statement=%s",
			 MyProcPid, nodeTag(parsetree), context, IsSubTransaction(), creating_extension, stmt_string);
	switch (nodeTag(parsetree))
	{
		case T_TransactionStmt:
			{
				TransactionStmt *stmt = (TransactionStmt *) parsetree;
				switch (stmt->kind)
				{
				case TRANS_STMT_BEGIN:
				case TRANS_STMT_START:
					MtmTx.isTransactionBlock = true;
					break;
				case TRANS_STMT_COMMIT:
					if (MtmTwoPhaseCommit(&MtmTx)) { // XXX: isn't this already handled by commit event?
						return;
					}
					break;
				case TRANS_STMT_PREPARE:
					MtmTx.isTwoPhase = true;
					strncpy(MtmTx.gid, stmt->gid, GIDSIZE);
					break;
				case TRANS_STMT_COMMIT_PREPARED:
				case TRANS_STMT_ROLLBACK_PREPARED:
					Assert(!MtmTx.isTwoPhase);
					strncpy(MtmTx.gid, stmt->gid, GIDSIZE);
					break;
				default:
					break;
				}
			}
			/* no break */
		case T_PlannedStmt:
		case T_ClosePortalStmt:
		case T_FetchStmt:
		case T_DoStmt:
		case T_CommentStmt:
		case T_PrepareStmt:
		case T_ExecuteStmt:
		case T_DeallocateStmt:
		case T_NotifyStmt:
		case T_ListenStmt:
		case T_UnlistenStmt:
		case T_LoadStmt:
		case T_ClusterStmt:
		case T_VariableShowStmt:
		case T_ReassignOwnedStmt:
		case T_LockStmt: // XXX: check whether we should replicate that
		case T_CheckPointStmt:
		case T_ReindexStmt:
		case T_ExplainStmt:
		case T_AlterSystemStmt:
			skipCommand = true;
			break;

		case T_CreatedbStmt:
		case T_DropdbStmt:
			elog(ERROR, "Multimaster doesn't support creating and dropping databases");
			break;

		case T_CreateSeqStmt:
			{
				CreateSeqStmt *stmt = (CreateSeqStmt *) parsetree;
				if (!MtmVolksWagenMode)
					AdjustCreateSequence(stmt->options);
			}
			break;

		case T_CreateTableSpaceStmt:
		case T_DropTableSpaceStmt:
			{
				if (MtmApplyContext != NULL)
				{
					MemoryContext oldContext = MemoryContextSwitchTo(MtmApplyContext);
					Assert(oldContext != MtmApplyContext);
					MtmTablespaceStmt = copyObject(parsetree);
					MemoryContextSwitchTo(oldContext);
					return;
				}
				else
				{
					skipCommand = true;
					MtmProcessDDLCommand(stmt_string, false);
				}
			}
			break;

		case T_VacuumStmt:
		{
			// VacuumStmt* vacuum = (VacuumStmt*)parsetree;
			skipCommand = true;
			if (!MtmVolksWagenMode)
			{
				if (context == PROCESS_UTILITY_TOPLEVEL) {
					MtmProcessDDLCommand(stmt_string, false);
					MtmTx.isDistributed = false;
				} else if (MtmApplyContext != NULL) {
					MemoryContext oldContext = MemoryContextSwitchTo(MtmApplyContext);
					Assert(oldContext != MtmApplyContext);
					MtmVacuumStmt = (VacuumStmt*)copyObject(parsetree);
					MemoryContextSwitchTo(oldContext);
					return;
				}
			}
			break;
		}
		case T_CreateDomainStmt:
			/* Detect temp tables access */
			{
				CreateDomainStmt *stmt = (CreateDomainStmt *) parsetree;
				HeapTuple	typeTup;
				Form_pg_type baseType;
				Form_pg_type elementType;
				Form_pg_class pgClassStruct;
				int32		basetypeMod;
				Oid			elementTypeOid;
				Oid			tableOid;
				HeapTuple pgClassTuple;
				HeapTuple elementTypeTuple;

				typeTup = typenameType(NULL, stmt->typeName, &basetypeMod);
				baseType = (Form_pg_type) GETSTRUCT(typeTup);
				elementTypeOid = baseType->typelem;
				ReleaseSysCache(typeTup);

				if (elementTypeOid == InvalidOid)
					break;

				elementTypeTuple = SearchSysCache1(TYPEOID, elementTypeOid);
				elementType = (Form_pg_type) GETSTRUCT(elementTypeTuple);
				tableOid = elementType->typrelid;
				ReleaseSysCache(elementTypeTuple);

				if (tableOid == InvalidOid)
					break;

				pgClassTuple = SearchSysCache1(RELOID, tableOid);
				pgClassStruct = (Form_pg_class) GETSTRUCT(pgClassTuple);
				if (pgClassStruct->relpersistence == 't')
					MyXactFlags |= XACT_FLAGS_ACCESSEDTEMPREL;
				ReleaseSysCache(pgClassTuple);
			}
			break;

		// case T_ExplainStmt:
		//	/*
		//	 * EXPLAIN ANALYZE can create side-effects.
		//	 * Better to catch that by some general mechanism of detecting
		//	 * catalog and heap writes.
		//	 */
		//	{
		//		ExplainStmt *stmt = (ExplainStmt *) parsetree;
		//		ListCell   *lc;

		//		skipCommand = true;
		//		foreach(lc, stmt->options)
		//		{
		//			DefElem	   *opt = (DefElem *) lfirst(lc);
		//			if (strcmp(opt->defname, "analyze") == 0)
		//				skipCommand = false;
		//		}
		//	}
		//	break;

		/* Save GUC context for consequent DDL execution */
		case T_DiscardStmt:
			{
				DiscardStmt *stmt = (DiscardStmt *) parsetree;

				if (!IsTransactionBlock() && stmt->target == DISCARD_ALL)
				{
					skipCommand = true;
					MtmGucDiscard();
				}
			}
			break;
		case T_VariableSetStmt:
			{
				VariableSetStmt *stmt = (VariableSetStmt *) parsetree;

				/* Prevent SET TRANSACTION from replication */
				if (stmt->kind == VAR_SET_MULTI)
					skipCommand = true;

				if (!IsTransactionBlock())
				{
					skipCommand = true;
					MtmGucSet(stmt, stmt_string);
				}
			}
			break;

		case T_IndexStmt:
			{
				IndexStmt *indexStmt = (IndexStmt *) parsetree;
				if (indexStmt->concurrent)
				{
					 if (context == PROCESS_UTILITY_TOPLEVEL) {
						 MtmProcessDDLCommand(stmt_string, false);
						 MtmTx.isDistributed = false;
						 skipCommand = true;
						 /*
						  * Index is created at replicas completely asynchronously, so to prevent unintended interleaving with subsequent
						  * commands in this session, just wait here for a while.
						  * It will help to pass regression tests but will not be enough for construction of real large indexes
						  * where difference between completion of this operation at different nodes is unlimited
						  */
						 MtmSleep(USECS_PER_SEC);
					 } else if (MtmApplyContext != NULL) {
						 MemoryContext oldContext = MemoryContextSwitchTo(MtmApplyContext);
						 Assert(oldContext != MtmApplyContext);
						 MtmIndexStmt = (IndexStmt*)copyObject(indexStmt);
						 MemoryContextSwitchTo(oldContext);
						 return;
					 }
				}
			}
			break;

		case T_TruncateStmt:
			skipCommand = false;
			// MtmLockCluster();
			break;

		case T_DropStmt:
			{
				DropStmt *stmt = (DropStmt *) parsetree;
				if (stmt->removeType == OBJECT_INDEX && stmt->concurrent)
				{
					if (context == PROCESS_UTILITY_TOPLEVEL) {
						MtmProcessDDLCommand(stmt_string, false);
						MtmTx.isDistributed = false;
						skipCommand = true;
					} else if (MtmApplyContext != NULL) {
						 MemoryContext oldContext = MemoryContextSwitchTo(MtmApplyContext);
						 Assert(oldContext != MtmApplyContext);
						 MtmDropStmt = (DropStmt*)copyObject(stmt);
						 MemoryContextSwitchTo(oldContext);
						 return;
					}
				}
				else if (stmt->removeType == OBJECT_FUNCTION && MtmTx.isReplicated)
				{
					/* Make it possible to drop functions which were not replicated */
					stmt->missing_ok = true;
				}
			}
			break;

		/* Copy need some special care */
		case T_CopyStmt:
		{
			CopyStmt *copyStatement = (CopyStmt *) parsetree;
			skipCommand = true;
			if (copyStatement->is_from) {
				ListCell *opt;
				RangeVar *relation = copyStatement->relation;

				if (relation != NULL)
				{
					Oid relid = RangeVarGetRelid(relation, NoLock, true);
					if (OidIsValid(relid))
					{
						Relation rel = heap_open(relid, ShareLock);
						if (RelationNeedsWAL(rel)) {
							MtmTx.containsDML = true;
						}
						heap_close(rel, ShareLock);
					}
				}

				foreach(opt, copyStatement->options)
				{
					DefElem	*elem = lfirst(opt);
					if (strcmp("local", elem->defname) == 0) {
						MtmTx.isDistributed = false; /* Skip */
						MtmTx.snapshot = INVALID_CSN;
						MtmTx.containsDML = false;
						break;
					}
				}
			}
		    case T_CreateFunctionStmt:
		    {
				if (MtmTx.isReplicated)
				{
					// disable functiob body cehck at replica
					check_function_bodies = false;
				}
			}
			break;
		}

		default:
			skipCommand = false;
			break;
	}

	if (!skipCommand && !MtmDDLStatement)
	{
		MTM_LOG3("Process DDL statement '%s', MtmTx.isReplicated=%d, "
				 "MtmIsLogicalReceiver=%d", stmt_string, MtmTx.isReplicated,
				 MtmIsLogicalReceiver);
		MtmProcessDDLCommand(stmt_string, true);
		executed = true;
		MtmDDLStatement = stmt_string;
	}
	else MTM_LOG3("Skip utility statement '%s': skip=%d, insideDDL=%d", stmt_string, skipCommand, MtmDDLStatement != NULL);

	prevMyXactAccessedTempRel = MyXactFlags & XACT_FLAGS_ACCESSEDTEMPREL;

	if (PreviousProcessUtilityHook != NULL)
	{
		PreviousProcessUtilityHook(pstmt, queryString,
										 context, params, queryEnv,
										 dest, completionTag);
	}
	else
	{
		standard_ProcessUtility(pstmt, queryString,
									context, params, queryEnv,
									dest, completionTag);
	}

#if 0
	if (!MtmVolksWagenMode && MtmTx.isDistributed && XactIsoLevel != XACT_REPEATABLE_READ) {
		MTM_ELOG(ERROR, "Isolation level %s is not supported by multimaster", isoLevelStr[XactIsoLevel]);
	}
#endif
	/* Allow replication of functions operating on temporary tables.
	 * Even through temporary table doesn't exist at replica, diasabling functoin body check makes it possible to create such function at replica.
	 * And it can be accessed later at replica if correspondent temporary table will be created.
	 * But disable replication of functions returning temporary tables: such functions can not be created at replica in any case.
	 */
	if (IsA(parsetree, CreateFunctionStmt))
	{
		if (MtmFunctionProfileDependsOnTempTable((CreateFunctionStmt*)parsetree))
		{
			prevMyXactAccessedTempRel = true;
		}
		if (prevMyXactAccessedTempRel)
			MyXactFlags |= XACT_FLAGS_ACCESSEDTEMPREL;
	}
	if (MyXactFlags & XACT_FLAGS_ACCESSEDTEMPREL)
	{
		MTM_LOG1("Xact accessed temp table, stopping replication of statement '%s'", stmt_string);
		MtmTx.isDistributed = false; /* Skip */
		MtmTx.snapshot = INVALID_CSN;
	}

	if (executed)
	{
		MtmFinishDDLCommand();
		MtmDDLStatement = NULL;
	}
	if (IsA(parsetree, CreateStmt))
	{
		CreateStmt* create = (CreateStmt*)parsetree;
		Oid relid = RangeVarGetRelid(create->relation, NoLock, true);
		if (relid != InvalidOid) {
			Oid constraint_oid;
			Bitmapset* pk = get_primary_key_attnos(relid, true, &constraint_oid);
			if (pk == NULL && !MtmVolksWagenMode && MtmIgnoreTablesWithoutPk) {
				elog(WARNING,
					 "Table %s.%s without primary will not be replicated",
					 create->relation->schemaname ? create->relation->schemaname : "public",
					 create->relation->relname);
			}
		}
	}
}

static void
MtmExecutorStart(QueryDesc *queryDesc, int eflags)
{
	if (!MtmTx.isReplicated && !MtmDDLStatement)
	{
		ListCell   *tlist;

		if (!MtmRemoteFunctions)
		{
			MtmInitializeRemoteFunctionsMap();
		}

		foreach(tlist, queryDesc->plannedstmt->planTree->targetlist)
		{
			TargetEntry *tle = (TargetEntry *) lfirst(tlist);
			if (tle->expr && IsA(tle->expr, FuncExpr))
			{
				Oid func_oid = ((FuncExpr*)tle->expr)->funcid;
				if (!hash_search(MtmRemoteFunctions, &func_oid, HASH_FIND, NULL))
				{
					Form_pg_proc funcform;
					bool is_sec_def;
					HeapTuple func_tuple = SearchSysCache1(PROCOID, ObjectIdGetDatum(func_oid));
					if (!HeapTupleIsValid(func_tuple))
						elog(ERROR, "cache lookup failed for function %u", func_oid);
					funcform = (Form_pg_proc) GETSTRUCT(func_tuple);
					is_sec_def = funcform->prosecdef;
					ReleaseSysCache(func_tuple);
					if (!is_sec_def)
					{
						continue;
					}
				}
				/*
				 * Execute security defined functions or functions marked as remote at replicated nodes.
				 * Them are executed as DDL statements.
				 * All data modifications done inside this function are not replicated.
				 * As a result generated content can vary at different nodes.
				 */
				MtmProcessDDLCommand(queryDesc->sourceText, true);
				MtmDDLStatement = queryDesc;
				break;
			}
		}
	}
	if (PreviousExecutorStartHook != NULL)
		PreviousExecutorStartHook(queryDesc, eflags);
	else
		standard_ExecutorStart(queryDesc, eflags);
}

static void
MtmExecutorFinish(QueryDesc *queryDesc)
{
	/*
	 * If tx didn't wrote to XLOG then there is nothing to commit on other nodes.
	 */
	if (MtmDoReplication) {
		CmdType operation = queryDesc->operation;
		EState *estate = queryDesc->estate;
		if (estate->es_processed != 0 && (operation == CMD_INSERT || operation == CMD_UPDATE || operation == CMD_DELETE)) {
			int i;
			for (i = 0; i < estate->es_num_result_relations; i++) {
				Relation rel = estate->es_result_relations[i].ri_RelationDesc;
				if (RelationNeedsWAL(rel)) {
					if (MtmIgnoreTablesWithoutPk) {
						if (!rel->rd_indexvalid) {
							RelationGetIndexList(rel);
						}
						if (rel->rd_replidindex == InvalidOid) {
							MtmMakeRelationLocal(RelationGetRelid(rel));
							continue;
						}
					}
					MTM_LOG3("MtmTx.containsDML = true // WAL");
					MtmTx.containsDML = true;
					break;
				}
			}
		}
	}

	if (PreviousExecutorFinishHook != NULL)
	{
		PreviousExecutorFinishHook(queryDesc);
	}
	else
	{
		standard_ExecutorFinish(queryDesc);
	}

	if (MtmDDLStatement == queryDesc)
	{
		MtmFinishDDLCommand();
		MtmDDLStatement = NULL;
	}
}

static void MtmSeqNextvalHook(Oid seqid, int64 next)
{
	if (MtmMonotonicSequences)
	{
		MtmSeqPosition pos;
		pos.seqid = seqid;
		pos.next = next;
		LogLogicalMessage("N", (char*)&pos, sizeof(pos), true);
	}
}

/*
 * Allow to replicate handcrafted heap inserts/updates.
 * Needed for scheduler.
 */
void
MtmToggleDML(void)
{
	MtmTx.containsDML = true;
}


void
MtmWaitForExtensionCreation(void)
{
	for (;;)
	{
		RangeVar   *rv;
		Oid			rel_oid;

		StartTransactionCommand();
		rv = makeRangeVar(MULTIMASTER_SCHEMA_NAME, "local_tables", -1);
		rel_oid = RangeVarGetRelid(rv, NoLock, true);
		CommitTransactionCommand();

		if (OidIsValid(rel_oid))
		{
			Mtm->extension_created = true;
			break;
		}

		MtmSleep(USECS_PER_SEC);
	}
}
