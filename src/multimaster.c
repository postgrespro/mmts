/*
 * multimaster.c
 *
 * Multimaster based on logical replication
 *
 */
// XXX: evict that
#include <unistd.h>
#include <sys/time.h>
#include <time.h>

#include "postgres.h"
#include "access/xtm.h"
#include "access/clog.h"
#include "access/transam.h"
#include "access/twophase.h"
#include "access/relscan.h"
#include "common/username.h"
#include "catalog/namespace.h"
#include "nodes/makefuncs.h"
#include "postmaster/autovacuum.h"
#include "postmaster/postmaster.h"
#include "replication/origin.h"
#include "replication/slot.h"
#include "replication/logical.h"
#include "replication/logicalfuncs.h"
#include "replication/walsender.h"
#include "storage/ipc.h"
#include "utils/guc.h"
#include "utils/builtins.h"
#include "funcapi.h"
#include "fmgr.h"
#include "miscadmin.h"
#include "pgstat.h"
#include "parser/parse_node.h"
#include "commands/extension.h"
#include "catalog/pg_class.h"
#include "commands/tablecmds.h"
#include "commands/publicationcmds.h"
#include "executor/spi.h"
#include "catalog/pg_type.h"
#include "tcop/tcopprot.h"
#include "catalog/pg_subscription.h"
#include "utils/syscache.h"
#include "catalog/objectaddress.h"
#include "utils/rel.h"
#include "catalog/indexing.h"

#include "multimaster.h"
#include "ddd.h"
#include "ddl.h"
#include "state.h"
#include "receiver.h"
#include "resolver.h"
#include "logger.h"

#include "compat.h"

typedef enum
{
	MTM_STATE_LOCK_ID
} MtmLockIds;

typedef struct
{
	TimestampTz	last_timestamp;
	slock_t		mutex;
} MtmTime;

static MtmTime *mtm_time;

#define MTM_SHMEM_SIZE (8*1024*1024)

void _PG_init(void);
void _PG_fini(void);

PG_MODULE_MAGIC;

PG_FUNCTION_INFO_V1(mtm_init_node);
PG_FUNCTION_INFO_V1(mtm_add_node);
PG_FUNCTION_INFO_V1(mtm_drop_node);

static size_t MtmGetTransactionStateSize(void);
static void	  MtmSerializeTransactionState(void* ctx);
static void	  MtmDeserializeTransactionState(void* ctx);
static void*  MtmCreateSavepointContext(void);
static void	  MtmRestoreSavepointContext(void* ctx);
static void	  MtmReleaseSavepointContext(void* ctx);
static void*  MtmSuspendTransaction(void);
static void   MtmResumeTransaction(void* ctx);

static void MtmShmemStartup(void);

static void launcher_init(void);
void launcher_main(Datum main_arg);
void drop_node_entry(int node_id);

MtmState* Mtm;

MemoryContext MtmApplyContext;

// XXX: do we really need all this guys (except ddd)?
static TransactionManager MtmTM =
{
	MtmDetectGlobalDeadLock,
	MtmGetTransactionStateSize,
	MtmSerializeTransactionState,
	MtmDeserializeTransactionState,
	PgInitializeSequence,
	MtmCreateSavepointContext,
	MtmRestoreSavepointContext,
	MtmReleaseSavepointContext,
	MtmSuspendTransaction,
	MtmResumeTransaction
};

LWLock *MtmCommitBarrier;
LWLock *MtmReceiverBarrier;
LWLock *MtmSyncpointLock;

// XXX
bool  MtmBackgroundWorker;
int	  MtmReplicationNodeId;
int	  MtmMaxNodes;

int	  MtmTransSpillThreshold; // XXX: align with receiver buffer size
int	  MtmHeartbeatSendTimeout;
int	  MtmHeartbeatRecvTimeout;
char* MtmRefereeConnStr;

static int	 MtmQueueSize;


static shmem_startup_hook_type PreviousShmemStartupHook;

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
	LWLockAcquire((LWLockId)&Mtm->locks[MTM_STATE_LOCK_ID], mode);
}

void MtmUnlock(void)
{
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

TimestampTz
MtmGetIncreasingTimestamp()
{
	TimestampTz now = GetCurrentTimestamp();

	/*
	 * Don't let time move backward; if it hasn't advanced, use incremented
	 * last value.
	 */
	SpinLockAcquire(&mtm_time->mutex);
	if (now <= mtm_time->last_timestamp)
		now = ++mtm_time->last_timestamp;
	else
		mtm_time->last_timestamp = now;
	SpinLockRelease(&mtm_time->mutex);

	return now;
}

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

static void* MtmCreateSavepointContext(void)
{
	return NULL;
}

static void	 MtmRestoreSavepointContext(void* ctx)
{
}

static void	 MtmReleaseSavepointContext(void* ctx)
{
}

static void* MtmSuspendTransaction(void)
{
	MtmCurrentTrans* ctx = malloc(sizeof(MtmCurrentTrans));
	*ctx = MtmTx;
	MtmBeginTransaction();
	return ctx;
}

static void MtmResumeTransaction(void* ctx)
{
	MtmTx = *(MtmCurrentTrans*)ctx;
	free(ctx);
}


/*
 * Initialize message
 */
void MtmInitMessage(MtmArbiterMessage* msg, MtmMessageCode code)
{
	memset(msg, '\0', sizeof(MtmArbiterMessage));

	msg->code = code;
	msg->connectivityMask = SELF_CONNECTIVITY_MASK;
	msg->node = Mtm->my_node_id;
}

/*
 * Perform initialization of multimaster state.
 * This function is called from shared memory startup hook (after completion of initialization of shared memory)
 */
static void
MtmStateShmemStartup()
{
	bool found;
	int i;

	LWLockAcquire(AddinShmemInitLock, LW_EXCLUSIVE);

	mtm_time = (MtmTime *) ShmemInitStruct("mtm_time", sizeof(MtmTime), &found);
	if (!found)
	{
		mtm_time->last_timestamp = 0;
		SpinLockInit(&mtm_time->mutex);
	}

	Mtm = (MtmState*)ShmemInitStruct(MULTIMASTER_NAME, sizeof(MtmState) + sizeof(BgwPool)*(MtmMaxNodes), &found);
	if (!found)
	{
		MemSet(Mtm, 0, sizeof(MtmState));
		Mtm->stop_new_commits = false;
		Mtm->recovered = false;
		Mtm->status = MTM_DISABLED; //MTM_INITIALIZATION;
		Mtm->recoverySlot = 0;
		Mtm->locks = GetNamedLWLockTranche(MULTIMASTER_NAME);
		Mtm->disabledNodeMask = (nodemask_t) -1;
		Mtm->clique = (nodemask_t) -1;
		Mtm->refereeGrant = false;
		Mtm->refereeWinnerId = 0;
		Mtm->stalledNodeMask = 0;
		Mtm->stoppedNodeMask = 0;
		Mtm->pglogicalReceiverMask = 0;
		Mtm->pglogicalSenderMask = 0;
		Mtm->recoveryCount = 0;
		Mtm->localTablesHashLoaded = false;
		Mtm->latestSyncpoint = InvalidXLogRecPtr;

		// XXX: change to dsa and make it per-receiver
		for (i = 0; i < MtmMaxNodes; i++)
			BgwPoolInit(&Mtm->pools[i], MtmExecutor, MtmQueueSize, 0);
	}

	RegisterXactCallback(MtmXactCallback2, NULL);

	MtmCommitBarrier = &(GetNamedLWLockTranche(MULTIMASTER_NAME)[MtmMaxNodes*2+1].lock);
	MtmReceiverBarrier = &(GetNamedLWLockTranche(MULTIMASTER_NAME)[MtmMaxNodes*2+2].lock);
	MtmSyncpointLock = &(GetNamedLWLockTranche(MULTIMASTER_NAME)[MtmMaxNodes*2+3].lock);

	TM = &MtmTM;
	LWLockRelease(AddinShmemInitLock);
}

static void
MtmShmemStartup(void)
{
	if (PreviousShmemStartupHook)
		PreviousShmemStartupHook();

	MtmDeadlockDetectorShmemStartup(MtmMaxNodes);
	MtmDDLReplicationShmemStartup();
	MtmStateShmemStartup();
}

/*
 * Check correctness of multimaster configuration
 */
static bool ConfigIsSane(void)
{
	bool ok = true;

	if (MtmMaxNodes < 1)
	{
		mtm_log(WARNING, "multimaster requires multimaster.max_nodes > 0");
		ok = false;
	}

	if (max_prepared_xacts < 1)
	{
		mtm_log(WARNING,
			 "multimaster requires max_prepared_transactions > 0, "
			 "because all transactions are implicitly two-phase");
		ok = false;
	}

	{
		int workers_required = 2 * MtmMaxNodes + 1;
		if (max_worker_processes < workers_required)
		{
			mtm_log(WARNING,
				 "multimaster requires max_worker_processes >= %d",
				 workers_required);
			ok = false;
		}
	}

	if (wal_level != WAL_LEVEL_LOGICAL)
	{
		mtm_log(WARNING,
			 "multimaster requires wal_level = 'logical', "
			 "because it is build on top of logical replication");
		ok = false;
	}

	if (max_wal_senders < MtmMaxNodes)
	{
		mtm_log(WARNING,
			 "multimaster requires max_wal_senders >= %d (multimaster.max_nodes), ",
			 MtmMaxNodes);
		ok = false;
	}

	if (max_replication_slots < MtmMaxNodes)
	{
		mtm_log(WARNING,
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
		6,
		0,
		MTM_MAX_NODES,
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

	MtmDeadlockDetectorInit(MtmMaxNodes);

	/*
	 * Request additional shared resources.	 (These are no-ops if we're not in
	 * the postmaster process.)	 We'll allocate or attach to the shared
	 * resources in mtm_shmem_startup().
	 */
	RequestAddinShmemSpace(MTM_SHMEM_SIZE + MtmMaxNodes*MtmQueueSize + sizeof(MtmTime));
	RequestNamedLWLockTranche(MULTIMASTER_NAME, 1 + MtmMaxNodes*2 + 3);

	dmq_init();
	dmq_receiver_start_hook = MtmOnNodeConnect;
	dmq_receiver_stop_hook = MtmOnNodeDisconnect;

	ResolverInit();

	MtmDDLReplicationInit();

	launcher_init();

	/*
	 * Install hooks.
	 */
	PreviousShmemStartupHook = shmem_startup_hook;
	shmem_startup_hook = MtmShmemStartup;
}

/*
 * Module unload callback
 *
 * XXX: check 'drop extension multimaster'
 */
void
_PG_fini(void)
{
	shmem_startup_hook = PreviousShmemStartupHook;
}

Datum
mtm_drop_node(PG_FUNCTION_ARGS)
{
	PG_RETURN_VOID();
}

Datum
mtm_add_node(PG_FUNCTION_ARGS)
{
	PG_RETURN_VOID();
}

/*
 * Genenerate global transaction identifier for two-pahse commit.
 * It should be unique for all nodes
 */
void
MtmGenerateGid(char *gid, TransactionId xid, int node_id)
{
	sprintf(gid, "MTM-%d-" XID_FMT, node_id, xid);
	return;
}

int
MtmGidParseNodeId(const char* gid)
{
	int node_id = -1;
	sscanf(gid, "MTM-%d-%*d", &node_id);
	return node_id;
}

TransactionId
MtmGidParseXid(const char* gid)
{
	TransactionId xid = InvalidTransactionId;
	sscanf(gid, "MTM-%*d-" XID_FMT, &xid);
	Assert(TransactionIdIsValid(xid));
	return xid;
}

bool
MtmAllApplyWorkersFinished()
{
	int i;

	for (i = 0; i < Mtm->nAllNodes; i++)
	{
		volatile int ntasks;

		if (i == Mtm->my_node_id - 1)
			continue;

		SpinLockAcquire(&Mtm->pools[i].lock);
		ntasks = Mtm->pools[i].active + Mtm->pools[i].pending;
		SpinLockRelease(&Mtm->pools[i].lock);

		mtm_log(MtmApplyBgwFinish, "MtmAllApplyWorkersFinished %d tasks not finished", ntasks);

		if (ntasks != 0)
			false;
	}

	return true;
}

/* create node entry */
// XXX: put all table names in constants
static void
create_node_entry(int node_id, char *connstr, bool is_self)
{
	char	   *sql;
	int			rc;

	if (SPI_connect() != SPI_OK_CONNECT)
		mtm_log(ERROR, "could not connect using SPI");
	PushActiveSnapshot(GetTransactionSnapshot());

	sql = psprintf("insert into mtm.nodes values (%d, $$%s$$, '%c')",
				   node_id, connstr, is_self ? 't' : 'f');
	rc = SPI_execute(sql, false, 0);
	if (rc < 0)
		mtm_log(ERROR, "Failed to save syncpoint");

	SPI_finish();
	PopActiveSnapshot();
}

/* delete node entry */
void
drop_node_entry(int node_id)
{
	char	   *sql;
	int			rc;

	if (SPI_connect() != SPI_OK_CONNECT)
		mtm_log(ERROR, "could not connect using SPI");
	PushActiveSnapshot(GetTransactionSnapshot());

	sql = psprintf("delete from mtm.nodes where id=%d",
				   node_id);
	rc = SPI_execute(sql, false, 0);
	if (rc < 0)
		mtm_log(ERROR, "Failed to save syncpoint");

	SPI_finish();
	PopActiveSnapshot();
}

MtmConfig *
MtmLoadConfig()
{
	MtmConfig  *cfg;
	int			rc;
	int			i;
	int			dest_id = 0;
	bool		inside_tx = IsTransactionState();

	cfg = (MtmConfig *) MemoryContextAlloc(TopMemoryContext,
										   sizeof(MtmConfig));

	if (!inside_tx)
		StartTransactionCommand();

	/* Load node_id's with connection strings from mtm.nodes */

	if (SPI_connect() != SPI_OK_CONNECT)
		mtm_log(ERROR, "could not connect using SPI");


	PushActiveSnapshot(GetTransactionSnapshot());

	rc = SPI_execute("select * from mtm.nodes order by id asc", true, 0);
	if (rc < 0)
		mtm_log(ERROR, "Failed to save syncpoint");

	if (rc == SPI_OK_SELECT)
	{
		TupleDesc	tupdesc	= SPI_tuptable->tupdesc;

		Assert(SPI_processed <= MtmMaxNodes);
		cfg->n_nodes = SPI_processed;
		cfg->my_node_id = 0;

		// XXX: allow zero nodes?

		for (i = 0; i < cfg->n_nodes; i++)
		{
			HeapTuple	tup = SPI_tuptable->vals[i];
			bool		isnull;
			int			node_id;
			char	   *connstr;
			bool		is_self;

			node_id = DatumGetInt32(SPI_getbinval(tup, tupdesc, 1, &isnull));
			Assert(!isnull);
			Assert(TupleDescAttr(tupdesc, 0)->atttypid == INT4OID);
			Assert(node_id == i + 1);

			connstr = SPI_getvalue(tup, tupdesc, 2);
			Assert(TupleDescAttr(tupdesc, 1)->atttypid == TEXTOID);

			is_self = DatumGetBool(SPI_getbinval(tup, tupdesc, 3, &isnull));
			Assert(!isnull);
			Assert(TupleDescAttr(tupdesc, 2)->atttypid == BOOLOID);

			/* Empty connstr mean that this is our node */
			if (is_self)
			{
				/* Ensure that there is only one tuple representing our node */
				Assert(cfg->my_node_id == 0);
				cfg->my_node_id = node_id;
			}
			else
			{
				/* Assume that connstr correctness was checked upon creation */
				cfg->nodes[i].conninfo = MemoryContextStrdup(TopMemoryContext, connstr);
				Assert(cfg->nodes[i].conninfo != NULL);
			}
		}

		Assert(cfg->my_node_id != 0);
	}
	else
	{
		mtm_log(ERROR, "Failed to load saved nodes");
	}

	/* Load origin_id's */
	for (i = 0; i < cfg->n_nodes; i++)
	{
		char	   *origin_name;
		RepOriginId	origin_id;

		if (i == cfg->my_node_id - 1)
			continue;

		origin_name = psprintf(MULTIMASTER_SLOT_PATTERN, i + 1);
		origin_id = replorigin_by_name(origin_name, true);

		Assert(origin_id != InvalidRepOriginId);
		cfg->nodes[i].origin_id = origin_id;
	}

	/* fill dest_id's */
	for (i = 0; i < cfg->n_nodes; i++)
	{
		if (i == cfg->my_node_id - 1)
			continue;

		cfg->nodes[i].destination_id = dest_id++;
	}

	SPI_finish();
	PopActiveSnapshot();

	if (!inside_tx)
		CommitTransactionCommand();

	return cfg;
}

Datum
mtm_init_node(PG_FUNCTION_ARGS)
{
	int			my_node_id = PG_GETARG_INT32(0);
	ArrayType *connstrs_arr = PG_GETARG_ARRAYTYPE_P(1);
	Datum	   *connstrs_datums;
	bool	   *connstrs_nulls;
	int			n_nodes;
	int			i;


	if (!ConfigIsSane())
		mtm_log(ERROR, "Multimaster config is insane, refusing to work");

	/* parse array with connstrings */
	Assert(ARR_ELEMTYPE(connstrs_arr) == TEXTOID);
	Assert(ARR_NDIM(connstrs_arr) == 1);
	deconstruct_array(connstrs_arr,
					  TEXTOID, -1, false, 'i',
					  &connstrs_datums, &connstrs_nulls, &n_nodes);


	for (i = 0; i < n_nodes; i++)
	{
		LogicalDecodingContext *ctx = NULL;
		char	   *logical_slot;

		if (i == my_node_id - 1)
			continue;

		/* Now create logical slot for our publication to this neighbour */
		logical_slot = psprintf(MULTIMASTER_SLOT_PATTERN, i + 1);
		ReplicationSlotCreate(logical_slot, true, RS_EPHEMERAL);
		ctx = CreateInitDecodingContext(MULTIMASTER_NAME, NIL,
								false,	/* do not build snapshot */
								logical_read_local_xlog_page, NULL, NULL,
								NULL);
		DecodingContextFindStartpoint(ctx);
		FreeDecodingContext(ctx);
		ReplicationSlotPersist();
		ReplicationSlotRelease();
	}

	/* create node entries */
	for (i = 0; i < n_nodes; i++)
	{
		bool is_self = i == my_node_id - 1;
		create_node_entry(i + 1, TextDatumGetCString(connstrs_datums[i]), is_self);
	}

	for (i = 0; i < n_nodes; i++)
	{
		char	   *origin_name;
		char	   *recovery_slot;

		if (i == my_node_id - 1)
			continue;

		/* create origin for this neighbour */
		origin_name = psprintf(MULTIMASTER_SLOT_PATTERN, i + 1);
		replorigin_create(origin_name);

		/* Create recovery slot to hold WAL files that we may need during recovery */
		recovery_slot = psprintf(MULTIMASTER_RECOVERY_SLOT_PATTERN, i + 1);
		ReplicationSlotCreate(recovery_slot, false, RS_PERSISTENT);
		ReplicationSlotReserveWal();
		/* Write this slot to disk */
		ReplicationSlotMarkDirty();
		ReplicationSlotSave();
		ReplicationSlotRelease();
	}

	/*
	 * Dummy subscription to indicate that this database contains configured
	 * multimaster. Used by launcher to start workers with necessary user_id's.
	 */
	{
		bool		nulls[Natts_pg_subscription];
		Datum		values[Natts_pg_subscription];
		Relation	rel;
		Oid			owner = GetUserId();
		Oid			subid;
		HeapTuple	tup;
		Datum		pub;
		ArrayType  *pubarr;


		rel = heap_open(SubscriptionRelationId, RowExclusiveLock);

		/* Check if name is used */
		subid = GetSysCacheOid2(SUBSCRIPTIONNAME, MyDatabaseId,
								CStringGetDatum(MULTIMASTER_NAME));
		if (OidIsValid(subid))
		{
			ereport(ERROR,
					(errcode(ERRCODE_DUPLICATE_OBJECT),
					errmsg("subscription \"%s\" already exists",
							MULTIMASTER_NAME)));
		}

		/* dummy publication name to satisfy GetSubscription() */
		pub = CStringGetTextDatum(MULTIMASTER_NAME);
		pubarr = construct_array(&pub, 1, TEXTOID, -1, false, 'i');

		/* form tuple for our subscription */
		memset(values, 0, sizeof(values));
		memset(nulls, false, sizeof(nulls));
		values[Anum_pg_subscription_subdbid - 1] =
			ObjectIdGetDatum(MyDatabaseId);
		values[Anum_pg_subscription_subname - 1] =
			DirectFunctionCall1(namein, CStringGetDatum(MULTIMASTER_NAME));
		values[Anum_pg_subscription_subowner - 1] = ObjectIdGetDatum(owner);
		/* disable so logical launcher won't try to start this one */
		values[Anum_pg_subscription_subenabled - 1] = BoolGetDatum(false);
		values[Anum_pg_subscription_subconninfo - 1] =
			CStringGetTextDatum("");
		nulls[Anum_pg_subscription_subslotname - 1] = true;
		values[Anum_pg_subscription_subsynccommit - 1] =
			CStringGetTextDatum("off");
		values[Anum_pg_subscription_subpublications - 1] =
			PointerGetDatum(pubarr);

		tup = heap_form_tuple(RelationGetDescr(rel), values, nulls);

		/* Insert tuple into catalog. */
		subid = CatalogTupleInsert(rel, tup);
		heap_freetuple(tup);

		recordDependencyOnOwner(SubscriptionRelationId, subid, owner);
		heap_close(rel, RowExclusiveLock);
	}

	/* liftoff */
	MtmMonitorStart(MyDatabaseId, GetUserId());

	PG_RETURN_VOID();
}


/*
 * Register static worker for launcher.
 */
static void
launcher_init()
{
	BackgroundWorker worker;

	/* Set up common data for worker */
	memset(&worker, 0, sizeof(worker));
	worker.bgw_flags = BGWORKER_SHMEM_ACCESS | BGWORKER_BACKEND_DATABASE_CONNECTION;
	worker.bgw_start_time = BgWorkerStart_ConsistentState;
	worker.bgw_restart_time = BGW_NEVER_RESTART;
	sprintf(worker.bgw_library_name, "multimaster");
	sprintf(worker.bgw_function_name, "launcher_main");
	snprintf(worker.bgw_name, BGW_MAXLEN, "mtm-launcher");
	snprintf(worker.bgw_type, BGW_MAXLEN, "mtm-launcher");
	RegisterBackgroundWorker(&worker);
}

/*
 * Scans for all databases with enabled multimaster.
 */
void
launcher_main(Datum main_arg)
{
	Relation	rel;
	HeapScanDesc scan;
	HeapTuple	tup;

	/* init this worker */
	pqsignal(SIGTERM, die);
	BackgroundWorkerUnblockSignals();

	/* Connect to a postgres database */
	BackgroundWorkerInitializeConnection(NULL, NULL, 0);

	/*
	 * Start a transaction so we can access pg_subscription, and get a snapshot.
	 * We don't have a use for the snapshot itself, but we're interested in
	 * the secondary effect that it sets RecentGlobalXmin.  (This is critical
	 * for anything that reads heap pages, because HOT may decide to prune
	 * them even if the process doesn't attempt to modify any tuples.)
	 */
	StartTransactionCommand();
	(void) GetTransactionSnapshot();

	rel = heap_open(SubscriptionRelationId, AccessShareLock);
	scan = heap_beginscan_catalog(rel, 0, NULL);

	while (HeapTupleIsValid(tup = heap_getnext(scan, ForwardScanDirection)))
	{
		Form_pg_subscription subform = (Form_pg_subscription) GETSTRUCT(tup);

		if (!subform->subenabled &&
			namestrcmp(&subform->subname, MULTIMASTER_NAME) == 0)
		{
			MtmMonitorStart(subform->subdbid, subform->subowner);
		}
	}

	heap_endscan(scan);
	heap_close(rel, AccessShareLock);

	CommitTransactionCommand();
}


/*
 * Subscription named 'multimaster' acts as a flag that that multimaster
 * extension was created and configured, so we can hijack transactions.
 * We can't hijack transactions before configuration is done because
 * configuration itself is going to need some transactions that better not
 * be aborted because of Mtm->status being DISABLED. Also subscription
 * is uniq with respect to (db_id, sub_name) so "All your base are belong
 * to us" won't happen.
 */
bool
MtmIsEnabled()
{
	return OidIsValid(get_subscription_oid(MULTIMASTER_NAME, true));
}
