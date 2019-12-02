/*
 * multimaster.c
 *
 * Multimaster based on logical replication
 *
 */
#include "postgres.h"

#include "access/xtm.h"
#include "nodes/makefuncs.h"
#include "replication/origin.h"
#include "replication/slot.h"
#include "replication/logical.h"
#include "storage/ipc.h"
#include "utils/builtins.h"
#include "commands/publicationcmds.h"
#include "commands/subscriptioncmds.h"
#include "executor/spi.h"
#include "catalog/pg_type.h"
#include "tcop/tcopprot.h"
#include "catalog/pg_subscription.h"
#include "catalog/pg_publication.h"
#include "utils/rel.h"
#include "commands/defrem.h"
#include "replication/message.h"
#include "utils/pg_lsn.h"
#include "replication/walreceiver.h"
#include "funcapi.h"
#include "miscadmin.h"
#include "pgstat.h"
#include "catalog/pg_authid.h"
#include "libpq/pqformat.h"

#include "multimaster.h"
#include "ddd.h"
#include "ddl.h"
#include "state.h"
#include "logger.h"
#include "commit.h"
#include "messaging.h"

#include "compat.h"

typedef enum
{
	MTM_STATE_LOCK_ID
}			MtmLockIds;

typedef struct
{
	TimestampTz last_timestamp;
	slock_t		mutex;
} MtmTime;

static MtmTime *mtm_time;

#define MTM_SHMEM_SIZE (8*1024*1024)

void		_PG_init(void);
void		_PG_fini(void);

PG_MODULE_MAGIC;

PG_FUNCTION_INFO_V1(mtm_after_node_create);
PG_FUNCTION_INFO_V1(mtm_after_node_drop);
PG_FUNCTION_INFO_V1(mtm_join_node);
PG_FUNCTION_INFO_V1(mtm_init_cluster);
PG_FUNCTION_INFO_V1(mtm_get_bgwpool_stat);

static size_t MtmGetTransactionStateSize(void);
static void MtmSerializeTransactionState(void *ctx);
static void MtmDeserializeTransactionState(void *ctx);
static void *MtmCreateSavepointContext(void);
static void MtmRestoreSavepointContext(void *ctx);
static void MtmReleaseSavepointContext(void *ctx);
static void *MtmSuspendTransaction(void);
static void MtmResumeTransaction(void *ctx);

static void MtmShmemStartup(void);

static void launcher_init(void);
void		launcher_main(Datum main_arg);
void		drop_node_entry(int node_id);

MtmShared  *Mtm;

MemoryContext MtmApplyContext;

/*  XXX: do we really need all this guys (except ddd)? */
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

/*  XXX */
bool		MtmBackgroundWorker;
int			MtmReplicationNodeId;

/*
 * Maximal size of transaction after which transaction is written to the disk.
 * Also defines bgwpool shared queue size (See BgwPoolStart routine).
 */
int			MtmTransSpillThreshold;

int			MtmHeartbeatSendTimeout;
int			MtmHeartbeatRecvTimeout;
char	   *MtmRefereeConnStr;
bool		MtmBreakConnection;


static shmem_startup_hook_type PreviousShmemStartupHook;


int
popcount(nodemask_t mask)
{
	int			i,
				count = 0;

	for (i = 0; i < MTM_MAX_NODES; i++)
	{
		if (BIT_CHECK(mask, i))
			count++;
	}
	return count;
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

void
MtmSleep(int64 usec)
{
	TimestampTz waketm = GetCurrentTimestamp() + usec;

	for (;;)
	{
		int			rc;
		TimestampTz sleepfor;

		CHECK_FOR_INTERRUPTS();

		sleepfor = waketm - GetCurrentTimestamp();
		if (sleepfor < 0)
			break;

		rc = WaitLatch(MyLatch,
					   WL_TIMEOUT | WL_POSTMASTER_DEATH,
					   sleepfor / 1000.0, WAIT_EVENT_BGWORKER_STARTUP);

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
MtmSerializeTransactionState(void *ctx)
{
	memcpy(ctx, &MtmTx, sizeof(MtmTx));
}

static void
MtmDeserializeTransactionState(void *ctx)
{
	memcpy(&MtmTx, ctx, sizeof(MtmTx));
}

static void *
MtmCreateSavepointContext(void)
{
	return NULL;
}

static void
MtmRestoreSavepointContext(void *ctx)
{
}

static void
MtmReleaseSavepointContext(void *ctx)
{
}

static void *
MtmSuspendTransaction(void)
{
	MtmCurrentTrans *ctx = malloc(sizeof(MtmCurrentTrans));

	*ctx = MtmTx;
	MtmBeginTransaction();
	return ctx;
}

static void
MtmResumeTransaction(void *ctx)
{
	MtmTx = *(MtmCurrentTrans *) ctx;
	free(ctx);
}

/*
 * Perform initialization of multimaster state.
 * This function is called from shared memory startup hook (after completion of initialization of shared memory)
 */
static void
MtmSharedShmemStartup()
{
	bool		found;
	int			i;

	LWLockAcquire(AddinShmemInitLock, LW_EXCLUSIVE);

	mtm_time = (MtmTime *) ShmemInitStruct("mtm_time", sizeof(MtmTime), &found);
	if (!found)
	{
		mtm_time->last_timestamp = 0;
		SpinLockInit(&mtm_time->mutex);
	}

	Mtm = (MtmShared *) ShmemInitStruct(MULTIMASTER_NAME, sizeof(MtmShared), &found);
	if (!found)
	{
		MemSet(Mtm, 0, sizeof(MtmShared));
		Mtm->localTablesHashLoaded = false;
		Mtm->latestSyncpoint = InvalidXLogRecPtr;

		Mtm->lock = &(GetNamedLWLockTranche(MULTIMASTER_NAME)[0].lock);
		Mtm->syncpoint_lock = &(GetNamedLWLockTranche(MULTIMASTER_NAME)[1].lock);

		ConditionVariableInit(&Mtm->commit_barrier_cv);
		ConditionVariableInit(&Mtm->receiver_barrier_cv);

		for (i = 0; i < MTM_MAX_NODES; i++)
		{
			Mtm->peers[i].receiver_pid = InvalidPid;
			Mtm->peers[i].sender_pid = InvalidPid;
			Mtm->peers[i].dmq_dest_id = -1;
			Mtm->peers[i].trim_lsn = InvalidXLogRecPtr;
		}
	}

	RegisterXactCallback(MtmXactCallback, NULL);

	TM = &MtmTM;
	LWLockRelease(AddinShmemInitLock);
}

static void
MtmShmemStartup(void)
{
	if (PreviousShmemStartupHook)
		PreviousShmemStartupHook();

	// MtmDeadlockDetectorShmemStartup(MTM_MAX_NODES);
	MtmDDLReplicationShmemStartup();
	MtmStateShmemStartup();
	MtmSharedShmemStartup();
	MtmGlobalTxShmemStartup();
}

void
_PG_init(void)
{
	/*
	 * In order to create our shared memory area, we have to be loaded via
	 * shared_preload_libraries.  If not, fall out without hooking into any of
	 * the main system.	 (We don't throw error here because it seems useful to
	 * allow the cs_* functions to be created even when the module isn't
	 * active.	 The functions must protect themselves against being called
	 * then, however.)
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
							2000,
							1,
							INT_MAX,
							PGC_BACKEND,
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
							MaxAllocSize / 1024,
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

	/* XXX */
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

	DefineCustomStringVariable(
							   "multimaster.remote_functions",
							   "List of function names which should be executed remotely at all multimaster nodes instead of executing them at master and replicating result of their work",
							   NULL,
							   &MtmRemoteFunctionsList,
							   "lo_creat,lo_create,lo_unlink,lo_open,loread,lowrite,lo_lseek,lo_tell,lo_close,lo_export"
							   "pathman.create_single_range_partition,pathman.drop_partitions,pathman.create_hash_partitions,pathman.create_range_partitions,pathman.split_range_partition,pathman.drop_range_partition,pathman.add_range_partition,pathman.attach_range_partition,pathman.detach_range_partition,pathman.prepend_range_partition,pathman.append_range_partition,pathman.set_auto,pathman.set_enable_parent,pathman.merge_range_partitions,pathman.add_to_pathman_config,pathman.set_spawn_using_bgw,pathman.set_init_callback,pathman.set_interval,pathman.partition_table_concurrently,"
							   "public.create_single_range_partition,public.drop_partitions,public.create_hash_partitions,public.create_range_partitions,public.split_range_partition,public.drop_range_partition,public.add_range_partition,public.attach_range_partition,public.detach_range_partition,public.prepend_range_partition,public.append_range_partition,public.set_auto,public.set_enable_parent,public.merge_range_partitions,public.add_to_pathman_config,public.set_spawn_using_bgw,public.set_init_callback,public.set_interval,public.partition_table_concurrently"
							   ,
							   PGC_USERSET, /* context */
							   GUC_LIST_INPUT,	/* flags */
							   NULL,	/* GucStringCheckHook check_hook */
							   MtmSetRemoteFunction,	/* GucStringAssignHook
														 * assign_hook */
							   NULL /* GucShowHook show_hook */
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

	// MtmDeadlockDetectorInit(MTM_MAX_NODES);

	/*
	 * Request additional shared resources.	 (These are no-ops if we're not in
	 * the postmaster process.)	 We'll allocate or attach to the shared
	 * resources in mtm_shmem_startup().
	 */
	RequestAddinShmemSpace(MTM_SHMEM_SIZE + sizeof(MtmTime));
	RequestNamedLWLockTranche(MULTIMASTER_NAME, 2);

	dmq_init(MtmHeartbeatSendTimeout);
	dmq_receiver_start_hook = MtmOnNodeConnect;
	dmq_receiver_stop_hook = MtmOnNodeDisconnect;
	dmq_sender_connect_hook = MtmOnDmqSenderConnect;
	dmq_sender_disconnect_hook = MtmOnDmqSenderDisconnect;

	ResolverInit();

	MtmDDLReplicationInit();

	launcher_init();

	MtmStateInit();
	MtmGlobalTxInit();

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

/*
 * Publication named 'multimaster' acts as a flag that that multimaster
 * extension was created and configured, so we can hijack transactions.
 * We can't hijack transactions before configuration is done because
 * configuration itself is going to need some transactions that better not
 * be aborted because of Mtm->status being DISABLED. Also publication
 * is uniq with respect to (db_id, pub_name) so "All your base are belong
 * to us" won't happen.
 */
bool
MtmIsEnabled()
{
	return OidIsValid(get_publication_oid(MULTIMASTER_NAME, true));
}

/*  XXX: delete that */
bool
MtmAllApplyWorkersFinished()
{
	int			i;

	for (i = 0; i < MTM_MAX_NODES; i++)
	{
		volatile int ntasks;

		LWLockAcquire(&Mtm->pools[i].lock, LW_SHARED);
		if (Mtm->pools[i].nWorkers <= 0 || i == Mtm->my_node_id - 1)
		{
			LWLockRelease(&Mtm->pools[i].lock);
			continue;
		}

		ntasks = Mtm->pools[i].active + Mtm->pools[i].pending;
		LWLockRelease(&Mtm->pools[i].lock);

		mtm_log(MtmApplyBgwFinish, "MtmAllApplyWorkersFinished %d tasks not finished", ntasks);

		if (ntasks != 0)
			return false;
	}

	return true;
}

/*****************************************************************************
 *
 * Node management stuff.
 *
 *****************************************************************************/

/*
 * Check correctness of multimaster configuration
 */
static bool
check_config(int node_id)
{
	bool		ok = true;
	int			workers_required;

	if (max_prepared_xacts < 1)
	{
		mtm_log(WARNING,
				"multimaster requires max_prepared_transactions > 0, "
				"because all transactions are implicitly two-phase");
		ok = false;
	}

	if (node_id <= 0 || node_id > MTM_MAX_NODES)
	{
		mtm_log(WARNING,
				"node_id should be in range from 1 to %d, but %d is given",
				MTM_MAX_NODES, node_id);
		ok = false;
	}

	workers_required = 2 * node_id + 1;
	if (max_worker_processes < workers_required)
	{
		mtm_log(WARNING,
				"multimaster requires max_worker_processes >= %d",
				workers_required);
		ok = false;
	}

	if (wal_level != WAL_LEVEL_LOGICAL)
	{
		mtm_log(WARNING,
				"multimaster requires wal_level = 'logical', "
				"because it is build on top of logical replication");
		ok = false;
	}

	if (max_wal_senders < node_id)
	{
		mtm_log(WARNING,
				"multimaster requires max_wal_senders >= %d (multimaster.max_nodes), ",
				node_id);
		ok = false;
	}

	if (max_replication_slots < node_id)
	{
		mtm_log(WARNING,
				"multimaster requires max_replication_slots >= %d (multimaster.max_nodes), ",
				node_id);
		ok = false;
	}

	return ok;
}

/*  XXX: check also that nodes are different? */
/*  XXX: add connection check to after create triggers? */
Datum
mtm_init_cluster(PG_FUNCTION_ARGS)
{
	char	   *my_conninfo = text_to_cstring(PG_GETARG_TEXT_P(0));
	ArrayType  *peers_arr = PG_GETARG_ARRAYTYPE_P(1);
	Datum	   *peers_datums;
	bool	   *peers_nulls;
	int			i,
				rc,
				n_peers;
	PGconn	  **peer_conns;
	StringInfoData local_query;

	/* parse array with peer connstrings */
	Assert(ARR_ELEMTYPE(peers_arr) == TEXTOID);
	Assert(ARR_NDIM(peers_arr) == 1);
	deconstruct_array(peers_arr,
					  TEXTOID, -1, false, 'i',
					  &peers_datums, &peers_nulls, &n_peers);

	if (n_peers + 1 >= MTM_MAX_NODES)
		mtm_log(ERROR,
				"multimaster allows at maximum %d nodes, but %d was given",
				MTM_MAX_NODES, n_peers + 1);

	if (n_peers < 1)
		mtm_log(ERROR, "at least one peer node should be specified");

	peer_conns = (PGconn **) palloc0(n_peers * sizeof(PGconn *));

	/* open connections to peers and create multimaster extension */
	for (i = 0; i < n_peers; i++)
	{
		PGresult   *res;
		char	   *conninfo = TextDatumGetCString(peers_datums[i]);
		StringInfoData query;
		int			j;

		/* connect */
		peer_conns[i] = PQconnectdb(conninfo);
		if (PQstatus(peer_conns[i]) != CONNECTION_OK)
		{
			char	   *msg = pchomp(PQerrorMessage(peer_conns[i]));

			PQfinish(peer_conns[i]);
			mtm_log(ERROR,
					"Could not connect to a node '%s' from this node: %s",
					conninfo, msg);
		}

		/* create extension */
		res = PQexec(peer_conns[i], "create extension if not exists multimaster");
		if (PQresultStatus(res) != PGRES_COMMAND_OK)
		{
			char	   *msg = pchomp(PQerrorMessage(peer_conns[i]));

			PQclear(res);
			PQfinish(peer_conns[i]);
			mtm_log(ERROR,
					"Failed to create multimaster extension on '%s': %s",
					conninfo, msg);
		}

		/* construct table contents for this node */
		initStringInfo(&query);
		appendStringInfoString(&query, "insert into " MTM_NODES " values ");
		for (j = 0; j < n_peers; j++)
		{
			appendStringInfo(&query, "(%d, $$%s$$, '%c', 'f'), ",
							 j + 2, TextDatumGetCString(peers_datums[j]),
							 j == i ? 't' : 'f');
		}
		appendStringInfo(&query, "(%d, $$%s$$, '%c', 'f')",
						 1, my_conninfo, 'f');

		res = PQexec(peer_conns[i], query.data);
		if (PQresultStatus(res) != PGRES_COMMAND_OK)
		{
			char	   *msg = pchomp(PQerrorMessage(peer_conns[i]));

			PQclear(res);
			PQfinish(peer_conns[i]);
			mtm_log(ERROR, "Failed to fill mtm.cluster_nodes on '%s': %s",
					conninfo, msg);
		}
		PQclear(res);

		PQfinish(peer_conns[i]);
	}

	/* fill our mtm.cluster_nodes */
	initStringInfo(&local_query);
	appendStringInfoString(&local_query, "insert into " MTM_NODES " values ");
	for (i = 0; i < n_peers; i++)
	{
		appendStringInfo(&local_query, "(%d, $$%s$$, 'f', 'f'), ",
						 i + 2, TextDatumGetCString(peers_datums[i]));
	}
	appendStringInfo(&local_query, "(%d, $$%s$$, 't', 'f')",
					 1, my_conninfo);

	if (SPI_connect() != SPI_OK_CONNECT)
		mtm_log(ERROR, "could not connect using SPI");
	PushActiveSnapshot(GetTransactionSnapshot());

	rc = SPI_execute(local_query.data, false, 0);
	if (rc != SPI_OK_INSERT)
		mtm_log(ERROR, "Failed to load node list");

	if (SPI_finish() != SPI_OK_FINISH)
		mtm_log(ERROR, "could not finish SPI");
	PopActiveSnapshot();

	PG_RETURN_VOID();
}

Datum
mtm_after_node_create(PG_FUNCTION_ARGS)
{
	CreatePublicationStmt *pub_stmt = makeNode(CreatePublicationStmt);
	TriggerData *trigdata = (TriggerData *) fcinfo->context;
	int			node_id;
	bool		node_id_isnull;
	bool		is_self;
	bool		is_self_isnull;
	char	   *conninfo;
	bool		conninfo_isnull;

	Assert(CALLED_AS_TRIGGER(fcinfo));
	Assert(TRIGGER_FIRED_FOR_ROW(trigdata->tg_event));
	Assert(TRIGGER_FIRED_BY_INSERT(trigdata->tg_event));

	node_id = DatumGetInt32(heap_getattr(trigdata->tg_trigtuple,
										 Anum_mtm_nodes_id,
										 RelationGetDescr(trigdata->tg_relation),
										 &node_id_isnull));
	Assert(!node_id_isnull);

	conninfo = text_to_cstring(DatumGetTextP(heap_getattr(trigdata->tg_trigtuple,
														  Anum_mtm_nodes_connifo,
														  RelationGetDescr(trigdata->tg_relation),
														  &conninfo_isnull)));

	is_self = DatumGetBool(heap_getattr(trigdata->tg_trigtuple,
										Anum_mtm_nodes_is_self,
										RelationGetDescr(trigdata->tg_relation),
										&is_self_isnull));
	Assert(!is_self_isnull);

	if (!check_config(node_id))
		mtm_log(ERROR, "multimaster can't start with current configs");

	mtm_log(NodeMgmt, "Creating node%d", node_id);

	if (is_self)
	{
		/*
		 * Create dummy pub. It will be used by backends to check whether
		 * multimaster is configured.
		 */
		pub_stmt->pubname = MULTIMASTER_NAME;
		pub_stmt->for_all_tables = false;
		pub_stmt->tables = NIL;
		pub_stmt->options = list_make1(
									   makeDefElem("publish", (Node *) makeString(pstrdup("insert, truncate")), -1)
			);
		CreatePublication(pub_stmt);

		/* liftoff */
		MtmMonitorStart(MyDatabaseId, GetUserId());
	}
	else
	{
		CreateSubscriptionStmt *cs_stmt = makeNode(CreateSubscriptionStmt);
		char	   *origin_name;
		int			saved_client_min_messages = client_min_messages;
		int			saved_log_min_messages = log_min_messages;

		Assert(!conninfo_isnull);

		/*
		 * Dummy subscription. It is used by launcher to start workers in
		 * databases where multimaster is configured (pg_subscription is
		 * shared catalog relation, so launcher can find it from postgres
		 * database). Also our workers and backends are subscribed to cache
		 * invalidations of pg_publication, so that can know aboun node
		 * creating/deletion.
		 */
		cs_stmt->subname = psprintf(MTM_SUBNAME_FMT, node_id);
		cs_stmt->conninfo = conninfo;
		cs_stmt->publication = list_make1(makeString(MULTIMASTER_NAME));
		cs_stmt->options = list_make4(
									  makeDefElem("slot_name", (Node *) makeString("none"), -1),
									  makeDefElem("create_slot", (Node *) makeString("false"), -1),
									  makeDefElem("connect", (Node *) makeString("false"), -1),
									  makeDefElem("enabled", (Node *) makeString("false"), -1)
			);

		/*
		 * supress unecessary and scary warning ('tables were not subscribed
		 * ..')
		 */
		client_min_messages = ERROR;
		log_min_messages = ERROR;

		CreateSubscription(cs_stmt, true);

		/* restore log_level's */
		client_min_messages = saved_client_min_messages;
		log_min_messages = saved_log_min_messages;

		/*
		 * Create origin for this neighbour. It is tempting to use
		 * 'pg_#{suboid}' but accessing syscache in MtmLoadConfig() will lead
		 * to deadlock if receiver tries to load config just before committing
		 * tx that modified subscriptions.
		 *
		 * Other way around is to write suboid to mtm.cluster_nodes tuple, but
		 * that is too much ado for now.
		 */
		origin_name = psprintf(MULTIMASTER_SLOT_PATTERN, node_id);
		replorigin_create(origin_name);
	}

	PG_RETURN_VOID();
}

Datum
mtm_after_node_drop(PG_FUNCTION_ARGS)
{
	TriggerData *trigdata = (TriggerData *) fcinfo->context;
	int			node_id;
	bool		node_id_isnull;
	bool		is_self;
	bool		is_self_isnull;

	Assert(CALLED_AS_TRIGGER(fcinfo));
	Assert(TRIGGER_FIRED_FOR_ROW(trigdata->tg_event));
	Assert(TRIGGER_FIRED_BY_DELETE(trigdata->tg_event));

	node_id = DatumGetInt32(heap_getattr(trigdata->tg_trigtuple,
										 Anum_mtm_nodes_id,
										 RelationGetDescr(trigdata->tg_relation),
										 &node_id_isnull));
	Assert(!node_id_isnull);
	Assert(node_id > 0 && node_id <= MTM_MAX_NODES);

	is_self = DatumGetBool(heap_getattr(trigdata->tg_trigtuple,
										Anum_mtm_nodes_is_self,
										RelationGetDescr(trigdata->tg_relation),
										&is_self_isnull));
	Assert(!is_self_isnull);

	mtm_log(NodeMgmt, "Dropping node%d", node_id);

	/*
	 * This will produce invalidation that others can consume and reload
	 * state.
	 */
	if (is_self)
	{
		DropStmt   *dp_stmt = makeNode(DropStmt);

		dp_stmt->removeType = OBJECT_PUBLICATION;
		dp_stmt->behavior = DROP_CASCADE;
		dp_stmt->concurrent = false;
		dp_stmt->missing_ok = false;
		dp_stmt->objects = list_make1(makeString(MULTIMASTER_NAME));
		RemoveObjects(dp_stmt);
	}
	else
	{
		DropSubscriptionStmt *ds_stmt = makeNode(DropSubscriptionStmt);

		ds_stmt->subname = psprintf(MTM_SUBNAME_FMT, node_id);
		DropSubscription(ds_stmt, true);
	}

	PG_RETURN_VOID();
}

/*
 * XXX: In my opinion this interface need to be revised:
 * manually specified node_id and end_lsn are a source of problems.
 */
Datum
mtm_join_node(PG_FUNCTION_ARGS)
{
	int			rc;
	int			new_node_id = PG_GETARG_INT32(0);
	XLogRecPtr	end_lsn = PG_GETARG_LSN(1);
	char	   *query;
	char	   *mtm_nodes;
	char	   *conninfo;
	PGconn	   *conn;
	PGresult   *res;
	MtmConfig  *cfg = MtmLoadConfig();
	XLogRecPtr	curr_lsn;
	int			i;
	MtmNode    *new_node;

	if (SPI_connect() != SPI_OK_CONNECT)
		mtm_log(ERROR, "could not connect using SPI");
	PushActiveSnapshot(GetTransactionSnapshot());

	/* serialize our mtm.cluster_nodes table into string */
	query = psprintf("select array_agg((n.id, n.conninfo, n.id = %d, 'f')::" MTM_NODES ") "
					 "from " MTM_NODES " n", new_node_id);
	rc = SPI_execute(query, true, 0);
	if (rc != SPI_OK_SELECT || SPI_processed != 1)
		mtm_log(ERROR, "Failed to load node list");
	mtm_nodes = SPI_getvalue(SPI_tuptable->vals[0], SPI_tuptable->tupdesc, 1);

	/* connect to a new node */
	new_node = MtmNodeById(cfg, new_node_id);
	if (new_node == NULL)
		mtm_log(ERROR, "Node %d not found", new_node_id);
	conninfo = new_node->conninfo;
	conn = PQconnectdb(conninfo);
	if (PQstatus(conn) != CONNECTION_OK)
	{
		char	   *msg = pchomp(PQerrorMessage(conn));

		PQfinish(conn);
		mtm_log(ERROR, "Could not connect to a node '%s': %s", conninfo, msg);
	}

	/* save info about basebackup */
	res = PQexec(conn, psprintf("insert into mtm.config values('basebackup', "
								"'{\"node_id\": %d,\"end_lsn\": " UINT64_FORMAT "}')",
								cfg->my_node_id, end_lsn));
	if (PQresultStatus(res) != PGRES_COMMAND_OK)
	{
		char	   *msg = pchomp(PQerrorMessage(conn));

		PQclear(res);
		PQfinish(conn);
		mtm_log(ERROR, "Can't fill mtm.config on '%s': %s", conninfo, msg);
	}
	PQclear(res);

	res = PQexec(conn, psprintf("insert into mtm.syncpoints values "
								"(%d, " UINT64_FORMAT ", pg_current_wal_lsn()::bigint, pg_current_wal_lsn()::bigint)",
								cfg->my_node_id, end_lsn));
	if (PQresultStatus(res) != PGRES_COMMAND_OK)
	{
		char	   *msg = pchomp(PQerrorMessage(conn));

		PQclear(res);
		PQfinish(conn);
		mtm_log(ERROR, "Can't fill mtm.syncpoints on '%s': %s", conninfo, msg);
	}
	PQclear(res);

	/*
	 * Create syncpoints for all our peers so that new node can safely start
	 * recovered replication connections.
	 */

	/* Hold all apply workers */
	for (i = 0; i < cfg->n_nodes; i++)
	{
		int			node_id = cfg->nodes[i].node_id;

		if (node_id == cfg->my_node_id)
			continue;

		LWLockAcquire(&Mtm->pools[node_id - 1].lock, LW_EXCLUSIVE);
		Mtm->pools[node_id - 1].n_holders += 1;
		LWLockRelease(&Mtm->pools[node_id - 1].lock);
	}

	/* Await for workers finish and create syncpoints */
	PG_TRY();
	{

		while (!MtmAllApplyWorkersFinished())
			MtmSleep(USECS_PER_SEC / 10);

		curr_lsn = GetXLogWriteRecPtr();

		for (i = 0; i < cfg->n_nodes; i++)
		{
			char	   *ro_name;
			RepOriginId ro_id;
			XLogRecPtr	olsn;
			char	   *msg;

			if (cfg->nodes[i].node_id == new_node_id)
				continue;

			ro_name = psprintf(MULTIMASTER_SLOT_PATTERN, cfg->nodes[i].node_id);
			ro_id = replorigin_by_name(ro_name, false);
			olsn = replorigin_get_progress(ro_id, false);

			msg = psprintf("F_%d_" UINT64_FORMAT "_" UINT64_FORMAT,
						   cfg->nodes[i].node_id, olsn, (XLogRecPtr) InvalidXLogRecPtr);

			replorigin_session_origin = cfg->nodes[i].origin_id;
			LogLogicalMessage("S", msg, strlen(msg) + 1, false);
			replorigin_session_origin = InvalidRepOriginId;
		}

		{
			char	   *msg = psprintf(UINT64_FORMAT, (XLogRecPtr) InvalidXLogRecPtr);

			LogLogicalMessage("S", msg, strlen(msg) + 1, false);
		}

	}
	PG_CATCH();
	{
		for (i = 0; i < cfg->n_nodes; i++)
		{
			int			node_id = cfg->nodes[i].node_id;

			if (node_id == cfg->my_node_id)
				continue;


			LWLockAcquire(&Mtm->pools[node_id - 1].lock, LW_EXCLUSIVE);
			Mtm->pools[node_id - 1].n_holders -= 1;
			LWLockRelease(&Mtm->pools[node_id - 1].lock);
		}
		ConditionVariableBroadcast(&Mtm->receiver_barrier_cv);
		PG_RE_THROW();
	}
	PG_END_TRY();

	/* release receiver holds */
	for (i = 0; i < cfg->n_nodes; i++)
	{
		int			node_id = cfg->nodes[i].node_id;

		if (node_id == cfg->my_node_id)
			continue;

		LWLockAcquire(&Mtm->pools[node_id - 1].lock, LW_EXCLUSIVE);
		Mtm->pools[node_id - 1].n_holders -= 1;
		LWLockRelease(&Mtm->pools[node_id - 1].lock);
	}
	ConditionVariableBroadcast(&Mtm->receiver_barrier_cv);

	/* as we going to write that lsn on a new node, let's sync it */
	XLogFlush(curr_lsn);

	/* fill MTM_NODES with a adjusted list of nodes */
	query = psprintf("insert into " MTM_NODES
					 " select * from unnest($$%s$$::" MTM_NODES "[]);",
					 mtm_nodes);
	res = PQexec(conn, query);
	if (PQresultStatus(res) != PGRES_COMMAND_OK)
	{
		char	   *msg = pchomp(PQerrorMessage(conn));

		PQclear(res);
		PQfinish(conn);
		mtm_log(ERROR, "Failed to insert new mtm.cluster_nodes on '%s': %s",
				conninfo, msg);
	}
	PQclear(res);

	/* call mtm.alter_sequences since n_nodes is changed */
	query = psprintf("select mtm.alter_sequences()");
	rc = SPI_execute(query, false, 0);
	if (rc != SPI_OK_SELECT)
		mtm_log(ERROR, "Failed to alter sequences");

	pfree(cfg);
	PQfinish(conn);
	if (SPI_finish() != SPI_OK_FINISH)
		mtm_log(ERROR, "could not finish SPI");
	PopActiveSnapshot();

	PG_RETURN_VOID();
}

/*
 * Load mtm config.
 *
 * In case of absense of configured nodes will return cfg->n_nodes = 0.
 */
MtmConfig *
MtmLoadConfig()
{
	MtmConfig  *cfg;
	int			rc;
	int			i;
	bool		inside_tx = IsTransactionState();
	Oid			save_userid;
	int			save_sec_context;

	cfg = (MtmConfig *) MemoryContextAllocZero(TopMemoryContext,
											   sizeof(MtmConfig));

	if (!inside_tx)
		StartTransactionCommand();

	/*
	 * Escalate our privileges, as current user may not have rights to access
	 * mtm schema.
	 */
	GetUserIdAndSecContext(&save_userid, &save_sec_context);
	SetUserIdAndSecContext(BOOTSTRAP_SUPERUSERID,
						   save_sec_context | SECURITY_LOCAL_USERID_CHANGE);

	/* Load node_id's with connection strings from mtm.cluster_nodes */

	if (SPI_connect() != SPI_OK_CONNECT)
		mtm_log(ERROR, "could not connect using SPI");

	PushActiveSnapshot(GetTransactionSnapshot());

	rc = SPI_execute("select * from " MTM_NODES " order by id asc", true, 0);
	if (rc < 0 || rc != SPI_OK_SELECT)
		mtm_log(ERROR, "Failed to load saved nodes");

	Assert(SPI_processed <= MTM_MAX_NODES);

	cfg->n_nodes = 0;
	cfg->my_node_id = 0;

	for (i = 0; i < SPI_processed; i++)
	{
		TupleDesc	tupdesc = SPI_tuptable->tupdesc;
		HeapTuple	tup = SPI_tuptable->vals[i];
		bool		isnull;
		int			node_id;
		char	   *connstr;
		bool		is_self;
		bool		init_done;

		node_id = DatumGetInt32(SPI_getbinval(tup, tupdesc, Anum_mtm_nodes_id, &isnull));
		Assert(!isnull);
		Assert(TupleDescAttr(tupdesc, Anum_mtm_nodes_id - 1)->atttypid == INT4OID);

		connstr = SPI_getvalue(tup, tupdesc, Anum_mtm_nodes_connifo);
		Assert(TupleDescAttr(tupdesc, Anum_mtm_nodes_connifo - 1)->atttypid == TEXTOID);

		is_self = DatumGetBool(SPI_getbinval(tup, tupdesc, Anum_mtm_nodes_is_self, &isnull));
		Assert(!isnull);
		Assert(TupleDescAttr(tupdesc, Anum_mtm_nodes_is_self - 1)->atttypid == BOOLOID);

		init_done = DatumGetBool(SPI_getbinval(tup, tupdesc, Anum_mtm_nodes_init_done, &isnull));
		Assert(!isnull);
		Assert(TupleDescAttr(tupdesc, Anum_mtm_nodes_init_done - 1)->atttypid == BOOLOID);

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
			cfg->nodes[cfg->n_nodes].conninfo = MemoryContextStrdup(TopMemoryContext, connstr);
			cfg->nodes[cfg->n_nodes].node_id = node_id;
			cfg->nodes[cfg->n_nodes].init_done = init_done;
			Assert(cfg->nodes[cfg->n_nodes].conninfo != NULL);
			cfg->n_nodes++;
		}
	}

	/* Load origin_id's */
	for (i = 0; i < cfg->n_nodes; i++)
	{
		char	   *origin_name;
		RepOriginId origin_id;
		int			node_id = cfg->nodes[i].node_id;

		origin_name = psprintf(MULTIMASTER_SLOT_PATTERN, node_id);
		origin_id = replorigin_by_name(origin_name, true);

		/* Assert(origin_id != InvalidRepOriginId || MtmIsMonitorWorker); */

		cfg->nodes[i].origin_id = origin_id;
	}

	/* load basebackup info if any */
	rc = SPI_execute("select (value->>'node_id')::int, (value->>'end_lsn')::bigint"
					 " from mtm.config where key='basebackup';", true, 0);
	if (rc < 0 || rc != SPI_OK_SELECT)
	{
		mtm_log(ERROR, "Failed to check basebackup key");
	}
	else if (SPI_processed > 0)
	{
		TupleDesc	tupdesc = SPI_tuptable->tupdesc;
		HeapTuple	tup = SPI_tuptable->vals[0];
		bool		isnull;

		Assert(SPI_processed == 1);

		cfg->backup_node_id = DatumGetInt32(SPI_getbinval(tup, tupdesc, 1, &isnull));
		Assert(!isnull);
		Assert(TupleDescAttr(tupdesc, 0)->atttypid == INT4OID);

		cfg->backup_end_lsn = DatumGetInt64(SPI_getbinval(tup, tupdesc, 2, &isnull));
		Assert(!isnull);
		Assert(TupleDescAttr(tupdesc, 1)->atttypid == INT8OID);
	}

	SPI_finish();
	PopActiveSnapshot();

	/* restore back current user context */
	SetUserIdAndSecContext(save_userid, save_sec_context);

	if (!inside_tx)
		CommitTransactionCommand();

	return cfg;
}

MtmConfig *
MtmReloadConfig(MtmConfig *old_cfg, mtm_cfg_change_cb node_add_cb,
				mtm_cfg_change_cb node_drop_cb, Datum arg)
{
	MtmConfig  *new_cfg;
	Bitmapset  *old_bms = NULL,
			   *new_bms = NULL,
			   *deleted,
			   *created;
	int			i,
				node_id;

	new_cfg = MtmLoadConfig();

	/* Set proper values in Mtm structure */
	if (new_cfg->my_node_id != 0)
		MtmStateFill(new_cfg);

	/*
	 * Construct bitmapsets from old and new mtm_config's and find out whether
	 * some nodes were added or deleted.
	 */
	if (old_cfg != NULL)
	{
		for (i = 0; i < old_cfg->n_nodes; i++)
			old_bms = bms_add_member(old_bms, old_cfg->nodes[i].node_id);
	}
	for (i = 0; i < new_cfg->n_nodes; i++)
		new_bms = bms_add_member(new_bms, new_cfg->nodes[i].node_id);

	deleted = bms_difference(old_bms, new_bms);
	created = bms_difference(new_bms, old_bms);

	/*
	 * Call launch/stop callbacks for added/deleted nodes.
	 */
	if (node_add_cb)
	{
		/*
		 * After a basebackup we should first run receiver from backup source
		 * node to start commiting prepared transaction to be able to finish
		 * logical slot creation (which wait for all currently running
		 * transactions to finish).
		 */
		if (new_cfg->backup_node_id > 0)
		{
			node_add_cb(new_cfg->backup_node_id, new_cfg, arg);
			bms_del_member(created, new_cfg->backup_node_id);
		}

		node_id = -1;
		while ((node_id = bms_next_member(created, node_id)) >= 0)
			node_add_cb(node_id, new_cfg, arg);
	}

	if (node_drop_cb)
	{
		node_id = -1;
		while ((node_id = bms_next_member(deleted, node_id)) >= 0)
			node_drop_cb(node_id, new_cfg, arg);
	}

	if (old_cfg != NULL)
		pfree(old_cfg);

	return new_cfg;
}

/* Helper to find node with specified id in cfg->nodes */
/*  XXX: add missing ok */
MtmNode *
MtmNodeById(MtmConfig *cfg, int node_id)
{
	int			i;

	for (i = 0; i < cfg->n_nodes; i++)
	{
		if (cfg->nodes[i].node_id == node_id)
			return &(cfg->nodes[i]);
	}

	return NULL;
}

/*****************************************************************************
 *
 * Launcher worker.
 *
 * During node boot searches for configured multimasters inspecting
 * pg_subscription and starts mtm-monitor.
 *
 *****************************************************************************/

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
	HASHCTL		hash_info;
	HTAB	   *already_started;

	/* init this worker */
	pqsignal(SIGTERM, die);
	BackgroundWorkerUnblockSignals();

	memset(&hash_info, 0, sizeof(hash_info));
	hash_info.entrysize = hash_info.keysize = sizeof(Oid);
	already_started = hash_create("already_started", 16, &hash_info,
								  HASH_ELEM | HASH_BLOBS);

	/* Connect to NULL db to access shared catalogs */
	BackgroundWorkerInitializeConnection(NULL, NULL, 0);

	/*
	 * Start a transaction so we can access pg_subscription, and get a
	 * snapshot. We don't have a use for the snapshot itself, but we're
	 * interested in the secondary effect that it sets RecentGlobalXmin.
	 * (This is critical for anything that reads heap pages, because HOT may
	 * decide to prune them even if the process doesn't attempt to modify any
	 * tuples.)
	 */
	StartTransactionCommand();
	(void) GetTransactionSnapshot();

	rel = heap_open(SubscriptionRelationId, AccessShareLock);
	scan = heap_beginscan_catalog(rel, 0, NULL);

	/* is there any mtm subscriptions in a given database? */
	while (HeapTupleIsValid(tup = heap_getnext(scan, ForwardScanDirection)))
	{
		Form_pg_subscription subform = (Form_pg_subscription) GETSTRUCT(tup);
		int			node_id;

		if (!subform->subenabled &&
			sscanf(NameStr(subform->subname), MTM_SUBNAME_FMT, &node_id) == 1 &&
			!hash_search(already_started, &subform->subdbid, HASH_FIND, NULL))
		{
			bool		found;

			MtmMonitorStart(subform->subdbid, subform->subowner);
			hash_search(already_started, &subform->subdbid, HASH_ENTER, &found);
			Assert(!found);
		}
	}

	heap_endscan(scan);
	heap_close(rel, AccessShareLock);

	CommitTransactionCommand();
}

#define BGWPOOL_STAT_COLS	(7)
Datum
mtm_get_bgwpool_stat(PG_FUNCTION_ARGS)
{
	TupleDesc	tupdesc;
	Tuplestorestate *tupstore;
	Datum		values[BGWPOOL_STAT_COLS];
	bool		nulls[BGWPOOL_STAT_COLS];
	int			i;

	ReturnSetInfo *rsinfo = (ReturnSetInfo *) fcinfo->resultinfo;
	MemoryContext per_query_ctx;
	MemoryContext oldcontext;

	/* Build a tuple descriptor for our result type */
	if (get_call_result_type(fcinfo, NULL, &tupdesc) != TYPEFUNC_COMPOSITE)
		elog(ERROR, "return type must be a row type");

	per_query_ctx = rsinfo->econtext->ecxt_per_query_memory;
	oldcontext = MemoryContextSwitchTo(per_query_ctx);

	tupstore = tuplestore_begin_heap(true, false, work_mem);
	rsinfo->returnMode = SFRM_Materialize;
	rsinfo->setResult = tupstore;
	rsinfo->setDesc = tupdesc;
	MemoryContextSwitchTo(oldcontext);

	for (i = 0; i < MTM_MAX_NODES; i++)
	{
		if (Mtm->pools[i].nWorkers == 0)
		{
			continue;
		}

		MemSet(values, 0, sizeof(values));
		MemSet(nulls, 0, sizeof(nulls));

		values[0] = Int32GetDatum(Mtm->pools[i].nWorkers);
		values[1] = Int32GetDatum(Mtm->pools[i].active);
		values[2] = Int32GetDatum(Mtm->pools[i].pending);
		values[3] = Int32GetDatum(Mtm->pools[i].size);
		values[4] = Int32GetDatum(Mtm->pools[i].head);
		values[5] = Int32GetDatum(Mtm->pools[i].tail);
		values[6] = CStringGetDatum(Mtm->pools[i].poolName);
		tuplestore_putvalues(tupstore, tupdesc, values, nulls);
	}

	/* clean up and return the tuplestore */
	tuplestore_donestoring(tupstore);

	return (Datum) 0;
}


/*****************************************************************************
 *
 * Network messages packing/unpacking.
 *
 *****************************************************************************/


StringInfo
MtmMesagePack(MtmMessage *anymsg)
{
	StringInfo	s = makeStringInfo();

	pq_sendbyte(s, anymsg->tag);

	switch (messageTag(anymsg))
	{
		case T_MtmTxResponse:
		{
			MtmTxResponse   *msg = (MtmTxResponse *) anymsg;

			pq_sendint32(s, msg->node_id);
			pq_sendbyte(s, msg->status);
			pq_sendint32(s, msg->term.ballot);
			pq_sendint32(s, msg->term.node_id);
			pq_sendint32(s, msg->errcode);
			pq_sendstring(s, msg->errmsg);
			break;
		}

		case T_MtmTxRequest:
		{
			MtmTxRequest   *msg = (MtmTxRequest *) anymsg;

			pq_sendbyte(s, msg->type);
			pq_sendint32(s, msg->term.ballot);
			pq_sendint32(s, msg->term.node_id);
			pq_sendstring(s, msg->gid);
			break;
		}

		case T_MtmTxStatusResponse:
		{
			MtmTxStatusResponse   *msg = (MtmTxStatusResponse *) anymsg;

			pq_sendint32(s, msg->node_id);
			pq_sendbyte(s, msg->status);
			pq_sendint32(s, msg->proposed.ballot);
			pq_sendint32(s, msg->proposed.node_id);
			pq_sendint32(s, msg->accepted.ballot);
			pq_sendint32(s, msg->accepted.node_id);
			pq_sendstring(s, msg->gid);
			break;
		}

		default:
			Assert(false);
	}

	return s;
}

MtmMessage *
MtmMesageUnpack(StringInfo s)
{
	MtmMessageTag msg_tag = pq_getmsgbyte(s);
	MtmMessage *anymsg;

	switch (msg_tag)
	{
		case T_MtmTxResponse:
		{
			MtmTxResponse   *msg = palloc0(sizeof(MtmTxResponse));

			msg->tag = msg_tag;
			msg->node_id = pq_getmsgint(s, 4);
			msg->status = pq_getmsgbyte(s);
			msg->term.ballot = pq_getmsgint(s, 4);
			msg->term.node_id = pq_getmsgint(s, 4);
			msg->errcode = pq_getmsgint(s, 4);
			msg->errmsg = pq_getmsgstring(s);

			anymsg = (MtmMessage *) msg;
			break;
		}

		case T_MtmTxRequest:
		{
			MtmTxRequest *msg = palloc0(sizeof(MtmTxRequest));

			msg->tag = msg_tag;
			msg->type = pq_getmsgbyte(s);
			msg->term.ballot = pq_getmsgint(s, 4);
			msg->term.node_id = pq_getmsgint(s, 4);
			msg->gid = pq_getmsgstring(s);

			anymsg = (MtmMessage *) msg;
			break;
		}

		case T_MtmTxStatusResponse:
		{
			MtmTxStatusResponse   *msg = palloc0(sizeof(MtmTxStatusResponse));

			msg->tag = msg_tag;
			msg->node_id = pq_getmsgint(s, 4);
			msg->status = pq_getmsgbyte(s);
			msg->proposed.ballot = pq_getmsgint(s, 4);
			msg->proposed.node_id = pq_getmsgint(s, 4);
			msg->accepted.ballot = pq_getmsgint(s, 4);
			msg->accepted.node_id = pq_getmsgint(s, 4);
			msg->gid = pq_getmsgstring(s);

			anymsg = (MtmMessage *) msg;
			break;
		}

		default:
			Assert(false);
	}

	return anymsg;
}
