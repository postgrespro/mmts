#include "postgres.h"
#include "fmgr.h"
#include "miscadmin.h"
#include "postmaster/postmaster.h"
#include "postmaster/bgworker.h"
#include "storage/s_lock.h"
#include "storage/spin.h"
#include "storage/proc.h"
#include "storage/pg_sema.h"
#include "storage/shmem.h"
#include "datatype/timestamp.h"
#include "utils/portal.h"
#include "tcop/pquery.h"
#include "utils/guc.h"
#include "tcop/tcopprot.h"

#include "bgwpool.h"
#include "multimaster.h"
#include "logger.h"

bool MtmIsPoolWorker;

bool MtmIsLogicalReceiver;
int  MtmMaxWorkers;

static BgwPool* MtmPool;

void BgwPoolDynamicWorkerMainLoop(Datum arg);

static void
BgwShutdownHandler(int sig)
{
	Assert(MtmPool != NULL);
	BgwPoolStop(MtmPool);

	/*
	 * set ProcDiePending for cases when we are waiting on latch somewhere
	 * deep inside our execute() function.
	 */
	die(sig);
}

static void
BgwPoolMainLoop(BgwPool* pool)
{
	int size;
	void* work;
	size_t payload = sizeof(MtmReceiverContext) + sizeof(size_t);
	MtmReceiverContext ctx;
	static PortalData fakePortal;

	mtm_log(BgwPoolEvent, "Start background worker %d, shutdown=%d", MyProcPid, pool->shutdown);

	MtmIsPoolWorker = true;

	// XXX: get rid of that
	MtmBackgroundWorker = true;
	MtmIsLogicalReceiver = true;
	MtmPool = pool;

	pqsignal(SIGINT, StatementCancelHandler);
	pqsignal(SIGQUIT, BgwShutdownHandler);
	pqsignal(SIGTERM, BgwShutdownHandler);
	pqsignal(SIGHUP, PostgresSigHupHandler);

	BackgroundWorkerUnblockSignals();
	BackgroundWorkerInitializeConnectionByOid(pool->db_id, pool->user_id, 0);
	ActivePortal = &fakePortal;
	ActivePortal->status = PORTAL_ACTIVE;
	ActivePortal->sourceText = "";

	receiver_mtm_cfg = MtmLoadConfig();

	while (true)
	{
		if (ConfigReloadPending)
		{
			ConfigReloadPending = false;
			ProcessConfigFile(PGC_SIGHUP);
		}

		PGSemaphoreLock(pool->available);

		// XXX: change to LWLock
		SpinLockAcquire(&pool->lock);
		if (pool->shutdown)
		{
			PGSemaphoreUnlock(pool->available);
			break;
		}
		size = * (int *) &pool->queue[pool->head];
		ctx  = * (MtmReceiverContext *) &pool->queue[pool->head + sizeof(size_t)];

		Assert(size < pool->size);
		work = palloc(size);
		pool->pending -= 1;
		pool->active += 1;
		if (pool->lastPeakTime == 0 && pool->active == pool->nWorkers && pool->pending != 0)
			pool->lastPeakTime = MtmGetSystemTime();

		if (pool->head + size + payload > pool->size)
		{
			memcpy(work, pool->queue, size);
			pool->head = INTALIGN(size);
		}
		else
		{
			memcpy(work, &pool->queue[pool->head + payload], size);
			pool->head += payload + INTALIGN(size);
		}

		if (pool->size == pool->head)
			pool->head = 0;

		if (pool->producerBlocked)
		{
			pool->producerBlocked = false;
			PGSemaphoreUnlock(pool->overflow);
			pool->lastPeakTime = 0;
		}

		SpinLockRelease(&pool->lock);

		/* Ignore cancel that arrived before we started current command */
		QueryCancelPending = false;

		pool->executor(work, size, &ctx);
		pfree(work);

		SpinLockAcquire(&pool->lock);
		pool->active -= 1;
		pool->lastPeakTime = 0;
		SpinLockRelease(&pool->lock);

		ConditionVariableBroadcast(&pool->syncpoint_cv);
	}

	SpinLockRelease(&pool->lock);
	mtm_log(BgwPoolEvent, "Shutdown background worker %d", MyProcPid);
}

// XXX: this is called during _PG_init because we need to allocate queue.
// Better to use DSM, so that can be done dynamically.
void
BgwPoolInit(BgwPool* pool, BgwPoolExecutor executor, size_t queueSize, size_t nWorkers)
{
	MtmPool = pool;

	pool->bgwhandles = (BackgroundWorkerHandle **) ShmemAlloc(MtmMaxWorkers * sizeof(BackgroundWorkerHandle *));
    pool->queue = (char*)ShmemAlloc(queueSize);
	if (pool->queue == NULL) { 
		elog(PANIC, "Failed to allocate memory for background workers pool: %zd bytes requested", queueSize);
	}
    pool->executor = executor;
	pool->available = PGSemaphoreCreate();
	pool->overflow = PGSemaphoreCreate();
	PGSemaphoreReset(pool->available);
	PGSemaphoreReset(pool->overflow);
    SpinLockInit(&pool->lock);
	ConditionVariableInit(&pool->syncpoint_cv);
	pool->shutdown = false;
    pool->producerBlocked = false;
    pool->head = 0;
    pool->tail = 0;
    pool->size = queueSize;
    pool->active = 0;
    pool->pending = 0;
	pool->nWorkers = nWorkers;
	pool->lastPeakTime = 0;
	pool->lastDynamicWorkerStartTime = 0;
}
 
timestamp_t BgwGetLastPeekTime(BgwPool* pool)
{
	return pool->lastPeakTime;
}

void BgwPoolDynamicWorkerMainLoop(Datum arg)
{
	BgwPoolMainLoop((BgwPool*)DatumGetPointer(arg));
}

void
BgwPoolStart(BgwPool* pool, char *poolName, Oid db_id, Oid user_id)
{
	strncpy(pool->poolName, poolName, MAX_NAME_LEN);
	pool->db_id = db_id;
	pool->user_id = user_id;
}

size_t BgwPoolGetQueueSize(BgwPool* pool)
{
	size_t used;
    SpinLockAcquire(&pool->lock);
	used = pool->head <= pool->tail ? pool->tail - pool->head : pool->size - pool->head + pool->tail;
    SpinLockRelease(&pool->lock);            
	return used;
}


static void BgwStartExtraWorker(BgwPool* pool)
{
	BackgroundWorker worker;
	BackgroundWorkerHandle* handle;

	if (pool->nWorkers >= MtmMaxWorkers)
		return;

	MemSet(&worker, 0, sizeof(BackgroundWorker));
	worker.bgw_flags = BGWORKER_SHMEM_ACCESS |  BGWORKER_BACKEND_DATABASE_CONNECTION;
	worker.bgw_start_time = BgWorkerStart_ConsistentState;
	worker.bgw_restart_time = MULTIMASTER_BGW_RESTART_TIMEOUT;
	worker.bgw_main_arg = PointerGetDatum(pool);
	sprintf(worker.bgw_library_name, "multimaster");
	sprintf(worker.bgw_function_name, "BgwPoolDynamicWorkerMainLoop");
	snprintf(worker.bgw_name, BGW_MAXLEN, "%s-dynworker-%d", pool->poolName, (int) pool->nWorkers + 1);

	pool->lastDynamicWorkerStartTime = MtmGetSystemTime();
	if (RegisterDynamicBackgroundWorker(&worker, &handle))
	{
		pool->bgwhandles[pool->nWorkers++] = handle;
	}
	else
	{
		elog(WARNING, "Failed to start dynamic background worker");
	}
}

void
BgwPoolExecute(BgwPool* pool, void* work, size_t size, MtmReceiverContext *ctx)
{
	size_t payload = sizeof(MtmReceiverContext) + sizeof(size_t);

	if (size + payload > pool->size)
	{
		/* 
		 * Size of work is larger than size of shared buffer: 
		 * run it immediately
		 */
		pool->executor(work, size, ctx);
		return;
	}
 
	SpinLockAcquire(&pool->lock);
	while (!pool->shutdown)
	{
		if ((pool->head <= pool->tail && pool->size - pool->tail < size + payload && pool->head < size)
			|| (pool->head > pool->tail && pool->head - pool->tail < size + payload))
		{
			if (pool->lastPeakTime == 0)
				pool->lastPeakTime = MtmGetSystemTime();

			pool->producerBlocked = true;
			SpinLockRelease(&pool->lock);
			PGSemaphoreLock(pool->overflow);
			SpinLockAcquire(&pool->lock);
		}
		else
		{
			pool->pending += 1;

			if (pool->active + pool->pending > pool->nWorkers)
				BgwStartExtraWorker(pool);

			if (pool->lastPeakTime == 0 && pool->active == pool->nWorkers && pool->pending != 0)
				pool->lastPeakTime = MtmGetSystemTime();

			*(int *)&pool->queue[pool->tail] = size;
			*(MtmReceiverContext *)&pool->queue[pool->tail + sizeof(size_t)] = *ctx;

			if (pool->size - pool->tail >= size + payload)
			{
				memcpy(&pool->queue[pool->tail + payload], work, size);
				pool->tail += payload + INTALIGN(size);
			}
			else
			{
				memcpy(pool->queue, work, size);
				pool->tail = INTALIGN(size);
			}

			if (pool->tail == pool->size)
				pool->tail = 0;

			PGSemaphoreUnlock(pool->available);
			break;
		}
	}
	SpinLockRelease(&pool->lock);
}

void BgwPoolStop(BgwPool* pool)
{
	pool->shutdown = true;
	PGSemaphoreUnlock(pool->available);
	PGSemaphoreUnlock(pool->overflow);
}

/*
 * Tell our lads to cancel currently active transactions.
 */
void
BgwPoolCancel(BgwPool* pool)
{
	int		i;

	for (i = 0; i < pool->nWorkers; i++)
	{
		BgwHandleStatus		status;
		pid_t				pid;

		status = GetBackgroundWorkerPid(pool->bgwhandles[i], &pid);
		if (status == BGWH_STARTED)
		{
			Assert(pid > 0);
			kill(pid, SIGINT);
		}
	}
}
