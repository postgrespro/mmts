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
#include "utils/syscache.h"
#include "utils/inval.h"
#include "utils/memutils.h"

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
subscription_change_cb(Datum arg, int cacheid, uint32 hashvalue)
{
	receiver_mtm_cfg_valid = false;
}


static void
BgwPoolMainLoop(BgwPool* pool)
{
	int size;
	void* work;
	int payload = INTALIGN(sizeof(MtmReceiverContext));
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

	// XXX: probably we should add static variable that signalizes that
	// we are between pool->active += 1 and pool->active -= 1, so if
	// we face an ERROR outside of PG_TRY we can decrement pool->active
	// from on_shem_exit_hook

	BackgroundWorkerUnblockSignals();
	BackgroundWorkerInitializeConnectionByOid(pool->db_id, pool->user_id, 0);
	ActivePortal = &fakePortal;
	ActivePortal->status = PORTAL_ACTIVE;
	ActivePortal->sourceText = "";

	receiver_mtm_cfg = MtmLoadConfig();
	/* Keep us informed about subscription changes. */
	CacheRegisterSyscacheCallback(SUBSCRIPTIONOID,
								  subscription_change_cb,
								  (Datum) 0);

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

		Assert(size < pool->size);
		work = palloc(size);
		pool->pending -= 1;
		pool->active += 1;
		if (pool->lastPeakTime == 0 && pool->active == pool->nWorkers && pool->pending != 0)
			pool->lastPeakTime = MtmGetSystemTime();

		if (pool->head + size + payload + sizeof(int) > pool->size)
		{
			ctx = * (MtmReceiverContext *) &pool->queue;
			memcpy(work, &pool->queue[payload], size);
			pool->head = payload + INTALIGN(size);
		}
		else
		{
			memcpy(&ctx, &pool->queue[pool->head + sizeof(int)], payload);
			memcpy(work, &pool->queue[pool->head + sizeof(int) + payload], size);
			pool->head += sizeof(int) + payload + INTALIGN(size);
		}

		/* wrap head */
		if (pool->head == pool->size)
			pool->head = 0;

		/*
		 * We should reset head and tail in order to accept messages bigger
		 * than half of buffer size.
		 */
		if (pool->head == pool->tail)
		{
			pool->head = 0;
			pool->tail = 0;
		}

		if (pool->producerBlocked)
		{
			pool->producerBlocked = false;
			PGSemaphoreUnlock(pool->overflow);
			pool->lastPeakTime = 0;
		}

		SpinLockRelease(&pool->lock);

		/* Ignore cancel that arrived before we started current command */
		QueryCancelPending = false;

		MtmExecutor(work, size, &ctx);
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
BgwPoolInit(BgwPool* pool, size_t queueSize, size_t nWorkers)
{
	MtmPool = pool;

	pool->bgwhandles = (BackgroundWorkerHandle **) ShmemAlloc(MtmMaxWorkers * sizeof(BackgroundWorkerHandle *));
	pool->queue = (char*)ShmemAlloc(INTALIGN(queueSize));
	if (pool->queue == NULL) { 
		elog(PANIC, "Failed to allocate memory for background workers pool: %zd bytes requested", queueSize);
	}
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
	pool->nWorkers = 0;
	pool->shutdown = false;
	pool->producerBlocked = false;
	pool->head = 0;
	pool->tail = 0;
	pool->active = 0;
	pool->pending = 0;
	pool->lastPeakTime = 0;
	pool->lastDynamicWorkerStartTime = 0;

	PGSemaphoreReset(pool->available);
	PGSemaphoreReset(pool->overflow);
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
	MemoryContext oldcontext;

	if (pool->nWorkers >= MtmMaxWorkers)
		return;

	MemSet(&worker, 0, sizeof(BackgroundWorker));
	worker.bgw_flags = BGWORKER_SHMEM_ACCESS |  BGWORKER_BACKEND_DATABASE_CONNECTION;
	worker.bgw_start_time = BgWorkerStart_ConsistentState;
	worker.bgw_restart_time = BGW_NEVER_RESTART;
	worker.bgw_main_arg = PointerGetDatum(pool);
	sprintf(worker.bgw_library_name, "multimaster");
	sprintf(worker.bgw_function_name, "BgwPoolDynamicWorkerMainLoop");
	snprintf(worker.bgw_name, BGW_MAXLEN, "%s-dynworker-%d", pool->poolName, (int) pool->nWorkers + 1);

	pool->lastDynamicWorkerStartTime = MtmGetSystemTime();

	oldcontext = MemoryContextSwitchTo(TopMemoryContext);

	if (RegisterDynamicBackgroundWorker(&worker, &handle))
	{
		pool->bgwhandles[pool->nWorkers++] = handle;
	}
	else
	{
		elog(WARNING, "Failed to start dynamic background worker");
	}

	MemoryContextSwitchTo(oldcontext);
}

void
BgwPoolExecute(BgwPool* pool, void* work, int size, MtmReceiverContext *ctx)
{
	int payload = INTALIGN(sizeof(MtmReceiverContext));

	// XXX: align with spill size and assert that
	if (size + sizeof(int) + payload > pool->size)
	{
		/* 
		 * Size of work is larger than size of shared buffer: 
		 * run it immediately
		 */
		MtmExecutor(work, size, ctx);
		return;
	}
 
	SpinLockAcquire(&pool->lock);
	while (!pool->shutdown)
	{
		/*
		 * If queue is not wrapped through the end of buffer (head <= tail) we can
		 * fit message either to the end (between tail and pool->size) or to the
		 * beginning (between queue beginning and head). In both cases we can fit
		 * size word after the tail.
		 * If queue is wrapped through the end of buffer (tail < head) we can fit
		 * message only between head and tail.
		 */
		if ((pool->head <= pool->tail &&
				(pool->size - pool->tail >= size + payload + sizeof(int) ||
				 pool->head >= size + payload))
			|| (pool->head > pool->tail &&
				pool->head - pool->tail >= size + payload + sizeof(int)))
		{
			pool->pending += 1;

			if (pool->active + pool->pending > pool->nWorkers)
				BgwStartExtraWorker(pool);

			if (pool->lastPeakTime == 0 && pool->active == pool->nWorkers && pool->pending != 0)
				pool->lastPeakTime = MtmGetSystemTime();

			/*
			 * We always have free space for size at tail, as everything is 
			 * int-aligded and when pool->tail becomes equal to pool->size it
			 * is switched to zero.
			 */
			*(int *) &pool->queue[pool->tail] = size;

			if (pool->size - pool->tail >= payload + size + sizeof(int))
			{
				memcpy(&pool->queue[pool->tail + sizeof(int)], ctx, payload);
				memcpy(&pool->queue[pool->tail + sizeof(int) + payload], work, size);
				pool->tail += sizeof(int) + payload + INTALIGN(size);
			}
			else
			{
				memcpy(pool->queue, ctx, payload);
				memcpy(&pool->queue[payload], work, size);
				pool->tail = payload + INTALIGN(size);
			}

			if (pool->tail == pool->size)
				pool->tail = 0;

			PGSemaphoreUnlock(pool->available);
			break;
		}
		else
		{
			if (pool->lastPeakTime == 0)
				pool->lastPeakTime = MtmGetSystemTime();

			pool->producerBlocked = true;
			SpinLockRelease(&pool->lock);
			PGSemaphoreLock(pool->overflow);
			SpinLockAcquire(&pool->lock);
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
