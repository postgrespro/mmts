#include "postgres.h"
#include "fmgr.h"
#include "miscadmin.h"
#include "pgstat.h"
#include "postmaster/postmaster.h"
#include "postmaster/bgworker.h"
#include "storage/dsm.h"
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



#define MSGLEN	(INTALIGN(size) + payload + sizeof(int))
#define MinSizeOfPoolState	offsetof(PoolState, queue)

int		MtmQueueSize;
bool	MtmIsPoolWorker;
bool	MtmIsLogicalReceiver;
int		MtmMaxWorkers;

static PoolState* MtmPool;

void BgwPoolDynamicWorkerMainLoop(Datum arg);

static void
BgwShutdownHandler(int sig)
{
	Assert(MtmPool != NULL);
	PoolStateShutdown(MtmPool);

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
BgwPoolMainLoop(dsm_handle pool_handler)
{
	int					size;
	void				*work;
	int					payload = INTALIGN(sizeof(MtmReceiverContext));
	MtmReceiverContext	ctx;
	static PortalData	fakePortal;
	Oid					db_id;
	Oid					user_id;
	dsm_segment			*seg;
	PoolState			*pool;

	/* Connect to the queue */
	Assert(!dsm_find_mapping(pool_handler));
	seg = dsm_attach(pool_handler);
	dsm_pin_mapping(seg);
	pool = dsm_segment_address(seg);
	MtmPool = pool;
	mtm_log(BgwPoolEvent, "[%d] Start background worker shutdown=%d", MyProcPid, pool->shutdown);

	MtmIsPoolWorker = true;

	// XXX: get rid of that
	MtmBackgroundWorker = true;
	MtmIsLogicalReceiver = true;

	pqsignal(SIGINT, ApplyCancelHandler);
	pqsignal(SIGQUIT, BgwShutdownHandler);
	pqsignal(SIGTERM, BgwShutdownHandler);
	pqsignal(SIGHUP, ApplyCancelHandler);

	// XXX: probably we should add static variable that signalizes that
	// we are between pool->active += 1 and pool->active -= 1, so if
	// we face an ERROR outside of PG_TRY we can decrement pool->active
	// from on_shem_exit_hook

	BackgroundWorkerUnblockSignals();
	memcpy(&db_id, MyBgworkerEntry->bgw_extra, sizeof(Oid));
	memcpy(&user_id, MyBgworkerEntry->bgw_extra + sizeof(Oid), sizeof(Oid));
	BackgroundWorkerInitializeConnectionByOid(db_id, user_id, 0);
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

		// XXX: change to LWLock
		SpinLockAcquire(&pool->lock);

		/* Worker caught the shutdown signal - release locks and return. */
		if (pool->shutdown)
		{
			SpinLockRelease(&pool->lock);
			break;
		}

		/* Empty queue */
		if (pool->head == pool->tail)
		{
			/*
			 * We need to prepare conditional variable before release of the
			 * lock because of at another case we will have a time gap before
			 * entering to a sleep process. If receiver send the signal before
			 * sleep preparation worker will go to a sleep and receiver will
			 * remain in opinion, that worker waked up and doing its work.
			 */
			ConditionVariablePrepareToSleep(&pool->available_cv);
			SpinLockRelease(&pool->lock);
			/*
			 * TODO: At this point receiver may have enough time to set shutdown
			 * sign, call ConditionVariableBroadcast(), and return.
			 * In this case worker never exit frim the sleep.
			 */
			ConditionVariableSleep(&pool->available_cv, PG_WAIT_EXTENSION);
			continue;
		}

		/* Wait for end of the node joining operation */
		while (pool->n_holders > 0 && !pool->shutdown)
		{
			SpinLockRelease(&pool->lock);
			ConditionVariableSleep(&Mtm->receiver_barrier_cv, PG_WAIT_EXTENSION);
			SpinLockAcquire(&pool->lock);
		}

		size = *(int *) &pool->queue[pool->head];
		Assert(size < pool->size);
		work = palloc(size);
		pool->pending -= 1;
		pool->active += 1;

		if (pool->head + MSGLEN > pool->size)
		{
			ctx = *(MtmReceiverContext *) &pool->queue;
			memcpy(work, &pool->queue[payload], size);
			pool->head = payload + INTALIGN(size);
		}
		else
		{
			memcpy(&ctx, &pool->queue[pool->head + sizeof(int)], payload);
			memcpy(work, &pool->queue[pool->head + sizeof(int) + payload], size);
			pool->head += MSGLEN;
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
			ConditionVariableBroadcast(&pool->overflow_cv);
		}

		SpinLockRelease(&pool->lock);

		MtmExecutor(work, size, &ctx);
		pfree(work);

		SpinLockAcquire(&pool->lock);
		pool->active -= 1;
		SpinLockRelease(&pool->lock);

		ConditionVariableBroadcast(&pool->syncpoint_cv);
	}

	dsm_detach(seg);
	mtm_log(BgwPoolEvent, "Shutdown background worker %d", MyProcPid);
}

void BgwPoolDynamicWorkerMainLoop(Datum arg)
{
	BgwPoolMainLoop((dsm_handle)DatumGetUInt32(arg));
}

/*
 * Call at the start the multimaster WAL receiver.
 */
void
BgwPoolStart(BgwPool* pool, char *poolName, Oid db_id, Oid user_id)
{
	dsm_segment	*seg;
	size_t		size = INTALIGN(MtmQueueSize);

	/* ToDo: remember a segment creation failure (and NULL) case. */
	seg = dsm_create(MinSizeOfPoolState + size, 0);
	Assert(seg != NULL);
	dsm_pin_segment(seg);
	dsm_pin_mapping(seg);
	pool->pool_handler = dsm_segment_handle(seg);
	pool->state = (PoolState *) dsm_segment_address(seg);
	Assert(pool->state != NULL);

	SpinLockInit(&pool->state->lock);
	ConditionVariableInit(&pool->state->available_cv);
	ConditionVariableInit(&pool->state->overflow_cv);
	ConditionVariableInit(&pool->state->syncpoint_cv);

	strncpy(pool->poolName, poolName, MAX_NAME_LEN);
	pool->db_id = db_id;
	pool->user_id = user_id;
	pool->nWorkers = 0;
	pool->state->shutdown = false;
	pool->state->producerBlocked = false;
	pool->state->head = 0;
	pool->state->tail = 0;
	pool->state->active = 0;
	pool->state->pending = 0;
	pool->state->size = size;
	pool->lastDynamicWorkerStartTime = 0;
}

size_t PoolStateGetQueueSize(PoolState* pool)
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
	worker.bgw_main_arg = UInt32GetDatum(pool->pool_handler);
	memcpy(worker.bgw_extra, &pool->db_id, sizeof(Oid));
	memcpy(worker.bgw_extra + sizeof(Oid), &pool->user_id, sizeof(Oid));
	sprintf(worker.bgw_library_name, "multimaster");
	sprintf(worker.bgw_function_name, "BgwPoolDynamicWorkerMainLoop");
	snprintf(worker.bgw_name, BGW_MAXLEN, "%s-dynworker-%d", pool->poolName, (int) pool->nWorkers + 1);

	pool->lastDynamicWorkerStartTime = GetCurrentTimestamp();

	oldcontext = MemoryContextSwitchTo(TopMemoryContext);

	if (RegisterDynamicBackgroundWorker(&worker, &handle))
		pool->bgwhandles[pool->nWorkers++] = handle;
	else
		elog(WARNING, "Failed to start dynamic background worker");

	MemoryContextSwitchTo(oldcontext);
}

/*
 * Blocking push of message (work + ctx + work size field) into the MTM Executor
 * queue. If message larger than size of queue - execute it immediately.
 * After return from routine work and ctx buffers can be reused safely.
 */
void
BgwPoolExecute(BgwPool* bgwpool, void* work, int size, MtmReceiverContext *ctx)
{
	PoolState	*pool = bgwpool->state;
	int			payload = INTALIGN(sizeof(MtmReceiverContext));

	Assert(pool != NULL);
	// XXX: align with spill size and assert that
	if (MSGLEN > pool->size)
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
			(pool->size - pool->tail >= MSGLEN || pool->head >= size + payload)) ||
			(pool->head > pool->tail && pool->head - pool->tail >= MSGLEN))
		{
			pool->pending += 1;

			if (pool->active + pool->pending > bgwpool->nWorkers)
				BgwStartExtraWorker(bgwpool);

			/*
			 * We always have free space for size at tail, as everything is 
			 * int-aligned and when pool->tail becomes equal to pool->size it
			 * is switched to zero.
			 */
			*(int *) &pool->queue[pool->tail] = size;

			if (pool->size - pool->tail >= MSGLEN)
			{
				memcpy(&pool->queue[pool->tail + sizeof(int)], ctx, payload);
				memcpy(&pool->queue[pool->tail + sizeof(int) + payload], work, size);
				pool->tail += MSGLEN;
			}
			else
			{
				/* Message can't fit into the end of queue. */
				memcpy(pool->queue, ctx, payload);
				memcpy(&pool->queue[payload], work, size);
				pool->tail = MSGLEN - sizeof(int);
			}

			if (pool->tail == pool->size)
				pool->tail = 0;

			ConditionVariableBroadcast(&pool->available_cv);
			break;
		}
		else
		{
			pool->producerBlocked = true;
			/* It is critical that the sleep preparation will stay here */
			ConditionVariablePrepareToSleep(&pool->overflow_cv);
			SpinLockRelease(&pool->lock);
			ConditionVariableSleep(&pool->overflow_cv, PG_WAIT_EXTENSION);
			SpinLockAcquire(&pool->lock);
		}
	}
	SpinLockRelease(&pool->lock);
}

/*
 * Initiate shutdown process of the worker: set shutdown sign and wake up all
 * another workers.
 */
void PoolStateShutdown(PoolState* pool)
{
	SpinLockAcquire(&pool->lock);
	pool->shutdown = true;
	ConditionVariableBroadcast(&pool->available_cv);
	SpinLockRelease(&pool->lock);
}

/*
 * Tell our lads to cancel currently active transactions.
 */
void
BgwPoolCancel(BgwPool* pool)
{
	int	i;

	for (i = 0; i < pool->nWorkers; i++)
	{
		BgwHandleStatus	status;
		pid_t			pid;

		status = GetBackgroundWorkerPid(pool->bgwhandles[i], &pid);
		if (status == BGWH_STARTED)
		{
			Assert(pid > 0);
			kill(pid, SIGINT);
		}
	}
}
