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

bool	MtmIsPoolWorker;
bool	MtmIsLogicalReceiver;
int		MtmMaxWorkers;

static BgwPool* MtmPool;

/* DSM Queue shared between receiver and its workers */
static char	*queue = NULL;

void BgwPoolDynamicWorkerMainLoop(Datum arg);


void
BgwPoolInit(BgwPool* pool)
{
	SpinLockInit(&pool->lock);
	pool->nWorkers = 0;
	pool->shutdown = false;
	pool->producerBlocked = false;
	pool->head = 0;
	pool->tail = 0;
	pool->active = 0;
	pool->pending = 0;
	pool->size = 0;
	pool->lastDynamicWorkerStartTime = 0;
	ConditionVariableInit(&pool->syncpoint_cv);
	ConditionVariableInit(&pool->available_cv);
	ConditionVariableInit(&pool->overflow_cv);
	pool->bgwhandles = (BackgroundWorkerHandle **) ShmemAlloc(MtmMaxWorkers *
											sizeof(BackgroundWorkerHandle *));
}

/*
 * Call at the start the multimaster WAL receiver.
 */
void
BgwPoolStart(BgwPool* pool, char *poolName, Oid db_id, Oid user_id)
{
	dsm_segment	*seg;
	size_t		size = INTALIGN(MtmTransSpillThreshold * 1024L * 2);

	/* ToDo: remember a segment creation failure (and NULL) case. */
	seg = dsm_create(size, 0);
	Assert(seg != NULL);
	dsm_pin_segment(seg);
	dsm_pin_mapping(seg);
	pool->dsmhandler = dsm_segment_handle(seg);
	queue = (char *) dsm_segment_address(seg);
	Assert(queue != NULL);

	strncpy(pool->poolName, poolName, MAX_NAME_LEN);
	pool->db_id = db_id;
	pool->user_id = user_id;
	pool->size = size;
}

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
BgwPoolMainLoop(BgwPool* poolDesc)
{
	int					size;
	void				*work;
	int					payload = INTALIGN(sizeof(MtmReceiverContext));
	MtmReceiverContext	ctx;
	static PortalData	fakePortal;
	dsm_segment			*seg;

	/* Connect to the queue */
	Assert(!dsm_find_mapping(poolDesc->dsmhandler));
	seg = dsm_attach(poolDesc->dsmhandler);
	queue = dsm_segment_address(seg);
	MtmPool = poolDesc;
	mtm_log(BgwPoolEvent, "[%d] Start background worker shutdown=%d",
												MyProcPid, poolDesc->shutdown);

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
	BackgroundWorkerInitializeConnectionByOid(poolDesc->db_id, poolDesc->user_id, 0);
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
		SpinLockAcquire(&poolDesc->lock);

		/* Worker caught the shutdown signal - release locks and return. */
		if (poolDesc->shutdown)
		{
			SpinLockRelease(&poolDesc->lock);
			break;
		}

		/* Empty queue */
		if (poolDesc->head == poolDesc->tail)
		{
			/*
			 * We need to prepare conditional variable before release of the
			 * lock because of at another case we will have a time gap before
			 * entering to a sleep process. If receiver send the signal before
			 * sleep preparation worker will go to a sleep and receiver will
			 * remain in opinion, that worker waked up and doing its work.
			 */
			ConditionVariablePrepareToSleep(&poolDesc->available_cv);
			SpinLockRelease(&poolDesc->lock);

			ConditionVariableSleep(&poolDesc->available_cv, PG_WAIT_EXTENSION);
			continue;
		}

		/* Wait for end of the node joining operation */
		while (poolDesc->n_holders > 0 && !poolDesc->shutdown)
		{
			SpinLockRelease(&poolDesc->lock);
			ConditionVariableSleep(&Mtm->receiver_barrier_cv, PG_WAIT_EXTENSION);
			SpinLockAcquire(&poolDesc->lock);
		}

		size = *(int *) &queue[poolDesc->head];
		Assert(size < poolDesc->size);
		work = palloc(size);
		poolDesc->pending -= 1;
		poolDesc->active += 1;

		if (poolDesc->head + MSGLEN > poolDesc->size)
		{
			ctx = *(MtmReceiverContext *) &queue;
			memcpy(work, &queue[payload], size);
			poolDesc->head = payload + INTALIGN(size);
		}
		else
		{
			memcpy(&ctx, &queue[poolDesc->head + sizeof(int)], payload);
			memcpy(work, &queue[poolDesc->head + sizeof(int) + payload], size);
			poolDesc->head += MSGLEN;
		}

		/* wrap head */
		if (poolDesc->head == poolDesc->size)
			poolDesc->head = 0;

		/*
		 * We should reset head and tail in order to accept messages bigger
		 * than half of buffer size.
		 */
		if (poolDesc->head == poolDesc->tail)
		{
			poolDesc->head = 0;
			poolDesc->tail = 0;
		}

		if (poolDesc->producerBlocked)
		{
			poolDesc->producerBlocked = false;
			ConditionVariableBroadcast(&poolDesc->overflow_cv);
		}

		SpinLockRelease(&poolDesc->lock);

		MtmExecutor(work, size, &ctx);
		pfree(work);

		SpinLockAcquire(&poolDesc->lock);
		poolDesc->active -= 1;
		SpinLockRelease(&poolDesc->lock);

		ConditionVariableBroadcast(&poolDesc->syncpoint_cv);
	}

	dsm_detach(seg);
	mtm_log(BgwPoolEvent, "Shutdown background worker %d", MyProcPid);
}

void BgwPoolDynamicWorkerMainLoop(Datum arg)
{
	BgwPoolMainLoop((BgwPool*) DatumGetPointer(arg));
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
BgwPoolExecute(BgwPool* pool, void* work, int size, MtmReceiverContext *ctx)
{
	int	payload = INTALIGN(sizeof(MtmReceiverContext));

	Assert(pool != NULL);
	Assert(queue != NULL);

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

			if (pool->active + pool->pending > pool->nWorkers)
				BgwStartExtraWorker(pool);

			/*
			 * We always have free space for size at tail, as everything is 
			 * int-aligned and when pool->tail becomes equal to pool->size it
			 * is switched to zero.
			 */
			*(int *) &queue[pool->tail] = size;

			if (pool->size - pool->tail >= MSGLEN)
			{
				memcpy(&queue[pool->tail + sizeof(int)], ctx, payload);
				memcpy(&queue[pool->tail + sizeof(int) + payload], work, size);
				pool->tail += MSGLEN;
			}
			else
			{
				/* Message can't fit into the end of queue. */
				memcpy(queue, ctx, payload);
				memcpy(&queue[payload], work, size);
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
 * Initiate shutdown process of workers: set shutdown sign and wake up all
 * workers.
 */
void PoolStateShutdown(BgwPool* pool)
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

	SpinLockAcquire(&pool->lock);
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
	SpinLockRelease(&pool->lock);
}
