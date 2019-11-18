#include "postgres.h"
#include "fmgr.h"
#include "miscadmin.h"
#include "pgstat.h"
#include "postmaster/postmaster.h"
#include "postmaster/bgworker.h"
#include "storage/dsm.h"
#include "storage/lwlock.h"
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


#define MSGLEN(sz)	(INTALIGN(sz) + INTALIGN(sizeof(MtmReceiverContext)) + sizeof(int))

bool		MtmIsPoolWorker;
bool		MtmIsLogicalReceiver;
int			MtmMaxWorkers;

/* DSM Queue shared between receiver and its workers */
static char *queue = NULL;

void		BgwPoolDynamicWorkerMainLoop(Datum arg);


/*
 * Call at the start the multimaster WAL receiver.
 */
void
BgwPoolStart(BgwPool *poolDesc, char *poolName, Oid db_id, Oid user_id)
{
	dsm_segment *seg;
	size_t		size = INTALIGN(MtmTransSpillThreshold * 1024L * 2);

	/* ToDo: remember a segment creation failure (and NULL) case. */
	seg = dsm_create(size, 0);
	if (seg == NULL)
		ereport(FATAL,
				(errcode(ERRCODE_INSUFFICIENT_RESOURCES),
				 errmsg("BgwPool can't create an DSM segment")));

	poolDesc->dsmhandler = dsm_segment_handle(seg);
	queue = (char *) dsm_segment_address(seg);
	Assert(queue != NULL);

	strncpy(poolDesc->poolName, poolName, MAX_NAME_LEN);
	poolDesc->db_id = db_id;
	poolDesc->user_id = user_id;

	poolDesc->nWorkers = 0;
	poolDesc->n_holders = 0;
	poolDesc->producerBlocked = false;
	poolDesc->head = 0;
	poolDesc->tail = 0;
	poolDesc->active = 0;
	poolDesc->pending = 0;
	poolDesc->size = size;
	poolDesc->lastDynamicWorkerStartTime = 0;
	ConditionVariableInit(&poolDesc->syncpoint_cv);
	ConditionVariableInit(&poolDesc->available_cv);
	ConditionVariableInit(&poolDesc->overflow_cv);
	poolDesc->bgwhandles = (BackgroundWorkerHandle **) palloc0(MtmMaxWorkers *
															   sizeof(BackgroundWorkerHandle *));
	LWLockInitialize(&poolDesc->lock, LWLockNewTrancheId());
	LWLockRegisterTranche(poolDesc->lock.tranche, "BGWPOOL_LWLOCK");
}

/*
 * Handler of receiver worker for SIGQUIT and SIGTERM signals
 */
static void
BgwShutdownHandler(SIGNAL_ARGS)
{
	int			save_errno = errno;

	ProcDiePending = true;
	InterruptPending = true;
	QueryCancelPending = true;

	SetLatch(MyLatch);

	errno = save_errno;
}

static void
subscription_change_cb(Datum arg, int cacheid, uint32 hashvalue)
{
	receiver_mtm_cfg_valid = false;
}

#define isLastWorkApplied(pool)	((pool->pending + pool->active <= 0) && \
									(pool->head == pool->tail))

static void
BgwPoolMainLoop(BgwPool *poolDesc)
{
	int			size;
	void	   *work;
	int			payload = INTALIGN(sizeof(MtmReceiverContext));
	MtmReceiverContext ctx;
	static PortalData fakePortal;
	dsm_segment *seg;

	/* Connect to the queue */
	Assert(!dsm_find_mapping(poolDesc->dsmhandler));
	seg = dsm_attach(poolDesc->dsmhandler);
	dsm_pin_mapping(seg);
	queue = dsm_segment_address(seg);

	mtm_log(BgwPoolEvent, "[%d] Start background worker.", MyProcPid);

	MtmIsPoolWorker = true;

	/* XXX: get rid of that */
	MtmBackgroundWorker = true;
	MtmIsLogicalReceiver = true;

	pqsignal(SIGINT, die);
	pqsignal(SIGQUIT, die);
	pqsignal(SIGTERM, BgwShutdownHandler);
	pqsignal(SIGHUP, PostgresSigHupHandler);

	/* XXX: probably we should add static variable that signalizes that */
	/* we are between pool->active += 1 and pool->active -= 1, so if */
	/* we face an ERROR outside of PG_TRY we can decrement pool->active */
	/* from on_shem_exit_hook */

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

	while (!ProcDiePending)
	{
		if (ConfigReloadPending)
		{
			ConfigReloadPending = false;
			ProcessConfigFile(PGC_SIGHUP);
		}

		CHECK_FOR_INTERRUPTS();

		LWLockAcquire(&poolDesc->lock, LW_EXCLUSIVE);

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
			LWLockRelease(&poolDesc->lock);

			if (!ProcDiePending)
				ConditionVariableSleep(&poolDesc->available_cv, PG_WAIT_EXTENSION);

			ConditionVariableCancelSleep();
			continue;
		}

		size = *(int *) &queue[poolDesc->head];
		Assert(size < poolDesc->size);
		work = palloc(size);
		poolDesc->pending -= 1;
		poolDesc->active += 1;

		if (poolDesc->head + MSGLEN(size) > poolDesc->size)
		{
			ctx = *(MtmReceiverContext *) &queue;
			memcpy(work, &queue[payload], size);
			poolDesc->head = payload + INTALIGN(size);
		}
		else
		{
			memcpy(&ctx, &queue[poolDesc->head + sizeof(int)], payload);
			memcpy(work, &queue[poolDesc->head + sizeof(int) + payload], size);
			poolDesc->head += MSGLEN(size);
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
			ConditionVariableSignal(&poolDesc->overflow_cv);
		}

		LWLockRelease(&poolDesc->lock);

		MtmExecutor(work, size, &ctx);
		pfree(work);

		LWLockAcquire(&poolDesc->lock, LW_EXCLUSIVE);
		poolDesc->active -= 1;

		if (isLastWorkApplied(poolDesc))
			ConditionVariableSignal(&poolDesc->syncpoint_cv);

		LWLockRelease(&poolDesc->lock);
	}

	dsm_detach(seg);
	mtm_log(BgwPoolEvent, "Shutdown background worker %d", MyProcPid);
}

void
BgwPoolDynamicWorkerMainLoop(Datum arg)
{
	BgwPoolMainLoop((BgwPool *) DatumGetPointer(arg));
}

static void
BgwStartExtraWorker(BgwPool *poolDesc)
{
	BackgroundWorker worker;
	BackgroundWorkerHandle *handle;
	pid_t		pid;
	BgwHandleStatus status;

	if (poolDesc->nWorkers >= MtmMaxWorkers)
		return;

	MemSet(&worker, 0, sizeof(BackgroundWorker));
	worker.bgw_flags = BGWORKER_SHMEM_ACCESS | BGWORKER_BACKEND_DATABASE_CONNECTION;
	worker.bgw_start_time = BgWorkerStart_ConsistentState;
	worker.bgw_restart_time = BGW_NEVER_RESTART;
	worker.bgw_notify_pid = MyProcPid;
	worker.bgw_main_arg = PointerGetDatum(poolDesc);
	sprintf(worker.bgw_library_name, "multimaster");
	sprintf(worker.bgw_function_name, "BgwPoolDynamicWorkerMainLoop");
	snprintf(worker.bgw_name, BGW_MAXLEN, "%s-dynworker-%d", poolDesc->poolName, (int) poolDesc->nWorkers + 1);

	poolDesc->lastDynamicWorkerStartTime = GetCurrentTimestamp();

	if (RegisterDynamicBackgroundWorker(&worker, &handle))
		poolDesc->bgwhandles[poolDesc->nWorkers++] = handle;
	else
		elog(WARNING, "Failed to start dynamic background worker");

	status = WaitForBackgroundWorkerStartup(handle, &pid);
	if (status != BGWH_STARTED)
		mtm_log(ERROR,  "could not start background process");
}

/*
 * Blocking push of message (work + ctx + work size field) into the MTM Executor
 * queue. If message larger than size of queue - execute it immediately.
 * After return from routine work and ctx buffers can be reused safely.
 */
void
BgwPoolExecute(BgwPool *poolDesc, void *work, int size, MtmReceiverContext *ctx)
{
	int			payload = INTALIGN(sizeof(MtmReceiverContext));

	Assert(poolDesc != NULL);
	Assert(queue != NULL);
	Assert(MSGLEN(size) <= poolDesc->size);

	LWLockAcquire(&poolDesc->lock, LW_EXCLUSIVE);

	/*
	 * If we are in a join state, we need to apply all the pending data, wait
	 * for all active workers and go into sleep mode until the end of the join
	 * operation.
	 */
	while (poolDesc->n_holders > 0 && !ProcDiePending)
	{
		ConditionVariablePrepareToSleep(&Mtm->receiver_barrier_cv);
		LWLockRelease(&poolDesc->lock);
		if (!ProcDiePending)
			ConditionVariableSleep(&Mtm->receiver_barrier_cv, PG_WAIT_EXTENSION);
		ConditionVariableCancelSleep();
		LWLockAcquire(&poolDesc->lock, LW_EXCLUSIVE);
	}

	while (!ProcDiePending)
	{
		/*
		 * If queue is not wrapped through the end of buffer (head <= tail) we
		 * can fit message either to the end (between tail and pool->size) or
		 * to the beginning (between queue beginning and head). In both cases
		 * we can fit size word after the tail. If queue is wrapped through
		 * the end of buffer (tail < head) we can fit message only between
		 * head and tail.
		 */
		if ((poolDesc->head <= poolDesc->tail &&
			 (poolDesc->size - poolDesc->tail >= MSGLEN(size) ||
			  poolDesc->head >= size + payload)) ||
			(poolDesc->head > poolDesc->tail &&
			 poolDesc->head - poolDesc->tail >= MSGLEN(size)))
		{
			poolDesc->pending += 1;

			if (poolDesc->active + poolDesc->pending > poolDesc->nWorkers)
				BgwStartExtraWorker(poolDesc);

			/*
			 * We always have free space for size at tail, as everything is
			 * int-aligned and when pool->tail becomes equal to pool->size it
			 * is switched to zero.
			 */
			*(int *) &queue[poolDesc->tail] = size;

			if (poolDesc->size - poolDesc->tail >= MSGLEN(size))
			{
				memcpy(&queue[poolDesc->tail + sizeof(int)], ctx, payload);
				memcpy(&queue[poolDesc->tail + sizeof(int) + payload], work, size);
				poolDesc->tail += MSGLEN(size);
			}
			else
			{
				/* Message can't fit into the end of queue. */
				memcpy(queue, ctx, payload);
				memcpy(&queue[payload], work, size);
				poolDesc->tail = MSGLEN(size) - sizeof(int);
			}

			if (poolDesc->tail == poolDesc->size)
				poolDesc->tail = 0;

			ConditionVariableSignal(&poolDesc->available_cv);
			break;
		}
		else
		{
			poolDesc->producerBlocked = true;
			/* It is critical that the sleep preparation will stay here */
			ConditionVariablePrepareToSleep(&poolDesc->overflow_cv);
			LWLockRelease(&poolDesc->lock);

			if (!ProcDiePending)
				ConditionVariableSleep(&poolDesc->overflow_cv, PG_WAIT_EXTENSION);

			ConditionVariableCancelSleep();
			LWLockAcquire(&poolDesc->lock, LW_EXCLUSIVE);
		}
	}
	LWLockRelease(&poolDesc->lock);
}

/*
 * Soft termination of the workers.
 *
 * Before WAL receiver exit it is need to wait until workers apply
 * the transactions, detach from queue and exit.
 */
void
BgwPoolShutdown(BgwPool *poolDesc)
{
	int			i;

	/* Send termination signal to each worker and wait for end of its work. */
	for (i = 0; i < MtmMaxWorkers; i++)
	{
		pid_t		pid;

		if (poolDesc->bgwhandles[i] == NULL ||
			GetBackgroundWorkerPid(poolDesc->bgwhandles[i], &pid) != BGWH_STARTED)
			continue;
		Assert(pid > 0);
		kill(pid, SIGTERM);
	}

	ConditionVariableBroadcast(&poolDesc->available_cv);
	ConditionVariableBroadcast(&poolDesc->overflow_cv);

	for (i = 0; i < MtmMaxWorkers; i++)
	{
		pid_t		pid;

		if (poolDesc->bgwhandles[i] == NULL ||
			GetBackgroundWorkerPid(poolDesc->bgwhandles[i], &pid) != BGWH_STARTED)
			continue;
		WaitForBackgroundWorkerShutdown(poolDesc->bgwhandles[i]);
		pfree(poolDesc->bgwhandles[i]);
	}

	/*
	 * Clear all handlers because at the next iteration of the receiver
	 * process will launch new pool of workers.
	 */
	memset(poolDesc->bgwhandles, 0, MtmMaxWorkers * sizeof(BackgroundWorkerHandle *));
	elog(LOG, "Shutdown of the receiver workers pool. Pool name = %s",
		 poolDesc->poolName);
}

/*
 * Hard termination of workers on some WAL receiver error.
 *
 * On error WAL receiver woll begin new iteration. But workers need to be killed
 * without finish of processing.
 * The queue will kept in memory, but its state will reset.
 */
void
BgwPoolCancel(BgwPool *poolDesc)
{
	int			i;

	/* Send termination signal to each worker and wait for end of its work. */
	for (i = 0; i < MtmMaxWorkers; i++)
	{
		pid_t		pid;

		if (poolDesc->bgwhandles[i] == NULL ||
			GetBackgroundWorkerPid(poolDesc->bgwhandles[i], &pid) != BGWH_STARTED)
			continue;
		Assert(pid > 0);
		kill(pid, SIGINT);
		WaitForBackgroundWorkerShutdown(poolDesc->bgwhandles[i]);
		pfree(poolDesc->bgwhandles[i]);
	}

	/* The pool shared structures can be reused and we need to clean data */
	poolDesc->nWorkers = 0;
	poolDesc->active = 0;
	poolDesc->pending = 0;
	poolDesc->producerBlocked = false;
	memset(poolDesc->bgwhandles, 0, MtmMaxWorkers * sizeof(BackgroundWorkerHandle *));
	elog(LOG, "Cancel of the receiver workers pool. Pool name = %s",
		 poolDesc->poolName);
}
