#include "postgres.h"
#include "access/xtm.h"
#include "fmgr.h"
#include "miscadmin.h"
#include "pgstat.h"
#include "postmaster/postmaster.h"
#include "postmaster/bgworker.h"
#include "storage/dsm.h"
#include "storage/ipc.h"
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
#include "state.h"
#include "logger.h"

/*
 * Store the size of tx body, position of it in the tx list and transaction
 * body in the shared work queue.
 */
#define MSGLEN(sz)	(2 * sizeof(int) + MAXALIGN(sz))

bool		MtmIsPoolWorker;
bool		MtmIsLogicalReceiver;
int			MtmMaxWorkers;

/* DSM Queue shared between receiver and its workers */
static char *queue = NULL;

void		BgwPoolDynamicWorkerMainLoop(Datum arg);
static void txl_clear(txlist_t *txlist);


/*
 * Call at the start the multimaster WAL receiver.
 */
void
BgwPoolStart(int sender_node_id, char *poolName, Oid db_id, Oid user_id)
{
	BgwPool *poolDesc = &Mtm->pools[sender_node_id - 1];
	dsm_segment *seg;
	size_t		size = INTALIGN(MtmTransSpillThreshold * 1024L * 2);

	poolDesc->sender_node_id = sender_node_id;

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
	poolDesc->size = size;
	poolDesc->lastDynamicWorkerStartTime = 0;
	ConditionVariableInit(&poolDesc->syncpoint_cv);
	ConditionVariableInit(&poolDesc->available_cv);
	ConditionVariableInit(&poolDesc->overflow_cv);
	poolDesc->bgwhandles = (BackgroundWorkerHandle **) palloc0(MtmMaxWorkers *
															   sizeof(BackgroundWorkerHandle *));
	poolDesc->receiver_pid = MyProcPid;
	LWLockInitialize(&poolDesc->lock, LWLockNewTrancheId());
	LWLockRegisterTranche(poolDesc->lock.tranche, "BGWPOOL_LWLOCK");

	LWLockInitialize(&poolDesc->txlist.lock, LWLockNewTrancheId());
	LWLockRegisterTranche(poolDesc->txlist.lock.tranche, "TXLIST_LWLOCK");
	txl_clear(&poolDesc->txlist);
	ConditionVariableInit(&poolDesc->txlist.syncpoint_cv);
	ConditionVariableInit(&poolDesc->txlist.transaction_cv);

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

static void
BgwPoolBeforeShmemExit(int status, Datum arg)
{
	BgwPool *poolDesc = (BgwPool *) DatumGetPointer(arg);
	pid_t receiver_pid;

	/*
	 * Dynamic workers never die one by one normally because receiver is
	 * completely clueless whether the worker managed to do his job before he
	 * exited, so he doesn't know whether (and how) should he reassign it to
	 * someone else. As another manifestation of this, receiver might hang
	 * forever in process_syncpoint if workers exited unless they notified
	 * him. So make sure to pull down the whole pool if we are exiting.
	 */
	LWLockAcquire(&poolDesc->lock, LW_SHARED);
	receiver_pid = poolDesc->receiver_pid;
	LWLockRelease(&poolDesc->lock);
	if (receiver_pid != InvalidPid)
	{
		kill(receiver_pid, SIGTERM);
		mtm_log(BgwPoolEventDebug, "killed main receiver %d", (int) receiver_pid);
	}
	mtm_log(BgwPoolEvent, "exiting");
}

static void
BgwPoolMainLoop(BgwPool *poolDesc)
{
	int			size;
	void	   *work;
	MtmReceiverWorkerContext rwctx;
	static PortalData fakePortal;
	dsm_segment *seg;

	MemSet(&rwctx, '\0', sizeof(MtmReceiverWorkerContext));
	rwctx.sender_node_id = poolDesc->sender_node_id;
	rwctx.mode = REPLMODE_NORMAL; /* parallel workers always apply normally */
	rwctx.txlist_pos = -1;
	before_shmem_exit(BgwPoolBeforeShmemExit, PointerGetDatum(poolDesc));
	TM->DetectGlobalDeadLockArg = PointerGetDatum(&rwctx.mode);

	/* Connect to the queue */
	Assert(!dsm_find_mapping(poolDesc->dsmhandler));
	/*
	 * Receiver is waiting for workers *after* detaching from dsm, so this
	 * commonly happens on recovery start: receiver in normal mode gets
	 * record, spins up worker but immediately exits once it learns we are in
	 * recovery.
	 */
	seg = dsm_attach(poolDesc->dsmhandler);
	if (seg == NULL)
		elog(FATAL, "dsm_attach failed, looks like receiver is exiting");
	dsm_pin_mapping(seg);
	queue = dsm_segment_address(seg);

	MtmIsPoolWorker = true;
	/* XXX: get rid of that */
	MtmBackgroundWorker = true;
	MtmIsLogicalReceiver = true;

	mtm_log(BgwPoolEvent, "[%d] Start background worker.", MyProcPid);

	pqsignal(SIGINT, die);
	pqsignal(SIGQUIT, die);
	pqsignal(SIGTERM, BgwShutdownHandler);
	pqsignal(SIGHUP, PostgresSigHupHandler);

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

		if (poolDesc->head + MSGLEN(size) > poolDesc->size)
		{
			rwctx.txlist_pos = *((int *) queue);
			memcpy(work, &queue[sizeof(int)], size);
			poolDesc->head = MSGLEN(size) - sizeof(int);
		}
		else
		{
			rwctx.txlist_pos = *((int *) &queue[poolDesc->head + sizeof(int)]);
			memcpy(work, &queue[poolDesc->head + 2 * sizeof(int)], size);
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

		MtmExecutor(work, size, &rwctx);
		pfree(work);
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
	{
		ereport(WARNING,
				(errcode(ERRCODE_CONFIGURATION_LIMIT_EXCEEDED),
				 errmsg("failed to start mtm dynamic background worker"),
				 errhint("You might need to increase max_worker_processes.")));
		return;
	}

	status = WaitForBackgroundWorkerStartup(handle, &pid);
	if (status != BGWH_STARTED)
		mtm_log(ERROR,  "could not start background process");
}

/*
 * Blocking push of message (work size field + ctx + work) into the MTM
 * Executor queue. A circular buffer is used; receiver pushes the whole
 * message in one go and worker reads it out similarly. We never wrap messages
 * around the queue end, so max work size is half of the queue len -- larger
 * jobs must go via file.
 *
 * After return from routine work and ctx buffers can be reused safely.
 */
void
BgwPoolExecute(BgwPool *poolDesc, void *work, int size, MtmReceiverWorkerContext *rwctx)
{
	int			txlist_pos;

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
		if (((poolDesc->head <= poolDesc->tail &&
			 (poolDesc->size - poolDesc->tail >= MSGLEN(size) ||
			  poolDesc->head >= MSGLEN(size) - sizeof(int))) ||
			(poolDesc->head > poolDesc->tail &&
			 poolDesc->head - poolDesc->tail >= MSGLEN(size))) &&
			/*
			 * This should normally be always true: during normal work we can'
			 * t get more than max_connections xacts because sender should
			 * wait for us, and during recovery bgwpool is not used at all.
			 * But there is no strict guarantee of course, and so better be
			 * safe about transitions between these states.
			 */
			(poolDesc->txlist.nelems < poolDesc->txlist.size))
		{
			txlist_pos = txl_store(&poolDesc->txlist, 1);

			if (poolDesc->txlist.nelems > poolDesc->nWorkers)
				BgwStartExtraWorker(poolDesc);

			/*
			 * We always have free space for size at tail, as everything is
			 * int-aligned and when pool->tail becomes equal to pool->size it
			 * is switched to zero.
			 */
			*(int *) &queue[poolDesc->tail] = size;

			if (poolDesc->size - poolDesc->tail >= MSGLEN(size))
			{
				*((int *) &queue[poolDesc->tail + sizeof(int)]) = txlist_pos;
				memcpy(&queue[poolDesc->tail + 2 * sizeof(int)], work, size);
				poolDesc->tail += MSGLEN(size);
			}
			else
			{
				/* Message can't fit into the end of queue. */
				*((int *) queue) = txlist_pos;
				memcpy(&queue[sizeof(int)], work, size);
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
	poolDesc->nWorkers = 0;
	poolDesc->producerBlocked = false;
	memset(poolDesc->bgwhandles, 0, MtmMaxWorkers * sizeof(BackgroundWorkerHandle *));
	txl_clear(&poolDesc->txlist);

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

	/*
	 * (at least currently) this is called only when receiver is already
	 * exiting; there is no point in giving each worker the pleasure to kill
	 * me
	 */
	LWLockAcquire(&poolDesc->lock, LW_EXCLUSIVE);
	poolDesc->receiver_pid = InvalidPid;
	LWLockRelease(&poolDesc->lock);

	/* Send termination signal to each worker and wait for end of its work. */
	if (poolDesc->bgwhandles != NULL) /* if we managed to create handles... */
	{
		for (i = 0; i < MtmMaxWorkers; i++)
		{
			if (poolDesc->bgwhandles[i] == NULL)
				continue;
			TerminateBackgroundWorker(poolDesc->bgwhandles[i]);
			WaitForBackgroundWorkerShutdown(poolDesc->bgwhandles[i]);
			pfree(poolDesc->bgwhandles[i]);
		}
	}

	/* The pool shared structures can be reused and we need to clean data */
	poolDesc->nWorkers = 0;
	poolDesc->producerBlocked = false;
	poolDesc->bgwhandles = NULL;
	txl_clear(&poolDesc->txlist);

	elog(LOG, "Cancel of the receiver workers pool. Pool name = %s",
		 poolDesc->poolName);
}

int
txl_store(txlist_t *txlist, int value)
{
	int			pos = 0;

	LWLockAcquire(&txlist->lock, LW_EXCLUSIVE);

	/* Search for an empty position */
	while (txlist->store[pos].value != 0)
	{
		Assert(pos < txlist->size);
		Assert(txlist->store[pos].prev == -1 || txlist->store[pos].prev != pos);
		Assert(txlist->store[pos].next == -1 || txlist->store[pos].next != pos);
		Assert(txlist->store[pos].prev == -1 || txlist->store[pos].prev != txlist->store[pos].next);
		pos++;
	}

	txlist->store[pos].value = value;
	txlist->store[pos].next = -1;
	txlist->store[pos].prev = txlist->tail;
	if (txlist->tail >= 0)
		txlist->store[txlist->store[pos].prev].next = pos;
	else
		Assert(txlist->head == -1);

	if (txlist->head == -1)
		txlist->head = pos;

	txlist->tail = pos;
	txlist->nelems++;

	Assert(txlist->nelems <= txlist->size);
	Assert(txlist->store[pos].prev == -1 || txlist->store[pos].prev != pos);
	Assert(txlist->store[pos].next == -1 || txlist->store[pos].next != pos);
	Assert(txlist->store[pos].prev == -1 || txlist->store[pos].prev != txlist->store[pos].next);

	LWLockRelease(&txlist->lock);

	return pos;
}

void
txl_remove(txlist_t *txlist, int txlist_pos)
{
	if (txlist_pos == -1)
		/* Transaction is applied by the receiver itself. */
		return;

	Assert(txlist->store[txlist_pos].value > 0);

	LWLockAcquire(&txlist->lock, LW_EXCLUSIVE);
	if (txlist_pos != txlist->head)
	{
		int			ppos = txlist->store[txlist_pos].prev;
		int			npos = txlist->store[txlist_pos].next;

		Assert(ppos != -1);
		txlist->store[ppos].next = npos;

		if (npos != -1)
			/* Element is not last */
			txlist->store[npos].prev = ppos;

		if (txlist_pos == txlist->tail)
		{
			txlist->tail = ppos;
			Assert(txlist->store[ppos].next == -1);
		}
	}
	else
	{
		/* Remove head element */
		int			npos = txlist->store[txlist_pos].next;

		txlist->head = npos;
		if (npos != -1)
			txlist->store[npos].prev = -1;
		else
			/* List will be empty */
			txlist->tail = -1;
	}

	txlist->store[txlist_pos].value = 0;
	txlist->store[txlist_pos].prev = -1;
	txlist->store[txlist_pos].next = -1;
	txlist->nelems--;

	if (txlist->nelems > 0)
		txl_wakeup_workers(txlist);

	LWLockRelease(&txlist->lock);
	Assert(txlist->nelems >= 0);
}

/*
 * We can commit transaction if no one syncpoints is before.
 */
static bool
can_commit(const txlist_t *txlist, int pos)
{
	int			prev = pos;

	if (pos == -1)
		return true;

	while ((prev = txlist->store[prev].prev) != -1)
	{
		Assert(txlist->store[prev].prev == -1 || txlist->store[prev].prev != prev);
		Assert(txlist->store[prev].next == -1 || txlist->store[prev].next != prev);
		Assert(txlist->store[prev].prev == -1 || txlist->store[prev].prev != txlist->store[prev].next);
		Assert(txlist->store[prev].value != 0);

		if (txlist->store[prev].value == 2)
			return false;
	}

	return true;
}

static void
txl_clear(txlist_t *txlist)
{
	LWLockAcquire(&txlist->lock, LW_EXCLUSIVE);
	memset(txlist->store, 0, txlist->size * sizeof(txlelem_t));
	txlist->head = -1;
	txlist->tail = -1;
	txlist->nelems = 0;
	LWLockRelease(&txlist->lock);
}

static bool
txl_syncpoint_at_head(txlist_t *txlist)
{
	bool		is_sp;

	if (txlist->head == -1)
		is_sp = false;
	else if (txlist->store[txlist->head].value == 2)
		is_sp = true;
	else
		is_sp = false;

	return is_sp;
}

/*
 * Wait until there are no pending syncpoints before us.
 *
 * This was created to handle syncpoint processing in apply worker while
 * preserving the strict barrier semantics: all xacts created before sp must
 * be written before applied sp, all xacts created after sp must be written
 * after applied sp. However, handling sp in workers is slightly cumbersome as
 * progress is reported by main receiver; moreover, if one day we still decide
 * to optimize this, we should avoid such waiting altogether: points 'we
 * definitely applied all xacts up to origin lsn n at our LSN x' and 'we
 * definitely have all origin's xacts since lsn n at >= our LSN y' would have
 * different x and y, but that's fine.
 */
void
txl_wait_syncpoint(txlist_t *txlist, int txlist_pos)
{
	Assert(txlist != NULL && txlist_pos >= 0);

	LWLockAcquire(&txlist->lock, LW_EXCLUSIVE);

	/* Wait until all synchronization points received before are committed. */
	while (true)
	{
		if (can_commit(txlist, txlist_pos))
		{
			LWLockRelease(&txlist->lock);
			break;
		}

		ConditionVariablePrepareToSleep(&txlist->transaction_cv);
		LWLockRelease(&txlist->lock);

		ConditionVariableSleep(&txlist->transaction_cv, PG_WAIT_EXTENSION);
		LWLockAcquire(&txlist->lock, LW_EXCLUSIVE);
	}
}

void
txl_wait_sphead(txlist_t *txlist, int txlist_pos)
{
	Assert(txlist_pos >= 0);

	/*
	 * Await for our pool workers to finish what they are currently doing.
	 */
	LWLockAcquire(&txlist->lock, LW_EXCLUSIVE);

	for (;;)
	{
		if (txlist_pos == txlist->head)
		{
			LWLockRelease(&txlist->lock);
			break;
		}

		ConditionVariablePrepareToSleep(&txlist->syncpoint_cv);
		LWLockRelease(&txlist->lock);

		ConditionVariableSleep(&txlist->syncpoint_cv, PG_WAIT_EXTENSION);
		LWLockAcquire(&txlist->lock, LW_EXCLUSIVE);
	}
}

void
txl_wait_txhead(txlist_t *txlist, int txlist_pos)
{
	/*
	 * Await for our pool workers to finish what they are currently doing.
	 */
	LWLockAcquire(&txlist->lock, LW_EXCLUSIVE);

	for (;;)
	{
		if (txlist_pos == txlist->head)
		{
			LWLockRelease(&txlist->lock);
			break;
		}

		ConditionVariablePrepareToSleep(&txlist->transaction_cv);
		LWLockRelease(&txlist->lock);

		ConditionVariableSleep(&txlist->transaction_cv, PG_WAIT_EXTENSION);
		LWLockAcquire(&txlist->lock, LW_EXCLUSIVE);
	}
}

void
txl_wakeup_workers(txlist_t *txlist)
{
	if (txl_syncpoint_at_head(txlist))
		ConditionVariableBroadcast(&txlist->syncpoint_cv);
	else
		ConditionVariableBroadcast(&txlist->transaction_cv);
}
