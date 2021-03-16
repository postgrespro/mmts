/*----------------------------------------------------------------------------
 *
 * ddd.c
 *
 * Distributed deadlock detector.
 *
 * Copyright (c) 2017-2020, Postgres Professional
 *
 *----------------------------------------------------------------------------
 */

#include "postgres.h"
#include "access/clog.h"
#include "access/twophase.h"
#include "access/transam.h"
#include "storage/lwlock.h"
#include "storage/ipc.h"
#include "storage/proc.h"
#include "utils/hsearch.h"
#include "utils/timeout.h"
#include "miscadmin.h"
#include "replication/origin.h"
#include "replication/message.h"
#include "utils/builtins.h"
#include "storage/lmgr.h"
#include "storage/procarray.h"

#include "multimaster.h"

#include "ddd.h"
#include "bytebuf.h"
#include "state.h"
#include "logger.h"
#include "commit.h"


/*
 * This DDD is based on following observations:
 *
 *    Situation when a transaction (say T1) in apply_worker (or receiver
 * itself) stucks on some lock created by a transaction in a local backend (say
 * T2) will definitely lead to a deadlock since T2 after being prepared and
 * replicated will fail to obtain lock that is already held by T1.
 *    Same reasoning may be applied to the situation when apply_worker (or
 * receiver) is waiting for an apply_worker (or receiver) belonging to other
 * origin -- no need to wait for a distributed deadlock detection and we may
 * just instantly abort.
 *    Only case for distributed deadlock that is left is when apply_worker
 * (or receiver) is waiting for another apply_worker from same origin. However,
 * such situation isn't possible since one origin node can not have two
 * conflicting prepared transaction simultaneously.
 *
 *    So we may construct distributed deadlock avoiding mechanism by disallowing
 * such edges. Now we may ask inverse question: what amount of wait graphs
 * with such edges are actually do not represent distributed deadlock? That may
 * happen in cases when holding transaction is purely local since it holding
 * locks only in SHARED mode. Only lock levels that are conflicting with this
 * modes are EXCLUSIVE and ACCESS EXCLUSIVE. In all other cases proposed
 * avoiding scheme should not yield false positives.
 *
 *     To cope with false positives in EXCLUSIVE and ACCESS EXCLUSIVE modes we
 * may throw exception not in WaitOnLock() when we first saw forbidden edge
 * but later during first call to local deadlock detector. This way we still
 * have `deadlock_timeout` second to grab that lock and database user also can
 * increase it on per-transaction basis if there are long-living read-only
 * transactions.
 *
 *     As a further optimization it is possible to check whether our lock is
 * EXCLUSIVE or higher so not to delay rollback till `deadlock_timeout` event.
 */
bool
MtmDetectGlobalDeadLock(PGPROC *proc)
{
	StringInfoData locktagbuf;
	LOCK	   *lock = proc->waitLock;
	bool		is_detected = false;
	Assert(proc == MyProc);

	/*
	 * These locks never participate in deadlocks, ignore them. Without it,
	 * spurious deadlocks might be reported due to concurrency on rel
	 * extension.
	 */
	if (LOCK_LOCKTAG(*lock) == LOCKTAG_RELATION_EXTEND ||
		(LOCK_LOCKTAG(*lock) == LOCKTAG_PAGE))
		return false;

	/*
	 * There is no need to check for deadlocks in recovery: all
	 * conflicting transactions must be eventually committed/aborted
	 * by the resolver. It would not be fatal, but restarting due to
	 * deadlock ERRORs might significantly slow down the recovery
	 */
	is_detected = (curr_replication_mode == REPLMODE_NORMAL);

	if (is_detected)
	{
		initStringInfo(&locktagbuf);
		DescribeLockTag(&locktagbuf, &lock->tag);
		mtm_log(LOG, "apply worker %d waits for %s on %s",
				MyProcPid,
				GetLockmodeName(lock->tag.locktag_lockmethodid, proc->waitLockMode),
				locktagbuf.data);
	}

	return is_detected;

}

#if 0

#define LOCK_BY_INDEX(i) ((LWLockId)&ddd_shared->locks[(i)])
#define EQUAL_GTID(x,y) ((x).node == (y).node && (x).xid == (y).xid)

#define LOCK_BUF_INIT_SIZE 4096
#define MAX_TRANSACTIONS  1024 // XXX
#define VISITED_NODE_MARK 0

typedef struct MtmEdge
{
	struct MtmEdge *next;		/* list of outgoing edges */
	struct MtmVertex *dst;
	struct MtmVertex *src;
} MtmEdge;

typedef struct MtmVertex
{
	struct MtmEdge *outgoingEdges;
	struct MtmVertex *collision;
	GlobalTransactionId gtid;
} MtmVertex;

typedef struct MtmGraph
{
	MtmVertex  *hashtable[MAX_TRANSACTIONS];
} MtmGraph;

typedef struct xid2GtidEntry
{
	TransactionId xid;
	GlobalTransactionId gtid;
} xid2GtidEntry;

typedef struct NodeDeadlockData
{
	void	   *lockGraphData;
	int			lockGraphAllocated;
	int			lockGraphUsed;
} NodeDeadlockData;

struct ddd_shared
{
	LWLockPadded *locks;
	int			n_nodes;
	NodeDeadlockData nodelocks[FLEXIBLE_ARRAY_MEMBER];
}		   *ddd_shared;

static HTAB *xid2gtid;

PG_FUNCTION_INFO_V1(mtm_dump_lock_graph);
PG_FUNCTION_INFO_V1(mtm_check_deadlock);

static shmem_startup_hook_type PreviousShmemStartupHook;

/*****************************************************************************
 *
 * Initialization
 *
 *****************************************************************************/

static Size
ddd_shmem_size(int n_nodes)
{
	Size		size = 0;

	size = add_size(size, sizeof(struct ddd_shared) +
					n_nodes * sizeof(NodeDeadlockData));
	size = add_size(size, hash_estimate_size(n_nodes * MaxBackends,
											 sizeof(xid2GtidEntry)));
	/* hope that three reallocs (1 + 2 + 4) is enough: */
	size = add_size(size, 7 * n_nodes * LOCK_BUF_INIT_SIZE);

	return MAXALIGN(size);
}

void
MtmDeadlockDetectorInit(int n_nodes)
{
	RequestAddinShmemSpace(ddd_shmem_size(n_nodes));
	RequestNamedLWLockTranche("mtm-ddd", n_nodes + 1);
}

void
MtmDeadlockDetectorShmemStartup(int n_nodes)
{
	bool		found;
	HASHCTL		hash_info;

	if (PreviousShmemStartupHook)
		PreviousShmemStartupHook();

	MemSet(&hash_info, 0, sizeof(hash_info));
	hash_info.keysize = sizeof(TransactionId);
	hash_info.entrysize = sizeof(xid2GtidEntry);

	LWLockAcquire(AddinShmemInitLock, LW_EXCLUSIVE);

	ddd_shared = ShmemInitStruct("mtm-ddd",
								 sizeof(struct ddd_shared),
								 &found);
	if (!found)
	{
		int			i;

		ddd_shared->n_nodes = n_nodes;
		ddd_shared->locks = GetNamedLWLockTranche("mtm-ddd");
		for (i = 0; i < ddd_shared->n_nodes; i++)
		{
			ddd_shared->nodelocks[i].lockGraphData = NULL;
			ddd_shared->nodelocks[i].lockGraphAllocated = 0;
			ddd_shared->nodelocks[i].lockGraphUsed = 0;
		}
	}

	xid2gtid = ShmemInitHash("mtm-ddd-xid2gtid",
							 n_nodes * MaxBackends, n_nodes * MaxBackends,
							 &hash_info, HASH_ELEM | HASH_BLOBS);

	LWLockRelease(AddinShmemInitLock);
}


/*****************************************************************************
 *
 * Graph traversal
 *
 *****************************************************************************/



static void
MtmGraphInit(MtmGraph *graph)
{
	memset(graph->hashtable, 0, sizeof(graph->hashtable));
}

static inline MtmVertex *
findVertex(MtmGraph *graph, GlobalTransactionId *gtid)
{
	uint32		h = gtid->xid % MAX_TRANSACTIONS;
	MtmVertex  *v;

	for (v = graph->hashtable[h]; v != NULL; v = v->collision)
	{
		if (EQUAL_GTID(v->gtid, *gtid))
		{
			return v;
		}
	}
	v = (MtmVertex *) palloc(sizeof(MtmVertex));
	v->gtid = *gtid;
	v->outgoingEdges = NULL;
	v->collision = graph->hashtable[h];
	graph->hashtable[h] = v;
	return v;
}

static void
MtmGraphAdd(MtmGraph *graph, GlobalTransactionId *gtid, int size)
{
	GlobalTransactionId *last = gtid + size;

	while (gtid != last)
	{
		MtmVertex  *src = findVertex(graph, gtid++);

		while (gtid->node != 0)
		{
			MtmVertex  *dst = findVertex(graph, gtid++);
			MtmEdge    *e = (MtmEdge *) palloc(sizeof(MtmEdge));

			e->dst = dst;
			e->src = src;
			e->next = src->outgoingEdges;
			src->outgoingEdges = e;
		}
		gtid += 1;
	}
}

static bool
recursiveTraverseGraph(MtmVertex *root, MtmVertex *v)
{
	MtmEdge    *e;

	v->gtid.node = VISITED_NODE_MARK;
	for (e = v->outgoingEdges; e != NULL; e = e->next)
	{
		if (e->dst == root)
		{
			return true;
		}
		else if (e->dst->gtid.node != VISITED_NODE_MARK && recursiveTraverseGraph(root, e->dst))
		{						/* loop */
			return true;
		}
	}
	return false;
}

static bool
MtmGraphFindLoop(MtmGraph *graph, GlobalTransactionId *root)
{
	MtmVertex  *v;

	for (v = graph->hashtable[root->xid % MAX_TRANSACTIONS]; v != NULL; v = v->collision)
	{
		if (EQUAL_GTID(v->gtid, *root))
		{
			if (recursiveTraverseGraph(v, v))
			{
				return true;
			}
			break;
		}
	}
	return false;
}


/*****************************************************************************
 *
 * Graph traversal
 *
 *****************************************************************************/


void
MtmUpdateLockGraph(int nodeId, void const *messageBody, int messageSize)
{
	int			allocated;

	Assert(nodeId > 0);

	LWLockAcquire(LOCK_BY_INDEX(nodeId), LW_EXCLUSIVE);

	allocated = ddd_shared->nodelocks[nodeId - 1].lockGraphAllocated;
	if (messageSize > allocated)
	{
		allocated = Max(Max(LOCK_BUF_INIT_SIZE, allocated * 2), messageSize);

		/* XXX: shmem leak */
		ddd_shared->nodelocks[nodeId - 1].lockGraphData = ShmemAlloc(allocated);
		if (ddd_shared->nodelocks[nodeId - 1].lockGraphData == NULL)
		{
			elog(PANIC, "Failed to allocate shared memory for lock graph: %d bytes requested",
				 allocated);
		}
		ddd_shared->nodelocks[nodeId - 1].lockGraphAllocated = allocated;
	}
	memcpy(ddd_shared->nodelocks[nodeId - 1].lockGraphData, messageBody, messageSize);
	ddd_shared->nodelocks[nodeId - 1].lockGraphUsed = messageSize;

	LWLockRelease(LOCK_BY_INDEX(nodeId));

	mtm_log(DeadlockUpdate, "Update deadlock graph for node %d size %d", nodeId, messageSize);
}

void
MtmDeadlockDetectorAddXact(TransactionId xid, GlobalTransactionId *gtid)
{
	xid2GtidEntry *entry;
	bool		found;

	Assert(TransactionIdIsValid(xid));

	LWLockAcquire(LOCK_BY_INDEX(0), LW_EXCLUSIVE);
	entry = (xid2GtidEntry *) hash_search(xid2gtid, &xid,
										  HASH_ENTER, &found);
	entry->gtid = *gtid;
	LWLockRelease(LOCK_BY_INDEX(0));

	Assert(!found);
}

void
MtmDeadlockDetectorRemoveXact(TransactionId xid)
{
	bool		found;

	Assert(TransactionIdIsValid(xid));

	LWLockAcquire(LOCK_BY_INDEX(0), LW_EXCLUSIVE);
	hash_search(xid2gtid, &xid, HASH_REMOVE, &found);
	LWLockRelease(LOCK_BY_INDEX(0));

	Assert(found);
}

static void
MtmGetGtid(TransactionId xid, GlobalTransactionId *gtid)
{
	xid2GtidEntry *entry;

	Assert(Mtm->my_node_id != 0);

	LWLockAcquire(LOCK_BY_INDEX(0), LW_SHARED);
	entry = (xid2GtidEntry *) hash_search(xid2gtid, &xid,
										  HASH_FIND, NULL);
	if (entry != NULL)
	{
		*gtid = entry->gtid;
	}
	else
	{
		const char *gid;

		gid = TwoPhaseGetGid(xid);
		if (gid[0] != '\0')
		{
			int			tx_node_id = MtmGidParseNodeId(gid);

			if (tx_node_id > 0)
			{
				/* ordinary global tx */
				gtid->node = tx_node_id;
				gtid->xid = MtmGidParseXid(gid);
				gtid->my_xid = xid;
			}
			else
			{
				/* user 2pc */
				/*
				 * XXX: that is wrong -- we need to save xid and node_id in
				 * user 2pc GIDs.
				 */
				Assert(tx_node_id == -1);
				gtid->node = Mtm->my_node_id;
				gtid->xid = xid;
				gtid->my_xid = xid;
			}
		}
		else
		{
			/*
			 * That should be local running tx or any recently committed tx.
			 */
			gtid->node = Mtm->my_node_id;
			gtid->xid = xid;
			gtid->my_xid = xid;
		}
	}
	LWLockRelease(LOCK_BY_INDEX(0));
}

static GlobalTransactionId
gtid_by_pgproc(PGPROC *proc)
{
	GlobalTransactionId gtid;
	PGXACT	   *pgxact = &ProcGlobal->allPgXact[proc->pgprocno];

	if (TransactionIdIsValid(pgxact->xid))
		MtmGetGtid(pgxact->xid, &gtid);
	else
		gtid = (GlobalTransactionId)
	{
		Mtm->my_node_id, proc->pgprocno, proc->pgprocno
	};

	return gtid;
}

static void
MtmDumpWaitForEdges(LOCK *lock, void *arg)
{
	ByteBuffer *buf = (ByteBuffer *) arg;
	SHM_QUEUE  *procLocks = &(lock->procLocks);
	PROCLOCK   *src_pl;
	LockMethod	lockMethodTable = GetLocksMethodTable(lock);
	PROC_QUEUE *waitQueue;
	PGPROC	   *prev,
			   *curr;
	int			numLockModes = lockMethodTable->numLockModes;

	/* dump hard edges */
	for (src_pl = (PROCLOCK *) SHMQueueNext(procLocks, procLocks,
											offsetof(PROCLOCK, lockLink));
		 src_pl;
		 src_pl = (PROCLOCK *) SHMQueueNext(procLocks, &src_pl->lockLink,
											offsetof(PROCLOCK, lockLink)))
	{
		GlobalTransactionId src_gtid,
					zero_gtid = {0, 0, 0};
		PGPROC	   *src_proc = src_pl->tag.myProc;
		PROCLOCK   *dst_pl;
		int			conflictMask;

		if (src_proc->waitLock != lock)
			continue;

		conflictMask = lockMethodTable->conflictTab[src_proc->waitLockMode];

		/* waiting transaction */
		src_gtid = gtid_by_pgproc(src_proc);
		ByteBufferAppend(buf, &src_gtid, sizeof(src_gtid));

		for (dst_pl = (PROCLOCK *) SHMQueueNext(procLocks, procLocks,
												offsetof(PROCLOCK, lockLink));
			 dst_pl;
			 dst_pl = (PROCLOCK *) SHMQueueNext(procLocks, &dst_pl->lockLink,
												offsetof(PROCLOCK, lockLink)))
		{
			GlobalTransactionId dst_gtid;
			int			lm;

			if (src_pl == dst_pl)
				continue;

			for (lm = 1; lm <= numLockModes; lm++)
			{
				if ((dst_pl->holdMask & LOCKBIT_ON(lm)) &&
					(conflictMask & LOCKBIT_ON(lm)))
				{
					/* transaction holding lock */
					dst_gtid = gtid_by_pgproc(dst_pl->tag.myProc);
					ByteBufferAppend(buf, &dst_gtid, sizeof(dst_gtid));
					mtm_log(DeadlockSerialize,
							"%d:" XID_FMT " (" XID_FMT ") -> %d:" XID_FMT " (" XID_FMT ")",
							src_gtid.node, src_gtid.xid, src_gtid.my_xid,
							dst_gtid.node, dst_gtid.xid, dst_gtid.my_xid);
					break;
				}
			}
		}

		/* end of lock owners list */
		ByteBufferAppend(buf, &zero_gtid, sizeof(GlobalTransactionId));
	}

	/* dump soft edges */
	waitQueue = &(lock->waitProcs);
	prev = (PGPROC *) waitQueue->links.next;
	curr = (PGPROC *) prev->links.next;
	while (curr != (PGPROC *) waitQueue->links.next)
	{
		GlobalTransactionId src_gtid,
					dst_gtid,
					zero_gtid = {0, 0, 0};

		src_gtid = gtid_by_pgproc(curr);
		dst_gtid = gtid_by_pgproc(prev);
		ByteBufferAppend(buf, &src_gtid, sizeof(src_gtid));
		ByteBufferAppend(buf, &dst_gtid, sizeof(dst_gtid));
		ByteBufferAppend(buf, &zero_gtid,
						 sizeof(GlobalTransactionId));

		mtm_log(DeadlockSerialize,
				"%d:" XID_FMT " (" XID_FMT ") ~> %d:" XID_FMT " (" XID_FMT ")",
				src_gtid.node, src_gtid.xid, src_gtid.my_xid,
				dst_gtid.node, dst_gtid.xid, dst_gtid.my_xid);

		prev = curr;
		curr = (PGPROC *) curr->links.next;
	}
}

static bool
MtmDetectGlobalDeadLockForXid(TransactionId xid)
{
	bool		hasDeadlock = false;
	ByteBuffer	buf;
	MtmGraph	graph;
	GlobalTransactionId gtid;
	int			i;

	Assert(TransactionIdIsValid(xid));

	ByteBufferAlloc(&buf);
	EnumerateLocks(MtmDumpWaitForEdges, &buf);

	Assert(replorigin_session_origin == InvalidRepOriginId);
	XLogFlush(LogLogicalMessage("L", buf.data, buf.used, false));

	MtmGraphInit(&graph);
	MtmGraphAdd(&graph, (GlobalTransactionId *) buf.data, buf.used / sizeof(GlobalTransactionId));
	ByteBufferFree(&buf);
	for (i = 0; i < ddd_shared->n_nodes; i++)
	{
		if (i + 1 != Mtm->my_node_id && BIT_CHECK(MtmGetEnabledNodeMask(false), i))
		{
			size_t		lockGraphSize;
			void	   *lockGraphData;

			LWLockAcquire(LOCK_BY_INDEX(i + 1), LW_SHARED);
			lockGraphSize = ddd_shared->nodelocks[i].lockGraphUsed;
			lockGraphData = palloc(lockGraphSize);
			memcpy(lockGraphData, ddd_shared->nodelocks[i].lockGraphData, lockGraphSize);
			LWLockRelease(LOCK_BY_INDEX(i + 1));

			if (lockGraphData == NULL)
			{
				return true;
			}
			else
			{
				MtmGraphAdd(&graph, (GlobalTransactionId *) lockGraphData, lockGraphSize / sizeof(GlobalTransactionId));
			}
		}
	}
	MtmGetGtid(xid, &gtid);
	hasDeadlock = MtmGraphFindLoop(&graph, &gtid);
	mtm_log(DeadlockCheck, "Distributed deadlock check by backend %d for %u:" XID_FMT " = %d",
			MyProcPid, gtid.node, gtid.xid, hasDeadlock);

	if (!hasDeadlock)
	{
		/* TimestampTz start_time = get_timeout_start_time(DEADLOCK_TIMEOUT); */
		mtm_log(DeadlockCheck, "Enable deadlock timeout in backend %d for transaction " XID_FMT,
				MyProcPid, xid);
		enable_timeout_after(DEADLOCK_TIMEOUT, DeadlockTimeout);
		/* set_timeout_start_time(DEADLOCK_TIMEOUT, start_time); */
	}

	return hasDeadlock;
}

bool
MtmDetectGlobalDeadLock(PGPROC *proc)
{
	PGXACT	   *pgxact = &ProcGlobal->allPgXact[proc->pgprocno];
	bool		found;
	RepOriginId saved_origin_id = replorigin_session_origin;

	/*
	 * There is no need to check for deadlocks in recovery: all our
	 * transactions must be eventually committed/aborted by the resolver.
	 */
	if (!MtmIsEnabled() || MtmGetCurrentStatus(false, false) != MTM_ONLINE)
		return false;

	mtm_log(DeadlockCheck, "Detect global deadlock for " XID_FMT " by backend %d", pgxact->xid, MyProcPid);

	if (!TransactionIdIsValid(pgxact->xid))
		return false;

	replorigin_session_origin = InvalidRepOriginId;
	found = MtmDetectGlobalDeadLockForXid(pgxact->xid);
	replorigin_session_origin = saved_origin_id;

	return found;
}


Datum
mtm_dump_lock_graph(PG_FUNCTION_ARGS)
{
	StringInfo	s = makeStringInfo();
	int			i;

	for (i = 0; i < ddd_shared->n_nodes; i++)
	{
		size_t		lockGraphSize;
		char	   *lockGraphData;

		LWLockAcquire(LOCK_BY_INDEX(i + 1), LW_SHARED);
		lockGraphSize = ddd_shared->nodelocks[i].lockGraphUsed;
		lockGraphData = palloc(lockGraphSize);
		memcpy(lockGraphData, ddd_shared->nodelocks[i].lockGraphData, lockGraphSize);
		LWLockRelease(LOCK_BY_INDEX(i + 1));

		if (lockGraphData)
		{
			GlobalTransactionId *gtid = (GlobalTransactionId *) lockGraphData;
			GlobalTransactionId *last = (GlobalTransactionId *) (lockGraphData + lockGraphSize);

			appendStringInfo(s, "node-%d lock graph: ", i + 1);
			while (gtid != last)
			{
				GlobalTransactionId *src = gtid++;

				appendStringInfo(s, "%d:" XID_FMT " -> ", src->node, src->xid);
				while (gtid->node != 0)
				{
					GlobalTransactionId *dst = gtid++;

					appendStringInfo(s, "%d:" XID_FMT ", ", dst->node, dst->xid);
				}
				gtid += 1;
			}
			appendStringInfo(s, "\n");
		}
	}
	return CStringGetTextDatum(s->data);
}

Datum
mtm_check_deadlock(PG_FUNCTION_ARGS)
{
	TransactionId xid = PG_GETARG_INT64(0);

	PG_RETURN_BOOL(MtmDetectGlobalDeadLockForXid(xid));
}

#endif
