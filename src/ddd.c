
#include "postgres.h"
#include "access/clog.h"
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

#include "multimaster.h"

#include "ddd.h"
#include "bytebuf.h"
#include "mm.h"
#include "state.h"
#include "logger.h"

#define LOCK_BY_INDEX(i) ((LWLockId)&ddd_shared->locks[(i)])
#define EQUAL_GTID(x,y) ((x).node == (y).node && (x).xid == (y).xid)

#define LOCK_BUF_INIT_SIZE 4096
#define MAX_TRANSACTIONS  1024 // XXX
#define VISITED_NODE_MARK 0

typedef struct MtmEdge {
	struct MtmEdge	   *next; /* list of outgoing edges */
	struct MtmVertex   *dst;
	struct MtmVertex   *src;
} MtmEdge;

typedef struct MtmVertex
{
	struct MtmEdge		   *outgoingEdges;
	struct MtmVertex	   *collision;
	GlobalTransactionId		gtid;
} MtmVertex;

typedef struct MtmGraph
{
	MtmVertex   *hashtable[MAX_TRANSACTIONS];
} MtmGraph;

typedef struct xid2GtidEntry
{
	TransactionId		xid;
	GlobalTransactionId gtid;
} xid2GtidEntry;

typedef struct NodeDeadlockData
{
	void   *lockGraphData;
	int		lockGraphAllocated;
	int		lockGraphUsed;
} NodeDeadlockData;

struct ddd_shared
{
	LWLockPadded	   *locks;
	int					n_nodes;
	NodeDeadlockData	nodelocks[FLEXIBLE_ARRAY_MEMBER];
} *ddd_shared;

static HTAB			   *xid2gtid;

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
	Size	size = 0;

	size = add_size(size, sizeof(struct ddd_shared) +
					n_nodes*sizeof(NodeDeadlockData));
	size = add_size(size, hash_estimate_size(n_nodes*MaxBackends,
					sizeof(xid2GtidEntry)));
	/* hope that three reallocs (1 + 2 + 4) is enough: */
	size = add_size(size, 7*n_nodes*LOCK_BUF_INIT_SIZE);

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
	bool found;
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
		int	i;

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
		n_nodes*MaxBackends, n_nodes*MaxBackends,
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

static inline MtmVertex*
findVertex(MtmGraph *graph, GlobalTransactionId *gtid)
{
    uint32 h = gtid->xid % MAX_TRANSACTIONS;
    MtmVertex* v;
    for (v = graph->hashtable[h]; v != NULL; v = v->collision) { 
        if (EQUAL_GTID(v->gtid, *gtid)) { 
            return v;
        }
    }
	v = (MtmVertex*)palloc(sizeof(MtmVertex));
    v->gtid = *gtid;
	v->outgoingEdges = NULL;
    v->collision = graph->hashtable[h];
    graph->hashtable[h] = v;
    return v;
}

static void
MtmGraphAdd(MtmGraph *graph, GlobalTransactionId *gtid, int size)
{
    GlobalTransactionId* last = gtid + size;
    while (gtid != last) { 
        MtmVertex* src = findVertex(graph, gtid++);
        while (gtid->node != 0) { 
            MtmVertex* dst = findVertex(graph, gtid++);
            MtmEdge* e = (MtmEdge*)palloc(sizeof(MtmEdge));
            e->dst = dst;
            e->src = src;
            e->next = src->outgoingEdges;
            src->outgoingEdges = e;
        }
		gtid += 1;
    }
}

static bool
recursiveTraverseGraph(MtmVertex* root, MtmVertex* v)
{
    MtmEdge* e;
    v->gtid.node = VISITED_NODE_MARK;
    for (e = v->outgoingEdges; e != NULL; e = e->next) {
        if (e->dst == root) { 
            return true;
        } else if (e->dst->gtid.node != VISITED_NODE_MARK && recursiveTraverseGraph(root, e->dst)) { /* loop */
            return true;
        } 
    }
    return false;        
}

static bool
MtmGraphFindLoop(MtmGraph* graph, GlobalTransactionId* root)
{
    MtmVertex* v;
    for (v = graph->hashtable[root->xid % MAX_TRANSACTIONS]; v != NULL; v = v->collision) { 
        if (EQUAL_GTID(v->gtid, *root)) { 
            if (recursiveTraverseGraph(v, v)) { 
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
MtmUpdateLockGraph(int nodeId, void const* messageBody, int messageSize)
{
	int allocated;

	Assert(nodeId > 0);

	LWLockAcquire(LOCK_BY_INDEX(nodeId), LW_EXCLUSIVE);

	allocated = ddd_shared->nodelocks[nodeId-1].lockGraphAllocated;
	if (messageSize > allocated) {
		allocated = Max(Max(LOCK_BUF_INIT_SIZE, allocated*2), messageSize);

		// XXX: shmem leak
		ddd_shared->nodelocks[nodeId-1].lockGraphData = ShmemAlloc(allocated);
		if (ddd_shared->nodelocks[nodeId-1].lockGraphData == NULL) {
			elog(PANIC, "Failed to allocate shared memory for lock graph: %d bytes requested",
				 allocated);
		}
		ddd_shared->nodelocks[nodeId-1].lockGraphAllocated = allocated;
	}
	memcpy(ddd_shared->nodelocks[nodeId-1].lockGraphData, messageBody, messageSize);
	ddd_shared->nodelocks[nodeId-1].lockGraphUsed = messageSize;

	LWLockRelease(LOCK_BY_INDEX(nodeId));

	mtm_log(DeadlockUpdate, "Update deadlock graph for node %d size %d", nodeId, messageSize);
}

void
MtmDeadlockDetectorAddXact(TransactionId xid, GlobalTransactionId *gtid)
{
	xid2GtidEntry   *entry;
	bool found;

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
	bool found;

	Assert(TransactionIdIsValid(xid));

	LWLockAcquire(LOCK_BY_INDEX(0), LW_EXCLUSIVE);
	hash_search(xid2gtid, &xid, HASH_REMOVE, &found);
	LWLockRelease(LOCK_BY_INDEX(0));

	Assert(found);
}

static void
MtmGetGtid(TransactionId xid, GlobalTransactionId* gtid)
{
	xid2GtidEntry   *entry;

	LWLockAcquire(LOCK_BY_INDEX(0), LW_SHARED);
	entry = (xid2GtidEntry *) hash_search(xid2gtid, &xid,
										  HASH_FIND, NULL);
	if (entry != NULL)
	{
		*gtid = entry->gtid;
	}
	else
	{
		// XXX: investigate how this assert happens
		// Assert(TransactionIdIsInProgress(xid));
		gtid->node = Mtm->my_node_id;
		gtid->xid = xid;
	}
	LWLockRelease(LOCK_BY_INDEX(0));
}


static void
MtmSerializeLock(PROCLOCK* proclock, void* arg)
{
	ByteBuffer* buf = (ByteBuffer*)arg;
	LOCK* lock = proclock->tag.myLock;
	PGPROC* proc = proclock->tag.myProc;
	GlobalTransactionId gtid;
	if (lock != NULL) {
		PGXACT* srcPgXact = &ProcGlobal->allPgXact[proc->pgprocno];

		if (TransactionIdIsValid(srcPgXact->xid) && proc->waitLock == lock) {
			LockMethod lockMethodTable = GetLocksMethodTable(lock);
			int numLockModes = lockMethodTable->numLockModes;
			int conflictMask = lockMethodTable->conflictTab[proc->waitLockMode];
			SHM_QUEUE *procLocks = &(lock->procLocks);
			int lm;

			MtmGetGtid(srcPgXact->xid, &gtid);	/* waiting transaction */

			ByteBufferAppend(buf, &gtid, sizeof(gtid));

			proclock = (PROCLOCK *) SHMQueueNext(procLocks, procLocks,
												 offsetof(PROCLOCK, lockLink));
			while (proclock)
			{
				if (proc != proclock->tag.myProc) {
					PGXACT* dstPgXact = &ProcGlobal->allPgXact[proclock->tag.myProc->pgprocno];
					if (TransactionIdIsValid(dstPgXact->xid)) {
						Assert(srcPgXact->xid != dstPgXact->xid);
						for (lm = 1; lm <= numLockModes; lm++)
						{
							if ((proclock->holdMask & LOCKBIT_ON(lm)) && (conflictMask & LOCKBIT_ON(lm)))
							{
								mtm_log(DeadlockSerialize, "%d: "XID_FMT"(%u) waits for "XID_FMT"(%u)",
										 MyProcPid, srcPgXact->xid, proc->pid,
										 dstPgXact->xid, proclock->tag.myProc->pid);
								MtmGetGtid(dstPgXact->xid, &gtid); /* transaction holding lock */
								ByteBufferAppend(buf, &gtid, sizeof(gtid));
								break;
							}
						}
					}
				}
				proclock = (PROCLOCK *) SHMQueueNext(procLocks, &proclock->lockLink,
													 offsetof(PROCLOCK, lockLink));
			}
			gtid.node = 0;
			gtid.xid = 0;
			ByteBufferAppend(buf, &gtid, sizeof(gtid)); /* end of lock owners list */
		}
	}
}

static bool
MtmDetectGlobalDeadLockForXid(TransactionId xid)
{
	bool hasDeadlock = false;
	ByteBuffer buf;
	MtmGraph graph;
	GlobalTransactionId gtid;
	int i;

	Assert(TransactionIdIsValid(xid));

	ByteBufferAlloc(&buf);
	EnumerateLocks(MtmSerializeLock, &buf);

	// Assert(replorigin_session_origin == InvalidRepOriginId);
	XLogFlush(LogLogicalMessage("L", buf.data, buf.used, false));

	MtmGraphInit(&graph);
	MtmGraphAdd(&graph, (GlobalTransactionId*)buf.data, buf.used/sizeof(GlobalTransactionId));
	ByteBufferFree(&buf);
	for (i = 0; i < ddd_shared->n_nodes; i++) {
		if (i+1 != Mtm->my_node_id && BIT_CHECK(MtmGetEnabledNodeMask(), i)) {
			size_t lockGraphSize;
			void* lockGraphData;

			LWLockAcquire(LOCK_BY_INDEX(i + 1), LW_SHARED);
			lockGraphSize = ddd_shared->nodelocks[i].lockGraphUsed;
			lockGraphData = palloc(lockGraphSize);
			memcpy(lockGraphData, ddd_shared->nodelocks[i].lockGraphData, lockGraphSize);
			LWLockRelease(LOCK_BY_INDEX(i + 1));

			if (lockGraphData == NULL) {
				return true;
			} else {
				MtmGraphAdd(&graph, (GlobalTransactionId*)lockGraphData, lockGraphSize/sizeof(GlobalTransactionId));
			}
		}
	}
	MtmGetGtid(xid, &gtid);
	hasDeadlock = MtmGraphFindLoop(&graph, &gtid);
	mtm_log(DeadlockCheck, "Distributed deadlock check by backend %d for %u:" XID_FMT " = %d",
		MyProcPid, gtid.node, gtid.xid, hasDeadlock);
	// if (!hasDeadlock) {
	// 	/* There is no deadlock loop in graph, but deadlock can be caused by lack of apply workers: if all of them are busy, then some transactions
	// 	 * can not be appied just because there are no vacant workers and it cause additional dependency between transactions which is not
	// 	 * refelected in lock graph
	// 	 */
	// 	timestamp_t lastPeekTime = minBgwGetLastPeekTime(&Mtm->pool);
	// 	if (lastPeekTime != 0 && MtmGetSystemTime() - lastPeekTime >= MSEC_TO_USEC(DeadlockTimeout)) {
	// 		hasDeadlock = true;
	// 		MTM_ELOG(WARNING, "Apply workers were blocked more than %d msec",
	// 			 (int)USEC_TO_MSEC(MtmGetSystemTime() - lastPeekTime));
	// 	} else {
	// 		MTM_LOG1("Enable deadlock timeout in backend %d for transaction %llu", MyProcPid, (long64)xid);
	// 		enable_timeout_after(DEADLOCK_TIMEOUT, DeadlockTimeout);
	// 	}
	// }

	if (!hasDeadlock)
	{
		// TimestampTz start_time = get_timeout_start_time(DEADLOCK_TIMEOUT);
		mtm_log(DeadlockCheck, "Enable deadlock timeout in backend %d for transaction " XID_FMT,
				MyProcPid, xid);
		enable_timeout_after(DEADLOCK_TIMEOUT, DeadlockTimeout);
		// set_timeout_start_time(DEADLOCK_TIMEOUT, start_time);
	}

	return hasDeadlock;
}

bool
MtmDetectGlobalDeadLock(PGPROC* proc)
{
	PGXACT* pgxact = &ProcGlobal->allPgXact[proc->pgprocno];

	mtm_log(DeadlockCheck, "Detect global deadlock for " XID_FMT " by backend %d", pgxact->xid, MyProcPid);

	if (!TransactionIdIsValid(pgxact->xid))
		return false;

	return MtmDetectGlobalDeadLockForXid(pgxact->xid);
}


Datum
mtm_dump_lock_graph(PG_FUNCTION_ARGS)
{
	StringInfo s = makeStringInfo();
	int i;
	for (i = 0; i < ddd_shared->n_nodes; i++)
	{
		size_t lockGraphSize;
		char  *lockGraphData;

		LWLockAcquire(LOCK_BY_INDEX(i + 1), LW_SHARED);
		lockGraphSize = ddd_shared->nodelocks[i].lockGraphUsed;
		lockGraphData = palloc(lockGraphSize);
		memcpy(lockGraphData, ddd_shared->nodelocks[i].lockGraphData, lockGraphSize);
		LWLockRelease(LOCK_BY_INDEX(i + 1));

		if (lockGraphData) {
			GlobalTransactionId *gtid = (GlobalTransactionId *) lockGraphData;
			GlobalTransactionId *last = (GlobalTransactionId *) (lockGraphData + lockGraphSize);
			appendStringInfo(s, "node-%d lock graph: ", i+1);
			while (gtid != last) {
				GlobalTransactionId *src = gtid++;
				appendStringInfo(s, "%d:"XID_FMT" -> ", src->node, src->xid);
				while (gtid->node != 0) {
					GlobalTransactionId *dst = gtid++;
					appendStringInfo(s, "%d:"XID_FMT", ", dst->node, dst->xid);
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
