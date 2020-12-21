#ifndef __DDD_H__
#define __DDD_H__

extern bool MtmDetectGlobalDeadLock(PGPROC *proc);

#if 0
extern void MtmDeadlockDetectorInit(int n_nodes);
extern void MtmDeadlockDetectorShmemStartup(int n_nodes);
extern void MtmUpdateLockGraph(int nodeId, void const *messageBody, int messageSize);
extern void MtmDeadlockDetectorRemoveXact(TransactionId xid);
extern void MtmDeadlockDetectorAddXact(TransactionId xid, GlobalTransactionId *gtid);
#endif

#endif
