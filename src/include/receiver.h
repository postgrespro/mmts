#ifndef MTM_RECEIVER_H
#define MTM_RECEIVER_H

typedef struct
{
	int node_id;
	bool is_recovery;
	bool parallel_allowed;
	uint64 session_id;
	XLogRecPtr end_lsn;
} MtmReceiverContext;

extern BackgroundWorkerHandle *MtmStartReceiver(int nodeId, Oid db_id, Oid user_id, pid_t monitor_pid);

extern void MtmExecutor(void* work, size_t size, MtmReceiverContext *rctx);
extern void MtmUpdateLsnMapping(int node_id, XLogRecPtr end_lsn);

extern void MtmBeginSession(int nodeId);
extern void MtmEndSession(int nodeId, bool unlock);

extern void MtmReceiverCreateSlot(char *conninfo, int my_node_id);

#endif