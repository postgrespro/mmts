#ifndef MTM_RECEIVER_H
#define MTM_RECEIVER_H

typedef struct
{
	int node_id;
	bool is_recovery;
	bool parallel_allowed;
	TimestampTz session_id;
	XLogRecPtr end_lsn;
} MtmReceiverContext;

extern void MtmStartReceiver(int nodeId, Oid db_id, Oid user_id);

extern void MtmExecutor(void* work, size_t size, MtmReceiverContext *rctx);
extern void MtmUpdateLsnMapping(int node_id, XLogRecPtr end_lsn);

extern void MtmBeginSession(int nodeId);
extern void MtmEndSession(int nodeId, bool unlock);

#endif