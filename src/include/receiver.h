#ifndef MTM_RECEIVER_H
#define MTM_RECEIVER_H

// #include "multimaster.h"
// #include "mm.h"

typedef struct
{
	int node_id;
	bool is_recovery;
	bool parallel_allowed;
	TimestampTz session_id;
	XLogRecPtr end_lsn;
} MtmReceiverContext;

extern void MtmStartReceivers(void);
extern void MtmStartReceiver(int nodeId, bool dynamic);

extern void MtmExecutor(void* work, size_t size, MtmReceiverContext *rctx);
extern void MtmUpdateLsnMapping(int node_id, XLogRecPtr end_lsn);

extern void MtmBeginSession(int nodeId);
extern void MtmEndSession(int nodeId, bool unlock);

#endif