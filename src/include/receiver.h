#ifndef MTM_RECEIVER_H
#define MTM_RECEIVER_H

#include "libpq-fe.h"

typedef enum
{
	REPLMODE_DISABLED,	/* stop the receiver */
	REPLMODE_RECOVERY,	/* pull changes of all origins */
	REPLMODE_NORMAL		/* pull only sender changes, apply in parallel */
} MtmReplicationMode;

extern char const *const MtmReplicationModeMnem[];

/*
 * Part of MtmReceiverContext used by both main receiver and parallel workers.
 * Exposed for bgwpool/apply needs.
 */
typedef struct
{
	int					sender_node_id;
	MtmReplicationMode	mode;
} MtmReceiverWorkerContext;

extern BackgroundWorkerHandle *MtmStartReceiver(int nodeId, Oid db_id, Oid user_id, pid_t monitor_pid);

extern void MtmExecutor(void *work, size_t size, MtmReceiverWorkerContext *rwctx);
extern void ApplyCancelHandler(SIGNAL_ARGS);
extern void MtmUpdateLsnMapping(int node_id, XLogRecPtr end_lsn);

extern void MtmBeginSession(int nodeId);
extern void MtmEndSession(int nodeId, bool unlock);

#endif
