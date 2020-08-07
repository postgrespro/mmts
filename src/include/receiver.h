#ifndef MTM_RECEIVER_H
#define MTM_RECEIVER_H

#include "libpq-fe.h"

typedef enum
{
	REPLMODE_DISABLED,	/* stop the receiver */
	REPLMODE_RECOVERY,	/* pull changes of all origins */
	REPLMODE_NORMAL		/* pull only sender changes, apply in parallel */
} MtmReplicationMode;

#define BGW_POOL_BY_NODE_ID(node_id) (&Mtm->pools[(node_id) - 1])

extern char const *const MtmReplicationModeMnem[];

/* forward decl to avoid including global_tx.h */
struct GlobalTx;

/*
 * Part of MtmReceiverContext used by both main receiver and parallel workers.
 * Exposed for bgwpool/apply needs.
 */
typedef struct
{
	int					sender_node_id;
	MtmReplicationMode	mode;
	/* allows to release gtx on ERROR in apply */
	struct GlobalTx		*gtx;
	/*
	 * For parallel workers: position of current job in txlist.
	 */
	int					txlist_pos;
	/*
	 * Info about xact currently being executed
	 */
	TransactionId		origin_xid;
	TransactionId		my_xid;
	/*
	 * true means this is xact with plain commit, so we cannot ignore
	 * apply failure
	 */
	bool				bdr_like;
} MtmReceiverWorkerContext;

extern void MtmExecutor(void *work, size_t size, MtmReceiverWorkerContext *rwctx);
extern void ApplyCancelHandler(SIGNAL_ARGS);
extern void MtmUpdateLsnMapping(int node_id, XLogRecPtr end_lsn);

extern void MtmBeginSession(int nodeId);
extern void MtmEndSession(int nodeId, bool unlock);

#endif
