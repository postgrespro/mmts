#ifndef __MULTIMASTER_H__
#define __MULTIMASTER_H__

#include "bytebuf.h"
#include "bgwpool.h"
#include "bkb.h"

#include "access/clog.h"
#include "pglogical_output/hooks.h"
#include "commands/vacuum.h"
#include "libpq-fe.h"

#include "dmq.h"
#include "mm.h"

#ifndef DEBUG_LEVEL
#define DEBUG_LEVEL 0
#endif


#define MTM_TAG "[MTM] "
#define MTM_ELOG(level,fmt,...) elog(level, MTM_TAG fmt, ## __VA_ARGS__)
#define MTM_ERRMSG(fmt,...)     errmsg(MTM_TAG fmt, ## __VA_ARGS__)

#if DEBUG_LEVEL == 0
#define MTM_LOG1(fmt, ...) ereport(LOG, \
								(errmsg("[MTM] " fmt, ## __VA_ARGS__), \
								errhidestmt(true), errhidecontext(true)))

#define MTM_LOG2(fmt, ...)
#define MTM_LOG3(fmt, ...)
#define MTM_LOG4(fmt, ...)
#elif  DEBUG_LEVEL == 1
#define MTM_LOG1(fmt, ...) fprintf(stderr, fmt "\n", ## __VA_ARGS__)
#define MTM_LOG2(fmt, ...) fprintf(stderr, fmt "\n", ## __VA_ARGS__)
#define MTM_LOG3(fmt, ...)
#define MTM_LOG4(fmt, ...)
#elif  DEBUG_LEVEL == 2
#define MTM_LOG1(fmt, ...) fprintf(stderr, fmt "\n", ## __VA_ARGS__)
#define MTM_LOG2(fmt, ...) fprintf(stderr, fmt "\n", ## __VA_ARGS__)
#define MTM_LOG3(fmt, ...) fprintf(stderr, fmt "\n", ## __VA_ARGS__)
#define MTM_LOG4(fmt, ...)
#elif  DEBUG_LEVEL >= 3
#define MTM_LOG1(fmt, ...) fprintf(stderr, fmt "\n", ## __VA_ARGS__)
#define MTM_LOG2(fmt, ...) fprintf(stderr, fmt "\n", ## __VA_ARGS__)
#define MTM_LOG3(fmt, ...) fprintf(stderr, fmt "\n", ## __VA_ARGS__)
#define MTM_LOG4(fmt, ...) fprintf(stderr, fmt "\n", ## __VA_ARGS__)
#endif

// #define MTM_TXFINISH 1

#ifndef MTM_TXFINISH
#define TXFINISH(fmt, ...)
#else
#define TXFINISH(fmt, ...) elog(LOG, MTM_TAG "[TXFINISH] " fmt, ## __VA_ARGS__)
#endif

// #define MTM_TRACE 1

#ifndef MTM_TRACE
#define MTM_TXTRACE(tx, event, ...)
#else
#define MTM_TXTRACE(tx, event, ...) \
		elog(LOG, MTM_TAG "%s, %lld, %u " event "\n", tx->gid, (long long)MtmGetSystemTime(), MyProcPid, ## __VA_ARGS__)
#endif

#define MULTIMASTER_NAME                 "multimaster"
#define MULTIMASTER_SLOT_PATTERN         "mtm_slot_%d"
#define MULTIMASTER_MIN_PROTO_VERSION    1
#define MULTIMASTER_MAX_PROTO_VERSION    1
#define MULTIMASTER_MAX_GID_SIZE         42
#define MULTIMASTER_MAX_SLOT_NAME_SIZE   16
#define MULTIMASTER_MAX_CONN_STR_SIZE    128
#define MULTIMASTER_MAX_HOST_NAME_SIZE   64
#define MULTIMASTER_MAX_CTL_STR_SIZE     256
#define MULTIMASTER_LOCK_BUF_INIT_SIZE   4096
#define MULTIMASTER_BROADCAST_SERVICE    "mtm_broadcast"
#define MULTIMASTER_ADMIN                "mtm_admin"
#define MULTIMASTER_PRECOMMITTED         "precommitted"
#define MULTIMASTER_PREABORTED           "preaborted"

#define MULTIMASTER_DEFAULT_ARBITER_PORT 5433

#define MB ((size_t)1024*1024)

#define USEC_TO_MSEC(t) ((t)/1000)
#define MSEC_TO_USEC(t) ((timestamp_t)(t)*1000)

#define Natts_mtm_ddl_log 2
#define Anum_mtm_ddl_log_issued		1
#define Anum_mtm_ddl_log_query		2

#define Natts_mtm_trans_state   15
#define Natts_mtm_nodes_state   17
#define Natts_mtm_cluster_state 20

typedef ulong64 lsn_t;
#define INVALID_LSN  InvalidXLogRecPtr

// XXX! need rename: that's actually a disconnectivity mask
#define SELF_CONNECTIVITY_MASK  (Mtm->nodes[MtmNodeId-1].connectivityMask)
#define EFFECTIVE_CONNECTIVITY_MASK  ( SELF_CONNECTIVITY_MASK | Mtm->stoppedNodeMask | ~Mtm->clique )

#define MTM_MAX_NODES 16

typedef enum
{
	MtmTxUnknown		= (1<<0),
	MtmTxNotFound		= (1<<1),
	MtmTxInProgress		= (1<<2),
	MtmTxPrepared		= (1<<3),
	MtmTxPreCommited	= (1<<4),
	MtmTxPreAborted		= (1<<5),
	MtmTxCommited		= (1<<6),
	MtmTxAborted		= (1<<7)
} MtmTxState;

typedef int MtmTxStateMask;

typedef enum
{
	PGLOGICAL_COMMIT,
	PGLOGICAL_PREPARE,
	PGLOGICAL_COMMIT_PREPARED,
	PGLOGICAL_ABORT_PREPARED,
	PGLOGICAL_PRECOMMIT_PREPARED
} PGLOGICAL_EVENT;


#define EQUAL_GTID(x,y) ((x).node == (y).node && (x).xid == (y).xid)

typedef enum
{
	MSG_INVALID,
	MSG_HANDSHAKE,
	MSG_PREPARED,
	MSG_PRECOMMIT,
	MSG_PRECOMMITTED,
	MSG_ABORTED,
	MSG_STATUS,
	MSG_HEARTBEAT,
	MSG_POLL_REQUEST,
	MSG_POLL_STATUS
} MtmMessageCode;

typedef enum
{
	MTM_DISABLED,       /* Node disabled */
	MTM_RECOVERY,       /* Node is in recovery process */
	MTM_RECOVERED,      /* Node is recovered by is not yet switched to ONLINE because
						 * not all sender/receivers are restarted
						 */
	MTM_ONLINE          /* Ready to receive client's queries */
} MtmNodeStatus;

typedef enum
{
	REPLMODE_EXIT,         /* receiver should exit */
	REPLMODE_RECOVERED,    /* recovery of receiver node is completed so drop old slot and restart replication from the current position in WAL */
	REPLMODE_RECOVERY,     /* perform recovery of the node by applying all data from the slot from specified point */
	REPLMODE_CREATE_NEW,   /* destination node is recovered: drop old slot and restart from roveredLsn position */
	REPLMODE_OPEN_EXISTED  /* normal mode: use existed slot or create new one and start receiving data from it from the remembered position */
} MtmReplicationMode;

typedef struct
{
	MtmMessageCode code;   /* Message code: MSG_PREPARE, MSG_PRECOMMIT, MSG_COMMIT, MSG_ABORT,... */
	MtmTxState     state;
    int            node;   /* Sender node ID */
	TransactionId  dxid;   /* Transaction ID at destination node */
	TransactionId  sxid;   /* Transaction ID at sender node */
    XidStatus      status; /* Transaction status */
	nodemask_t     connectivityMask; /* Connectivity bitmask at the sender of message */
	pgid_t         gid;    /* Global transaction identifier */
} MtmArbiterMessage;

typedef struct
{
	char hostName[MULTIMASTER_MAX_HOST_NAME_SIZE];
	char connStr[MULTIMASTER_MAX_CONN_STR_SIZE];
	int arbiterPort;
	int postmasterPort;
} MtmConnectionInfo;

typedef struct
{
	Oid sourceTable;
	nodemask_t targetNodes;
} MtmCopyRequest;

typedef struct
{
	MtmConnectionInfo con;
	/* Pool of background workers for applying logical replication */
	BgwPool pool;
	nodemask_t  connectivityMask;      /* Connectivity mask at this node */
	int         senderPid;
	int         receiverPid;
	lsn_t       flushPos;
	lsn_t       restartLSN;
	RepOriginId originId;
	int         timeline;
	bool		manualRecovery;
	DmqDestinationId	destination_id;
} MtmNodeInfo;

typedef struct
{
	bool extension_created;
	MtmNodeStatus status;              /* Status of this node */
	char *statusReason;                /* A human-readable description of why the current status was set */
	int recoverySlot;                  /* NodeId of recovery slot or 0 if none */
	LWLockPadded *locks;               /* multimaster lock tranche */
	nodemask_t disabledNodeMask;       /* Bitmask of disabled nodes */
	nodemask_t clique;                 /* Bitmask of nodes that are connected and we allowed to connect/send wal/receive wal with them */
	bool       refereeGrant;           /* Referee allowed us to work with half of the nodes */
	int        refereeWinnerId;        /* Node that won referee contest */
	nodemask_t stalledNodeMask;        /* Bitmask of stalled nodes (node with dropped replication slot which makes it not possible automatic recovery of such node) */
	nodemask_t stoppedNodeMask;        /* Bitmask of stopped (permanently disabled nodes) */
	nodemask_t pglogicalReceiverMask;  /* bitmask of started pglogic receivers */
	nodemask_t pglogicalSenderMask;    /* bitmask of started pglogic senders */
	bool   localTablesHashLoaded;      /* Whether data from local_tables table is loaded in shared memory hash table */
	int    nAllNodes;                  /* Total number of nodes */
	int    recoveryCount;              /* Number of completed recoveries */
	int    donorNodeId;                /* Cluster node from which this node was populated */
	lsn_t recoveredLSN;                /* LSN at the moment of recovery completion */
	MtmNodeInfo nodes[1];              /* [Mtm->nAllNodes]: per-node data */
} MtmState;

typedef struct MtmFlushPosition
{
	dlist_node node;
	int        node_id;
	lsn_t      local_end;
	lsn_t      remote_end;
} MtmFlushPosition;


extern char const* const MtmNodeStatusMnem[];
extern char const* const MtmTxnStatusMnem[];
extern char const* const MtmMessageKindMnem[];

extern MtmState* Mtm;

extern int   MtmReplicationNodeId;
extern int   MtmNodes;
extern char* MtmDatabaseName;
extern int   MtmTransSpillThreshold;
extern int   MtmHeartbeatSendTimeout;
extern int   MtmHeartbeatRecvTimeout;
extern bool  MtmPreserveCommitOrder;

extern MtmConnectionInfo* MtmConnections;
extern bool MtmMajorNode;
extern bool MtmBackgroundWorker;
extern char* MtmRefereeConnStr;
extern bool MtmIsRecoverySession;

extern void  MtmXactCallback2(XactEvent event, void *arg);
extern void  MtmMonitorInitialize(void);
extern bool MtmIsUserTransaction(void);
extern void MtmGenerateGid(char *gid, TransactionId xid);
extern int  MtmGidParseNodeId(const char* gid);
extern TransactionId MtmGidParseXid(const char* gid);

extern void MtmWaitForExtensionCreation(void);
extern void erase_option_from_connstr(const char *option, char *connstr);

extern void ResolverMain(void);
extern void ResolverInit(void);
extern void ResolveTransactionsForNode(int node_id);
extern void ResolveAllTransactions(void);
extern char *MtmTxStateMnem(MtmTxState state);

extern void  MtmStartReceivers(void);
extern void  MtmStartReceiver(int nodeId, bool dynamic);

extern MtmReplicationMode MtmGetReplicationMode(int nodeId);
extern void  MtmExecutor(void* work, size_t size);



extern void  MtmLock(LWLockMode mode);
extern void  MtmUnlock(void);
extern void  MtmLockNode(int nodeId, LWLockMode mode);
extern bool  MtmTryLockNode(int nodeId, LWLockMode mode);
extern void  MtmUnlockNode(int nodeId);

extern void  MtmStopNode(int nodeId, bool dropSlot);
extern void  MtmRecoverNode(int nodeId);
extern void  MtmResumeNode(int nodeId);

extern void  MtmSleep(timestamp_t interval);

extern void  MtmSetCurrentTransactionGID(char const* gid, int node_id);

extern void  MtmSetCurrentTransactionCSN(void);

extern TransactionId MtmGetCurrentTransactionId(void);
extern XidStatus MtmGetCurrentTransactionStatus(void);

extern bool  MtmIsRecoveredNode(int nodeId);
extern void  MtmUpdateNodeConnectionInfo(MtmConnectionInfo* conn, char const* connStr);
extern void  MtmSetupReplicationHooks(struct PGLogicalHooks* hooks);
extern bool  MtmRecoveryCaughtUp(int nodeId, lsn_t walEndPtr);
extern void  MtmCheckRecoveryCaughtUp(int nodeId, lsn_t slotLSN);

extern void  MtmHandleApplyError(void);

extern void  MtmUpdateLsnMapping(int nodeId, lsn_t endLsn);
extern lsn_t MtmGetFlushPosition(int nodeId);

extern void MtmResetTransaction(void);
extern void MtmReleaseRecoverySlot(int nodeId);
extern PGconn *PQconnectdb_safe(const char *conninfo, int timeout);
extern void MtmBeginSession(int nodeId);
extern void MtmEndSession(int nodeId, bool unlock);

extern bool MtmFilterTransaction(char* record, int size);

extern void MtmInitMessage(MtmArbiterMessage* msg, MtmMessageCode code);

extern void MtmRefereeInitialize(void);
extern void MtmUpdateControlFile(void);

extern void MtmCheckSlots(void);

#endif
