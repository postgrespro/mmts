#ifndef __MULTIMASTER_H__
#define __MULTIMASTER_H__

#include "postgres.h"

#include "bytebuf.h"
#include "bgwpool.h"
#include "bkb.h"

#include "storage/lwlock.h"

#include "dmq.h"
#include "mm.h"
#include "resolver.h"

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


typedef ulong64 lsn_t;
#define INVALID_LSN  InvalidXLogRecPtr

// XXX! need rename: that's actually a disconnectivity mask
#define SELF_CONNECTIVITY_MASK  (Mtm->nodes[MtmNodeId-1].connectivityMask)
#define EFFECTIVE_CONNECTIVITY_MASK  ( SELF_CONNECTIVITY_MASK | Mtm->stoppedNodeMask | ~Mtm->clique )

#define MTM_MAX_NODES 16

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
	MSG_COMMITTED,
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

typedef struct
{
	MtmMessageCode code;   /* Message code: MSG_PREPARE, MSG_PRECOMMIT, MSG_COMMIT, MSG_ABORT,... */
	MtmTxState     state;
    int            node;   /* Sender node ID */
	TransactionId  dxid;   /* Transaction ID at destination node */
	TransactionId  sxid;   /* Transaction ID at sender node */
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
	MtmNodeInfo nodes[1];              /* [Mtm->nAllNodes]: per-node data */
} MtmState;

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

extern LWLock *MtmCommitBarrier;

extern void MtmXactCallback2(XactEvent event, void *arg);
extern bool MtmIsUserTransaction(void);
extern void MtmGenerateGid(char *gid, TransactionId xid);
extern int  MtmGidParseNodeId(const char* gid);
extern TransactionId MtmGidParseXid(const char* gid);

extern void MtmWaitForExtensionCreation(void);
extern void erase_option_from_connstr(const char *option, char *connstr);

extern void  MtmLock(LWLockMode mode);
extern void  MtmUnlock(void);
extern void  MtmLockNode(int nodeId, LWLockMode mode);
extern bool  MtmTryLockNode(int nodeId, LWLockMode mode);
extern void  MtmUnlockNode(int nodeId);

extern void  MtmSleep(timestamp_t interval);

extern void  MtmSetCurrentTransactionGID(char const* gid, int node_id);
extern void  MtmSetCurrentTransactionCSN(void);
extern TransactionId MtmGetCurrentTransactionId(void);
extern void MtmResetTransaction(void);

extern PGconn *PQconnectdb_safe(const char *conninfo, int timeout);

extern void MtmInitMessage(MtmArbiterMessage* msg, MtmMessageCode code);

extern void MtmUpdateControlFile(void);

extern void MtmCheckSlots(void);

#endif
