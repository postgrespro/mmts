/*-------------------------------------------------------------------------
 *
 * mm.h
 *	General definitions
 *
 *
 * Portions Copyright (c) 1996-2018, PostgreSQL Global Development Group
 * Portions Copyright (c) 1994, Regents of the University of California
 *
 *-------------------------------------------------------------------------
 */
#ifndef MULTIMASTER_H
#define MULTIMASTER_H

#include "postgres.h"

#include "storage/lwlock.h"
#include "access/xact.h"

#include "bgwpool.h"
#include "dmq.h"				/* DmqDestinationId */
#include "resolver.h"			/* XXX: rework message and get rid of this */

#define MULTIMASTER_SCHEMA_NAME          "mtm"
#define MULTIMASTER_NAME                 "multimaster"
#define MULTIMASTER_SLOT_PATTERN         "mtm_slot_%d"
#define MULTIMASTER_RECOVERY_SLOT_PATTERN "mtm_recovery_slot_%d"
#define MULTIMASTER_LOCAL_TABLES_TABLE   "local_tables"
#define MULTIMASTER_MAX_CONN_STR_SIZE    128
#define MULTIMASTER_MAX_HOST_NAME_SIZE   64
#define MULTIMASTER_MAX_CTL_STR_SIZE     256
#define MULTIMASTER_BROADCAST_SERVICE    "mtm_broadcast"
#define MULTIMASTER_ADMIN                "mtm_admin"
#define MULTIMASTER_PRECOMMITTED         "precommitted"
#define MULTIMASTER_PREABORTED           "preaborted"
#define MULTIMASTER_SYNCPOINT_INTERVAL    10*1024*1024

#define MTM_MAX_NODES 16

typedef char pgid_t[GIDSIZE];
typedef uint64 nodemask_t;

#define LSN_FMT "%" INT64_MODIFIER "x"

#define BIT_CHECK(mask, bit) (((mask) & ((nodemask_t)1 << (bit))) != 0)
#define BIT_CLEAR(mask, bit) (mask &= ~((nodemask_t)1 << (bit)))
#define BIT_SET(mask, bit)   (mask |= ((nodemask_t)1 << (bit)))
#define ALL_BITS ((nodemask_t)~0)

#define EQUAL_GTID(x,y) ((x).node == (y).node && (x).xid == (y).xid)


/* Identifier of global transaction */
typedef struct
{
	int			node;			/* One based id of node initiating transaction */
	TransactionId xid;			/* Transaction ID at this node */
} GlobalTransactionId;

typedef struct
{
	TransactionId xid;			/* local transaction ID	*/
	bool		isTwoPhase;		/* user level 2PC */
	bool		isDistributed;	/* transaction performed INSERT/UPDATE/DELETE
								 * and has to be replicated to other nodes */
	bool		containsDML;	/* transaction contains DML statements */
	pgid_t		gid;			/* global transaction identifier (used by 2pc) */
} MtmCurrentTrans;

typedef struct MtmSeqPosition
{
	Oid			seqid;
	int64		next;
} MtmSeqPosition;

typedef enum
{
	PGLOGICAL_COMMIT,
	PGLOGICAL_PREPARE,
	PGLOGICAL_COMMIT_PREPARED,
	PGLOGICAL_ABORT_PREPARED,
	PGLOGICAL_PRECOMMIT_PREPARED
} PGLOGICAL_EVENT;

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
	MTM_DISABLED,				/* Node disabled */
	MTM_RECOVERY,				/* Node is in recovery process */
	MTM_RECOVERED,				/* Node is recovered by is not yet switched to
								 * ONLINE because not all sender/receivers are
								 * restarted */
	MTM_ONLINE					/* Ready to receive client's queries */
} MtmNodeStatus;

typedef struct
{
	MtmMessageCode code;		/* Message code: MSG_PREPARE, MSG_PRECOMMIT,
								 * MSG_COMMIT, MSG_ABORT,... */
	MtmTxState	state;
	int			node;			/* Sender node ID */
	TransactionId dxid;			/* Transaction ID at destination node */
	TransactionId sxid;			/* Transaction ID at sender node */
	nodemask_t	connectivityMask;	/* Connectivity bitmask at the sender of
									 * message */
	pgid_t		gid;			/* Global transaction identifier */
} MtmArbiterMessage;

typedef struct
{
	char		hostName[MULTIMASTER_MAX_HOST_NAME_SIZE];
	char		connStr[MULTIMASTER_MAX_CONN_STR_SIZE];
	int			postmasterPort;
} MtmConnectionInfo;

typedef struct
{
	Oid			sourceTable;
	nodemask_t	targetNodes;
} MtmCopyRequest;

typedef struct
{
	MtmConnectionInfo con;
	/* Pool of background workers for applying logical replication */
	BgwPool		pool;
	nodemask_t	connectivityMask;	/* Connectivity mask at this node */
	int			senderPid;
	int			receiverPid;
	XLogRecPtr	flushPos;
	RepOriginId originId;
	int			timeline;
	bool		manualRecovery;
	DmqDestinationId destination_id;
} MtmNodeInfo;

typedef struct
{
	bool		extension_created;
	bool		stop_new_commits;
	bool		recovered;
	XLogRecPtr	latestSyncpoint;
	MtmNodeStatus status;		/* Status of this node */
	char	   *statusReason;	/* A human-readable description of why the
								 * current status was set */
	int			recoverySlot;	/* NodeId of recovery slot or 0 if none */
	LWLockPadded *locks;		/* multimaster lock tranche */
	nodemask_t	disabledNodeMask;	/* Bitmask of disabled nodes */
	nodemask_t	clique;			/* Bitmask of nodes that are connected and we
								 * allowed to connect/send wal/receive wal
								 * with them */
	bool		refereeGrant;	/* Referee allowed us to work with half of the
								 * nodes */
	int			refereeWinnerId;	/* Node that won referee contest */
	nodemask_t	stalledNodeMask;	/* Bitmask of stalled nodes (node with
									 * dropped replication slot which makes it
									 * not possible automatic recovery of such
									 * node) */
	nodemask_t	stoppedNodeMask;	/* Bitmask of stopped (permanently
									 * disabled nodes) */
	nodemask_t	pglogicalReceiverMask;	/* bitmask of started pglogic
										 * receivers */
	nodemask_t	pglogicalSenderMask;	/* bitmask of started pglogic senders */
	bool		localTablesHashLoaded;	/* Whether data from local_tables
										 * table is loaded in shared memory
										 * hash table */
	int			nAllNodes;		/* Total number of nodes */
	int			recoveryCount;	/* Number of completed recoveries */
	int			donorNodeId;	/* Cluster node from which this node was
								 * populated */
	MtmNodeInfo nodes[1];		/* [Mtm->nAllNodes]: per-node data */
} MtmState;

extern MtmState *Mtm;

/* XXX: to delete */
extern int	MtmReplicationNodeId;
extern int	MtmNodes;
extern char *MtmDatabaseName;
extern MtmConnectionInfo *MtmConnections;
extern MtmCurrentTrans MtmTx;
extern MemoryContext MtmApplyContext;
extern char *MtmDatabaseUser;

/* Locks */
extern LWLock *MtmCommitBarrier;
extern LWLock *MtmReceiverBarrier;
extern LWLock *MtmSyncpointLock;

/* bgworker identities */
extern bool MtmBackgroundWorker;
extern bool MtmIsLogicalReceiver;
extern bool MtmIsReceiver;
extern bool MtmIsPoolWorker;

/* GUCs */
extern int	MtmTransSpillThreshold;
extern int	MtmHeartbeatSendTimeout;
extern int	MtmHeartbeatRecvTimeout;
extern bool MtmPreserveCommitOrder;
extern bool MtmMajorNode;
extern char *MtmRefereeConnStr;
extern int	MtmMaxWorkers;
extern int	MtmMaxNodes;
extern int	MtmNodeId;


/*  XXX! need rename: that's actually a disconnectivity mask */
#define SELF_CONNECTIVITY_MASK  (Mtm->nodes[MtmNodeId-1].connectivityMask)
#define EFFECTIVE_CONNECTIVITY_MASK  ( SELF_CONNECTIVITY_MASK | Mtm->stoppedNodeMask | ~Mtm->clique )


extern bool MtmTwoPhaseCommit(MtmCurrentTrans *x);
extern void MtmXactCallback2(XactEvent event, void *arg);
extern void MtmGenerateGid(char *gid, TransactionId xid);
extern int	MtmGidParseNodeId(const char *gid);
extern TransactionId MtmGidParseXid(const char *gid);
extern void MtmWaitForExtensionCreation(void);
extern void MtmSleep(timestamp_t interval);
extern void MtmUpdateControlFile(void);
extern void MtmCheckSlots(void);
extern TimestampTz MtmGetIncreasingTimestamp(void);
extern bool MtmAllApplyWorkersFinished(void);


/* XXX: to delete */
extern bool MtmIsUserTransaction(void);
extern void erase_option_from_connstr(const char *option, char *connstr);

extern void MtmLock(LWLockMode mode);
extern void MtmUnlock(void);
extern void MtmLockNode(int nodeId, LWLockMode mode);
extern bool MtmTryLockNode(int nodeId, LWLockMode mode);
extern void MtmUnlockNode(int nodeId);

extern void MtmSetCurrentTransactionGID(char const *gid, int node_id);
extern void MtmSetCurrentTransactionCSN(void);
extern TransactionId MtmGetCurrentTransactionId(void);
extern void MtmResetTransaction(void);

extern PGconn *PQconnectdb_safe(const char *conninfo, int timeout);

extern void MtmInitMessage(MtmArbiterMessage *msg, MtmMessageCode code);



#endif							/* MULTIMASTER_H */
