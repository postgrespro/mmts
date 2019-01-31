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
// XXX: change to one NODENAME_FMT
#define MTM_SUBNAME_FMT					 "mtm_sub_%d"
#define MTM_DMQNAME_FMT					 "node%d"
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


/*
 * Definitions for the "mtm.cluster_nodes" table.
 */
#define MTM_NODES					"mtm.cluster_nodes"
#define Natts_mtm_nodes				4
#define Anum_mtm_nodes_id			1	/* node_id, same accross cluster */
#define Anum_mtm_nodes_connifo		2	/* connection string */
#define Anum_mtm_nodes_is_self		3	/* is that tuple for our node? */
#define Anum_mtm_nodes_init_done	4	/* did monitor already create slots? */

/*
 * Definitions for the "mtm.cluster_status" type.
 */
#define Natts_mtm_status			5
#define Anum_mtm_status_node_id		1
#define Anum_mtm_status_status		2
#define Anum_mtm_status_n_nodes		3
#define Anum_mtm_status_n_connected	4
#define Anum_mtm_status_n_enabled	5

/*
 * Definitions for the "mtm.node_info" type.
 */
#define Natts_mtm_node_info					6
#define Anum_mtm_node_info_enabled			1
#define Anum_mtm_node_info_connected		2
#define Anum_mtm_node_info_sender_pid		3
#define Anum_mtm_node_info_receiver_pid		4
#define Anum_mtm_node_info_n_workers		5
#define Anum_mtm_node_info_receiver_status	6


/* Identifier of global transaction */
typedef struct
{
	int			node;			/* One based id of node initiating transaction */
	TransactionId xid;			/* Transaction ID at this node */
} GlobalTransactionId;

typedef struct
{
	bool		contains_ddl;
	bool		contains_dml;	/* transaction contains DML statements */
	bool		accessed_temp;
	bool		explicit_twophase;	/* user level 2PC */
	bool		distributed;
	pgid_t		gid;			/* global transaction identifier (only in case
								 * of explicit_twophase) */
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
	Oid			sourceTable;
	nodemask_t	targetNodes;
} MtmCopyRequest;

typedef struct
{
	int			node_id;
	char	   *conninfo;
	RepOriginId	origin_id;
	bool		init_done;
} MtmNode;

typedef struct
{
	int		n_nodes;
	int		my_node_id;
	MtmNode	nodes[MTM_MAX_NODES];
} MtmConfig;

extern MtmConfig *receiver_mtm_cfg;
extern bool receiver_mtm_cfg_valid;

typedef struct
{
	int			my_node_id;
	bool		stop_new_commits;
	bool		recovered;
	XLogRecPtr	latestSyncpoint;
	MtmNodeStatus status;		/* Status of this node */
	char	   *statusReason;	/* A human-readable description of why the
								 * current status was set */
	int			recoverySlot;	/* NodeId of recovery slot or 0 if none */
	LWLockPadded *locks;		/* multimaster lock tranche */
	nodemask_t	selfConnectivityMask;
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
	int			dmq_dest_ids[MTM_MAX_NODES];
	BgwPool		pools[FLEXIBLE_ARRAY_MEMBER];		/* [Mtm->nAllNodes]: per-node data */
} MtmState;

extern MtmState *Mtm;

/* XXX: to delete */
extern int	MtmReplicationNodeId;
extern MtmCurrentTrans MtmTx;
extern MemoryContext MtmApplyContext;

/* Locks */
extern LWLock *MtmCommitBarrier;
extern LWLock *MtmReceiverBarrier;
extern LWLock *MtmSyncpointLock;

/* bgworker identities */
extern bool MtmBackgroundWorker;
extern bool MtmIsLogicalReceiver;
extern bool MtmIsReceiver;
extern bool MtmIsPoolWorker;
extern bool MtmIsMonitorWorker;

/* GUCs */
extern int	MtmTransSpillThreshold;
extern int	MtmHeartbeatSendTimeout;
extern int	MtmHeartbeatRecvTimeout;
extern char *MtmRefereeConnStr;
extern int	MtmMaxWorkers;
extern int	MtmMaxNodes;

/*  XXX! need rename: that's actually a disconnectivity mask */
#define SELF_CONNECTIVITY_MASK  (Mtm->selfConnectivityMask)
#define EFFECTIVE_CONNECTIVITY_MASK  ( SELF_CONNECTIVITY_MASK | Mtm->stoppedNodeMask | ~Mtm->clique )

extern bool MtmTwoPhaseCommit(void);
extern void MtmBeginTransaction(void);

extern void MtmXactCallback2(XactEvent event, void *arg);
extern void MtmGenerateGid(char *gid, TransactionId xid, int node_id);
extern int	MtmGidParseNodeId(const char *gid);
extern TransactionId MtmGidParseXid(const char *gid);
extern void MtmSleep(timestamp_t interval);
extern TimestampTz MtmGetIncreasingTimestamp(void);
extern bool MtmAllApplyWorkersFinished(void);
extern MtmConfig *MtmLoadConfig(void);

typedef void (*mtm_cfg_change_cb)(int node_id, MtmConfig *new_cfg, Datum arg);

extern MtmConfig *MtmReloadConfig(MtmConfig *old_cfg,
								  mtm_cfg_change_cb node_add_cb,
								  mtm_cfg_change_cb node_drop_cb,
								  Datum arg);
extern MtmNode *MtmNodeById(MtmConfig *cfg, int node_id);

extern void MtmStateFill(MtmConfig *cfg);

extern void MtmLock(LWLockMode mode);
extern void MtmUnlock(void);
extern void MtmLockNode(int nodeId, LWLockMode mode);
extern bool MtmTryLockNode(int nodeId, LWLockMode mode);
extern void MtmUnlockNode(int nodeId);

extern void MtmInitMessage(MtmArbiterMessage *msg, MtmMessageCode code);

extern bool MtmIsEnabled(void);

#endif							/* MULTIMASTER_H */
