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
	TransactionId xid;			/* Transaction ID at origin node */
	TransactionId my_xid;			/* Transaction ID at our node */
} GlobalTransactionId;

typedef struct
{
	bool		contains_temp_ddl;
	bool		contains_persistent_ddl;
	bool		contains_dml;
	bool		distributed;
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
	MSG_PREPARED,
	MSG_PRECOMMITTED,
	MSG_COMMITTED,
	MSG_ABORTED
} MtmMessageCode;

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
	int			n_nodes;
	int			my_node_id;
	int			backup_node_id;
	XLogRecPtr	backup_end_lsn;
	MtmNode	nodes[MTM_MAX_NODES];
} MtmConfig;

extern MtmConfig *receiver_mtm_cfg;
extern bool receiver_mtm_cfg_valid;

typedef struct
{
	LWLock	   *lock;
	LWLock	   *syncpoint_lock;

	int			my_node_id;
	XLogRecPtr	latestSyncpoint;
	bool		localTablesHashLoaded;	/* Whether data from local_tables
										 * table is loaded in shared memory
										 * hash table */

	volatile slock_t cb_lock;
	int n_committers;
	int n_commit_holders;
	ConditionVariable commit_barrier_cv;

	ConditionVariable receiver_barrier_cv;

	struct {
		MtmReplicationMode	receiver_mode;
		pid_t				sender_pid;
		pid_t				receiver_pid;
		int					dmq_dest_id;
		XLogRecPtr			trim_lsn;
	} peers[MTM_MAX_NODES];
	BgwPool	pools[MTM_MAX_NODES];		/* [Mtm->nAllNodes]: per-node data */
} MtmShared;

extern MtmShared *Mtm;

/* XXX: to delete */
extern int	MtmReplicationNodeId;
extern MtmCurrentTrans MtmTx;
extern MemoryContext MtmApplyContext;

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
extern bool	MtmBreakConnection;

extern void MtmSleep(int64 interval);
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

extern bool MtmIsEnabled(void);

extern void MtmToggleReplication(void);

#endif							/* MULTIMASTER_H */
