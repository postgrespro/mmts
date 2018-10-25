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
#ifndef MM_H
#define MM_H

#include "postgres.h"

#include "access/xact.h"

#define MULTIMASTER_SCHEMA_NAME          "mtm"
#define MULTIMASTER_LOCAL_TABLES_TABLE   "local_tables"

#define MAX_NODES 64

typedef char pgid_t[GIDSIZE];

/* Identifier of global transaction */
typedef struct
{
	int node;          /* One based id of node initiating transaction */
	TransactionId xid; /* Transaction ID at this node */
} GlobalTransactionId;

typedef struct {
	TransactionId xid;	  /* local transaction ID	*/
	// GlobalTransactionId gtid; /* global transaction ID assigned by coordinator of transaction */
	bool  isTwoPhase;	  /* user level 2PC */
	// bool  isReplicated;	  /* transaction on replica */
	bool  isDistributed;  /* transaction performed INSERT/UPDATE/DELETE and has to be replicated to other nodes */
	// bool  isPrepared;	  /* transaction is prepared at first stage of 2PC */
	// bool  isSuspended;	  /* prepared transaction is suspended because coordinator node is switch to offline */
	bool  isTransactionBlock; /* is transaction block */
	bool  containsDML;	  /* transaction contains DML statements */
	// bool  isActive;		  /* transaction is active (nActiveTransaction counter is incremented) */
	// XidStatus status;	  /* transaction status */
	// csn_t snapshot;		  /* transaction snapshot */
	// csn_t csn;			  /* CSN */
	pgid_t gid;			  /* global transaction identifier (used by 2pc) */
} MtmCurrentTrans;


typedef struct MtmSeqPosition
{
	Oid		seqid;
	int64	next;
} MtmSeqPosition;

/* XXX: drop that */
typedef long long long64; /* we are not using int64 here because we want to use %lld format for this type */
typedef unsigned long long ulong64; /* we are not using uint64 here because we want to use %lld format for this type */

typedef ulong64 nodemask_t;


#define BIT_CHECK(mask, bit) (((mask) & ((nodemask_t)1 << (bit))) != 0)
#define BIT_CLEAR(mask, bit) (mask &= ~((nodemask_t)1 << (bit)))
#define BIT_SET(mask, bit)   (mask |= ((nodemask_t)1 << (bit)))
#define ALL_BITS ((nodemask_t)~0)

extern bool				MtmIsLogicalReceiver;
extern bool				MtmBackgroundWorker;
extern int				MtmMaxNodes;
extern int				MtmNodeId;
extern MtmCurrentTrans	MtmTx;
extern MemoryContext	MtmApplyContext;
extern char			   *MtmDatabaseUser;


extern bool		MtmTwoPhaseCommit(MtmCurrentTrans* x);



#endif							/* MM_H */