/*----------------------------------------------------------------------------
 *
 * global_tx.h
 *	  Persistent and in-memory state necessary for our E3PC-like atomic commit
 #	  protocol.
 *
 * Copyright (c) 2016-2019, Postgres Professional
 *
 *----------------------------------------------------------------------------
 */
#ifndef GLOBAL_TX_H
#define GLOBAL_TX_H

#include "multimaster.h"

typedef struct
{
	int		ballot;
	int		node_id;
} GlobalTxTerm;

#define InvalidGTxTerm ((GlobalTxTerm) {0, 0})
/*
 * This term with ballot 1 and fake 0 node id is less than any term generated
 * by resolver; it is used by the coordinator itself.
 */
#define InitialGTxTerm ((GlobalTxTerm) {1, 0})

typedef enum
{
	GTXInvalid = 0,  /* we never gave a vote */
	GTXPreCommitted, /* voted for commit */
	GTXPreAborted,	 /* voted for abort */
	GTXCommitted,	 /* definitely know xact is committed */
	GTXAborted		 /* definitely know xact is aborted */
} GlobalTxStatus;

extern char const *const GlobalTxStatusMnem[];

typedef enum
{
	GTRS_AwaitStatus, /* 1a sent, wait for 1b */
	GTRS_AwaitAcks    /* 2a sent, wait for 2b */
} GlobalTxResolvingStage;

typedef struct
{
	GlobalTxTerm	proposal; /* nextBal in terms of The Part-Time Paliament */
	GlobalTxTerm	accepted; /* prevBal in terms of The Part-Time Paliament */
	GlobalTxStatus	status; /* prevDec in terms of The Part-Time Paliament */
} GTxState;

typedef struct
{
	char		gid[GIDSIZE];
	/*
	 * end_lsn of PREPARE record if this is my xact, origin_lsn if someone
	 * else'. We carry it with resolving requests to avoid double voting for
	 * the same xact.
	 */
	XLogRecPtr	coordinator_end_lsn;
	BackendId	acquired_by;
	/* paxos voting state for this xact */
	GTxState	state;
	/*
	 * True if we have actually PREPAREd the transaction. We might be
	 * requested to participate in resolving before PREPARE is applied; then
	 * this is false and voting state is persisted in gtx_proposals table
	 * instead of prepare's state_3pc.
	 *
	 * We must participate in resolving before PREPARE is eaten as otherwise
	 * resolving and recovery might deadlock each other, at least with >= 5
	 * nodes. e.g.
	 * - 1 prepares T1 on 1, 2, 3
	 * - 4 prepares conflicting T2 on 4, 5
	 * - Everyone fails, and only 2, 3, 5 go up. They can't recover without
	 *   xacts first because T1 and T2 conflict.
	 * There was an idea of using generations to unconditionally abort one
	 * of them (T2 in the example above), but it has never made its way fully.
	 *
	 * Current implementation has its pain points (issues of migrating voting
	 * state from gtx_proposals to PREPARE is very unpleasant), but OTOH it
	 * provides total separation between recovery and resolving (paxos) logic.
	 */
	bool		prepared;

	/* resolver things */
	bool		orphaned;	/* Indication for resolver that current tx needs
							 * to be picked up. Comes from a failed backend or
							 * a disabled node. */
	GTxState	phase1_acks[MTM_MAX_NODES];
	/*
	 * Technically phase2 ack contains just one term, which is acked. However,
	 * we 1) collect decrees (in 'status') to perform sanity checks
	 * 2) make it GTxState to reuse quorum() function.
	 */
	GTxState	phase2_acks[MTM_MAX_NODES];
	GlobalTxResolvingStage resolver_stage;
} GlobalTx;

typedef struct
{
	LWLock	   *lock;
	HTAB	   *gid2gtx;
} gtx_shared_data;

extern gtx_shared_data *gtx_shared;

void MtmGlobalTxInit(void);
void MtmGlobalTxShmemStartup(void);
GlobalTx *GlobalTxAcquire(const char *gid, bool create, bool *is_new);
void GlobalTxRelease(GlobalTx *gtx, bool remove);
void GlobalTxAtExit(int code, Datum arg);
void GlobalTxLoadAll(void);
char *serialize_gtx_state(GlobalTxStatus status, GlobalTxTerm term_prop,
						  GlobalTxTerm term_acc);
int term_cmp(GlobalTxTerm t1, GlobalTxTerm t2);
void parse_gtx_state(const char *state, GlobalTxStatus *status,
				GlobalTxTerm *term_prop, GlobalTxTerm *term_acc);
GlobalTxTerm GlobalTxGetMaxProposal(void);
void GlobalTxSaveInTable(const char *gid, XLogRecPtr coordinator_end_lsn,
						 GlobalTxStatus status,
						 GlobalTxTerm term_prop, GlobalTxTerm term_acc);
void GlobalTxDeleteFromTable(const char *gid);
void GlobalTxGCInTableProposals(void);
void GlobalTxMarkOrphaned(int node_id);

char *GlobalTxToString(GlobalTx *gtx);

#endif							/* GLOBAL_TX_H */
