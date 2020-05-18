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

typedef struct GlobalTx
{
	char		gid[GIDSIZE];
	XLogRecPtr	coordinator_end_lsn;
	BackendId	acquired_by;
	/* paxos voting state for this xact */
	GTxState	state;
	/* transient thing used to rm shmem entry on error */
	bool		prepared;

	/* resolver corner */
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
GlobalTx *GlobalTxAcquire(const char *gid, bool create);
GlobalTx *GetMyGlobalTx(void);
void GlobalTxRelease(GlobalTx *gtx);
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
