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
	GlobalTxTerm	proposal; /* nextBal in terms of The Part-Time Parliament */
	GlobalTxTerm	accepted; /* prevBal in terms of The Part-Time Parliament */
	GlobalTxStatus	status; /*
							 * prevDec in terms of The Part-Time Parliament
							 * (or special never voted | commit | abort)
							 */
} GTxState;

/*
 * Constant xact metadata which we encode into state_3pc. We could (and
 * previously did) carry that directly in gid, but this intervenes with
 * explicit 2PC usage: applier must know generation of the xact, and
 * scribbling over user-provided gid is ugly and/or inefficient.
 */
typedef struct
{
	int coordinator; /* node id who initiated the transaction */
	TransactionId xid; /* xid at coordinator */
	uint64 gen_num; /* the number of generation xact belongs to */
	nodemask_t configured; /* mask of configured nodes of this generation;
							* the idea was to use this by resolver, but it
							* wasn't finished. We shouldn't have any problems
							* with this anyway if all xacts created before
							* first node add-rm are resolved before the
							* second one is started
							*/
} XactInfo;

typedef struct GlobalTx
{
	char		gid[GIDSIZE];
	XactInfo	xinfo;
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
void GlobalTxEnsureBeforeShmemExitHook(void);
GlobalTx *GlobalTxAcquire(const char *gid, bool create, bool nowait_own_live,
						  bool *busy, int coordinator);
void GlobalTxRelease(GlobalTx *gtx);
void GlobalTxAtExit(int code, Datum arg);
void GlobalTxLoadAll(void);
char *serialize_xstate(XactInfo *xinfo, GTxState *gtx_state);
int term_cmp(GlobalTxTerm t1, GlobalTxTerm t2);
int deserialize_xstate(const char *state, XactInfo *xinfo, GTxState *gtx_state,
					   int elevel);
GlobalTxTerm GlobalTxGetMaxProposal(void);
void GlobalTxSaveInTable(const char *gid, XLogRecPtr coordinator_end_lsn,
						 GlobalTxStatus status,
						 GlobalTxTerm term_prop, GlobalTxTerm term_acc);
void GlobalTxMarkOrphaned(int node_id);

char *GlobalTxToString(GlobalTx *gtx);

#endif							/* GLOBAL_TX_H */
