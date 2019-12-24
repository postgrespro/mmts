/*----------------------------------------------------------------------------
 *
 * global_tx.h
 *	  Persistent and in-memory state necessary for our E3PC-like atommic commit
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

typedef enum
{
	GTXInvalid = 0,
	GTXPrepared,
	GTXPreCommitted,
	GTXPreAborted,
	GTXCommitted,
	GTXAborted
} GlobalTxStatus;

typedef enum
{
	GTRS_AwaitStatus,
	GTRS_AwaitAcks
} GlobalTxResolvingStage;

typedef struct
{
	GlobalTxStatus	status;
	GlobalTxTerm	proposal;
	GlobalTxTerm	accepted;
} GTxState;

typedef struct 
{
	char		gid[GIDSIZE];
	BackendId	acquired_by;
	GTxState	state;
	bool		orphaned;	/* Indication for resolver that current tx needs
							 * to be picked up. Comes from a failed backend or
							 * a disabled node. */
	GTxState	phase1_acks[MTM_MAX_NODES];
	GTxState	phase2_acks[MTM_MAX_NODES];
	bool		in_table;	/* True when gtx state was written in proposals
							 * table because we received status request before
							 * it was prepared on our node. */
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
void GlobalTxRelease(GlobalTx *gtx);
void GlobalTxAtExit(int code, Datum arg);
void GlobalTxLoadAll(void);
char *serialize_gtx_state(GlobalTxStatus status, GlobalTxTerm term_prop,
						  GlobalTxTerm term_acc);
int term_cmp(GlobalTxTerm t1, GlobalTxTerm t2);
void parse_gtx_state(const char *state, GlobalTxStatus *status,
				GlobalTxTerm *term_prop, GlobalTxTerm *term_acc);
GlobalTxTerm GlobalTxGetMaxProposal(void);
void GlobalTxSaveInTable(const char *gid, GlobalTxStatus status,
						 GlobalTxTerm term_prop, GlobalTxTerm term_acc);
void GlobalTxMarkOrphaned(int node_id);

#endif							/* GLOBAL_TX_H */
