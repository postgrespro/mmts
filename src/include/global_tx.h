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

typedef enum
{
	GTXInvalid = 0,
	GTXPrepared,
	GTXPreCommitted,
	GTXPreAborted,
	GTXCommitted,
	GTXAborted
} GlobalTxStatus;

typedef struct
{
	GlobalTxStatus	status;
	GlobalTxTerm	proposal;
	GlobalTxTerm	accepted;
} GTxState;

typedef struct 
{
	char		gid[GIDSIZE];
	int			acquired_by;
	GTxState	state;
	bool		orphaned;	/* Indication for resolver that current tx needs
							 * to be picked up. Comes from a failed backend or
							 * a disabled node. */
	GTxState	remote_states[MTM_MAX_NODES];
	bool		resolver_acks[MTM_MAX_NODES];
	bool		in_table;	/* True when gtx state was written in proposals
							 * table because we received status request before
							 * it was prepared on our node. */
} GlobalTx;

void MtmGlobalTxInit(void);
void MtmGlobalTxShmemStartup(void);
GlobalTx *GlobalTxAcquire(const char *gid, bool create);
void GlobalTxRelease(GlobalTx *gtx);
void GlobalTxAtAbort(int code, Datum arg);
void GlobalTxLoadAll(void);
char *serialize_gtx_state(GlobalTxStatus status, GlobalTxTerm term_prop,
						  GlobalTxTerm term_acc);
int term_cmp(GlobalTxTerm t1, GlobalTxTerm t2);
void parse_gtx_state(const char *state, GlobalTxStatus *status,
				GlobalTxTerm *term_prop, GlobalTxTerm *term_acc);

#endif							/* GLOBAL_TX_H */