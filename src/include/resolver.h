#ifndef RESOLVER_H
#define RESOLVER_H

#include "postmaster/bgworker.h"

typedef enum
{
	MtmTxUnknown		= (1<<0),
	MtmTxNotFound		= (1<<1),
	MtmTxInProgress		= (1<<2),
	MtmTxPrepared		= (1<<3),
	MtmTxPreCommited	= (1<<4),
	MtmTxPreAborted		= (1<<5),
	MtmTxCommited		= (1<<6),
	MtmTxAborted		= (1<<7)
} MtmTxState;

typedef int MtmTxStateMask;

extern void ResolverMain(Datum main_arg);
extern void ResolverInit(void);
extern BackgroundWorkerHandle *ResolverStart(Oid db_id, Oid user_id);
extern void ResolveTransactionsForNode(int node_id, int n_all_nodes);
extern void ResolveAllTransactions(int n_all_nodes);
extern void ResolveForRefereeWinner(int n_all_nodes);
extern char *MtmTxStateMnem(MtmTxState state);

#endif  /* RESOLVER_H */
