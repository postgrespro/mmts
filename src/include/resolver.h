#ifndef RESOLVER_H
#define RESOLVER_H

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

extern void ResolverMain(void);
extern void ResolverInit(void);
extern void ResolveTransactionsForNode(int node_id);
extern void ResolveAllTransactions(void);
extern char *MtmTxStateMnem(MtmTxState state);

#endif  /* RESOLVER_H */
