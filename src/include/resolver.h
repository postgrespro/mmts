#ifndef RESOLVER_H
#define RESOLVER_H

#include "postmaster/bgworker.h"

extern void ResolverMain(Datum main_arg);
extern void ResolverInit(void);
extern BackgroundWorkerHandle *ResolverStart(Oid db_id, Oid user_id);
extern void ResolveForRefereeWinner(int n_all_nodes);
// extern char *MtmTxStateMnem(MtmTxState state);
void ResolverWake(void);

#endif							/* RESOLVER_H */
