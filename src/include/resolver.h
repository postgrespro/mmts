#ifndef RESOLVER_H
#define RESOLVER_H

#include "postmaster/bgworker.h"

extern void ResolverMain(Datum main_arg);
void ResolverWake(void);

#endif							/* RESOLVER_H */
