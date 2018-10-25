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

#define MAX_NODES 64

/* Identifier of global transaction */
typedef struct
{
	int node;          /* One based id of node initiating transaction */
	TransactionId xid; /* Transaction ID at this node */
} GlobalTransactionId;

/* XXX: drop that */
typedef long long long64; /* we are not using int64 here because we want to use %lld format for this type */
typedef unsigned long long ulong64; /* we are not using uint64 here because we want to use %lld format for this type */

typedef ulong64 nodemask_t;


#define BIT_CHECK(mask, bit) (((mask) & ((nodemask_t)1 << (bit))) != 0)
#define BIT_CLEAR(mask, bit) (mask &= ~((nodemask_t)1 << (bit)))
#define BIT_SET(mask, bit)   (mask |= ((nodemask_t)1 << (bit)))
#define ALL_BITS ((nodemask_t)~0)


extern bool		MtmBackgroundWorker;
extern int		MtmNodeId;


#endif							/* MM_H */