/*----------------------------------------------------------------------------
 *
 * ddl.h
 *	  Statement based replication of DDL commands.
 *
 * Portions Copyright (c) 1996-2019, PostgreSQL Global Development Group
 * Portions Copyright (c) 1994, Regents of the University of California
 *
 *----------------------------------------------------------------------------
 */

#ifndef COMMIT_H
#define COMMIT_H

#include "postgres.h"
#include "access/xact.h"

#include "messaging.h"

extern void MtmGenerateGid(char *gid, int node_id, TransactionId xid, uint64 gen_num,
						   nodemask_t configured);
extern uint64 MtmGidParseGenNum(const char *gid);
extern int	MtmGidParseNodeId(const char *gid);
extern TransactionId MtmGidParseXid(const char *gid);
extern nodemask_t MtmGidParseConfigured(const char *gid);

extern bool MtmTwoPhaseCommit(void);
extern void MtmBeginTransaction(void);
extern void MtmXactCallback(XactEvent event, void *arg);

extern bool MtmExplicitPrepare(char *gid);
extern void MtmExplicitFinishPrepared(bool isTopLevel, char *gid, bool isCommit);

#endif
