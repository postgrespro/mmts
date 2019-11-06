/*----------------------------------------------------------------------------
 *
 * ddl.h
 *	  Statement based replication of DDL commands.
 *
 * Portions Copyright (c) 1996-2018, PostgreSQL Global Development Group
 * Portions Copyright (c) 1994, Regents of the University of California
 *
 *----------------------------------------------------------------------------
 */

#ifndef DML_H
#define DML_H

#include "utils/relcache.h"

/* GUCs */
extern bool		MtmMonotonicSequences;
extern char	   *MtmRemoteFunctionsList;
extern bool		MtmRemoteFunctionsUpdating;
extern bool		MtmVolksWagenMode;
extern bool		MtmIgnoreTablesWithoutPk;


extern void MtmDDLReplicationInit(void);
extern void MtmDDLReplicationShmemStartup(void);
extern bool MtmIsRelationLocal(Relation rel);
extern void MtmDDLResetStatement(void);
extern void MtmApplyDDLMessage(const char *messageBody, bool transactional);
extern void MtmDDLResetApplyState(void);
extern void MtmSetRemoteFunction(char const* list, void* extra);
extern void MtmToggleDML(void);
extern void MtmMakeTableLocal(char const* schema, char const* name, bool locked);
extern void multimaster_fmgr_hook(FmgrHookEventType event, FmgrInfo *flinfo, Datum *private);

#endif
