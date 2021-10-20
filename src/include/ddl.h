/*----------------------------------------------------------------------------
 *
 * ddl.h
 *	  Statement based replication of DDL commands.
 *
 * Portions Copyright (c) 1996-2018, PostgreSQL Global Development Group
 * Portions Copyright (c) 1994, Regents of the University of California
 * Portions Copyright (c) 2021, Postgres Professional
 *
 *----------------------------------------------------------------------------
 */

#ifndef DML_H
#define DML_H

#include "utils/relcache.h"

/* GUCs */
extern bool MtmMonotonicSequences;
extern char *MtmRemoteFunctionsList;
extern bool MtmRemoteFunctionsUpdating;
extern bool MtmVolksWagenMode;
extern bool MtmIgnoreTablesWithoutPk;

typedef enum
{
	MTM_DDL_IN_PROGRESS_NOTHING,
	MTM_DDL_IN_PROGRESS_TX,
	MTM_DDL_IN_PROGRESS_NONTX,
} MtmDDLInProgress;

extern MtmDDLInProgress DDLApplyInProgress;

extern void MtmDDLReplicationInit(void);
extern void MtmDDLReplicationShmemStartup(void);
extern void temp_schema_reset_all(int my_node_id);
extern bool MtmIsRelationLocal(Relation rel);
extern void MtmDDLResetStatement(void);
extern void MtmApplyDDLMessage(const char *messageBody, bool transactional);
extern void MtmDDLResetApplyState(void);
extern void MtmSetRemoteFunction(char const *list, void *extra);
extern void MtmToggleDML(void);
extern void MtmMakeTableLocal(char const *schema, char const *name, bool locked);
extern void multimaster_fmgr_hook(FmgrHookEventType event, FmgrInfo *flinfo, Datum *private);

#endif
