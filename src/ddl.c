/*----------------------------------------------------------------------------
 *
 * ddl.c
 *	  Statement based replication of DDL commands.
 *
 * Portions Copyright (c) 1996-2018, PostgreSQL Global Development Group
 * Portions Copyright (c) 1994, Regents of the University of California
 *
 *----------------------------------------------------------------------------
 */

#include "postgres.h"

#include "utils/guc_tables.h"
#include "tcop/utility.h"
#include "utils/rel.h"
#include "utils/syscache.h"
#include "executor/executor.h"
#include "catalog/pg_proc.h"
#include "parser/parse_type.h"
#include "parser/parse_func.h"
#include "commands/sequence.h"
#include "tcop/pquery.h"
#include "utils/snapmgr.h"
#include "nodes/makefuncs.h"
#include "catalog/pg_constraint.h"
#include "catalog/pg_namespace.h"
#include "executor/spi.h"
#include "utils/lsyscache.h"
#include "catalog/indexing.h"
#include "commands/tablespace.h"
#include "parser/parse_utilcmd.h"
#include "commands/defrem.h"
#include "utils/regproc.h"
#include "replication/message.h"
#include "access/relscan.h"
#include "commands/vacuum.h"
#include "utils/inval.h"
#include "replication/origin.h"
#include "miscadmin.h"

#include "mm.h"
#include "ddl.h"
#include "logger.h"
#include "commit.h"

#include "multimaster.h"


// XXX: isQueryUsingTempRelation() may be helpful


// XXX: is it defined somewhere?
#define GUC_KEY_MAXLEN					255
#define MTM_GUC_HASHSIZE				100
#define MULTIMASTER_MAX_LOCAL_TABLES	256

#define Natts_mtm_local_tables 2
#define Anum_mtm_local_tables_rel_schema 1
#define Anum_mtm_local_tables_rel_name	 2

struct DDLSharedState
{
	LWLock   *localtab_lock;
} *ddl_shared;

typedef struct MtmGucEntry
{
	char		key[GUC_KEY_MAXLEN];
	dlist_node	list_node;
	char	   *value;
} MtmGucEntry;

typedef struct {
	NameData schema;
	NameData name;
} MtmLocalTablesTuple;

/* GUCs */
bool	MtmVolksWagenMode;
bool	MtmMonotonicSequences;
char   *MtmRemoteFunctionsList;
bool	MtmIgnoreTablesWithoutPk;

static void const* MtmDDLStatement;

static Node *MtmCapturedDDL;
static bool DDLApplyInProgress;

static HTAB *MtmGucHash = NULL;
static dlist_head MtmGucList = DLIST_STATIC_INIT(MtmGucList);

static HTAB	   *MtmRemoteFunctions;
static HTAB	   *MtmLocalTables;

static ExecutorStart_hook_type PreviousExecutorStartHook;
static ExecutorFinish_hook_type PreviousExecutorFinishHook;
static ProcessUtility_hook_type PreviousProcessUtilityHook;
static seq_nextval_hook_t PreviousSeqNextvalHook;

static void MtmSeqNextvalHook(Oid seqid, int64 next);
static void MtmExecutorStart(QueryDesc *queryDesc, int eflags);
static void MtmExecutorFinish(QueryDesc *queryDesc);

static void MtmProcessUtility(PlannedStmt *pstmt,
				const char *queryString,
				ProcessUtilityContext context, ParamListInfo params,
				QueryEnvironment *queryEnv, DestReceiver *dest,
				char *completionTag);

static void MtmProcessUtilityReciever(PlannedStmt *pstmt,
				const char *queryString,
				ProcessUtilityContext context, ParamListInfo params,
				QueryEnvironment *queryEnv, DestReceiver *dest,
				char *completionTag);

static void MtmProcessUtilitySender(PlannedStmt *pstmt,
				const char *queryString,
				ProcessUtilityContext context, ParamListInfo params,
				QueryEnvironment *queryEnv, DestReceiver *dest,
				char *completionTag);

static void MtmGucUpdate(const char *key, char *value);
static void MtmInitializeRemoteFunctionsMap(void);
static char *MtmGucSerialize(void);
static void MtmMakeRelationLocal(Oid relid);
static void AdjustCreateSequence(List *options);

PG_FUNCTION_INFO_V1(mtm_make_table_local);


/*****************************************************************************
 *
 * Init
 *
 *****************************************************************************/

void
MtmDDLReplicationInit()
{
	Size	size = 0;

	size = add_size(size, sizeof(struct DDLSharedState));
	size = add_size(size, hash_estimate_size(MULTIMASTER_MAX_LOCAL_TABLES,
											 sizeof(Oid)));
	size = MAXALIGN(size);

	RequestAddinShmemSpace(size);

	RequestNamedLWLockTranche("mtm-ddl", 1);

	PreviousExecutorStartHook = ExecutorStart_hook;
	ExecutorStart_hook = MtmExecutorStart;

	PreviousExecutorFinishHook = ExecutorFinish_hook;
	ExecutorFinish_hook = MtmExecutorFinish;

	PreviousProcessUtilityHook = ProcessUtility_hook;
	ProcessUtility_hook = MtmProcessUtility;

	PreviousSeqNextvalHook = SeqNextvalHook;
	SeqNextvalHook = MtmSeqNextvalHook;
}

void
MtmDDLReplicationShmemStartup(void)
{
	HASHCTL		info;
	bool		found;

	memset(&info, 0, sizeof(info));
	info.entrysize = info.keysize = sizeof(Oid);

	LWLockAcquire(AddinShmemInitLock, LW_EXCLUSIVE);

	ddl_shared = ShmemInitStruct("ddl",
								sizeof(struct DDLSharedState),
								&found);

	if (!found)
	{
		ddl_shared->localtab_lock = &(GetNamedLWLockTranche("mtm-ddl"))->lock;
	}

	MtmLocalTables = ShmemInitHash("MtmLocalTables",
		MULTIMASTER_MAX_LOCAL_TABLES, MULTIMASTER_MAX_LOCAL_TABLES,
		&info, HASH_ELEM | HASH_BLOBS);


	LWLockRelease(AddinShmemInitLock);
}

/*****************************************************************************
 *
 * Guc handling
 *
 *****************************************************************************/

// XXX: move to ShmemStart?
static void
MtmGucInit(void)
{
	HASHCTL		hash_ctl;
	char	   *current_role;
	MemoryContext oldcontext;

	MemSet(&hash_ctl, 0, sizeof(hash_ctl));
	hash_ctl.keysize = GUC_KEY_MAXLEN;
	hash_ctl.entrysize = sizeof(MtmGucEntry);
	hash_ctl.hcxt = TopMemoryContext;
	MtmGucHash = hash_create("MtmGucHash",
						MTM_GUC_HASHSIZE,
						&hash_ctl,
						HASH_ELEM | HASH_CONTEXT);

	/*
	 * If current role is not equal to MtmDatabaseUser, than set it before
	 * any other GUC vars.
	 *
	 * XXX: try to avoid using MtmDatabaseUser somehow
	 */
	oldcontext = MemoryContextSwitchTo(TopMemoryContext);
	current_role = GetConfigOptionByName("session_authorization", NULL, false);
	// XXX if (current_role && *current_role && strcmp(MtmDatabaseUser, current_role) != 0)
		MtmGucUpdate("session_authorization", current_role);
	MemoryContextSwitchTo(oldcontext);
}

static void
MtmGucDiscard()
{
	dlist_iter iter;

	if (dlist_is_empty(&MtmGucList))
		return;

	dlist_foreach(iter, &MtmGucList)
	{
		MtmGucEntry *cur_entry = dlist_container(MtmGucEntry, list_node, iter.cur);
		pfree(cur_entry->value);
	}
	dlist_init(&MtmGucList);

	hash_destroy(MtmGucHash);
	MtmGucHash = NULL;
}

static inline void
MtmGucUpdate(const char *key, char *value)
{
	MtmGucEntry *hentry;
	bool found;

	if (!MtmGucHash)
		MtmGucInit();

	hentry = (MtmGucEntry*)hash_search(MtmGucHash, key, HASH_ENTER, &found);
	if (found)
	{
		pfree(hentry->value);
		dlist_delete(&hentry->list_node);
	}
	hentry->value = value;
	dlist_push_tail(&MtmGucList, &hentry->list_node);
}

static inline void
MtmGucRemove(const char *key)
{
	MtmGucEntry *hentry;
	bool found;

	if (!MtmGucHash)
		MtmGucInit();

	hentry = (MtmGucEntry*)hash_search(MtmGucHash, key, HASH_FIND, &found);
	if (found)
	{
		pfree(hentry->value);
		dlist_delete(&hentry->list_node);
		hash_search(MtmGucHash, key, HASH_REMOVE, NULL);
	}
}

static void
MtmGucSet(VariableSetStmt *stmt, const char *queryStr)
{
	MemoryContext oldcontext;

	if (!MtmGucHash)
		MtmGucInit();

	oldcontext = MemoryContextSwitchTo(TopMemoryContext);

	switch (stmt->kind)
	{
		case VAR_SET_VALUE:
			MtmGucUpdate(stmt->name, ExtractSetVariableArgs(stmt));
			break;

		case VAR_SET_DEFAULT:
			MtmGucRemove(stmt->name);
			break;

		case VAR_RESET:
			if (strcmp(stmt->name, "session_authorization") == 0)
				MtmGucRemove("role");
			MtmGucRemove(stmt->name);
			break;

		case VAR_RESET_ALL:
			/* XXX: shouldn't we keep auth/role here? */
			MtmGucDiscard();
			break;

		case VAR_SET_CURRENT:
		case VAR_SET_MULTI:
			break;
	}

	MemoryContextSwitchTo(oldcontext);
}

/*
 * the bare comparison function for GUC names
 */
static int
_guc_name_compare(const char *namea, const char *nameb)
{
	/*
	 * The temptation to use strcasecmp() here must be resisted, because the
	 * array ordering has to remain stable across setlocale() calls. So, build
	 * our own with a simple ASCII-only downcasing.
	 */
	while (*namea && *nameb)
	{
		char		cha = *namea++;
		char		chb = *nameb++;

		if (cha >= 'A' && cha <= 'Z')
			cha += 'a' - 'A';
		if (chb >= 'A' && chb <= 'Z')
			chb += 'a' - 'A';
		if (cha != chb)
			return cha - chb;
	}
	if (*namea)
		return 1;				/* a is longer */
	if (*nameb)
		return -1;				/* b is longer */
	return 0;
}

static int
_var_name_cmp(const void *a, const void *b)
{
	const struct config_generic *confa = *(struct config_generic * const *) a;
	const struct config_generic *confb = *(struct config_generic * const *) b;

	return _guc_name_compare(confa->name, confb->name);
}

static struct config_generic *
fing_guc_conf(const char *name)
{
	int num;
	struct config_generic **vars;
	const char **key = &name;
	struct config_generic **res;

	num = GetNumConfigOptions();
	vars = get_guc_variables();

	res = (struct config_generic **) bsearch((void *) &key,
										(void *) vars,
										num, sizeof(struct config_generic *),
										_var_name_cmp);

	return res ? *res : NULL;
}

char *
MtmGucSerialize(void)
{
	StringInfo serialized_gucs;
	dlist_iter iter;
	const char *search_path;

	if (!MtmGucHash)
		MtmGucInit();

	serialized_gucs = makeStringInfo();

	dlist_foreach(iter, &MtmGucList)
	{
		MtmGucEntry *cur_entry = dlist_container(MtmGucEntry, list_node, iter.cur);
		struct config_generic *gconf;

		if (strcmp(cur_entry->key, "search_path") == 0)
			continue;

		appendStringInfoString(serialized_gucs, "SET ");
		appendStringInfoString(serialized_gucs, cur_entry->key);
		appendStringInfoString(serialized_gucs, " TO ");

		gconf = fing_guc_conf(cur_entry->key);
		if (gconf && (gconf->vartype == PGC_STRING ||
					  gconf->vartype == PGC_ENUM ||
					  (gconf->flags & (GUC_UNIT_MEMORY | GUC_UNIT_TIME))))
		{
			appendStringInfoString(serialized_gucs, "'");
			appendStringInfoString(serialized_gucs, cur_entry->value);
			appendStringInfoString(serialized_gucs, "'");
		}
		else
		{
			appendStringInfoString(serialized_gucs, cur_entry->value);
		}
		appendStringInfoString(serialized_gucs, "; ");
	}

	/*
	 * Crutch for scheduler. It sets search_path through SetConfigOption()
	 * so our callback do not react on that.
	 */
	search_path = GetConfigOption("search_path", false, true);
	if (strcmp(search_path, "\"\"") != 0)
		appendStringInfo(serialized_gucs, "SET search_path TO %s; ", search_path);
	else
		appendStringInfo(serialized_gucs, "SET search_path TO ''; ");

	return serialized_gucs->data;
}



/*****************************************************************************
 *
 * Capture DDL statements and send them down to subscribers
 *
 *****************************************************************************/

static void
MtmProcessDDLCommand(char const* queryString, bool transactional)
{
	if (transactional)
	{
		char *gucCtx = MtmGucSerialize();
		queryString = psprintf("%s %s", gucCtx, queryString);

		/* Transactional DDL */
		mtm_log(DDLStmtOutgoing, "Sending DDL: %s", queryString);
		LogLogicalMessage("D", queryString, strlen(queryString) + 1, true);
	}
	else
	{
		/* Concurrent DDL */
		mtm_log(DDLStmtOutgoing, "Sending concurrent DDL: %s", queryString);
		XLogFlush(LogLogicalMessage("C", queryString, strlen(queryString) + 1, false));
	}
}

static void
MtmFinishDDLCommand()
{
	LogLogicalMessage("E", "", 1, true);
}


/*
 * Check whether given type is temporary.
 * As LookupTypeName() can emit notices raise client_min_messages to ERROR
 * level to avoid duplicated notices.
 */
static bool
MtmIsTempType(TypeName* typeName)
{
	bool isTemp = false;
	int saved_client_min_messages = client_min_messages;

	client_min_messages = ERROR;

	if (typeName != NULL)
	{
		Type typeTuple = LookupTypeName(NULL, typeName, NULL, false);
		if (typeTuple != NULL)
		{
			Form_pg_type typeStruct = (Form_pg_type) GETSTRUCT(typeTuple);
		    Oid relid = typeStruct->typrelid;
		    ReleaseSysCache(typeTuple);

			if (relid != InvalidOid)
			{
				HeapTuple classTuple = SearchSysCache1(RELOID, relid);
				Form_pg_class classStruct = (Form_pg_class) GETSTRUCT(classTuple);
				if (classStruct->relpersistence == RELPERSISTENCE_TEMP)
					isTemp = true;
				ReleaseSysCache(classTuple);
			}
		}
	}

	client_min_messages = saved_client_min_messages;
	return isTemp;
}

static bool
MtmFunctionProfileDependsOnTempTable(CreateFunctionStmt* func)
{
	ListCell* elem;

	if (MtmIsTempType(func->returnType))
	{
		return true;
	}
	foreach (elem, func->parameters)
	{
		FunctionParameter* param = (FunctionParameter*) lfirst(elem);
		if (MtmIsTempType(param->argType))
		{
			return true;
		}
	}
	return false;
}

static void
MtmProcessUtility(PlannedStmt *pstmt, const char *queryString,
				  ProcessUtilityContext context, ParamListInfo params,
				  QueryEnvironment *queryEnv, DestReceiver *dest,
				  char *completionTag)
{
	if (MtmIsLogicalReceiver)
	{
		MtmProcessUtilityReciever(pstmt, queryString, context, params,
								  queryEnv, dest, completionTag);
	}
	else
	{
		MtmProcessUtilitySender(pstmt, queryString, context, params,
								  queryEnv, dest, completionTag);
	}

}


/*
 * Process utility statements on receiver side.
 *
 * Some DDL isn't allowed to run inside transaction, so we are copying parse
 * tree into MtmCapturedDDL and preventing it's execution by not calling
 * standard_ProcessUtility() at the end of hook.
 *
 * Later MtmApplyDDLMessage() checks MtmCapturedDDL and executes it if something
 * was caught.
 *
 * DDLApplyInProgress ensures that this hook will only called for DDL
 * originated from MtmApplyDDLMessage(). In other cases of DDL happening in
 * receiver (e.g calling DDL from trigger) this hook does nothing.
 */
static void
MtmProcessUtilityReciever(PlannedStmt *pstmt, const char *queryString,
				  ProcessUtilityContext context, ParamListInfo params,
				  QueryEnvironment *queryEnv, DestReceiver *dest,
				  char *completionTag)
{
	Node *parsetree = pstmt->utilityStmt;

	/* catch only DDL produced by SPI in MtmApplyDDLMessage() */
	if (DDLApplyInProgress)
	{
		MemoryContext oldMemContext = MemoryContextSwitchTo(MtmApplyContext);
		bool		captured = false;

		mtm_log(DDLProcessingTrace,
				"MtmProcessUtilityReciever: tag=%s, context=%d, issubtrans=%d,  statement=%s",
				CreateCommandTag(parsetree), context, IsSubTransaction(), queryString);

		Assert(oldMemContext != MtmApplyContext);
		Assert(MtmApplyContext != NULL);

		/* copy parsetrees of interest to MtmCapturedDDL */
		switch (nodeTag(parsetree))
		{
			case T_CreateTableSpaceStmt:
			case T_DropTableSpaceStmt:
			// case T_VacuumStmt:
				Assert(MtmCapturedDDL == NULL);
				MtmCapturedDDL = copyObject(parsetree);
				captured = true;
				break;

			case T_IndexStmt:
			{
				IndexStmt *stmt = (IndexStmt *) parsetree;
				if (stmt->concurrent)
				{
					Assert(MtmCapturedDDL == NULL);
					MtmCapturedDDL = (Node *) copyObject(stmt);
					captured = true;
				}
				break;
			}

			case T_DropStmt:
			{
				DropStmt *stmt = (DropStmt *) parsetree;
				if (stmt->removeType == OBJECT_INDEX && stmt->concurrent)
				{
					Assert(MtmCapturedDDL == NULL);
					MtmCapturedDDL = (Node *) copyObject(stmt);
					captured = true;
				}
				/* Make it possible to drop functions which were not replicated */
				else if (stmt->removeType == OBJECT_FUNCTION)
				{
					stmt->missing_ok = true;
				}
				break;
			}

			/* disable functiob body check at replica */
			case T_CreateFunctionStmt:
				check_function_bodies = false;
				break;

			case T_CreateSeqStmt:
			{
				CreateSeqStmt *stmt = (CreateSeqStmt *) parsetree;
				if (!MtmVolksWagenMode)
					AdjustCreateSequence(stmt->options);
				break;
			}

			default:
				break;
		}

		MemoryContextSwitchTo(oldMemContext);

		/* prevent captured statement from execution */
		if (captured)
		{
			mtm_log(DDLProcessingTrace, "MtmCapturedDDL = %s", CreateCommandTag((Node *) MtmCapturedDDL));
			return;
		}
	}

	if (PreviousProcessUtilityHook != NULL)
	{
		PreviousProcessUtilityHook(pstmt, queryString,
								   context, params, queryEnv,
								   dest, completionTag);
	}
	else
	{
		standard_ProcessUtility(pstmt, queryString,
								context, params, queryEnv,
								dest, completionTag);
	}
}


static void
MtmProcessUtilitySender(PlannedStmt *pstmt, const char *queryString,
				  ProcessUtilityContext context, ParamListInfo params,
				  QueryEnvironment *queryEnv, DestReceiver *dest,
				  char *completionTag)
{
	bool skipCommand = false;
	bool executed = false;
	bool prevMyXactAccessedTempRel;
	Node *parsetree = pstmt->utilityStmt;
	int stmt_start = pstmt->stmt_location > 0 ? pstmt->stmt_location : 0;
	int stmt_len = pstmt->stmt_len > 0 ? pstmt->stmt_len : strlen(queryString + stmt_start);
	char *stmt_string = palloc(stmt_len + 1);
	bool		isTopLevel = (context == PROCESS_UTILITY_TOPLEVEL);

	strncpy(stmt_string, queryString + stmt_start, stmt_len);
	stmt_string[stmt_len] = 0;

	mtm_log(DDLProcessingTrace,
			"MtmProcessUtilitySender tag=%d, context=%d, issubtrans=%d,  statement=%s",
			nodeTag(parsetree), context, IsSubTransaction(), stmt_string);

	switch (nodeTag(parsetree))
	{
		case T_TransactionStmt:
		{
			TransactionStmt *stmt = (TransactionStmt *) parsetree;
			switch (stmt->kind)
			{
				case TRANS_STMT_COMMIT:
					if (MtmTwoPhaseCommit())
						return;
					break;

				case TRANS_STMT_PREPARE:
					if (!MtmExplicitPrepare(stmt->gid))
					{
						/* report unsuccessful commit in completionTag */
						if (completionTag)
							strcpy(completionTag, "ROLLBACK");
					}
					return;

				case TRANS_STMT_COMMIT_PREPARED:
					MtmExplicitFinishPrepared(isTopLevel, stmt->gid, true);
					return;

				case TRANS_STMT_ROLLBACK_PREPARED:
					MtmExplicitFinishPrepared(isTopLevel, stmt->gid, false);
					return;

				default:
					break;
			}
		}
			/* no break */
		case T_PlannedStmt:
		case T_FetchStmt:
		case T_DoStmt:
		case T_CommentStmt:
		case T_PrepareStmt:
		case T_ExecuteStmt:
		case T_DeallocateStmt:
		case T_NotifyStmt:
		case T_ListenStmt:
		case T_UnlistenStmt:
		case T_LoadStmt:
		case T_ClusterStmt:
		case T_VariableShowStmt:
		case T_ReassignOwnedStmt: // XXX?
		case T_LockStmt:
		case T_CheckPointStmt:
		case T_ReindexStmt:
		case T_AlterSystemStmt:
		case T_CreatedbStmt:
		case T_DropdbStmt:
		case T_DeclareCursorStmt:
		case T_ClosePortalStmt:
		case T_VacuumStmt:
			skipCommand = true;
			break;

		case T_CreateSeqStmt:
		{
			CreateSeqStmt *stmt = (CreateSeqStmt *) parsetree;
			if (!MtmVolksWagenMode)
				AdjustCreateSequence(stmt->options);
			break;
		}

		case T_CreateTableSpaceStmt:
		case T_DropTableSpaceStmt:
		{
			skipCommand = true;
			MtmProcessDDLCommand(stmt_string, false);
			break;
		}

		/* Detect temp tables access */
		case T_CreateDomainStmt:
		{
			CreateDomainStmt *stmt = (CreateDomainStmt *) parsetree;
			HeapTuple	typeTup;
			Form_pg_type baseType;
			Form_pg_type elementType;
			Form_pg_class pgClassStruct;
			int32		basetypeMod;
			Oid			elementTypeOid;
			Oid			tableOid;
			HeapTuple pgClassTuple;
			HeapTuple elementTypeTuple;

			typeTup = typenameType(NULL, stmt->typeName, &basetypeMod);
			baseType = (Form_pg_type) GETSTRUCT(typeTup);
			elementTypeOid = baseType->typelem;
			ReleaseSysCache(typeTup);

			if (elementTypeOid == InvalidOid)
				break;

			elementTypeTuple = SearchSysCache1(TYPEOID, elementTypeOid);
			elementType = (Form_pg_type) GETSTRUCT(elementTypeTuple);
			tableOid = elementType->typrelid;
			ReleaseSysCache(elementTypeTuple);

			if (tableOid == InvalidOid)
				break;

			pgClassTuple = SearchSysCache1(RELOID, tableOid);
			pgClassStruct = (Form_pg_class) GETSTRUCT(pgClassTuple);
			if (pgClassStruct->relpersistence == 't')
				MyXactFlags |= XACT_FLAGS_ACCESSEDTEMPREL;
			ReleaseSysCache(pgClassTuple);

			break;
		}

		/*
		 * Explain will not call ProcessUtility for passed CreateTableAsStmt,
		 * but will run it manually, so we will not catch it in a standart way.
		 * So catch it in a non-standart way.
		 */
		case T_ExplainStmt:
		{
			ExplainStmt	   *stmt = (ExplainStmt *) parsetree;
			Query		   *query = (Query *) stmt->query;
			ListCell	   *lc;

			skipCommand = true;
			if (query->commandType == CMD_UTILITY &&
				IsA(query->utilityStmt, CreateTableAsStmt))
			{
				foreach(lc, stmt->options)
				{
					DefElem	   *opt = (DefElem *) lfirst(lc);
					if (strcmp(opt->defname, "analyze") == 0)
						skipCommand = false;
				}
			}
			break;
		}

		/* Save GUC context for consequent DDL execution */
		case T_DiscardStmt:
		{
			DiscardStmt *stmt = (DiscardStmt *) parsetree;

			if (!IsTransactionBlock() && stmt->target == DISCARD_ALL)
			{
				skipCommand = true;
				MtmGucDiscard();
			}
			break;
		}

		case T_VariableSetStmt:
		{
			VariableSetStmt *stmt = (VariableSetStmt *) parsetree;

			/* Prevent SET TRANSACTION from replication */
			if (stmt->kind == VAR_SET_MULTI)
				skipCommand = true;

			if (!IsTransactionBlock())
			{
				skipCommand = true;
				MtmGucSet(stmt, stmt_string);
			}

			break;
		}

		/*
		* Index is created at replicas completely asynchronously, so to prevent unintended interleaving with subsequent
		* commands in this session, just wait here for a while.
		* It will help to pass regression tests but will not be enough for construction of real large indexes
		* where difference between completion of this operation at different nodes is unlimited
		*/
		case T_IndexStmt:
		{
			IndexStmt *indexStmt = (IndexStmt *) parsetree;
			if (indexStmt->concurrent && context == PROCESS_UTILITY_TOPLEVEL)
			{
				MtmProcessDDLCommand(stmt_string, false);
				skipCommand = true;
				pg_usleep(USECS_PER_SEC); /* XXX */
			}
			break;
		}

		case T_DropStmt:
		{
			DropStmt *stmt = (DropStmt *) parsetree;
			if (stmt->removeType == OBJECT_INDEX && stmt->concurrent)
			{
				if (context == PROCESS_UTILITY_TOPLEVEL) {
					MtmProcessDDLCommand(stmt_string, false);
					skipCommand = true;
				}
			}
			break;
		}

		/* Copy need some special care */
		case T_CopyStmt:
		{
			CopyStmt *copyStatement = (CopyStmt *) parsetree;
			skipCommand = true;
			if (copyStatement->is_from)
			{
				RangeVar *relation = copyStatement->relation;

				if (relation != NULL)
				{
					Oid relid = RangeVarGetRelid(relation, NoLock, true);
					if (OidIsValid(relid))
					{
						Relation rel = heap_open(relid, ShareLock);
						if (RelationNeedsWAL(rel)) {
							MtmTx.contains_dml = true;
						}
						heap_close(rel, ShareLock);
					}
				}
			}
			break;
		}

		default:
			skipCommand = false;
			break;
	}

	if (!skipCommand && !MtmDDLStatement)
	{
		mtm_log(DDLProcessingTrace,
				"Process DDL statement '%s', MtmTx.isReplicated=%d, MtmIsLogicalReceiver=%d",
				stmt_string, MtmIsLogicalReceiver,
				MtmIsLogicalReceiver);
		MtmProcessDDLCommand(stmt_string, true);
		executed = true;
		MtmDDLStatement = stmt_string;
	}
	else
		mtm_log(DDLProcessingTrace,
				"Skip utility statement '%s': skip=%d, insideDDL=%d",
				stmt_string, skipCommand, MtmDDLStatement != NULL);

	prevMyXactAccessedTempRel = MyXactFlags & XACT_FLAGS_ACCESSEDTEMPREL;

	if (PreviousProcessUtilityHook != NULL)
	{
		PreviousProcessUtilityHook(pstmt, queryString,
										 context, params, queryEnv,
										 dest, completionTag);
	}
	else
	{
		standard_ProcessUtility(pstmt, queryString,
									context, params, queryEnv,
									dest, completionTag);
	}

	/* Allow replication of functions operating on temporary tables.
	 * Even through temporary table doesn't exist at replica, diasabling functoin body check makes it possible to create such function at replica.
	 * And it can be accessed later at replica if correspondent temporary table will be created.
	 * But disable replication of functions returning temporary tables: such functions can not be created at replica in any case.
	 */
	if (IsA(parsetree, CreateFunctionStmt))
	{
		if (MtmFunctionProfileDependsOnTempTable((CreateFunctionStmt*)parsetree))
		{
			prevMyXactAccessedTempRel = true;
		}
		if (prevMyXactAccessedTempRel)
			MyXactFlags |= XACT_FLAGS_ACCESSEDTEMPREL;
	}

	if (executed)
	{
		MtmFinishDDLCommand();
		MtmDDLStatement = NULL;

		if (MyXactFlags & XACT_FLAGS_ACCESSEDTEMPREL)
		{
			mtm_log(DDLProcessingTrace,
					"Xact accessed temp table, stopping replication of statement '%s'",
					stmt_string);
			MtmTx.contains_temp_ddl = true;
			MyXactFlags &= ~XACT_FLAGS_ACCESSEDTEMPREL;
		}
		else
		{
			MtmTx.contains_persistent_ddl = true;
		}
	}

	// if (IsA(parsetree, CreateStmt))
	// {
	// 	CreateStmt* create = (CreateStmt*)parsetree;
	// 	Oid relid = RangeVarGetRelid(create->relation, NoLock, true);
	// 	if (relid != InvalidOid) {
	// 		Oid constraint_oid;
	// 		Bitmapset* pk = get_primary_key_attnos(relid, true, &constraint_oid);
	// 		if (pk == NULL && !MtmVolksWagenMode && MtmIgnoreTablesWithoutPk) {
	// 			elog(WARNING,
	// 				 "Table %s.%s without primary will not be replicated",
	// 				 create->relation->schemaname ? create->relation->schemaname : "public",
	// 				 create->relation->relname);
	// 		}
	// 	}
	// }
}

static void
MtmExecutorStart(QueryDesc *queryDesc, int eflags)
{
	if (!MtmIsLogicalReceiver && !MtmDDLStatement && MtmIsEnabled())
	{
		ListCell   *tlist;

		if (!MtmRemoteFunctions)
		{
			MtmInitializeRemoteFunctionsMap();
		}

		foreach(tlist, queryDesc->plannedstmt->planTree->targetlist)
		{
			TargetEntry *tle = (TargetEntry *) lfirst(tlist);
			if (tle->expr && IsA(tle->expr, FuncExpr))
			{
				Oid func_oid = ((FuncExpr*) tle->expr)->funcid;

				if (hash_search(MtmRemoteFunctions, &func_oid, HASH_FIND, NULL))
				{
					MtmProcessDDLCommand(queryDesc->sourceText, true);
					MtmDDLStatement = queryDesc;
					break;
				}
			}
		}
	}
	if (PreviousExecutorStartHook != NULL)
		PreviousExecutorStartHook(queryDesc, eflags);
	else
		standard_ExecutorStart(queryDesc, eflags);
}

static void
MtmExecutorFinish(QueryDesc *queryDesc)
{
	/*
	 * If tx didn't wrote to XLOG then there is nothing to commit on other nodes.
	 */

	CmdType operation = queryDesc->operation;
	EState *estate = queryDesc->estate;

	if (MtmIsEnabled())
	{
		if (estate->es_processed != 0 && (operation == CMD_INSERT || operation == CMD_UPDATE || operation == CMD_DELETE)) {
			int i;
			for (i = 0; i < estate->es_num_result_relations; i++) {
				Relation rel = estate->es_result_relations[i].ri_RelationDesc;
				if (RelationNeedsWAL(rel)) {
					if (MtmIgnoreTablesWithoutPk) {
						if (!rel->rd_indexvalid) {
							RelationGetIndexList(rel);
						}
						if (rel->rd_replidindex == InvalidOid) {
							// XXX
							MtmMakeRelationLocal(RelationGetRelid(rel));
							continue;
						}
					}
					MtmTx.contains_dml = true;
					break;
				}
			}
		}
	}

	if (PreviousExecutorFinishHook != NULL)
		PreviousExecutorFinishHook(queryDesc);
	else
		standard_ExecutorFinish(queryDesc);

	if (MtmDDLStatement == queryDesc && MtmIsEnabled())
	{
		MtmFinishDDLCommand();
		MtmDDLStatement = NULL;
	}
}


/*****************************************************************************
 *
 * DDL apply
 *
 *****************************************************************************/


void
MtmApplyDDLMessage(const char *messageBody, bool transactional)
{
	int rc;

	/* Write DDL to our WAL in case smbd going to recover from us */
	Assert(replorigin_session_origin != InvalidRepOriginId);
	LogLogicalMessage(transactional ? "D" : "C",
					  messageBody, strlen(messageBody) + 1, transactional);

	mtm_log(DDLStmtIncoming, "%d: Executing utility statement %s",
			MyProcPid, messageBody);

	debug_query_string = messageBody;
	ActivePortal->sourceText = messageBody;

	/*
	 * Set proper context for running receiver DDL.
	 *
	 * MtmProcessUtilityReciever() will work only when DDLApplyInProgress is
	 * set ti true. Captured non-transactional DDL will be placed into
	 * MtmCapturedDDL. In case of error both of this variables are reset by
	 * MtmDDLResetApplyState().
	 */
	Assert(DDLApplyInProgress == false);
	Assert(MtmCapturedDDL == NULL);

	DDLApplyInProgress = true;
	SPI_connect();
	rc = SPI_execute(messageBody, false, 0);
	SPI_finish();
	DDLApplyInProgress = false;

	if (rc < 0)
		elog(ERROR, "Failed to execute utility statement %s", messageBody);

	if (MtmCapturedDDL)
	{
		MemoryContextSwitchTo(MtmApplyContext);
		PushActiveSnapshot(GetTransactionSnapshot());

		// XXX: assert that was non-transactional ddl

		switch (nodeTag(MtmCapturedDDL))
		{
			// case T_VacuumStmt:
			// {
			// 	ExecVacuum((VacuumStmt *) MtmCapturedDDL, 1);
			// 	break;
			// }
			case T_IndexStmt:
			{
				IndexStmt *indexStmt = (IndexStmt *) MtmCapturedDDL;
				Oid relid =	RangeVarGetRelidExtended(indexStmt->relation,
													ShareUpdateExclusiveLock,
														0,
														NULL,
														NULL);
				/* Run parse analysis ... */
				indexStmt = transformIndexStmt(relid, indexStmt, messageBody);

				DefineIndex(relid,		/* OID of heap relation */
							indexStmt,
							InvalidOid, /* no predefined OID */
							InvalidOid, /* no parent index */
							InvalidOid, /* no parent constraint */
							false,		/* is_alter_table */
							true,		/* check_rights */
							true,		/* check_not_in_use */
							false,		/* skip_build */
							false);		/* quiet */

				break;
			}
			case T_DropStmt:
				RemoveObjects((DropStmt *) MtmCapturedDDL);
				break;

			case T_CreateTableSpaceStmt:
				CreateTableSpace((CreateTableSpaceStmt *) MtmCapturedDDL);
				break;

			case T_DropTableSpaceStmt:
				DropTableSpace((DropTableSpaceStmt *) MtmCapturedDDL);
				break;

			default:
				Assert(false);

		}

		pfree(MtmCapturedDDL);
		MtmCapturedDDL = NULL;
	}

	if (ActiveSnapshotSet())
		PopActiveSnapshot();

	/* Log "E" message to reset DDLInProgress in decoder */
	if (transactional)
		MtmFinishDDLCommand();

	debug_query_string = NULL;
}

void
MtmDDLResetApplyState()
{
	MtmCapturedDDL = NULL;
	DDLApplyInProgress = false;
}


/*****************************************************************************
 *
 * Local tables handling
 *
 *****************************************************************************/

Datum
mtm_make_table_local(PG_FUNCTION_ARGS)
{
	Oid	reloid = PG_GETARG_OID(0);
	RangeVar   *rv;
	Relation	rel;
	TupleDesc	tupDesc;
	HeapTuple	tup;
	Datum		values[Natts_mtm_local_tables];
	bool		nulls[Natts_mtm_local_tables];

	MtmMakeRelationLocal(reloid);

	rv = makeRangeVar(MULTIMASTER_SCHEMA_NAME, MULTIMASTER_LOCAL_TABLES_TABLE, -1);
	rel = heap_openrv(rv, RowExclusiveLock);
	if (rel != NULL) {
		char* tableName = get_rel_name(reloid);
		Oid	  schemaid = get_rel_namespace(reloid);
		char* schemaName = get_namespace_name(schemaid);

		tupDesc = RelationGetDescr(rel);

		/* Form a tuple. */
		memset(nulls, false, sizeof(nulls));

		values[Anum_mtm_local_tables_rel_schema - 1] = CStringGetDatum(schemaName);
		values[Anum_mtm_local_tables_rel_name - 1] = CStringGetDatum(tableName);

		tup = heap_form_tuple(tupDesc, values, nulls);

		/* Insert the tuple to the catalog and update the indexes. */
		CatalogTupleInsert(rel, tup);

		/* Cleanup. */
		heap_freetuple(tup);
		heap_close(rel, RowExclusiveLock);

		MtmTx.contains_dml = true;
	}
	return false;
}

static void
MtmMakeRelationLocal(Oid relid)
{
	if (OidIsValid(relid))
	{
		LWLockAcquire(ddl_shared->localtab_lock, LW_EXCLUSIVE);
		hash_search(MtmLocalTables, &relid, HASH_ENTER, NULL);
		LWLockRelease(ddl_shared->localtab_lock);
	}
}

void
MtmMakeTableLocal(char const* schema, char const* name)
{
	RangeVar* rv = makeRangeVar((char*)schema, (char*)name, -1);
	Oid relid = RangeVarGetRelid(rv, NoLock, true);
	MtmMakeRelationLocal(relid);
}

static void
MtmLoadLocalTables(void)
{
	RangeVar	   *rv;
	Relation		rel;
	SysScanDesc		scan;
	HeapTuple		tuple;

	Assert(IsTransactionState());

	rv = makeRangeVar(MULTIMASTER_SCHEMA_NAME, MULTIMASTER_LOCAL_TABLES_TABLE, -1);
	rel = heap_openrv_extended(rv, RowExclusiveLock, true);
	if (rel != NULL) {
		scan = systable_beginscan(rel, 0, true, NULL, 0, NULL);

		while (HeapTupleIsValid(tuple = systable_getnext(scan)))
		{
			MtmLocalTablesTuple	*t = (MtmLocalTablesTuple*) GETSTRUCT(tuple);
			MtmMakeTableLocal(NameStr(t->schema), NameStr(t->name));
		}

		systable_endscan(scan);
		heap_close(rel, RowExclusiveLock);
	}
}

bool
MtmIsRelationLocal(Relation rel)
{
	bool found;

	LWLockAcquire(ddl_shared->localtab_lock, LW_SHARED);
	if (!Mtm->localTablesHashLoaded)
	{
		LWLockRelease(ddl_shared->localtab_lock);
		LWLockAcquire(ddl_shared->localtab_lock, LW_EXCLUSIVE);
		if (!Mtm->localTablesHashLoaded)
		{
			MtmLoadLocalTables();
			Mtm->localTablesHashLoaded = true;
		}
	}

	hash_search(MtmLocalTables, &RelationGetRelid(rel), HASH_FIND, &found);
	LWLockRelease(ddl_shared->localtab_lock);

	return found;
}

/*****************************************************************************
 *
 * Remote functions handling
 *
 *****************************************************************************/

void
MtmSetRemoteFunction(char const* list, void* extra)
{
	if (MtmRemoteFunctions) {
		hash_destroy(MtmRemoteFunctions);
		MtmRemoteFunctions = NULL;
	}
}

static void
MtmInitializeRemoteFunctionsMap()
{
	HASHCTL info;
	char* p, *q;
	int n_funcs = 1;
	FuncCandidateList clist;
	Oid			save_userid;
	int			save_sec_context;
	Oid			mtm_nsp_oid,
				mtm_owner_oid;
	HeapTuple	nsp_tuple;
	Form_pg_namespace mtm_namespace_tuple;


	/* get mtm namespace owner */
	mtm_nsp_oid = get_namespace_oid(MULTIMASTER_SCHEMA_NAME, false);
	nsp_tuple = SearchSysCache1(NAMESPACEOID, ObjectIdGetDatum(mtm_nsp_oid));
	if (!HeapTupleIsValid(nsp_tuple))
		elog(ERROR, "cache lookup failed for namespace %s", MULTIMASTER_SCHEMA_NAME);
	mtm_namespace_tuple = (Form_pg_namespace) GETSTRUCT(nsp_tuple);
	mtm_owner_oid = mtm_namespace_tuple->nspowner;
	ReleaseSysCache(nsp_tuple);

	for (p = MtmRemoteFunctionsList; (q = strchr(p, ',')) != NULL; p = q + 1, n_funcs++);

	Assert(MtmRemoteFunctions == NULL);

	memset(&info, 0, sizeof(info));
	info.entrysize = info.keysize = sizeof(Oid);
	info.hcxt = TopMemoryContext;
	MtmRemoteFunctions = hash_create("MtmRemoteFunctions", n_funcs, &info,
									 HASH_ELEM | HASH_BLOBS | HASH_CONTEXT);

	/*
	 * Escalate our privileges, as current user may not have rights to access
	 * mtm schema.
	 */
	GetUserIdAndSecContext(&save_userid, &save_sec_context);
	SetUserIdAndSecContext(mtm_owner_oid,
						   save_sec_context | SECURITY_LOCAL_USERID_CHANGE);

	p = pstrdup(MtmRemoteFunctionsList);
	do {
		q = strchr(p, ',');
		if (q != NULL) {
			*q++ = '\0';
		}
		clist = FuncnameGetCandidates(stringToQualifiedNameList(p), -1, NIL, false, false, true);
		if (clist == NULL) {
			mtm_log(WARNING, "Failed to lookup function %s", p);
		} else if (clist->next != NULL) {
			elog(ERROR, "Ambigious function %s", p);
		} else {
			hash_search(MtmRemoteFunctions, &clist->oid, HASH_ENTER, NULL);
		}
		p = q;
	} while (p != NULL);

	clist = FuncnameGetCandidates(stringToQualifiedNameList("mtm.alter_sequences"), -1, NIL, false, false, true);
	if (clist != NULL) {
		hash_search(MtmRemoteFunctions, &clist->oid, HASH_ENTER, NULL);
	}

	/* restore back current user context */
	SetUserIdAndSecContext(save_userid, save_sec_context);
}

/*****************************************************************************
 *
 * Sequences handling
 *
 *****************************************************************************/

static void
MtmSeqNextvalHook(Oid seqid, int64 next)
{
	if (MtmMonotonicSequences && MtmIsEnabled())
	{
		MtmSeqPosition pos;
		pos.seqid = seqid;
		pos.next = next;
		LogLogicalMessage("N", (char*)&pos, sizeof(pos), true);
	}
}

static void
AdjustCreateSequence(List *options)
{
	bool has_increment = false, has_start = false;
	ListCell   *option;

	if (!MtmIsEnabled())
		return;

	foreach(option, options)
	{
		DefElem    *defel = (DefElem *) lfirst(option);
		if (strcmp(defel->defname, "increment") == 0)
			has_increment = true;
		else if (strcmp(defel->defname, "start") == 0)
			has_start = true;
	}

	if (!has_increment)
	{
		DefElem *defel = makeDefElem("increment", (Node *) makeInteger(MtmMaxNodes), -1);
		options = lappend(options, defel);
	}

	if (!has_start)
	{
		DefElem *defel = makeDefElem("start", (Node *) makeInteger(Mtm->my_node_id), -1);
		options = lappend(options, defel);
	}
}

/*****************************************************************************
 *
 * Various
 *
 *****************************************************************************/


void
MtmDDLResetStatement()
{
	MtmDDLStatement = NULL;
}

/*
 * Allow to replicate handcrafted heap inserts/updates.
 * Needed for scheduler.
 */
void
MtmToggleDML(void)
{
	MtmTx.contains_dml = true;
}
