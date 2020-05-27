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
#include "commands/partition.h"
#include "commands/tablecmds.h"
#include "parser/parse_type.h"
#include "parser/parse_func.h"
#include "commands/sequence.h"
#include "tcop/pquery.h"
#include "utils/snapmgr.h"
#include "nodes/makefuncs.h"
#include "nodes/nodeFuncs.h"
#include "catalog/pg_constraint.h"
#include "catalog/pg_namespace.h"
#include "executor/spi.h"
#include "utils/lsyscache.h"
#include "catalog/indexing.h"
#include "commands/tablespace.h"
#include "commands/typecmds.h"
#include "parser/parse_utilcmd.h"
#include "commands/defrem.h"
#include "utils/regproc.h"
#include "replication/message.h"
#include "access/relscan.h"
#include "commands/vacuum.h"
#include "utils/inval.h"
#include "utils/builtins.h"
#include "replication/origin.h"
#include "catalog/pg_authid.h"
#include "storage/ipc.h"
#include "miscadmin.h"

#include "ddl.h"
#include "logger.h"
#include "commit.h"

#include "multimaster.h"

/*  XXX: is it defined somewhere? */
#define GUC_KEY_MAXLEN					255
#define MTM_GUC_HASHSIZE				100
#define MULTIMASTER_MAX_LOCAL_TABLES	256

#define Natts_mtm_local_tables 2
#define Anum_mtm_local_tables_rel_schema 1
#define Anum_mtm_local_tables_rel_name	 2

struct DDLSharedState
{
	LWLock	   *localtab_lock;
}		   *ddl_shared;

typedef struct MtmGucEntry
{
	char		key[GUC_KEY_MAXLEN];
	dlist_node	list_node;
} MtmGucEntry;

typedef struct
{
	NameData	schema;
	NameData	name;
} MtmLocalTablesTuple;

/* GUCs */
bool		MtmVolksWagenMode;
bool		MtmMonotonicSequences;
char	   *MtmRemoteFunctionsList;
bool		MtmIgnoreTablesWithoutPk;

static char MtmTempSchema[NAMEDATALEN];
static bool TempDropRegistered;

static void const *MtmDDLStatement;

static Node *MtmCapturedDDL;
static bool DDLApplyInProgress;

static HTAB *MtmGucHash = NULL;
static dlist_head MtmGucList = DLIST_STATIC_INIT(MtmGucList);

static HTAB *MtmRemoteFunctions;
static bool MtmRemoteFunctionsValid;
static HTAB *MtmLocalTables;

static ExecutorStart_hook_type PreviousExecutorStartHook;
static ExecutorFinish_hook_type PreviousExecutorFinishHook;
static ProcessUtility_hook_type PreviousProcessUtilityHook;
static seq_nextval_hook_t PreviousSeqNextvalHook;


/* Set given temp namespace in receiver */
PG_FUNCTION_INFO_V1(mtm_set_temp_schema);

static void MtmSeqNextvalHook(Oid seqid, int64 next);
static void MtmExecutorStart(QueryDesc *queryDesc, int eflags);
static void MtmExecutorFinish(QueryDesc *queryDesc);

static void MtmProcessUtility(PlannedStmt *pstmt,
				  const char *queryString,
				  ProcessUtilityContext context, ParamListInfo params,
				  QueryEnvironment *queryEnv, DestReceiver *dest,
				  char *completionTag);

static void MtmProcessUtilityReceiver(PlannedStmt *pstmt,
						  const char *queryString,
						  ProcessUtilityContext context, ParamListInfo params,
						  QueryEnvironment *queryEnv, DestReceiver *dest,
						  char *completionTag);

static void MtmProcessUtilitySender(PlannedStmt *pstmt,
						const char *queryString,
						ProcessUtilityContext context, ParamListInfo params,
						QueryEnvironment *queryEnv, DestReceiver *dest,
						char *completionTag);

static void MtmGucUpdate(const char *key);
static void MtmInitializeRemoteFunctionsMap(void);
static char *MtmGucSerialize(void);
static void MtmMakeRelationLocal(Oid relid, bool locked);
static List *AdjustCreateSequence(List *options);

static void MtmProcessDDLCommand(char const *queryString, bool transactional);
static void MtmFinishDDLCommand(void);

PG_FUNCTION_INFO_V1(mtm_make_table_local);

/*****************************************************************************
 *
 * Init
 *
 *****************************************************************************/

void
MtmDDLReplicationInit()
{
	Size		size = 0;

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
 * Temp DDL handling.
 *
 * EE version allows to prepare transactions with temporary objects. Data of
 * temp tables will not be decoded, but DDL will and must be sent to peer
 * nodes. That allows to handle such cases as:
 *	  * CREATE (persistent) table AS (temporary) -- receiver needs definition
 *		of previous temp table to create current persistent.
 *	  * CREATE FUNCTION foo(x my_temp_table) -- same
 *	  * DROP (persistent) object RECURSIVE -- can touch to some temp object.
 *
 * Each backend along with DDL to be replicated sends call to
 * `select mtm.set_temp_schema('%s');` where %s constructed out of node_id and
 * backend_id. Upon exit backends are write 'DROP SCHEMA ...' logical message
 * commanding receivers to drop all temp stuff produced by that backend.
 *
 * Each receiver during execution of ddl will come across `set_temp_schema()`
 * and will create or set mentioned namespace as temporary (in a same way as
 * parallel workers do that). After transaction execution on_commit_actions
 * also should be cleaned (see comments in apply code).
 *
 *****************************************************************************/

/* Drop temp schemas on peer nodes */
static void
temp_schema_reset(void)
{
	Assert(TempDropRegistered);

	MtmProcessDDLCommand(
						 psprintf("select mtm.set_temp_schema('%s'); "
								  "DROP SCHEMA IF EXISTS %s_toast CASCADE; "
								  "DROP SCHEMA IF EXISTS %s CASCADE;",
								  MtmTempSchema, MtmTempSchema, MtmTempSchema),
						 false
		);
	MtmFinishDDLCommand();
}

/* Exit callback to call temp_schema_reset() */
static void
temp_schema_at_exit(int status, Datum arg)
{
	Assert(TempDropRegistered);
	AbortOutOfAnyTransaction();
	StartTransactionCommand();
	temp_schema_reset();
	CommitTransactionCommand();
}

/* Register cleanup callback and generate temp schema name */
static void
temp_schema_init(void)
{
	if (!TempDropRegistered)
	{
		char	   *temp_schema;
		unsigned short drandom_seed[3] = {0, 0, 0};
		double	   rand1;
		double	   rand2;

		/*
		 * borrowed from float.c
		 * TODO: unify with state.c usage
		 */
		{
			TimestampTz now = GetCurrentTimestamp();
			uint64		iseed;

			/* Mix the PID with the most predictable bits of the timestamp */
			iseed = (uint64) now ^ ((uint64) MyProcPid << 32);
			drandom_seed[0] = (unsigned short) iseed;
			drandom_seed[1] = (unsigned short) (iseed >> 16);
			drandom_seed[2] = (unsigned short) (iseed >> 32);
		}
		rand1 = pg_erand48(drandom_seed);
		rand2 = pg_erand48(drandom_seed);

		/*
		 * NB: namespace.c:isMtmTemp() assumes 'mtm_tmp_' prefix for mtm temp
		 * tables to defuse autovacuum.
		 *
		 * rand1 and rand2 are a kludge providing sorta unique name; without
		 * it, schema name can be reused immediately after backend exit before
		 * it was dropped at peers. An alternative which existed previously --
		 * running full-fledged xact with transactional ddl in on_shmem_exit
		 * hook -- is hard because of commit.c on on_shmem_exit cleanup
		 * interference and fragile (what if xact aborted?)
		 */
		temp_schema = psprintf("mtm_tmp_%d_%d_%llx%llx",
							   Mtm->my_node_id, MyBackendId,
							   (unsigned long long) (rand1 * ((unsigned long long) 1 << 48)),
							   (unsigned long long) (rand2 * ((unsigned long long) 1 << 48)));
		memcpy(&MtmTempSchema, temp_schema, strlen(temp_schema) + 1);
		before_shmem_exit(temp_schema_at_exit, (Datum) 0);
		TempDropRegistered = true;
		pfree(temp_schema);
	}
}

Datum
mtm_set_temp_schema(PG_FUNCTION_ARGS)
{
	char	   *temp_namespace = text_to_cstring(PG_GETARG_TEXT_P(0));
	char	   *temp_toast_namespace = psprintf("%s_toast", temp_namespace);
	Oid			nsp_oid;
	Oid			toast_nsp_oid;

	if (!SearchSysCacheExists1(NAMESPACENAME, PointerGetDatum(temp_namespace)))
	{
		nsp_oid = NamespaceCreate(temp_namespace, BOOTSTRAP_SUPERUSERID, true);
		toast_nsp_oid = NamespaceCreate(temp_toast_namespace, BOOTSTRAP_SUPERUSERID, true);
		CommandCounterIncrement();
	}
	else
	{
		nsp_oid = get_namespace_oid(temp_namespace, false);
		toast_nsp_oid = get_namespace_oid(temp_toast_namespace, false);
	}

	SetTempNamespaceState(nsp_oid, toast_nsp_oid);
	PG_RETURN_VOID();
}

/*****************************************************************************
 *
 * Guc handling
 *
 *****************************************************************************/

/*  XXX: move to ShmemStart? */
static void
MtmGucInit(void)
{
	HASHCTL		hash_ctl;
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
	 * If current role is not equal to MtmDatabaseUser, than set it before any
	 * other GUC vars.
	 *
	 * XXX: try to avoid using MtmDatabaseUser somehow
	 */
	oldcontext = MemoryContextSwitchTo(TopMemoryContext);

	/*
	 * XXX if (current_role && *current_role && strcmp(MtmDatabaseUser,
	 * current_role) != 0)
	 */
	MtmGucUpdate("session_authorization");
	MemoryContextSwitchTo(oldcontext);
}

static void
MtmGucDiscard()
{
	if (dlist_is_empty(&MtmGucList))
		return;

	dlist_init(&MtmGucList);

	hash_destroy(MtmGucHash);
	MtmGucHash = NULL;
}

static inline void
MtmGucUpdate(const char *key)
{
	MtmGucEntry *hentry;
	bool		found;

	if (!MtmGucHash)
		MtmGucInit();

	hentry = (MtmGucEntry *) hash_search(MtmGucHash, key, HASH_ENTER, &found);
	if (found)
		dlist_delete(&hentry->list_node);

	dlist_push_tail(&MtmGucList, &hentry->list_node);
}

static inline void
MtmGucRemove(const char *key)
{
	MtmGucEntry *hentry;
	bool		found;

	if (!MtmGucHash)
		MtmGucInit();

	hentry = (MtmGucEntry *) hash_search(MtmGucHash, key, HASH_FIND, &found);
	if (found)
	{
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
			MtmGucUpdate(stmt->name);
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
	const struct config_generic *confa = *(struct config_generic *const *) a;
	const struct config_generic *confb = *(struct config_generic *const *) b;

	return _guc_name_compare(confa->name, confb->name);
}

static struct config_generic *
fing_guc_conf(const char *name)
{
	int			num;
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
	StringInfo	serialized_gucs = makeStringInfo();
	dlist_iter	iter;
	const char *search_path;
	bool		found;
	Oid			ceUserId = GetUserId();
	Oid			csUserId = GetSessionUserId();
	bool		useRole = is_member_of_role(csUserId, ceUserId);

	if (!MtmGucHash)
		MtmGucInit();

	Assert(TempDropRegistered);
	appendStringInfoString(serialized_gucs, "RESET session_authorization; ");
	appendStringInfo(serialized_gucs, "select mtm.set_temp_schema('%s'); ", MtmTempSchema);

	hash_search(MtmGucHash, "session_authorization", HASH_FIND, &found);
	if (found)
	{
		MemoryContext oldcontext;

		oldcontext = MemoryContextSwitchTo(TopMemoryContext);
		hash_search(MtmGucHash, "role", HASH_FIND, &found);
		if ((found) && (ceUserId == csUserId))

			/*
			 * We need to do this because SET LOCAL return only WARNING if is
			 * used out of transaction block. DDL will be passed to another
			 * nodes and will set "role" variable at current node.
			 */
			MtmGucRemove("role");
		else if ((!found) && (ceUserId != csUserId) && useRole)

			/*
			 * We need to do this because SECURITY DEFINER changed current
			 * user value quietly.
			 */
			MtmGucUpdate("role");
		MemoryContextSwitchTo(oldcontext);
	}

	dlist_foreach(iter, &MtmGucList)
	{
		MtmGucEntry *cur_entry = dlist_container(MtmGucEntry, list_node, iter.cur);
		struct config_generic *gconf;
		const char *gucValue;

		if (strcmp(cur_entry->key, "search_path") == 0)
			continue;

		appendStringInfoString(serialized_gucs, "SET ");
		appendStringInfoString(serialized_gucs, cur_entry->key);
		appendStringInfoString(serialized_gucs, " TO ");

		/*
		 * Current effective user can have more privileges than session user
		 * (increase in rights by SECURITY DEFINER, for example). In this case
		 * we need to set session authorization role in the current user
		 * value.
		 */
		if (strcmp(cur_entry->key, "session_authorization") == 0)
			gucValue = GetUserNameFromId(useRole ? csUserId : ceUserId, false);
		else
			gucValue = GetConfigOption(cur_entry->key, false, true);

		gconf = fing_guc_conf(cur_entry->key);
		if (gconf && (gconf->vartype == PGC_STRING ||
					  gconf->vartype == PGC_ENUM ||
					  (gconf->flags & (GUC_UNIT_MEMORY | GUC_UNIT_TIME))))
		{
			appendStringInfoString(serialized_gucs, "'");
			appendStringInfoString(serialized_gucs, gucValue);
			appendStringInfoString(serialized_gucs, "'");
		}
		else
			appendStringInfoString(serialized_gucs, gucValue);

		appendStringInfoString(serialized_gucs, "; ");
	}

	/*
	 * Crutch for scheduler. It sets search_path through SetConfigOption() so
	 * our callback do not react on that.
	 */
	search_path = GetConfigOption("search_path", false, true);
	if (strcmp(search_path, "\"\"") == 0 || strlen(search_path) == 0)
		appendStringInfo(serialized_gucs, "SET search_path TO ''; ");
	else
		appendStringInfo(serialized_gucs, "SET search_path TO %s; ", search_path);

	return serialized_gucs->data;
}



/*****************************************************************************
 *
 * Capture DDL statements and send them down to subscribers
 *
 *****************************************************************************/

static void
MtmProcessDDLCommand(char const *queryString, bool transactional)
{
	if (transactional)
	{
		char	   *gucCtx;

		temp_schema_init();
		gucCtx = MtmGucSerialize();
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


static void
MtmProcessUtility(PlannedStmt *pstmt, const char *queryString,
				  ProcessUtilityContext context, ParamListInfo params,
				  QueryEnvironment *queryEnv, DestReceiver *dest,
				  char *completionTag)
{
	if (MtmIsLogicalReceiver)
	{
		MtmProcessUtilityReceiver(pstmt, queryString, context, params,
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
MtmProcessUtilityReceiver(PlannedStmt *pstmt, const char *queryString,
						  ProcessUtilityContext context, ParamListInfo params,
						  QueryEnvironment *queryEnv, DestReceiver *dest,
						  char *completionTag)
{
	Node	   *parsetree = pstmt->utilityStmt;

	/* catch only DDL produced by SPI in MtmApplyDDLMessage() */
	if (DDLApplyInProgress)
	{
		MemoryContext oldMemContext = MemoryContextSwitchTo(MtmApplyContext);
		bool		captured = false;

		mtm_log(DDLProcessingTrace,
				"MtmProcessUtilityReceiver: tag=%s, context=%d, issubtrans=%d,  statement=%s",
				CreateCommandTag(parsetree), context, IsSubTransaction(), queryString);

		Assert(oldMemContext != MtmApplyContext);
		Assert(MtmApplyContext != NULL);

		/* copy parsetrees of interest to MtmCapturedDDL */
		switch (nodeTag(parsetree))
		{
			case T_CreateTableSpaceStmt:
			case T_DropTableSpaceStmt:
				Assert(MtmCapturedDDL == NULL);
				MtmCapturedDDL = copyObject(parsetree);
				captured = true;
				break;

			case T_IndexStmt:
				{
					IndexStmt  *stmt = (IndexStmt *) parsetree;

					if (stmt->concurrent)
					{
						Assert(MtmCapturedDDL == NULL);
						MtmCapturedDDL = (Node *) copyObject(stmt);
						captured = true;
					}
					break;
				}

			case T_PartitionStmt:
				{
					PartitionStmt *stmt = (PartitionStmt *) parsetree;

					if (stmt->concurrent)
					{
						Assert(MtmCapturedDDL == NULL);
						MtmCapturedDDL = (Node *) copyObject(stmt);
						captured = true;
					}
					break;
				}

			case T_AlterEnumStmt:
				{
					AlterEnumStmt *stmt = (AlterEnumStmt *) parsetree;

					Assert(MtmCapturedDDL == NULL);
					MtmCapturedDDL = (Node *) copyObject(stmt);
					captured = true;
					break;
				}

			case T_DropStmt:
				{
					DropStmt   *stmt = (DropStmt *) parsetree;

					if (stmt->removeType == OBJECT_INDEX && stmt->concurrent)
					{
						Assert(MtmCapturedDDL == NULL);
						MtmCapturedDDL = (Node *) copyObject(stmt);
						captured = true;
					}

					/*
					 * Make it possible to drop functions which were not
					 * replicated
					 */
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
						stmt->options = AdjustCreateSequence(stmt->options);
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
	bool		skipCommand = false;
	bool		executed = false;
	Node	   *parsetree = pstmt->utilityStmt;
	int			stmt_start = pstmt->stmt_location > 0 ? pstmt->stmt_location : 0;
	int			stmt_len = pstmt->stmt_len > 0 ? pstmt->stmt_len : strlen(queryString + stmt_start);
	char	   *stmt_string = palloc(stmt_len + 1);
	bool		isTopLevel = (context == PROCESS_UTILITY_TOPLEVEL);

	strncpy(stmt_string, queryString + stmt_start, stmt_len);
	stmt_string[stmt_len] = 0;

	temp_schema_init();

	mtm_log(DDLProcessingTrace,
			"MtmProcessUtilitySender tag=%d, context=%d, issubtrans=%d,  statement=%s",
			nodeTag(parsetree), context, IsSubTransaction(), stmt_string);

	switch (nodeTag(parsetree))
	{
		case T_TransactionStmt:
			{
				TransactionStmt *stmt = (TransactionStmt *) parsetree;

				/*
				 * hack: if we are going to commit/prepare but our transaction
				 * block is already aborted, we'd better just fast pass this
				 * over to the core code before checking whether mtm state
				 * allows to commit (and generally starting complicated commit
				 * procedure). We expect PrepareTransactionBlock not to fail
				 * after this. Hackish, as it repurposes
				 * TransactionBlockStatusCode.
				 */
				if ((stmt->kind == TRANS_STMT_COMMIT ||
					 stmt->kind == TRANS_STMT_PREPARE) &&
					TransactionBlockStatusCode() == 'E')
				{
					skipCommand = true;
					break;
				}

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
		case T_ReassignOwnedStmt:
			//XXX ?
				case T_LockStmt :
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
					stmt->options = AdjustCreateSequence(stmt->options);
				break;
			}

		case T_CreateTableSpaceStmt:
		case T_DropTableSpaceStmt:
			skipCommand = true;
			MtmProcessDDLCommand(stmt_string, false);
			break;

			/*
			 * Explain will not call ProcessUtility for passed
			 * CreateTableAsStmt, but will run it manually, so we will not
			 * catch it in a standard way. So catch it in a non-standard way.
			 */
		case T_ExplainStmt:
			{
				ExplainStmt *stmt = (ExplainStmt *) parsetree;
				Query	   *query = (Query *) stmt->query;
				ListCell   *lc;

				skipCommand = true;
				if (query->commandType == CMD_UTILITY &&
					IsA(query->utilityStmt, CreateTableAsStmt))
				{
					foreach(lc, stmt->options)
					{
						DefElem    *opt = (DefElem *) lfirst(lc);

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

				if (stmt->target == DISCARD_TEMP)
					temp_schema_reset();

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

					/*
					 * Catch GUC assignment after it will be performed, as it
					 * still may fail.
					 */
				}

				break;
			}

			/*
			 * Index is created at replicas completely asynchronously, so to
			 * prevent unintended interleaving with subsequent commands in
			 * this session, just wait here for a while. It will help to pass
			 * regression tests but will not be enough for construction of
			 * real large indexes where difference between completion of this
			 * operation at different nodes is unlimited
			 */
		case T_IndexStmt:
			{
				IndexStmt  *indexStmt = (IndexStmt *) parsetree;

				if (indexStmt->concurrent && context == PROCESS_UTILITY_TOPLEVEL)
				{
					/*
					 * Our brand new straightforward deadlock detector (bail
					 * out whenever receiver is waiting for anyone,
					 * essentially) is not aware of non-transactional DDL yet,
					 * which creates false deadlocks in regression tests.
					 * Technically this is true for any other non-tx DDL, but
					 * other forms haven't created problems for now.
					 */
					ereport(ERROR,
							(errcode(ERRCODE_FEATURE_NOT_SUPPORTED),
							 errmsg("multimaster doesn't support CREATE INDEX CONCURRENTLY")));
					/*
					 * MtmProcessDDLCommand(stmt_string, false);
					 * skipCommand = true;
					 */
				}
				break;
			}

		case T_PartitionStmt:
			{
				PartitionStmt *stmt = (PartitionStmt *) parsetree;

				if (stmt->concurrent && context == PROCESS_UTILITY_TOPLEVEL)
				{
					MtmProcessDDLCommand(stmt_string, false);
					skipCommand = true;
				}
				break;
			}

		case T_DropStmt:
			{
				DropStmt   *stmt = (DropStmt *) parsetree;

				if (stmt->removeType == OBJECT_INDEX && stmt->concurrent &&
					context == PROCESS_UTILITY_TOPLEVEL)
				{
					/* c.f. CREATE INDEX CONCURRENTLY */
					ereport(ERROR,
							(errcode(ERRCODE_FEATURE_NOT_SUPPORTED),
							 errmsg("multimaster doesn't support DROP INDEX CONCURRENTLY")));
				}
				break;
			}

			/* Copy need some special care */
		case T_CopyStmt:
			{
				CopyStmt   *copyStatement = (CopyStmt *) parsetree;

				skipCommand = true;
				if (copyStatement->is_from)
				{
					RangeVar   *relation = copyStatement->relation;

					if (relation != NULL)
					{
						Oid			relid = RangeVarGetRelid(relation, NoLock, true);

						if (OidIsValid(relid))
						{
							Relation	rel = heap_open(relid, ShareLock);

							if (RelationNeedsWAL(rel))
								MtmTx.contains_dml = true;

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

	/* Catch GUC assignment */
	if (nodeTag(parsetree) == T_VariableSetStmt)
	{
		VariableSetStmt *stmt = (VariableSetStmt *) parsetree;

		if (!IsTransactionBlock())
		{
			MtmGucSet(stmt, stmt_string);
		}
	}

	if (executed)
	{
		MtmFinishDDLCommand();
		MtmDDLStatement = NULL;
		MtmTx.contains_ddl = true;
	}
}

static bool
targetList_walker(Node *node, void *context)
{
	if (node == NULL)
		return false;

	if (IsA(node, FuncExpr))
	{
		Oid			func_oid = ((FuncExpr *) node)->funcid;

		if (hash_search(MtmRemoteFunctions, &func_oid, HASH_FIND, NULL))
			return true;
	}

	return expression_tree_walker(node, targetList_walker, context);
}

static bool
search_for_remote_functions(PlanState *node, void *context)
{
	if (node == NULL)
		return false;

	if (targetList_walker((Node *) node->plan->targetlist, NULL))
		return true;

	return planstate_tree_walker(node, search_for_remote_functions, NULL);
}

static void
MtmExecutorStart(QueryDesc *queryDesc, int eflags)
{
	if (PreviousExecutorStartHook != NULL)
		PreviousExecutorStartHook(queryDesc, eflags);
	else
		standard_ExecutorStart(queryDesc, eflags);

	/*
	 * Explain can contain remote functions. But we don't need to send it to
	 * another nodes of multimaster.
	 */
	if (eflags & EXEC_FLAG_EXPLAIN_ONLY)
		return;

	if (!MtmIsLogicalReceiver && !MtmDDLStatement && MtmIsEnabled())
	{
		if (!MtmRemoteFunctionsValid)
		{
			PG_TRY();
			{
				MtmInitializeRemoteFunctionsMap();
			}
			PG_CATCH();
			{
				errcontext("DETAIL: During parsing of remote functions string: '%s'", MtmRemoteFunctionsList);
				PG_RE_THROW();
			}
			PG_END_TRY();
		}

		Assert(queryDesc->planstate);

		/*
		 * If any node contains function from the remote functions list in its
		 * target list than we will send all query as a DDL command.
		 */
		if (search_for_remote_functions(queryDesc->planstate, NULL))
		{
			MtmProcessDDLCommand(queryDesc->sourceText, true);
			MtmDDLStatement = queryDesc;
			MtmTx.contains_ddl = true;
		}
		else
			mtm_log(DDLProcessingTrace, "The query plan don't contain any remote functions");
	}
}

static void
MtmExecutorFinish(QueryDesc *queryDesc)
{
	/*
	 * If tx didn't wrote to XLOG then there is nothing to commit on other
	 * nodes.
	 */

	CmdType		operation = queryDesc->operation;
	EState	   *estate = queryDesc->estate;
	PlannedStmt *pstmt = queryDesc->plannedstmt;

	if (MtmIsEnabled())
	{
		if (operation == CMD_INSERT || operation == CMD_UPDATE ||
			operation == CMD_DELETE || pstmt->hasModifyingCTE)
		{
			int			i;

			for (i = 0; i < estate->es_num_result_relations; i++)
			{
				Relation	rel = estate->es_result_relations[i].ri_RelationDesc;

				if (RelationNeedsWAL(rel))
				{
					if (MtmIgnoreTablesWithoutPk)
					{
						if (!rel->rd_indexvalid)
							RelationGetIndexList(rel);

						if (rel->rd_replidindex == InvalidOid)
						{
							/* XXX */
							MtmMakeRelationLocal(RelationGetRelid(rel), false);
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
		/* XXX try to filter out matviews in rowfilter */
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
	int			rc;

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
	 * MtmProcessUtilityReceiver() will work only when DDLApplyInProgress is
	 * set to true. Captured non-transactional DDL will be placed into
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

		/* XXX: assert that was non-transactional ddl */

		switch (nodeTag(MtmCapturedDDL))
		{
			case T_IndexStmt:
				{
					IndexStmt  *indexStmt = (IndexStmt *) MtmCapturedDDL;
					Oid			relid = RangeVarGetRelidExtended(indexStmt->relation,
																 ShareUpdateExclusiveLock,
																 0,
																 NULL,
																 NULL);

					/* Run parse analysis ... */
					indexStmt = transformIndexStmt(relid, indexStmt, messageBody);

					DefineIndex(relid,	/* OID of heap relation */
								indexStmt,
								InvalidOid, /* no predefined OID */
								InvalidOid, /* no parent index */
								InvalidOid, /* no parent constraint */
								false,	/* is_alter_table */
								true,	/* check_rights */
								true,	/* check_not_in_use */
								false,	/* skip_build */
								false); /* quiet */

					break;
				}

			case T_PartitionStmt:
				{
					Oid			relid;
					PartitionStmt *pstmt = (PartitionStmt *) MtmCapturedDDL;

					relid = RangeVarGetRelid(pstmt->relation, NoLock, false);
					create_partitions(pstmt->partSpec,
									  relid,
									  pstmt->concurrent ? PDT_CONCURRENT : PDT_REGULAR);
				}
				break;

			case T_DropStmt:
			{
				DropStmt *stmt = (DropStmt *) MtmCapturedDDL;

				switch (stmt->removeType)
				{
					case OBJECT_INDEX:
					case OBJECT_TABLE:
					case OBJECT_SEQUENCE:
					case OBJECT_VIEW:
					case OBJECT_MATVIEW:
					case OBJECT_FOREIGN_TABLE:
						RemoveRelations(stmt);
						break;
					default:
						RemoveObjects(stmt);
						break;
				}
			}
				break;

			case T_CreateTableSpaceStmt:
				CreateTableSpace((CreateTableSpaceStmt *) MtmCapturedDDL);
				break;

			case T_DropTableSpaceStmt:
				DropTableSpace((DropTableSpaceStmt *) MtmCapturedDDL);
				break;

			case T_AlterEnumStmt:
				AlterEnum((AlterEnumStmt *) MtmCapturedDDL, true);
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
	Oid			reloid = PG_GETARG_OID(0);
	RangeVar   *rv;
	Relation	rel;
	TupleDesc	tupDesc;
	HeapTuple	tup;
	Datum		values[Natts_mtm_local_tables];
	bool		nulls[Natts_mtm_local_tables];

	MtmMakeRelationLocal(reloid, false);

	rv = makeRangeVar(MULTIMASTER_SCHEMA_NAME, MULTIMASTER_LOCAL_TABLES_TABLE, -1);
	rel = heap_openrv(rv, RowExclusiveLock);
	if (rel != NULL)
	{
		char	   *tableName = get_rel_name(reloid);
		Oid			schemaid = get_rel_namespace(reloid);
		char	   *schemaName = get_namespace_name(schemaid);

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
MtmMakeRelationLocal(Oid relid, bool locked)
{
	if (OidIsValid(relid))
	{
		if (!locked)
			LWLockAcquire(ddl_shared->localtab_lock, LW_EXCLUSIVE);
		hash_search(MtmLocalTables, &relid, HASH_ENTER, NULL);
		if (!locked)
			LWLockRelease(ddl_shared->localtab_lock);
	}
}

void
MtmMakeTableLocal(char const *schema, char const *name, bool locked)
{
	RangeVar   *rv = makeRangeVar((char *) schema, (char *) name, -1);
	Oid			relid = RangeVarGetRelid(rv, NoLock, true);

	MtmMakeRelationLocal(relid, locked);
}

static void
MtmLoadLocalTables(void)
{
	RangeVar   *rv;
	Relation	rel;
	SysScanDesc scan;
	HeapTuple	tuple;

	Assert(IsTransactionState());
	Assert(LWLockHeldByMeInMode(ddl_shared->localtab_lock, LW_EXCLUSIVE));

	rv = makeRangeVar(MULTIMASTER_SCHEMA_NAME, MULTIMASTER_LOCAL_TABLES_TABLE, -1);
	rel = heap_openrv_extended(rv, RowExclusiveLock, true);
	if (rel != NULL)
	{
		scan = systable_beginscan(rel, 0, true, NULL, 0, NULL);

		while (HeapTupleIsValid(tuple = systable_getnext(scan)))
		{
			MtmLocalTablesTuple *t = (MtmLocalTablesTuple *) GETSTRUCT(tuple);

			MtmMakeTableLocal(NameStr(t->schema), NameStr(t->name), true);
		}

		systable_endscan(scan);
		heap_close(rel, RowExclusiveLock);
	}
}

bool
MtmIsRelationLocal(Relation rel)
{
	bool		found;

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
MtmSetRemoteFunction(char const *list, void *extra)
{
	MtmRemoteFunctionsValid = false;
}

static void
MtmInitializeRemoteFunctionsMap()
{
	HASHCTL		info;
	char	   *p,
			   *q;
	int			n_funcs = 1;
	FuncCandidateList clist;
	Oid			save_userid;
	int			save_sec_context;

	Assert(!MtmRemoteFunctionsValid);

	for (p = MtmRemoteFunctionsList; (q = strchr(p, ',')) != NULL; p = q + 1, n_funcs++);

	if (MtmRemoteFunctions)
		hash_destroy(MtmRemoteFunctions);

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
	SetUserIdAndSecContext(BOOTSTRAP_SUPERUSERID,
						   save_sec_context | SECURITY_LOCAL_USERID_CHANGE);

	p = pstrdup(MtmRemoteFunctionsList);
	do
	{
		q = strchr(p, ',');
		if (q != NULL)
			*q++ = '\0';

		clist = FuncnameGetCandidates(stringToQualifiedNameList(p), -1, NIL, false, false, true);
		if (clist == NULL)
			mtm_log(DEBUG1, "Can't resolve function '%s', postponing that", p);
		else
		{
			while (clist != NULL)
			{
				mtm_log(DEBUG1, "multimaster.remote_functions: add '%s'", p);
				hash_search(MtmRemoteFunctions, &clist->oid, HASH_ENTER, NULL);
				clist = clist->next;
			}
		}
		p = q;
	} while (p != NULL);

	clist = FuncnameGetCandidates(stringToQualifiedNameList("mtm.alter_sequences"), -1, NIL, false, false, true);
	if (clist != NULL)
		hash_search(MtmRemoteFunctions, &clist->oid, HASH_ENTER, NULL);

	/* restore back current user context */
	SetUserIdAndSecContext(save_userid, save_sec_context);

	MtmRemoteFunctionsValid = true;
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
		LogLogicalMessage("N", (char *) &pos, sizeof(pos), true);
	}
}

static List *
AdjustCreateSequence(List *options)
{
	bool		has_increment = false;
	bool		has_start = false;
	ListCell   *option;
	DefElem    *defel;

	if (!MtmIsEnabled())
		return options;

	foreach(option, options)
	{
		defel = (DefElem *) lfirst(option);
		if (strcmp(defel->defname, "increment") == 0)
			has_increment = true;
		else if (strcmp(defel->defname, "start") == 0)
			has_start = true;
	}

	if (!has_increment)
	{
		MtmConfig  *mtm_cfg = MtmLoadConfig();
		int			i;
		int			max_node;

		max_node = mtm_cfg->my_node_id;
		for (i = 0; i < mtm_cfg->n_nodes; i++)
		{
			if (max_node < mtm_cfg->nodes[i].node_id)
				max_node = mtm_cfg->nodes[i].node_id;
		}

		defel = makeDefElem("increment", (Node *) makeInteger(max_node), -1);
		options = lappend(options, defel);
	}

	if (!has_start)
	{
		defel = makeDefElem("start", (Node *) makeInteger(Mtm->my_node_id), -1);
		options = lappend(options, defel);
	}

	return options;
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
