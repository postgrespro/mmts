/*----------------------------------------------------------------------------
 *
 * logger.h
 *		GUC-controlled map from application meaningful log tags to actual log
 *		levels.
 *
 * Portions Copyright (c) 1996-2018, PostgreSQL Global Development Group
 * Portions Copyright (c) 1994, Regents of the University of California
 *
 *----------------------------------------------------------------------------
 */

#include "postgres.h"

#include "postmaster/bgworker.h"
#include "utils/elog.h"
#include "utils/memutils.h"

/*
 * this hack allows to use mtm_log with direct log level (e.g. ERROR), see
 * mtm_log
 */
#define FIRST_UNUSED_ERRCODE (PANIC + 1)

/* keep it in sync with mtm_log_gucs */
typedef enum MtmLogTag
{
	/* general */
	MtmTxTrace				= FIRST_UNUSED_ERRCODE,
	MtmTxFinish,

	/* coordinator */
	MtmCoordinatorTrace,

	/* dmq */
	DmqStateIntermediate,
	DmqStateFinal,
	DmqTraceOutgoing,
	DmqTraceIncoming,
	DmqTraceShmMq,
	DmqPqTiming,

	/* resolver */
	ResolverState,
	ResolverTx,
	ResolverTasks,

	/* status worker */
	StatusRequest,

	/* pool */
	BgwPoolEvent,
	BgwPoolEventDebug,

	/* ddd */
	DeadlockCheck,
	DeadlockUpdate,
	DeadlockSerialize,

	/* ddl */
	DDLStmtOutgoing,
	DDLStmtIncoming,
	DDLProcessingTrace,

	/* walsender's proto */
	ProtoTraceFilter,
	ProtoTraceSender,
	ProtoTraceMessage,
	ProtoTraceState,

	/* receiver */
	MtmReceiverState,
	MtmReceiverStateDebug,
	MtmReceiverFilter,
	MtmApplyMessage,
	MtmApplyTrace,
	MtmApplyError,
	MtmApplyBgwFinish,
	MtmReceiverFeedback,

	/* state */
	MtmStateMessage,
	MtmStateSwitch,
	MtmStateDebug,

	/* syncpoints */
	SyncpointCreated,
	SyncpointApply,

	/* Node add/drop */
	NodeMgmt
} MtmLogTag;

typedef struct MtmLogGuc
{
	const char *name;
	int	 default_val;
	int	 val;
} MtmLogGuc;

extern MtmLogGuc mtm_log_gucs[];

#define MTM_TAG "[MTM]%s"

/*
 * I tried to use get_ps_display instead of MyBgworkerEntry, but it returns
 * only dynamic 'activity' part which doesn't include bgw name. Apparently
 * there is no way to retrieve main part. Weird.
 */
extern bool MtmBackgroundWorker; /* avoid including multimaster.h for this */
extern char *walsender_name; /* same for pglogical_proto.h */
static inline char *
am(void)
{
	char *res = " ";
	char *name = NULL;

	if (MtmBackgroundWorker)
		name = MyBgworkerEntry->bgw_name;
	else if (walsender_name)
		name = walsender_name;
	if (name)
	{
		/* this is for elog, so alloc in ErrorContext where fmt is evaluated */
		MemoryContext old_ctx = MemoryContextSwitchTo(ErrorContext);
		res = psprintf(" [%s] ", name);
		MemoryContextSwitchTo(old_ctx);
	}
	return res;
}

#define MTM_ERRMSG(fmt,...) errmsg(MTM_TAG fmt, am(), ## __VA_ARGS__)

/*
 * tag can either one of MtmLogTag values (in which case corresponding GUC
 * defines the actual log level) or direct level like ERROR
 */
#define mtm_log(tag, fmt, ...) ereport( \
		((tag) >= FIRST_UNUSED_ERRCODE ? \
		 mtm_log_gucs[tag - FIRST_UNUSED_ERRCODE].val : (tag)), \
		(errmsg(MTM_TAG fmt, \
				am(), ## __VA_ARGS__), \
		 errhidestmt(true), errhidecontext(true)))
