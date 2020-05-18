/*----------------------------------------------------------------------------
 *
 * logger.h
 *		Minimalistic map from application meaningful log tags to actual log
 *		levels. Right now mapping is compiled, but later we can add some GUC
 *		list on top of that to allow override log levels for specific tags in
 *		runtime.
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

typedef enum MtmLogTag
{
	/* general */
	MtmTxTrace				= LOG,
	MtmTxFinish				= LOG,

	/* dmq */
	DmqStateIntermediate	= DEBUG2,
	DmqStateFinal			= LOG,
	DmqTraceOutgoing		= DEBUG2,
	DmqTraceIncoming		= DEBUG2,
	DmqTraceShmMq			= DEBUG2,
	DmqPqTiming				= DEBUG2,

	/* resolver */
	ResolverState			= LOG,
	ResolverTx				= LOG,
	ResolverTasks			= LOG,

	/* status worker */
	StatusRequest			= LOG,

	/* pool */
	BgwPoolEvent			= LOG,
	BgwPoolEventDebug		= LOG,

	/* ddd */
	DeadlockCheck			= DEBUG1,
	DeadlockUpdate			= DEBUG1,
	DeadlockSerialize		= DEBUG1,

	/* ddl */
	DDLStmtOutgoing			= DEBUG1,
	DDLStmtIncoming			= DEBUG1,
	DDLProcessingTrace		= DEBUG1,

	/* broadcast service */
	BroadcastNotice			= DEBUG1,

	/* walsender's proto */
	ProtoTraceFilter		= DEBUG1,
	ProtoTraceSender		= DEBUG2,
	ProtoTraceMode			= LOG,
	ProtoTraceMessage		= DEBUG1,
	ProtoTraceState			= LOG,

	/* receiver */
	MtmReceiverStart		= LOG,
	MtmReceiverFilter		= DEBUG1,
	MtmApplyMessage			= LOG,
	MtmApplyTrace			= LOG,
	MtmApplyError			= LOG,
	MtmApplyBgwFinish		= LOG,
	MtmReceiverFeedback		= DEBUG1,

	/* state */
	MtmStateMessage			= LOG,
	MtmStateSwitch			= LOG,
	MtmStateDebug			= LOG,

	/* syncpoints */
	SyncpointCreated		= LOG,
	SyncpointApply			= LOG,

	/* Node add/drop */
	NodeMgmt				= LOG
} MtmLogTag;

#define MTM_TAG "[MTM]%s"

/*
 * I tried to use get_ps_display instead of MyBgworkerEntry, but it returns
 * only dynamic 'activity' part which doesn't include bgw name. Apparently
 * there is no way to retrieve main part. Weird.
 */
extern bool MtmBackgroundWorker; /* avoid including multimaster.h for this */
static inline char *
am(void)
{
	char *res = " ";
	if (MtmBackgroundWorker)
	{
		/* this is for elog, so alloc in ErrorContext where fmt is evaluated */
		MemoryContext old_ctx = MemoryContextSwitchTo(ErrorContext);
		res = psprintf(" [%s] ", MyBgworkerEntry->bgw_name);
		MemoryContextSwitchTo(old_ctx);
	}
	return res;
}

#define MTM_ERRMSG(fmt,...) errmsg(MTM_TAG fmt, am(), ## __VA_ARGS__)

#define mtm_log(tag, fmt, ...) ereport(tag, \
								(errmsg(MTM_TAG fmt, \
										am(), ## __VA_ARGS__), \
								errhidestmt(true), errhidecontext(true)))
