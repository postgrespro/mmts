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
#include "utils/elog.h"

typedef enum MtmLogTag
{
	/* general */
	MtmTxTrace				= LOG,
	MtmTxFinish				= LOG,

	/* dmq */
	DmqStateIntermediate	= DEBUG1,
	DmqStateFinal			= LOG,
	DmqTraceOutgoing		= DEBUG2,
	DmqTraceIncoming		= DEBUG2,
	DmqTraceShmMq			= DEBUG1,
	DmqPqTiming				= DEBUG2,

	/* resolver */
	ResolverTasks			= LOG,
	ResolverTraceTxMsg		= LOG,
	ResolverTxFinish		= LOG,

	/* status worker */
	StatusRequest			= LOG,

	/* pool */
	BgwPoolEvent			= LOG,

	/* ddd */
	DeadlockCheck			= LOG,
	DeadlockUpdate			= DEBUG1,
	DeadlockSerialize		= DEBUG3,

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
	MtmReceiverFilter		= LOG,
	MtmApplyMessage			= DEBUG1,
	MtmApplyTrace			= DEBUG2,
	MtmApplyError			= LOG,
	MtmApplyBgwFinish		= LOG,

	/* state */
	MtmStateSwitch			= LOG,
	MtmStateMessage			= LOG,

	/* syncpoints */
	SyncpointCreated		= LOG,
	SyncpointApply			= LOG,

	/* Node add/drop */
	NodeMgmt				= LOG
} MtmLogTag;

// XXX: also meaningful process name would be cool

#define MTM_TAG "[MTM] "

#define MTM_ERRMSG(fmt,...) errmsg(MTM_TAG fmt, ## __VA_ARGS__)

#define mtm_log(tag, fmt, ...) ereport(tag, \
								(errmsg(MTM_TAG fmt, ## __VA_ARGS__), \
								errhidestmt(true), errhidecontext(true)))
