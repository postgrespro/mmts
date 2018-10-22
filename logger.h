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
	MtmTxTrace				= DEBUG1,
	MtmTxFinish				= DEBUG1,

	/* dmq */
	DmqStateIntermediate	= DEBUG1,
	DmqStateFinal			= LOG,
	DmqTraceOutgoing		= DEBUG2,
	DmqTraceIncoming		= DEBUG2,
	DmqTraceShmMq			= DEBUG1,

	/* resolver */
	ResolverTasks			= LOG,
	ResolverTraceTxMsg		= LOG,
	ResolverTxFinish		= LOG,

	/* status worker */
	StatusRequest			= LOG,



} MtmLogTag;

// XXX: also meaningful process name would be cool

#define mtm_log(tag, fmt, ...) ereport(tag, \
								(errmsg("[MTM] " fmt, ## __VA_ARGS__), \
								errhidestmt(true), errhidecontext(true)))