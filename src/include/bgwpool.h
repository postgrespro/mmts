#ifndef __BGWPOOL_H__
#define __BGWPOOL_H__

#include "storage/lwlock.h"
#include "storage/pg_sema.h"
#include "postmaster/bgworker.h"
#include "storage/condition_variable.h"
#include "storage/dsm.h"

#include "receiver.h"

#define MAX_DBNAME_LEN 30
#define MAX_DBUSER_LEN 30
#define MAX_NAME_LEN 30
#define MULTIMASTER_BGW_RESTART_TIMEOUT BGW_NEVER_RESTART /* seconds */


/*
 * Shared data of BgwPool
 */
typedef struct
{
	LWLock				lock;
	ConditionVariable	syncpoint_cv;
	int					n_holders;

	/* Tell workers that queue contains a number of work. */
	ConditionVariable	available_cv;

	/*
	 * Queue is full. We can't insert a work data into the queue and wait while
	 * any worker will take over a piece of data from queue and we will do an
	 * attempt to try to add the work data into the queue.
	 */
	ConditionVariable	overflow_cv;

	/* Queue state */
	size_t		head;
	size_t		tail;
	size_t		size; /* Size of queue aligned to INT word */

	/* Worker state */
	size_t		active;
	size_t		pending;
	bool		producerBlocked;

	char		poolName[MAX_NAME_LEN];
	Oid			db_id;
	Oid			user_id;
	dsm_handle	dsmhandler; /* DSM descriptor. Workers use it for attaching */

	size_t					nWorkers; /* a number of pool workers launched */
	TimestampTz				lastDynamicWorkerStartTime;
	/* Handlers of workers at the pool */
	BackgroundWorkerHandle **bgwhandles;
} BgwPool;


extern void BgwPoolStart(BgwPool* pool, char *poolName, Oid db_id, Oid user_id);
extern void BgwPoolExecute(BgwPool* pool, void* work, int size, MtmReceiverContext *ctx);
extern void BgwPoolShutdown(BgwPool* poolDesc);
extern void BgwPoolCancel(BgwPool* pool);

#endif
