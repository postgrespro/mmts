#ifndef __BGWPOOL_H__
#define __BGWPOOL_H__

#include "storage/s_lock.h"
#include "storage/spin.h"
#include "storage/pg_sema.h"
#include "postmaster/bgworker.h"
#include "storage/condition_variable.h"
#include "storage/dsm.h"

#include "receiver.h"

#define MAX_DBNAME_LEN 30
#define MAX_DBUSER_LEN 30
#define MAX_NAME_LEN 30
#define MULTIMASTER_BGW_RESTART_TIMEOUT BGW_NEVER_RESTART /* seconds */


typedef struct
{
	volatile slock_t	lock;

	/* Tell workers that queue contains a number of work. */
	ConditionVariable	available_cv;

	/*
	 * Queue is full. We can't insert a work data into the queue and wait while
	 * any worker will take over a piece of data from queue and we will do an
	 * attempt to try to add the work data into the queue.
	 */
	ConditionVariable	overflow_cv;
	ConditionVariable	syncpoint_cv;
	int			n_holders;

	/* Queue state */
	size_t		head;
	size_t		tail;
	size_t		size; /* Size of queue aligned to INT word */

	/* Worker state */
	size_t		active;
	size_t		pending;
	bool		producerBlocked;
	bool		shutdown;
	/* Must be last field at the structure */
	char		queue[FLEXIBLE_ARRAY_MEMBER];
} PoolState;

typedef struct
{
	char		poolName[MAX_NAME_LEN];
	Oid			db_id;
	Oid			user_id;

	dsm_handle	pool_handler; /* DSM descriptor. Workers use it for attaching */
	PoolState	*state;

	size_t					nWorkers; /* a number of pool workers launched */
	TimestampTz				lastDynamicWorkerStartTime;
	/* Handlers of workers at the pool */
	BackgroundWorkerHandle	*bgwhandles[FLEXIBLE_ARRAY_MEMBER];
} BgwPool;


#define SizeOfBgwPool(nodes)	(offsetof(BgwPool, bgwhandles) + sizeof(BackgroundWorkerHandle *) * nodes)
extern int MtmQueueSize;

typedef BgwPool*(*BgwPoolConstructor)(void);

extern void BgwPoolStart(BgwPool* pool, char *poolName, Oid db_id, Oid user_id);
extern void BgwPoolExecute(BgwPool* pool, void* work, int size, MtmReceiverContext *ctx);
extern size_t PoolStateGetQueueSize(PoolState* pool);
extern void PoolStateShutdown(PoolState* pool);
extern void BgwPoolCancel(BgwPool* pool);

#endif
