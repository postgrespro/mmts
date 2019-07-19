#ifndef __BGWPOOL_H__
#define __BGWPOOL_H__

#include "storage/s_lock.h"
#include "storage/spin.h"
#include "storage/pg_sema.h"
#include "postmaster/bgworker.h"
#include "storage/condition_variable.h"

#include "receiver.h"

#define MAX_DBNAME_LEN 30
#define MAX_DBUSER_LEN 30
#define MAX_NAME_LEN 30
#define MULTIMASTER_BGW_RESTART_TIMEOUT BGW_NEVER_RESTART /* seconds */

typedef struct
{
    volatile slock_t lock;
	PGSemaphore available;
	PGSemaphore overflow;
	ConditionVariable syncpoint_cv;
	int		n_holders;
    size_t head;
    size_t tail;
    size_t size;
    size_t active;
    size_t pending;
	size_t nWorkers;
	TimestampTz lastDynamicWorkerStartTime;
    bool   producerBlocked;
	bool   shutdown;
	char   poolName[MAX_NAME_LEN];
	Oid		db_id;
	Oid		user_id;
    char*  queue;
	BackgroundWorkerHandle **bgwhandles;
} BgwPool;

typedef BgwPool*(*BgwPoolConstructor)(void);

extern void BgwPoolStart(BgwPool* pool, char *poolName, Oid db_id, Oid user_id);

extern void BgwPoolInit(BgwPool* pool, size_t queueSize, size_t nWorkers);

extern void BgwPoolExecute(BgwPool* pool, void* work, int size, MtmReceiverContext *ctx);

extern size_t BgwPoolGetQueueSize(BgwPool* pool);

extern void BgwPoolStop(BgwPool* pool);

extern void BgwPoolCancel(BgwPool* pool);

#endif
