#ifndef __BGWPOOL_H__
#define __BGWPOOL_H__

#include "storage/s_lock.h"
#include "storage/spin.h"
#include "storage/pg_sema.h"
#include "postmaster/bgworker.h"
#include "storage/condition_variable.h"

#include "receiver.h"

typedef void(*BgwPoolExecutor)(void* work, size_t size, MtmReceiverContext *ctx);

typedef long timestamp_t;


#define MAX_DBNAME_LEN 30
#define MAX_DBUSER_LEN 30
#define MAX_NAME_LEN 30
#define MULTIMASTER_BGW_RESTART_TIMEOUT BGW_NEVER_RESTART /* seconds */

extern timestamp_t MtmGetSystemTime(void);   /* non-adjusted current system time */
extern timestamp_t MtmGetCurrentTime(void);  /* adjusted current system time */

typedef struct
{
    BgwPoolExecutor executor;
    volatile slock_t lock;
	PGSemaphore available;
	PGSemaphore overflow;
	ConditionVariable syncpoint_cv;
    size_t head;
    size_t tail;
    size_t size;
    size_t active;
    size_t pending;
	size_t nWorkers;
	time_t lastPeakTime;
	timestamp_t lastDynamicWorkerStartTime;
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

extern void BgwPoolInit(BgwPool* pool, BgwPoolExecutor executor, size_t queueSize, size_t nWorkers);

extern void BgwPoolExecute(BgwPool* pool, void* work, size_t size, MtmReceiverContext *ctx);

extern size_t BgwPoolGetQueueSize(BgwPool* pool);

extern timestamp_t BgwGetLastPeekTime(BgwPool* pool);

extern void BgwPoolStop(BgwPool* pool);

extern void BgwPoolCancel(BgwPool* pool);

#endif