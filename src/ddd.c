/*----------------------------------------------------------------------------
 *
 * ddd.c
 *
 * Distributed deadlock detector.
 *
 * Copyright (c) 2017-2020, Postgres Professional
 *
 *----------------------------------------------------------------------------
 */

#include "postgres.h"
#include "access/clog.h"
#include "access/twophase.h"
#include "access/transam.h"
#include "storage/lwlock.h"
#include "storage/ipc.h"
#include "storage/proc.h"
#include "utils/hsearch.h"
#include "utils/timeout.h"
#include "miscadmin.h"
#include "replication/origin.h"
#include "replication/message.h"
#include "utils/builtins.h"
#include "storage/lmgr.h"
#include "storage/procarray.h"

#include "multimaster.h"

#include "ddd.h"
#include "bytebuf.h"
#include "state.h"
#include "logger.h"
#include "commit.h"


/*
 * This DDD is based on following observations:
 *
 *    Situation when a transaction (say T1) in apply_worker (or receiver
 * itself) stucks on some lock created by a transaction in a local backend (say
 * T2) will definitely lead to a deadlock since T2 after being prepared and
 * replicated will fail to obtain lock that is already held by T1.
 *    Same reasoning may be applied to the situation when apply_worker (or
 * receiver) is waiting for an apply_worker (or receiver) belonging to other
 * origin -- no need to wait for a distributed deadlock detection and we may
 * just instantly abort.
 *    Only case for distributed deadlock that is left is when apply_worker
 * (or receiver) is waiting for another apply_worker from same origin. However,
 * such situation isn't possible since one origin node can not have two
 * conflicting prepared transaction simultaneously.
 *
 *    So we may construct distributed deadlock avoiding mechanism by disallowing
 * such edges. Now we may ask inverse question: what amount of wait graphs
 * with such edges are actually do not represent distributed deadlock? That may
 * happen in cases when holding transaction is purely local since it holding
 * locks only in SHARED mode. Only lock levels that are conflicting with this
 * modes are EXCLUSIVE and ACCESS EXCLUSIVE. In all other cases proposed
 * avoiding scheme should not yield false positives.
 *
 *     To cope with false positives in EXCLUSIVE and ACCESS EXCLUSIVE modes we
 * may throw exception not in WaitOnLock() when we first saw forbidden edge
 * but later during first call to local deadlock detector. This way we still
 * have `deadlock_timeout` second to grab that lock and database user also can
 * increase it on per-transaction basis if there are long-living read-only
 * transactions.
 *
 *     As a further optimization it is possible to check whether our lock is
 * EXCLUSIVE or higher so not to delay rollback till `deadlock_timeout` event.
 */
bool
MtmDetectGlobalDeadLock(PGPROC *proc)
{
	StringInfoData locktagbuf;
	LOCK	   *lock = proc->waitLock;
	bool		is_detected = false;
	Assert(proc == MyProc);

	/*
	 * These locks never participate in deadlocks, ignore them. Without it,
	 * spurious deadlocks might be reported due to concurrency on rel
	 * extension.
	 */
	if (LOCK_LOCKTAG(*lock) == LOCKTAG_RELATION_EXTEND ||
		(LOCK_LOCKTAG(*lock) == LOCKTAG_PAGE))
		return false;

	/*
	 * There is no need to check for deadlocks in recovery: all
	 * conflicting transactions must be eventually committed/aborted
	 * by the resolver. It would not be fatal, but restarting due to
	 * deadlock ERRORs might significantly slow down the recovery
	 */
	is_detected = (curr_replication_mode == REPLMODE_NORMAL);

	if (is_detected)
	{
		initStringInfo(&locktagbuf);
		DescribeLockTag(&locktagbuf, &lock->tag);
		mtm_log(LOG, "apply worker %d waits for %s on %s",
				MyProcPid,
				GetLockmodeName(lock->tag.locktag_lockmethodid, proc->waitLockMode),
				locktagbuf.data);
	}

	return is_detected;

}
