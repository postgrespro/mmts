#include "postgres.h"

/* mkdir */
#include <sys/stat.h>
#include <sys/types.h>

#include "access/twophase.h"
#include "access/xlogutils.h"
#include "access/xlog_internal.h"
#include "executor/spi.h"
#include "utils/snapmgr.h"
#include "nodes/makefuncs.h"
#include "catalog/namespace.h"
#include "catalog/pg_type.h"
#include "tcop/tcopprot.h"
#include "pgstat.h"
#include "storage/ipc.h"
#include "miscadmin.h"			/* PostmasterPid */
#include "utils/syscache.h"
#include "utils/inval.h"
#include "replication/slot.h"
#include "replication/origin.h"
#include "miscadmin.h"
#include "replication/logicalfuncs.h"
#include "replication/message.h"
#include "utils/builtins.h"
#include "funcapi.h"
#include "libpq/pqformat.h"

#include "multimaster.h"
#include "bkb.h"
#include "commit.h"
#include "state.h"
#include "syncpoint.h"
#include "logger.h"
#include "messaging.h"

char const *const MtmNeighborEventMnem[] =
{
	"MTM_NEIGHBOR_CLIQUE_DISABLE",
	"MTM_NEIGHBOR_WAL_RECEIVER_START",
	"MTM_NEIGHBOR_WAL_RECEIVER_ERROR",
	"MTM_NEIGHBOR_WAL_SENDER_START_RECOVERY",
	"MTM_NEIGHBOR_WAL_SENDER_START_RECOVERED",
	"MTM_NEIGHBOR_RECOVERY_CAUGHTUP",
	"MTM_NEIGHBOR_WAL_SENDER_STOP"
};

char const *const MtmEventMnem[] =
{
	"MTM_REMOTE_DISABLE",
	"MTM_CLIQUE_DISABLE",
	"MTM_CLIQUE_MINORITY",
	"MTM_ARBITER_RECEIVER_START",
	"MTM_RECOVERY_START1",
	"MTM_RECOVERY_START2",
	"MTM_RECOVERY_FINISH1",
	"MTM_RECOVERY_FINISH2",
	"MTM_NONRECOVERABLE_ERROR"
};

char const *const MtmNodeStatusMnem[] =
{
	"isolated",
	"disabled",
	"catchup",
	"recovery",
	"online"
};

static char const *const MtmStatusInGenMnem[] =
{
	"dead",
	"recovery",
	"online"
};

struct MtmState
{
	/*
	 * Persistent state.
	 *
	 * My current generation, never goes backwards.
	 * (this is not MtmGeneration because atomic provides fast path in
	 * MtmConsiderGenSwitch)
	 */
	pg_atomic_uint64 current_gen_num;
	nodemask_t current_gen_members;
	nodemask_t current_gen_configured;
	/*
	 * subset of current_gen_members which definitely has all xacts of gens
	 * < current_gen.num; always has at least one node. From these nodes we
	 * can recover to participate in this gen.
	 */
	nodemask_t donors;
	/*
	 * Last generation I was online in. Must be persisted to disk before
	 * updating current_gen; used for determining donors who definitely hold
	 * all possibly committed prepares of previous gens.
	 */
	uint64 last_online_in;
	/*
	 * Oldest gen for which we I have voted.
	 * Used for not voting twice and to keep the promise 'once we voted for n,
	 * don't update last_online_in to any num < n', which allows to learn
	 * who are donors during the voting.
	 */
	MtmGeneration last_vote;

	/* Guards generation switch */
	LWLock	   *gen_lock;
	/*
	 * However, gen switcher must also take this barrier as keeping LWLock
	 * during PREPARE is not nice.
	 */
	slock_t cb_lock;
	int			n_committers;
	int			n_prepare_holders;
	ConditionVariable commit_barrier_cv;
	/*
	 * Voters exclude each other and gen switch, but don't change current gen
	 * and thus allow (e.g. heartbeat sender) to peek it, hence the second
	 * lock protecting last_vote.
	 */
	LWLock		*vote_lock;


	/*
	 * Last generation where each other node was online, collected via
	 * heartbeats. Used to determine donor during catchup, when others
	 * don't wait for us yet but we decrease the lag.
	 *
	 * Each element is updated only by the corresponding dmq receiver, so
	 * use atomics instead of adding locking.
	 */
	pg_atomic_uint64 others_last_online_in[MTM_MAX_NODES];

	/*
	 * Connectivity state, maintained by dmq.
	 * dmq_* masks don't contain myself; MtmGetConnectedMaskWithMe handles that.
	 */
	nodemask_t	dmq_receivers_mask;
	nodemask_t	dmq_senders_mask;
	/* Whom others see to the best of our knowledge */
	nodemask_t	connectivity_matrix[MTM_MAX_NODES];
	/* Protects the whole connectivity state. Make it spinlock? */
	LWLock		*connectivity_lock;

	/*
	 * Direction to receviers how they should work:
	 * RECEIVE_MODE_NORMAL or RECEIVE_MODE_DISABLED or donor node id.
	 * Modifications are protected by excl gen_lock or shared vote_lock + excl
	 * vote_lock.
	 */
	pg_atomic_uint32 receive_mode;

	pid_t campaigner_pid;
	bool  campaigner_on_tour; /* protected by vote_lock */

	/* receiver reports its progress in recovery here */
	int		catchup_node_id;
	instr_time	catchup_ts;
	slock_t catchup_lock;

	/*
	 * Attempt to clear the referee grant until it succeeds.
	 * This could be bool except the paranoia in RefereeClearGrant.
	 */
	uint64 referee_grant_turn_in_pending;

	/*
	 * making current code compilable while I haven't fixed up things
	 */
	LWLock	   *lock;
	nodemask_t connected_mask;
	nodemask_t receivers_mask;
	nodemask_t senders_mask;
	nodemask_t enabled_mask;
	nodemask_t clique;
	nodemask_t configured_mask;


	bool		referee_grant;
	int			referee_winner_id;

	bool		recovered;
	int			recovery_slot;

	MtmNodeStatus status;
}		   *mtm_state;

void CampaignerMain(Datum main_arg);
static void CampaignerWake(void);

static void MtmSetReceiveMode(uint32 mode);

static void AcquirePBByHolder(void);

static bool MtmIsConnectivityClique(nodemask_t mask);
static nodemask_t MtmGetConnectivityClique(bool locked);

/* serialization functions */
static void MtmStateSave(void);
static void MtmStateLoad(void);

static void pubsub_change_cb(Datum arg, int cacheid, uint32 hashvalue);

static void GetLoggedPreparedXactState(HTAB *txset);

PG_FUNCTION_INFO_V1(mtm_node_info);
PG_FUNCTION_INFO_V1(mtm_status);
PG_FUNCTION_INFO_V1(mtm_state_create);
PG_FUNCTION_INFO_V1(mtm_get_logged_prepared_xact_state);

static bool pb_preparers_incremented = false;
static bool pb_holders_incremented = false;

static bool config_valid = false;

/*
 * -----------------------------------
 * Startup
 * -----------------------------------
 */

void
MtmStateInit()
{
	RequestAddinShmemSpace(sizeof(struct MtmState));
	RequestNamedLWLockTranche("mtm_state_locks", 3);
}

void
MtmStateShmemStartup()
{
	bool		found;

	LWLockAcquire(AddinShmemInitLock, LW_EXCLUSIVE);

	mtm_state = ShmemInitStruct("mtm_state", sizeof(struct MtmState), &found);

	if (!found)
	{
		int i;

		MemSet(mtm_state, '\0', sizeof(struct MtmState));
		mtm_state->gen_lock = &(GetNamedLWLockTranche("mtm_state_locks")[0].lock);
		mtm_state->connectivity_lock = &(GetNamedLWLockTranche("mtm_state_locks")[1].lock);
		mtm_state->vote_lock = &(GetNamedLWLockTranche("mtm_state_locks")[2].lock);

		pg_atomic_init_u64(&mtm_state->current_gen_num, MtmInvalidGenNum);
		for (i = 0; i < MTM_MAX_NODES; i++)
		{
			pg_atomic_init_u64(&mtm_state->others_last_online_in[i], MtmInvalidGenNum);
		}

		SpinLockInit(&mtm_state->cb_lock);
		ConditionVariableInit(&mtm_state->commit_barrier_cv);

		pg_atomic_init_u32(&mtm_state->receive_mode, RECEIVE_MODE_DISABLED);

		SpinLockInit(&mtm_state->catchup_lock);
		mtm_state->catchup_node_id = MtmInvalidNodeId;
	}

	LWLockRelease(AddinShmemInitLock);
}

/*
 * State initialization called by monitor. It is problematic to do this
 * earlier (at shmem_startup_hook) as we need our Mtm->my_node_id which is
 * fetched from table and set in shmem by monitor.
 */
void
MtmStateStartup(void)
{
	/* xxx: AcquirePBByHolder */
	LWLockAcquire(mtm_state->gen_lock, LW_EXCLUSIVE);
	MtmStateLoad();
	/* restore receive_mode */
	switch (MtmGetCurrentStatusInGen())
	{
		case MTM_GEN_ONLINE:
			pg_atomic_write_u32(&mtm_state->receive_mode, RECEIVE_MODE_NORMAL);
			break;
		case MTM_GEN_RECOVERY:
			{
				int donor = first_set_bit(mtm_state->donors) + 1;
				Assert(donor > 0);
				pg_atomic_write_u32(&mtm_state->receive_mode, donor);
				break;
			}
		case MTM_GEN_DEAD:
			pg_atomic_write_u32(&mtm_state->receive_mode, RECEIVE_MODE_DISABLED);
			break;
	}
	LWLockRelease(mtm_state->gen_lock);
}

/* Create persistent state during cluster initialization */
Datum
mtm_state_create(PG_FUNCTION_ARGS)
{
	/*
	 * Initial node ids normally are 1..n_nodes, but we pass array of node ids
	 * here to allow tests configure sparse numbers.
	 */
	ArrayType  *node_ids_arr = PG_GETARG_ARRAYTYPE_P(0);
	Datum	   *node_ids_datums;
	bool	   *node_ids_nulls;
	int			n_nodes;
	int			i;

	/* parse array with node ids */
	Assert(ARR_ELEMTYPE(node_ids_arr) == INT4OID);
	Assert(ARR_NDIM(node_ids_arr) == 1);
	deconstruct_array(node_ids_arr,
					  INT4OID,
					  4, true, 'i',
					  &node_ids_datums, &node_ids_nulls, &n_nodes);


	/*
	 * Initially, all members are online in gen 1.
	 * Nobody should be messing up with mtm_state at this point, but just in
	 * case (e.g. previous cluster?), take lock.
	 */
	LWLockAcquire(mtm_state->gen_lock, LW_EXCLUSIVE);
	pg_atomic_write_u64(&mtm_state->current_gen_num, 1);
	mtm_state->current_gen_members = 0;
	mtm_state->current_gen_configured = 0;
	for (i = 0; i < n_nodes; i++)
	{
		int node_id = DatumGetInt32(node_ids_datums[i]);

		Assert(node_id >= 1);
		BIT_SET(mtm_state->current_gen_members, node_id - 1);
		BIT_SET(mtm_state->current_gen_configured, node_id - 1);
	}
	mtm_state->donors = mtm_state->current_gen_members;
	mtm_state->last_online_in = 1;
	mtm_state->last_vote = ((MtmGeneration) {1, mtm_state->current_gen_members});
	MtmStateSave();
	/*
	 * zero out gen num again: we are not ready until monitor hasn't done
	 * MtmStateStartup, re-reading it from disk
	 */
	pg_atomic_write_u64(&mtm_state->current_gen_num, MtmInvalidNodeId);
	LWLockRelease(mtm_state->gen_lock);
	PG_RETURN_VOID();
}

/*
 * -----------------------------------
 * Generation management
 * -----------------------------------
 */

uint64
MtmGetCurrentGenNum(void)
{
	return pg_atomic_read_u64(&mtm_state->current_gen_num);
}

MtmGeneration
MtmGetCurrentGen(bool locked)
{
	MtmGeneration res;

	if (!locked)
		LWLockAcquire(mtm_state->gen_lock, LW_SHARED);
	Assert(LWLockHeldByMe(mtm_state->gen_lock) || pb_preparers_incremented);

	res = (MtmGeneration)
	{
		.num = pg_atomic_read_u64(&mtm_state->current_gen_num),
		.members = mtm_state->current_gen_members,
		.configured = mtm_state->current_gen_configured
	};

	if (!locked)
		LWLockRelease(mtm_state->gen_lock);

	return res;
}

/* TODO: make messaging layer for logical messages like existing dmq one */
static void
PackGenAndDonors(StringInfo s, MtmGeneration gen, nodemask_t donors)
{
	initStringInfo(s);
	pq_sendint64(s, gen.num);
	pq_sendint64(s, gen.members);
	pq_sendint64(s, gen.configured);
	pq_sendint64(s, donors);
}

/* Switch into newer generation, if not yet */
void
MtmConsiderGenSwitch(MtmGeneration gen, nodemask_t donors)
{
	/* generations with the same number must be the identic */
#ifdef USE_ASSERT_CHECKING
	LWLockAcquire(mtm_state->gen_lock, LW_SHARED);
	if (pg_atomic_read_u64(&mtm_state->current_gen_num) == gen.num)
	{
		Assert(mtm_state->current_gen_members == gen.members);
		Assert(mtm_state->current_gen_configured == gen.configured);
	}
	LWLockRelease(mtm_state->gen_lock);
#endif

	/* fast path executed normally */
	if (likely(pg_atomic_read_u64(&mtm_state->current_gen_num) >= gen.num))
		return;

	/* ok, most probably the switch is going to happen */
	LWLockAcquire(mtm_state->gen_lock, LW_EXCLUSIVE);

	/*
	 * Doesn't happen normally, this means dmq receiver appeared earlier than
	 * monitor started. Should handle this nicer.
	 */
	if (pg_atomic_read_u64(&mtm_state->current_gen_num) == MtmInvalidGenNum)
		elog(ERROR, "multimaster is not initialized yet");

	/* check once again under lock */
	if (pg_atomic_read_u64(&mtm_state->current_gen_num) >= gen.num)
	{
		LWLockRelease(mtm_state->gen_lock);
		return;
	}

	/*
	 * Exclude all concurrent PREPAREs.
	 *
	 * Barrier between stopping applying/creating prepares from old gen and
     * starting writing new gen prepares, embodied on donors by
     * ParallelSafe<gen> record, is crucial; once any new gen PREPARE appeared
     * in WAL, accepting old one must be forbidden because recovery up to
     * ParallelSafe (or any prepare from new gen) is a criterion that we have
     * recovered to participate in this gen and thus got all committable xacts
     * of older gens: receivers enter normal mode (pulling only origin's
     * xacts) at this spot with usual dangers of out-of-order apply.
	 *
	 * Backends don't use gen_lock for that though because
	 *  - Doing PrepareTransactionBlock/CommitTransactionCommand under lwlock
	 *    is formidable.
	 *  - lwlocks are unfair.
	 * XXX these arguments seem somewhat weak. The first should be
	 * investigated and the second can be hacked around with sleep request.
	 */
	AcquirePBByHolder();

	/* voting for generation n <= m is pointless if gen m was already elected */
	if (mtm_state->last_vote.num < gen.num)
		mtm_state->last_vote = gen; /* will be fsynced below along with rest of state */

	/* update current gen */
	pg_atomic_write_u64(&mtm_state->current_gen_num, gen.num);
	mtm_state->current_gen_members = gen.members;
	mtm_state->current_gen_configured = gen.configured;
	mtm_state->donors = donors;

	/*
	 * xxx SetLatch of all backends here? Waiting for acks after gen switch
	 * might be hopeless. Currently backends check for it after timeout...
	 */

	/* Probably we are not member of this generation... */
	if (!BIT_CHECK(gen.members, Mtm->my_node_id - 1) ||

		/*
		 * .. or gen doesn't have quorum by design, nor this is a referee
		 * granted gen where quorum is not required
		 */
		(!Quorum(popcount(gen.configured), popcount(gen.members)) &&
		 !IS_REFEREE_GEN(gen.members, gen.configured)) ||
		/*
		 * .. or we have voted for greater last_vote.num, which means we've
		 * promised that the highest gen among gens with num < last_vote.num
		 * in which we ever can be online (and thus create xacts) is
		 * last_online_in on the moment of voting. To keep that promise,
		 * prevent getting ONLINE in gens with < last_vote.num numbers.
		 */
		mtm_state->last_vote.num > gen.num)
	{
		/*
		 * Then we can never create xacts in this gen. Shut down receivers
		 * and nudge campaigner to recover.
		 */
		MtmSetReceiveMode(RECEIVE_MODE_DISABLED);
		MtmStateSave();

		mtm_log(MtmStateSwitch, "[STATE] switched to dead in generation num=" UINT64_FORMAT ", members=%s, donors=%s, last_vote.num=" UINT64_FORMAT,
				gen.num,
				maskToString(gen.members),
				maskToString(donors),
				mtm_state->last_vote.num);

		ReleasePB();
		LWLockRelease(mtm_state->gen_lock);
		CampaignerWake();
		return;
	}

	/*
	 * Decide whether we need to recover in this generation or not.
	 */
	if (BIT_CHECK(donors, Mtm->my_node_id - 1))
	{
		XLogRecPtr msg_xptr;
		StringInfoData s;

		/* no need to recover, we already have all xacts of lower gens */
		mtm_state->last_online_in = gen.num;

		/*
		 * Write to WAL ParallelSafe<genm> message, which is a mark for
		 * those who will recover from us in this generation that they are
		 * recovered: all following xacts can't commit without approval of all
		 * new gen members, all committed xacts of previous generations lie
		 * before ParallelSafe.
		 * Note that any PREPARE from new gen would perfectly do this job as
		 * well; this just guarantees convergence in the absence of xacts.
		 */
		/* xxx we should add versioning to logical messages */
		PackGenAndDonors(&s, gen, donors);
		msg_xptr = LogLogicalMessage("P", s.data, s.len, false);
		pfree(s.data);
		XLogFlush(msg_xptr);
		MtmStateSave(); /* fsync state update */

		MtmSetReceiveMode(RECEIVE_MODE_NORMAL);
		mtm_log(MtmStateSwitch, "[STATE] switched to online in generation num=" UINT64_FORMAT ", members=%s, donors=%s as donor, ParallelSafe logged at %X/%X",
				gen.num,
				maskToString(gen.members),
				maskToString(donors),
				(uint32) (msg_xptr >> 32), (uint32) msg_xptr);
	}
	else
	{
		/*
		 * Need recovery -- use random donor for that.
		 */
		int donor;

		MtmStateSave(); /* fsync state update */

		donor = first_set_bit(donors) + 1;
		Assert(donor > 0);
		MtmSetReceiveMode(donor);
		mtm_log(MtmStateSwitch, "[STATE] switched to recovery in generation num=" UINT64_FORMAT ", members=%s, donors=%s, donor=%d",
				gen.num,
				maskToString(gen.members),
				maskToString(donors),
				donor);
	}


	ReleasePB();
	LWLockRelease(mtm_state->gen_lock);
}

/*
 * Handle ParallelSafe arrived to receiver. Getting it in recovery mode means
 * we made all prepares of previous gens and can safely switch to
 * MTM_GEN_ONLINE.
 *
 * Note that we don't relog the message. It's fine because 1) P.S. is
 * idempotent, i.e. getting it twice is ok. We must process it at least once
 * though. 2) Nodes interested in these records will eventually learn 'donors'
 * who logged it and receive P.S. directly from one of them (unless yet
 * another gen switch happened). So, forwarding it wouldn't harm safety, but
 * there is no need in it.
 *
 * Returns true if the record can't be applied due to wrong receiver mode.
 */
bool
MtmHandleParallelSafe(MtmGeneration ps_gen, nodemask_t ps_donors,
					  bool is_recovery, XLogRecPtr end_lsn)
{
	/* make sure we are at least in ParallelSafe's gen */
	MtmConsiderGenSwitch(ps_gen, ps_donors);

	/* definitely not interested in this P.S. if we are already in higher gen */
	if (ps_gen.num < MtmGetCurrentGenNum())
		return false;

	/*
	 * Ok, grab the excl lock as we are going to need it if P.S. will actually
	 * make us ONLINE. We could do unlocked check whether we are already
	 * online, but performance here doesn't matter as P.S. is logged only
	 * on live nodes / networking changes.
	 */
	LWLockAcquire(mtm_state->gen_lock, LW_EXCLUSIVE);
	AcquirePBByHolder();

	/*
	 * Not interested in this P.S. if we are in newer gen. Otherwise, still
	 * not interested if we are already ONLINE in this one or can never be
	 * online in it (due to promise or just not being a member).
	 */
	if (ps_gen.num != MtmGetCurrentGenNum() ||
		MtmGetCurrentStatusInGen() != MTM_GEN_RECOVERY)
	{
		ReleasePB();
		LWLockRelease(mtm_state->gen_lock);
		return false;
	}

	/*
	 * Catching P.S. in normal mode and promoting to ONLINE is not allowed; we
	 * probably just have given out all prepares before it to parallel workers
	 * without applying them. Reconnect in recovery.
	 */
	if (!is_recovery)
	{
		ReleasePB();
		LWLockRelease(mtm_state->gen_lock);
		return true;
	}

	/*
	 * Ok, so this parallel safe indeed switches us into ONLINE.
	 */
   mtm_state->last_online_in = ps_gen.num;
   MtmStateSave();

   MtmSetReceiveMode(RECEIVE_MODE_NORMAL);
   if (IS_REFEREE_ENABLED() && popcount(ps_gen.configured) == 2)
   {
	   /*
		* In referee mode we may switch to online by applying P.S. only in
		* full generation; referee winner doesn't need recovery and switches
		* to online directly in MtmConsiderGenSwitch in both referee gen and
		* the following full gen.
		*/
	   Assert(popcount(ps_gen.members) == 2);
	   /*
		* Now that both nodes are online we can clear the grant.
		*/
	   mtm_state->referee_grant_turn_in_pending = ps_gen.num;
   }
   mtm_log(MtmStateSwitch, "[STATE] switched to online in generation num=" UINT64_FORMAT ", members=%s, donors=%s by applying ParallelSafe logged at %X/%X",
				ps_gen.num,
				maskToString(ps_gen.members),
				maskToString(ps_donors),
				(uint32) (end_lsn >> 32), (uint32) end_lsn);

   ReleasePB();
   LWLockRelease(mtm_state->gen_lock);
   return false;
}

/*
 * Node status in current generation. Closely follows MtmConsiderGenSwitch logic.
 */
MtmStatusInGen
MtmGetCurrentStatusInGen(void)
{
	int me = Mtm->my_node_id;
	uint64 current_gen_num;

	if (me == MtmInvalidNodeId)
		elog(ERROR, "multimaster is not configured");

	Assert(LWLockHeldByMe(mtm_state->gen_lock) || pb_preparers_incremented);
	/*
	 * If we care about MTM_GEN_DEAD/MTM_GEN_RECOVERY distinction, should also
	 * keep either vote_lock or excl gen_lock, but some callers don't, so no
	 * assertion.
	 */

	current_gen_num = pg_atomic_read_u64(&mtm_state->current_gen_num);
	if (current_gen_num == MtmInvalidGenNum)
		elog(ERROR, "multimaster is not initialized yet");
	if (mtm_state->last_online_in == current_gen_num)
		return MTM_GEN_ONLINE; /* ready to do xacts */
	/*
	 * We can hope to get eventually ONLINE in current generation iff we are
	 * member of it, its members form quorum and voting promises don't forbid
	 * us that.
	 */
	else if (BIT_CHECK(mtm_state->current_gen_members, me - 1) &&
			 Quorum(popcount(mtm_state->current_gen_configured),
					popcount(mtm_state->current_gen_members)) &&
			 pg_atomic_read_u64(&mtm_state->current_gen_num) == mtm_state->last_vote.num)
		return MTM_GEN_RECOVERY;
	else
		return MTM_GEN_DEAD; /* can't ever be online there */
}

/* most callers held lock, hence the second func instead of arg */
MtmStatusInGen
MtmGetCurrentStatusInGenNotLocked(void)
{
	MtmStatusInGen res;

	LWLockAcquire(mtm_state->gen_lock, LW_SHARED);
	res = MtmGetCurrentStatusInGen();
	LWLockRelease(mtm_state->gen_lock);
	return res;
}

/*
 * Mtm current status accessor for user facing code. Augments
 * MtmGetCurrentStatusInGen with connectivity state: see, even if we are
 * online in current gen, immediately telling user that node is online might
 * be disappointing as e.g. we could instantly lost connection with all other
 * nodes without learning about generation excluding us.
 *
 * Additionally distinguishes between 'need recovery, but have no idea from
 * whom' and 'recovering from some node'.
 */
MtmNodeStatus
MtmGetCurrentStatus(bool gen_locked, bool vote_locked)
{
	MtmStatusInGen status_in_gen;
	MtmNodeStatus res;

	/* doesn't impress with elegance, really */
	if (!gen_locked)
		LWLockAcquire(mtm_state->gen_lock, LW_SHARED);
	if (!vote_locked)
		LWLockAcquire(mtm_state->vote_lock, LW_SHARED);

	Assert(LWLockHeldByMe(mtm_state->gen_lock) || pb_preparers_incremented);
	Assert(LWLockHeldByMe(mtm_state->vote_lock) ||
		   LWLockHeldByMeInMode(mtm_state->gen_lock, LW_EXCLUSIVE));

	status_in_gen = MtmGetCurrentStatusInGen();
	if (status_in_gen == MTM_GEN_DEAD)
	{
		if (pg_atomic_read_u32(&mtm_state->receive_mode) == RECEIVE_MODE_DISABLED)
			res = MTM_DISABLED;
		else
			res = MTM_CATCHUP;
	}
	else
	{
		/*
		 * Our generation is viable, but check whether we see all its
		 * members. This is a subtle thing, probably deserving an improvement.
		 *
		 * The goal here is the following: if we are MTM_GEN_ONLINE in curr
		 * gen, connectivity for it is ok during this check and stays so
		 * hereafter, we shouldn't ERROR out later due to generation switches.
		 * Simply speaking, if you got success for "select 't'" from all nodes
		 * and no network/nodes failures happen, you obviously expect things
		 * to work.
		 *
		 * The first thing to ensure is that connectivity clique includes all
		 * current gen members. If it doesn't, campaigner will try to re-elect
		 * the generation. Note that simply checking connected mask is not
		 * enough; for instance, if during cluster boot node A (with gen ABC)
		 * sees B and C, but B <-> C don't see each other (or A is not aware
		 * of the connections yet), campaigner on 1 would try to exclude one
		 * of them. However, calculating clique on each xact start might be
		 * expensive; it is not hard to delegate this to dmq sender/receivers
		 * though -- TODO.
		 *
		 * Second, even if the connectivity right now is good, we must be sure
		 * campaigner doesn't operate an older data which might not be so
		 * good, lest he'd still attempt re-election. campaigner_on_tour
		 * serves this purpose.
		 *
		 * Now, since we don't attempt to poll other nodes here (and being
		 * cumbersome and expensive this is hardly worthwhile) we protect only
		 * from our campaigner reballoting if all goes well, but not the
		 * others, of course. e.g. races like
		 * - initially everyone in gen 1 <A, B, C>
		 * - A doesn't see B <-> C and successfully ballots for gen 2 <A, B>
		 * - "select 't'" gives ok at A and B
		 * - it also gives ok at C if C's clique is <A, B, C>, but C is not
		 *   aware of gen 2's election at all.
		 * are theoretically still possible.
		 *
		 * I haven't seen such races in tests though (unlike created by nodes
		 * own campaigner ones). Just in case, all this stuff doesn't
		 * influence safety; this is just a matter of deciding when to open
		 * the shop to the client.
		 */
		if (!is_submask(mtm_state->current_gen_members,
						MtmGetConnectivityClique(false)) ||
			mtm_state->campaigner_on_tour)
			res = MTM_ISOLATED;
		else if (status_in_gen == MTM_GEN_RECOVERY)
			res = MTM_RECOVERY;
		else
			res = MTM_ONLINE;
	}

	if (!vote_locked)
		LWLockRelease(mtm_state->vote_lock);
	if (!gen_locked)
		LWLockRelease(mtm_state->gen_lock);
	return res;
}

/*
 * The campaigner bgw, responsible for rising new generation elections.
 */

static BackgroundWorkerHandle *
CampaignerStart(Oid db_id, Oid user_id)
{
	BackgroundWorker worker;
	BackgroundWorkerHandle *handle;

	MemSet(&worker, 0, sizeof(BackgroundWorker));
	worker.bgw_flags = BGWORKER_SHMEM_ACCESS | BGWORKER_BACKEND_DATABASE_CONNECTION;
	worker.bgw_start_time = BgWorkerStart_ConsistentState;
	worker.bgw_restart_time = BGW_NEVER_RESTART;

	memcpy(worker.bgw_extra, &db_id, sizeof(Oid));
	memcpy(worker.bgw_extra + sizeof(Oid), &user_id, sizeof(Oid));

	sprintf(worker.bgw_library_name, "multimaster");
	sprintf(worker.bgw_function_name, "CampaignerMain");
	snprintf(worker.bgw_name, BGW_MAXLEN, "mtm-campaigner");

	if (!RegisterDynamicBackgroundWorker(&worker, &handle))
		elog(ERROR, "failed to start campaigner worker");

	return handle;
}

static void
CampaignerWake(void)
{
	/* using latch would be nicer */
	if (mtm_state->campaigner_pid != 0)
		kill(mtm_state->campaigner_pid, SIGUSR1);
}

static void
CampaignerOnExit(int code, Datum arg)
{
	mtm_state->campaigner_pid = 0;
}

/* xxx we have 3 copies now, time to unite them */
static void
attach_node(int node_id, MtmConfig *new_cfg, Datum arg)
{
	dmq_attach_receiver(psprintf(MTM_DMQNAME_FMT, node_id), node_id - 1);
}

static void
detach_node(int node_id, MtmConfig *new_cfg, Datum arg)
{
	/* detach incoming queues from this node */
	dmq_detach_receiver(psprintf(MTM_DMQNAME_FMT, node_id));
}

/* TODO: unite with resolver.c */
static void
scatter(MtmConfig *mtm_cfg, nodemask_t cmask, char *stream_name, StringInfo msg)
{
	int			i;

	/*
	 * XXX: peeking Mtm->peers here is weird. e.g. nothing prevents rot of
	 * dest_id when dmq will actually send msg: we might send message to
	 * wrong node if node was removed and added in the middle. It is better
	 * to change dmq API to idenfity counterparties by user-supplied ints
	 * which can be mapped into internal dmq's handles for efficiency.
	 *
	 */
	for (i = 0; i < mtm_cfg->n_nodes; i++)
	{
		int			node_id = mtm_cfg->nodes[i].node_id;
		DmqDestinationId dest_id;

		LWLockAcquire(Mtm->lock, LW_SHARED);
		dest_id = Mtm->peers[node_id - 1].dmq_dest_id;
		LWLockRelease(Mtm->lock);

		if (dest_id >= 0 && BIT_CHECK(cmask, node_id - 1))
			dmq_push_buffer(dest_id, stream_name, msg->data, msg->len);
	}
}

/* report that receiver had caught up */
void
MtmReportReceiverCaughtup(int node_id)
{
	instr_time cur_time;

	INSTR_TIME_SET_CURRENT(cur_time);
	SpinLockAcquire(&mtm_state->catchup_lock);
	mtm_state->catchup_node_id = node_id;
	mtm_state->catchup_ts = cur_time;
	SpinLockRelease(&mtm_state->catchup_lock);
	mtm_log(MtmStateDebug, "caughtup from node %d", node_id);
}


/*
 * Set receive_mode to recover from random most advanced node (having greatest
 * last_online_in) among given connected ones.
 */
static uint64 SetCatchupDonor(nodemask_t connected)
{
	int i;
	int most_advanced_node = MtmInvalidNodeId;
	uint64 most_advanced_gen_num;
	uint32 curr_receive_mode = pg_atomic_read_u32(&mtm_state->receive_mode);

	most_advanced_gen_num = MtmInvalidGenNum;
	for (i = 0; i < MTM_MAX_NODES; i++)
	{
		if (BIT_CHECK(connected, i))
		{
			uint64 gen_num = pg_atomic_read_u64(&mtm_state->others_last_online_in[i]);
			if (gen_num > most_advanced_gen_num)
			{
				most_advanced_node = i + 1;
				most_advanced_gen_num = gen_num;
			}
		}
	}
	/*
	 * If cluster has only one node, it can't be in MTM_GEN_DEAD and this
	 * function should never be called. If > 1 node, it ought to be called
	 * with majority of connected nodes, i.e. connected must have at least one
	 * node apart from me. (me has 0 value in others_last_online_in, it's
	 * quite useless though harmless to recover from myself)
	 */
	Assert(most_advanced_gen_num != MtmInvalidGenNum);

	/*
	 * XXX: it is actually possible that *our* last_online_in is higher than
	 * most_advanced_gen_num, though we are in dead gen -- it means there are not
	 * enough recovered nodes around me, but someone caught up and elected
	 * minority gen, e.g.
	 *  - 123 do a lot of xacts in gen n, 45 lag behind
	 *  - now only 145 live, 45 catching up
	 *  - 4 caught up and elected minority (dead) gen 14 with num n + 1.
	 * Here we still configure recovery from random node. This is harmless,
	 * but we could reflect this situation in monitoring better.
	 */

	/* Don't change donor unless we have a good reason to do that */
	if (!IS_RECEIVE_MODE_DONOR(curr_receive_mode) ||
		!BIT_CHECK(connected, curr_receive_mode - 1) ||
		(pg_atomic_read_u64(&mtm_state->others_last_online_in[curr_receive_mode - 1]) <
		 most_advanced_gen_num))
	{
		MtmSetReceiveMode(most_advanced_node);
		mtm_log(MtmStateSwitch, "set to catch up from node %d with max last_online_in=" UINT64_FORMAT " collected among connected=%s",
				most_advanced_node,
				most_advanced_gen_num,
				maskToString(connected));
	}
	return most_advanced_gen_num;
}

/*
 * Examine current gen, last_online_in (of me and neightbours), connectivity
 * and start balloting for new generation if it makes sense: vote myself and
 * return 'true' with filled candidate_gen, cohort, my_last_online_in.
 * To make sane decision we rely on heartbeats supplying us with fairly fresh
 * current gen and others' last_online_in.
 *
 * TODO: currently there is no special handling of dynamic membership change
 * and thus it is broken. Basically, we would need to ensure that
 *  a) two disjoint sets of nodes can't vote for two different gens with the
 *    same number;
 *  b) preserve 'any online member of gen n has all committable xacts of all
 *    gens < n' property.
 * To this end, we should make membership (configuration) change process
 * two-phased, each being usual global xact. After start_add_rm_node
 * committed, node must elect (and participate) only in election of gens with
 * new conf. Second phase (commit of finish_add_rm_node) is executed only when
 *  1) old majority switched to gen with new conf
 *  2) old majority doesn't have prepares stamped with old conf generation (
 *     this is important only for node rm -- without this, after two rms we
 *     might be left with xacts which can't be resolved even with majority
 *     online)
 *  finish_add_rm_node is committed in new conf gen, so its commit means
 *  majority of new conf gen is online there, ensuring b).
 *  Adding and removing node one-by-one with barrier in 1) ensures a)
 *
 */
static bool
CampaignMyself(MtmConfig *mtm_cfg, MtmGeneration *candidate_gen,
			   nodemask_t *cohort, uint64 *my_last_online_in)
{
	nodemask_t connected_mask_with_me;
	nodemask_t clique;
	bool	   is_curr_gen_connected;

	/*
	 * Basebackup'ed node must recover from donor until it obtains syncpoints
	 * allowing to pull properly from the rest of nodes.
	 */
	if (mtm_cfg->backup_node_id != MtmInvalidNodeId)
	{
		mtm_log(MtmStateDebug, "setting to recover from donor %d after basebackup",
				mtm_cfg->backup_node_id);
		MtmSetReceiveMode(mtm_cfg->backup_node_id);
		return false;
	}

	/*
	 * Exclude voter and gen switchers. Get locks before peeking connectivity
	 * to forbid checking out campaigner_on_tour until we decide whether we're
	 * going to campaign basing on this connectivity.
	 */
	LWLockAcquire(mtm_state->gen_lock, LW_SHARED);
	LWLockAcquire(mtm_state->vote_lock, LW_EXCLUSIVE);

	LWLockAcquire(mtm_state->connectivity_lock, LW_SHARED);
	connected_mask_with_me = MtmGetConnectedMaskWithMe(true);
	clique = MtmGetConnectivityClique(true);
	is_curr_gen_connected = MtmIsConnectivityClique(mtm_state->current_gen_members);
	LWLockRelease(mtm_state->connectivity_lock);

	mtm_log(MtmStateDebug, "CampaignMyself: current_gen.num=" UINT64_FORMAT ", current_gen.members=%s, current_gen.configured=%s, StatusInGen=%s, last_online_in=" UINT64_FORMAT ", last_vote.num=" UINT64_FORMAT ", clique=%s, connected_mask_with_me=%s, is_curr_gen_connected=%d",
			pg_atomic_read_u64(&mtm_state->current_gen_num),
			maskToString(mtm_state->current_gen_members),
			maskToString(mtm_state->current_gen_configured),
			MtmStatusInGenMnem[MtmGetCurrentStatusInGen()],
			mtm_state->last_online_in,
			mtm_state->last_vote.num,
			maskToString(clique),
			maskToString(connected_mask_with_me),
			is_curr_gen_connected);

	/*
	 * If I am online in curr gen (definitely its member) and all members are
	 * interconnected, the situation is fine for me: don't ballot. This
	 * shorthack is caused by 'majority clique always elects its gen' rule
	 * which is necessary for xact resolution liveness (see below). Without
	 * it, there is a small chance offline node turn on might temporarily
	 * throw out live node(s) from the cluster because there could be several
	 * cliques. For example,
	 * - nodes 23 online in its gen, 1 is down
	 * - 1 gets up. For a short period of time, 2 sees two cliques
	 *   12 and 23 as 1<->3 connection is not yet established/gossiped.
	 *   Clique must be chosen deterministically to avoid flip-flopping in
	 *   sausage-like topologies, 2 picks 12 and ballots 12 & 23,
	 *   exluding 3 from the cluster.
	 * The only reason for 2 balloting at all here, while 1 is not
	 * recovered yet is xact resolution liveness, c.f. handle_1a.
	 */
	if (MtmGetCurrentStatusInGen() == MTM_GEN_ONLINE && is_curr_gen_connected)
	{
		mtm_log(MtmStateDebug, "not campaigning as curr gen is connected and I am online");
		goto no_interesting_candidates;
	}

	/*
	 * No point to campaign if there is no quorum clique with me at all. But
	 * if we are in dead gen and thus need recovery, tell receivers to catchup
	 * from the most advanced node if we see the majority (this is what
	 * e.g. allows non-clique-members to keep up in stable sausage case) -- or
	 * declare we don't know to recover from whom if we aren't. The latter
	 * really doesn't changes anything signficant but useful for monitoring.
	 *
	 * Campaigning also has little sense if the calculated clique doesn't
	 * contain us. This might happen as we obtain others connectivity masks
	 * from dmq heartbeats, so mask existence means dmq receiver is certainly
	 * live, but dmq sender is not necessarily.
	 *
	 * However, proceed balloting if referee mode is enabled and we are online
	 * (i.e. election of referee granted gen is not blocked by our need to
	 * recover) regardless of connectivity -- the possiblity of being online
	 * without majority (second node) is the whole point of referee.
	 * TODO: it would be nice to have heartbeats with referee to avoid winding
	 * generation numbers in vain if we know here that referee is unavailable.
	 * It would also be useful for monitoring.
	 */
	if ((!MtmQuorum(mtm_cfg, popcount(clique)) ||
		 !BIT_CHECK(clique, Mtm->my_node_id - 1)) &&
		!(IS_REFEREE_ENABLED() && MtmGetCurrentStatusInGen() == MTM_GEN_ONLINE))
	{
		/*
		 * Don't change receive_mode though when we are in recovery/online as
		 * peers might get back again without changing the gen, and nobody
		 * would restore receive_mode to correct value. We know donors in
		 * recovery and don't need them at all in online anyway.
		 */
		if (MtmGetCurrentStatusInGen() == MTM_GEN_DEAD)
		{
			if (MtmQuorum(mtm_cfg, popcount(connected_mask_with_me)))
				SetCatchupDonor(connected_mask_with_me);
			else
				MtmSetReceiveMode(RECEIVE_MODE_DISABLED);
		}
		mtm_log(MtmStateDebug, "not campaigning as there is no quorum connectivity clique with me");
		goto no_interesting_candidates;
	}

	if (MtmGetCurrentStatusInGen() == MTM_GEN_DEAD)
	{
		uint64 donor_loi;

		/*
		 * So I can't ever participate in this gen; ensure I am catching up
		 * from right donor. This is useful even if I am member of current
		 * gen, e.g. if it its minority gen and I am not the most advanced
		 * node.
		 */
		donor_loi = SetCatchupDonor(connected_mask_with_me);

		/*
		 * Now, if we are going to ballot for adding me back (forcing others
		 * to wait for me) make sure recovery lag is not too high.
		 * However, if most advanced nodes' last_online_in <= ours, there are
		 * no committable xacts which we miss, so skip the check.
		 */
		if (!BIT_CHECK(mtm_state->current_gen_members, Mtm->my_node_id - 1) &&
			(mtm_state->last_online_in < donor_loi))
		{
			int catchup_node_id;
			instr_time catchup_ts;
			instr_time cur_time;

			SpinLockAcquire(&mtm_state->catchup_lock);
			catchup_node_id = mtm_state->catchup_node_id;
			catchup_ts = mtm_state->catchup_ts;
			SpinLockRelease(&mtm_state->catchup_lock);

			if (catchup_node_id != pg_atomic_read_u32(&mtm_state->receive_mode))
			{
				mtm_log(MtmStateDebug, "not proposing new gen with me because %s",
						catchup_node_id == MtmInvalidNodeId ?
						"we are not caught up" :
						psprintf("catchup donor is %d but it should be %d",
								 catchup_node_id,
								 pg_atomic_read_u32(&mtm_state->receive_mode)));
				goto no_interesting_candidates; /* wrong donor */
			}

			/*
			 * TODO: it would be better use configurable lag size instead of
			 * relying on walsender caughtup_cb as caughtup_cb probably might
			 * never be reached on some workloads
			 */

			INSTR_TIME_SET_CURRENT(cur_time);
			INSTR_TIME_SUBTRACT(cur_time, catchup_ts);
			/* cutoff is choosen somewhat arbitrary */
			if (INSTR_TIME_GET_MILLISEC(cur_time) >= MtmHeartbeatRecvTimeout * 5)
			{
				mtm_log(MtmStateDebug, "not proposing new gen with me because last catchup was %f ms ago",
						INSTR_TIME_GET_MILLISEC(cur_time));
				goto no_interesting_candidates; /* stale caughtup report */
			}
		}
	}

	/*
	 * Okay, form list of candidates. We want to
	 * 1) add myself, if not present in current gen;
	 * 2) exclude any non-clique member;
	 *
	 * We should not add any non-already present member but me because it
	 * might be arbitrary lagging.
	 */
	candidate_gen->members = mtm_state->current_gen_members & clique;
	BIT_SET(candidate_gen->members, Mtm->my_node_id - 1);
	candidate_gen->configured = mtm_cfg->mask;

	/*
	 * Note that candidate members might be minority when live majority clique
	 * happens to contain nodes which weren't present in latest gen and thus
	 * are probably in deep recovery -- they are intentionally not included in
	 * candidates. We still ballot in this case, even if I am member of
	 * current gen and thus don't need new gen to declare "I'm caught up". The
	 * is needed to ensure transaction resolution liveness: it assumes any
	 * stable live connecitivity clique forming majority eventually elects
	 * generation with its members (unless previous was already the same)
	 * regardless of recovery progress; c.f. handle_1a for details.
	 * (note that we need this weirdness only for >4 nodes, as with less
	 * nodes xact resolution cannot deadlock (at least one of
	 * coordinators of two conflicting xacts is member of live majority, and
	 * coordinator may unconditionally abort his prepares before PC), so we
	 * may avoid balloting for <=3 nodes.)
	 *
	 * This might seem to have a downside though: in case of short flip
	 * flopping like
	 * - AB working normally, C if offline and in deep recovery
	 * - AC becomes the clique for a short time, generation with the only
	 *   member A is elected by AC
	 * - AB now the clique again and B is forced into recovery as it skipped
	 *   generation.
	 * However, recovery of B in this case would be very short as there are
	 * no committed transactions it misses.
	 */

	/*
	 * The only reason to revote for gen with the same members is
	 * impossibility to be online in current gen (at least) because that would
	 * violate our promise given during voting for gen m never to be online in
	 * gen n: last_online_in on the moment of voting < n < m.
	 * Re-voting would help here.
	 */
	if (candidate_gen->members == mtm_state->current_gen_members &&
		(MtmGetCurrentStatusInGen() != MTM_GEN_DEAD ||
		 mtm_state->last_vote.num == pg_atomic_read_u64(&mtm_state->current_gen_num)))
	{
		mtm_log(MtmStateDebug, "not re-balloting for gen with candidates=%s as my current gen members are the same, StatusInGen=%s, last_vote=" UINT64_FORMAT,
				maskToString(candidate_gen->members),
				MtmStatusInGenMnem[MtmGetCurrentStatusInGen()],
				mtm_state->last_vote.num);
		goto no_interesting_candidates;
	}

	/*
	 * All right, we have meaningful candidates, let's ballot.
	 * Vote myself. However, if our last_vote is exactly as candidates, reuse
	 * it instead of advancement: this prevents healthy node needlessly going
	 * into recovery in races like
	 * - C dies, AB are alive
	 * - A ballots for 10 <AB>
	 * - B agrees to 10 <AB>
	 * - A declares 10 <AB> as elected and gets online there; B is not aware
	 *   of this yet
	 * - B proposes (would propose, if we inc last_vote here) 11 <AB>
	 * - A agrees and says its last_online_in is 10
	 * - B last_online_in is < 10, so it is not donor in 11 <AB>, which
	 *   pointlessly puts it into recovery
	 *
	 * last_vote reusage is obviously safe. It also doesn't harm liveness: if
	 * cohort refuses the campaign due to gen num being too low, responder
	 * will report that so we would bump last_vote further. Liveness related
	 * to 'can't be online in gen < last_vote' promise is also not affected
	 * since, well, we *can* be online in last_vote gen.
	 */
	mtm_state->campaigner_on_tour = true;
	if (candidate_gen->members == mtm_state->last_vote.members &&
		candidate_gen->configured == mtm_state->last_vote.configured)
	{
		candidate_gen->num = mtm_state->last_vote.num;
	}
	else
	{
		candidate_gen->num = mtm_state->last_vote.num + 1;
		mtm_state->last_vote = *candidate_gen;
		MtmStateSave();
	}

	mtm_log(MtmStateSwitch, "proposed and voted myself for gen num=" UINT64_FORMAT ", members=%s, configured=%s, clique=%s",
			candidate_gen->num,
			maskToString(candidate_gen->members),
			maskToString(candidate_gen->configured),
			maskToString(clique));

	/* include myself in donor calculation */
	*my_last_online_in = mtm_state->last_online_in;

	LWLockRelease(mtm_state->vote_lock);
	LWLockRelease(mtm_state->gen_lock);

	*cohort = clique;
	BIT_CLEAR(*cohort, Mtm->my_node_id - 1);
	return true;

no_interesting_candidates:
	LWLockRelease(mtm_state->vote_lock);
	LWLockRelease(mtm_state->gen_lock);
	return false;
}

/* ensures we collect responses for our current tour, not older ones */
static bool
CampaignerGatherHook(MtmMessage *anymsg, Datum arg)
{
	uint64 gen_num = DatumGetUInt64(arg);
	MtmGenVoteResponse *msg = (MtmGenVoteResponse *) anymsg;

	/* campaigner never gets other messsages */
	Assert(anymsg->tag == T_MtmGenVoteResponse);
	return gen_num == msg->gen_num;
}

/*
 * Having voted myself, now request others, i.e. the clique (must have already
 * checked it forms majority).
 */
static void
CampaignTour(MemoryContext campaigner_ctx, MtmConfig **mtm_cfg,
			 MtmGeneration candidate_gen, nodemask_t cohort, uint64 my_last_online_in)
{
	int		   sconn_cnt[DMQ_N_MASK_POS];
	int nvotes;
	MtmGenVoteRequest request_msg;
	MtmGenVoteResponse *messages[MTM_MAX_NODES];
	int senders[MTM_MAX_NODES];
	int			n_messages;
	int i;
	uint64 max_last_vote_num = MtmInvalidGenNum;
	uint64 max_last_online_in = my_last_online_in;
	nodemask_t donors = 0;

	/*
	 * TODO: it would be nice to dmq_reattach_shm_mq here (normally it is done
	 * via dmq_stream_subscribe, but here we are permanently subscribed).
	 *
	 */
	dmq_get_sendconn_cnt(cohort, sconn_cnt);

	request_msg.tag = T_MtmGenVoteRequest;
	request_msg.gen = candidate_gen;
	scatter(*mtm_cfg, cohort, "mon",
			MtmMessagePack((MtmMessage *) &request_msg));

	gather(cohort, (MtmMessage **) messages, senders, &n_messages,
		   CampaignerGatherHook, UInt64GetDatum(candidate_gen.num),
		   sconn_cnt, MtmInvalidGenNum);
	nvotes = 1; /* myself already voted */
	/*
	 * When node votes for generation n, it promises never become online in
	 * generations < n henceforth. Thus, its last_online_in on the moment of
	 * voting is essentially freezed for (until in terms of logical clocks)
	 * generation n. Which means once we've collected majority of votes, max
	 * last_online_in among the voters is the greatest < n generation which
	 * can do xacts, and nodes who had such last_online_in are donors of n --
	 * they definitely contain all xacts < max_last_online_in, and they will
	 * stop doing xacts of max_last_online_in generation itself once they
	 * learn about n election, effectively preventing any further commits of
	 * them.
	 */
	BIT_SET(donors, Mtm->my_node_id - 1); /* start iteration on myself */
	for (i = 0; i < n_messages; i++)
	{
		MtmGenVoteResponse *msg = messages[i];
		Assert(msg->tag == T_MtmGenVoteResponse);

		mtm_log(MtmStateDebug, "CampaignTour: got '%s' from %d",
				MtmMesageToString((MtmMessage *) msg), senders[i]);

		if (msg->gen_num == candidate_gen.num && msg->vote_ok)
		{
			nvotes++;
			if (msg->last_online_in == max_last_online_in)
				BIT_SET(donors, senders[i] - 1); /* one more such donor */
			else if (msg->last_online_in > max_last_online_in)
			{
				donors = 0; /* found more advanced donor(s) */
				BIT_SET(donors, senders[i] - 1);
				max_last_online_in = msg->last_online_in;
			}
		}
		if (!msg->vote_ok && msg->last_vote_num > max_last_vote_num)
			max_last_vote_num = msg->last_vote_num;
	}

	if (MtmQuorum(*mtm_cfg, nvotes)) /* victory */
	{
		mtm_log(MtmStateSwitch, "won election of gen num=" UINT64_FORMAT ", members=%s, configured=%s, donors=%s",
				candidate_gen.num,
				maskToString(candidate_gen.members),
				maskToString(candidate_gen.configured),
				maskToString(donors));

		MtmConsiderGenSwitch(candidate_gen, donors);
		/* TODO: probably force heartbeat here for faster convergence? */
	}
	else
	{
		mtm_log(MtmStateSwitch, "failed election of gen num=" UINT64_FORMAT ", members=%s, configured=%s, nvotes=%d",
				candidate_gen.num,
				maskToString(candidate_gen.members),
				maskToString(candidate_gen.configured),
				nvotes);
	}
	LWLockAcquire(mtm_state->gen_lock, LW_SHARED);
	LWLockAcquire(mtm_state->vote_lock, LW_EXCLUSIVE);
	/* if anyone complained about our last_vote being too low, bump it */
	if (max_last_vote_num != MtmInvalidGenNum)
	{
		if (max_last_vote_num > mtm_state->last_vote.num)
		{
			mtm_state->last_vote.num = max_last_vote_num;
			mtm_state->last_vote.members = 0;
			mtm_state->last_vote.configured = 0;
			MtmStateSave();
		}
	}
	mtm_state->campaigner_on_tour = false;
	LWLockRelease(mtm_state->vote_lock);
	LWLockRelease(mtm_state->gen_lock);
}

/* get referee vote for me in given generation */
static void
CampaignReferee(MtmGeneration candidate_gen)
{
	PGconn	   *conn;
	PGresult   *res;
	char		sql[128];
	nodemask_t donors = 0;

	/*
	 * TODO: hanging the entire monitor is definitely not nice in case
	 * e.g. the packets are being dropped. Should add TCP timeouts or
	 * something here (as well as in another connection to referee).
	 */
	conn = PQconnectdb(MtmRefereeConnStr);
	if (PQstatus(conn) != CONNECTION_OK)
	{
		mtm_log(WARNING, "couldn't connect to referee to request the grant: %s",
				PQerrorMessage(conn));
		PQfinish(conn);
		mtm_state->campaigner_on_tour = false;
		return;
	}

	Assert(Mtm->my_node_id == first_set_bit(candidate_gen.members) + 1);
	sprintf(sql, "select referee.request_grant(%d, " UINT64_FORMAT ")",
			Mtm->my_node_id, candidate_gen.num);
	res = PQexec(conn, sql);
	if (PQresultStatus(res) != PGRES_TUPLES_OK)
	{
		mtm_log(WARNING, "'%s' failed: %s", sql, PQresultErrorMessage(res));
		PQclear(res);
		PQfinish(conn);
		mtm_state->campaigner_on_tour = false;
		/*
		 * Though the query errored out, we could already managed to acquire
		 * the grant (imagine network failure after commit). And if second
		 * node just blinked and we won't attempt to get the grant again in
		 * the nearest future, leaving it acquired is quite bad (it would
		 * prevent election of second node). So remember to try clearing it.
		 *
		 * It is ever set to MtmInvalidGenNum by campaigner itself, so ok to
		 * do without locks.
		 */
		mtm_state->referee_grant_turn_in_pending = pg_atomic_read_u64(
			&mtm_state->current_gen_num);
		return;
	}
	/*
	 * The result is meaningless (function would error out on conflict), but
	 * it is expected to return single row of null function result.
	 */
	if (PQntuples(res) != 1 || PQnfields(res) != 1)
	{
		mtm_log(WARNING, "'%s' returned unexpected result (ntuples=%d, nfields=%d, val=%s)",
				sql, PQntuples(res), PQnfields(res), PQgetvalue(res, 0, 0));
		PQclear(res);
		PQfinish(conn);
		mtm_state->campaigner_on_tour = false;
		return;
	}

	/* ok, we got the grant */
	PQclear(res);
	PQfinish(conn);
	mtm_log(MtmStateSwitch, "got referee grant in generation num=" UINT64_FORMAT,
			candidate_gen.num);

	/* I am the only donor in my referee-granted generation */
	BIT_SET(donors, Mtm->my_node_id - 1);
	MtmConsiderGenSwitch(candidate_gen, donors);
	mtm_state->campaigner_on_tour = false;
}

/*
 * Turn in the referee grant. There are two conditions for doing this:
 * 1) (normal): we have switched into full generation with both nodes and
 *    both are recovered (online) in it. Then any grant with gen num < this
 *    full safely can (and should) be cleared.
 * 2) this node potentially managed to acquire the grant but it is not going
 *    to use it -- c.f. failure in CampaignReferee. Such hanged grant can be
 *    safely deleted with 'node_id = me and gen_num <= last_vote' clause:
 *    if I am currently in full generation, I definitely won't ever get into
 *    my referee granted generation with <= last_vote gen num.
 */
static void
RefereeClearGrant(void)
{
	PGconn	   *conn;
	PGresult   *res;
	uint64 full_online_gen_num;
	uint64 my_last_vote_num;
	int i;
	char		sql[128];

	LWLockAcquire(mtm_state->gen_lock, LW_SHARED);
	LWLockAcquire(mtm_state->vote_lock, LW_SHARED);
	/*
	 * can't clear the grant if I am not online in full (two nodes) gen
	 */
	if (popcount(mtm_state->current_gen_members) != 2 ||
		MtmGetCurrentStatusInGen() != MTM_GEN_ONLINE)
	{
		mtm_log(MtmStateDebug, "not clearing referee flag as I am not online in full gen: current_gen_members=%s, StatusInGen=%s",
				maskToString(mtm_state->current_gen_members),
				MtmStatusInGenMnem[MtmGetCurrentStatusInGen()]);
		mtm_state->referee_grant_turn_in_pending = MtmInvalidGenNum;
		LWLockRelease(mtm_state->vote_lock);
		LWLockRelease(mtm_state->gen_lock);
		return;
	}

	/* can clear the grant only when second node recovers */
	for (i = 0; i < MTM_MAX_NODES; i++)
	{
		if (BIT_CHECK(mtm_state->current_gen_members, i) &&
			i + 1 != Mtm->my_node_id &&
			(pg_atomic_read_u64(&mtm_state->others_last_online_in[i]) <
			 mtm_state->last_online_in))
		{
			mtm_log(MtmStateDebug, "not clearing referee grant as node %d is not online in current generation (num=" UINT64_FORMAT ") yet",
					i + 1, pg_atomic_read_u64(&mtm_state->current_gen_num));
			LWLockRelease(mtm_state->vote_lock);
			LWLockRelease(mtm_state->gen_lock);
			return;
		}
	}

	full_online_gen_num = pg_atomic_read_u64(&mtm_state->current_gen_num);
	my_last_vote_num = mtm_state->last_vote.num;
	LWLockRelease(mtm_state->vote_lock);
	LWLockRelease(mtm_state->gen_lock);

	/* ok, clear the grant */
	conn = PQconnectdb(MtmRefereeConnStr);
	if (PQstatus(conn) != CONNECTION_OK)
	{
		mtm_log(WARNING, "couldn't connect to referee to clear the grant: %s",
				PQerrorMessage(conn));
		PQfinish(conn);
		return;
	}

	sprintf(sql, "delete from referee.decision where gen_num < " UINT64_FORMAT
			" or (node_id = %d and gen_num <= " UINT64_FORMAT ")",
			full_online_gen_num, Mtm->my_node_id, my_last_vote_num);
	res = PQexec(conn, sql);
	if (PQresultStatus(res) != PGRES_COMMAND_OK)
	{
		mtm_log(WARNING, "'%s' failed: %s", sql, PQresultErrorMessage(res));
		PQclear(res);
		PQfinish(conn);
		return;
	}
	if (atoi(PQcmdTuples(res)) > 0)
		mtm_log(MtmStateDebug, "grant cleared");
	/* done */
	LWLockAcquire(mtm_state->gen_lock, LW_SHARED);
	/*
	 * It is next to impossible, but if between deletion and this point a
	 * couple of gen switches happened, we might accidently reset
	 * referee_grant_turn_in_pending of newer gen and thus never clear the
	 * grant; to avoid this, check the gen num in which clear request was
	 * created.
	 */
	if (mtm_state->referee_grant_turn_in_pending <= full_online_gen_num)
		mtm_state->referee_grant_turn_in_pending = MtmInvalidGenNum;
	LWLockRelease(mtm_state->gen_lock);
	PQclear(res);
	PQfinish(conn);
}

void
CampaignerMain(Datum main_arg)
{
	Oid			db_id,
				user_id;
	/* Exists to track dmq_attach|detach_receiver */
	MtmConfig  *mtm_cfg = NULL;
	/* for message packing/unpacking and maskToString */
	MemoryContext campaigner_ctx =	AllocSetContextCreate(TopMemoryContext,
														  "CampaignerContext",
														  ALLOCSET_DEFAULT_SIZES);
	static unsigned short drandom_seed[3] = {0, 0, 0};

	MtmBackgroundWorker = true;
	mtm_log(MtmStateMessage, "campaigner bgw started");
	before_shmem_exit(CampaignerOnExit, (Datum) 0);
	mtm_state->campaigner_pid = MyProcPid;

	/*
	 * Note that StartBackgroundWorker already set reasonable handlers,
	 * e.g. SIGUSR1 sets latch.
	 */
	/* die gracefully not in signal handler but in CHECK_FOR_INTERRUPTS */
	pqsignal(SIGTERM, die);
	BackgroundWorkerUnblockSignals();

	memcpy(&db_id, MyBgworkerEntry->bgw_extra, sizeof(Oid));
	memcpy(&user_id, MyBgworkerEntry->bgw_extra + sizeof(Oid), sizeof(Oid));
	/* Connect to a database */
	BackgroundWorkerInitializeConnectionByOid(db_id, user_id, 0);

	/* Keep us informed about subscription changes. */
	CacheRegisterSyscacheCallback(SUBSCRIPTIONOID,
								  pubsub_change_cb,
								  (Datum) 0);

	mtm_cfg = MtmReloadConfig(mtm_cfg, attach_node, detach_node, (Datum) NULL);
	config_valid = true;
	dmq_stream_subscribe("genvoteresp");

	/* maskToString also eats memory */
	MemoryContextSwitchTo(campaigner_ctx);

	/* borrowed from float.c */
	{
		TimestampTz now = GetCurrentTimestamp();
		uint64		iseed;

		/* Mix the PID with the most predictable bits of the timestamp */
		iseed = (uint64) now ^ ((uint64) MyProcPid << 32);
		drandom_seed[0] = (unsigned short) iseed;
		drandom_seed[1] = (unsigned short) (iseed >> 16);
		drandom_seed[2] = (unsigned short) (iseed >> 32);
	}

	/*
	 * After reboot we can't reliably say whether turn in is required, attempt
	 * it in case there is a need.
	 */
	if (IS_REFEREE_ENABLED())
		mtm_state->referee_grant_turn_in_pending = pg_atomic_read_u64(
			&mtm_state->current_gen_num);

	for (;;)
	{
		int			rc;
		MtmGeneration candidate_gen;
		nodemask_t cohort;
		uint64 my_last_online_in;
		bool	tour;

		/* cleanup message pack/unpack allocations */
		MemoryContextReset(campaigner_ctx);

		CHECK_FOR_INTERRUPTS();

		AcceptInvalidationMessages();
		if (!config_valid)
		{
			mtm_log(LOG, "reloading config");
			mtm_cfg = MtmReloadConfig(mtm_cfg, attach_node, detach_node, (Datum) NULL);
			config_valid = true;
		}
		/* xact in MtmReloadConfig could've knocked down our ctx */
		MemoryContextSwitchTo(campaigner_ctx);

		/* do the job */
		tour = CampaignMyself(mtm_cfg, &candidate_gen, &cohort, &my_last_online_in);

		if (tour)
		{
			if (!IS_REFEREE_GEN(candidate_gen.members, candidate_gen.configured))
			{
				/* normal case, poll neighbours */
				CampaignTour(campaigner_ctx, &mtm_cfg, candidate_gen, cohort,
							 my_last_online_in);
			}
			else
			{
				/*
				 * we are in referee mode and this gen requires only referee
				 * permission for election; request it
				 */
				CampaignReferee(candidate_gen);
			}
		}

		if (IS_REFEREE_ENABLED() &&
			mtm_state->referee_grant_turn_in_pending != MtmInvalidGenNum)
			RefereeClearGrant();

		/*
		 * Generally there is no need to have short timeout as we are wakened
		 * explicitly on network changes. However, campaign might fail to
		 * other reasons, e.g. two nodes might want to add themselves at the
		 * same time under the same gen num. To reduce voting contention, add
		 * randomized retry timeout like in Raft.
		 */
		rc = WaitLatch(MyLatch,
					   WL_LATCH_SET | WL_TIMEOUT | WL_POSTMASTER_DEATH,
					   3000 * pg_erand48(drandom_seed),
					   PG_WAIT_EXTENSION);

		/* Emergency bailout if postmaster has died */
		if (rc & WL_POSTMASTER_DEATH)
			proc_exit(1);

		if (rc & WL_LATCH_SET)
			ResetLatch(MyLatch);
	}
}

/*
 * Process request to vote for new gen, probably actually voting for it.
 * We must send answer in any case to prevent sender from infinite waiting.
 */
static void
HandleGenVoteRequest(MtmConfig *mtm_cfg, MtmGenVoteRequest *req,
					 int sender_node_id, int dest_id)
{
	StringInfo	packed_msg;
	MtmGenVoteResponse resp;
	nodemask_t clique = MtmGetConnectivityClique(false);

	mtm_log(MtmStateDebug, "HandleGenVoteRequest: got '%s' from %d",
			MtmMesageToString((MtmMessage *) req), sender_node_id);

	MemSet(&resp, '\0', sizeof(MtmGenVoteResponse));
	resp.tag = T_MtmGenVoteResponse;
	resp.gen_num = req->gen.num;

	/* Exclude voter and gen switchers */
	LWLockAcquire(mtm_state->gen_lock, LW_SHARED);
	LWLockAcquire(mtm_state->vote_lock, LW_EXCLUSIVE);

	/*
	 * If our generation num is already equal to requested or even higher,
	 * refuse the vote. Checks below ensure safety wouldn't be damaged anyway,
	 * however ignoring this might needlessly put healthy node into recovery:
	 * if C died, both A and B vote for excluding it and A already switched
	 * into AB gen while B is not aware about the election yet, A's 'ok' reply
	 * will force B into recovery as its last_online_in of A is higher than B
	 * one.
	 *
	 * Requester must soon learn about our current gen and change his mind
	 * anyway.
	 */
	if (MtmGetCurrentGenNum() >= req->gen.num)
	{
		resp.vote_ok = false;
	}
	/* already voted for exactly this gen, can safely confirm it again */
	else if (EQUAL_GENS(mtm_state->last_vote, req->gen))
	{
		resp.vote_ok = true;
		resp.last_online_in = mtm_state->last_online_in;
	}
	else if (mtm_state->last_vote.num >= req->gen.num)
	{
		/* already voted for lower gen, can't do that again */
		resp.vote_ok = false;
		resp.last_vote_num = mtm_state->last_vote.num;
	}
	else
	{
		/*
		 * Ok, we can vote for the proposed gen. Let's check if it makes sense:
		 *  1) We would like to adhere to the rule 'node can add only  itself
		 *     to new gen' to prevent election of lagging nodes. This is
		 *     already checked by the proposer, but his info could be stale,
		 *     so it seems useful to verify it at the voter side, c.f.
		 *     generations2.md.
		 *  2) It should conform to our idea of the clique.
		 *  3) Set of configured nodes should match.
		 */
		nodemask_t curr_gen_members_and_proposer = mtm_state->current_gen_members;

		BIT_SET(curr_gen_members_and_proposer, sender_node_id - 1);
		if (is_submask(req->gen.members, curr_gen_members_and_proposer) &&
			is_submask(req->gen.members, clique) &&
			req->gen.configured == mtm_cfg->mask)
		{
			MtmStatusInGen old_status_in_gen = MtmGetCurrentStatusInGen();

			resp.vote_ok = true;
			resp.last_online_in = mtm_state->last_online_in;

			/* persist the vote */
			mtm_state->last_vote = req->gen;
			MtmStateSave();

			/*
			 * If we are not online in current generation, probably after
			 * giving this vote we are forbidden to ever become so -- this
			 * allows the generation campaigner to use last_online_in in our
			 * answer to reliably determine donors.
			 */
			if (old_status_in_gen == MTM_GEN_RECOVERY &&
				MtmGetCurrentStatusInGen() == MTM_GEN_DEAD)
			{
				mtm_log(MtmStateMessage, "switched to dead in generation num=" UINT64_FORMAT "after giving vote for generation num=" UINT64_FORMAT,
						MtmGetCurrentGenNum(),
						req->gen.num);
				MtmSetReceiveMode(RECEIVE_MODE_DISABLED);
			}
		}
		else
		{
			resp.vote_ok = false;
		}
		resp.last_vote_num = MtmInvalidGenNum;
	}

	LWLockRelease(mtm_state->vote_lock);
	LWLockRelease(mtm_state->gen_lock);

	mtm_log(MtmStateDebug, "HandleGenVoteRequest: replying '%s' to %d",
			MtmMesageToString((MtmMessage *) &resp), sender_node_id);

	packed_msg = MtmMessagePack((MtmMessage *) &resp);
	dmq_push_buffer(dest_id, "genvoteresp", packed_msg->data, packed_msg->len);
}

static char *
MtmReceiveModeMnem(uint32 mode)
{
	if (mode == RECEIVE_MODE_DISABLED)
		return "disabled";
	else if (mode == RECEIVE_MODE_NORMAL)
		return "normal";
	else
		return psprintf("recovery from node %d", mode);
}

static void
MtmSetReceiveMode(uint32 mode)
{
	pg_atomic_write_u32(&mtm_state->receive_mode, mode);
	/* waking up receivers while disabled is not dangerous but pointless */
	if (mode != RECEIVE_MODE_DISABLED)
		ConditionVariableBroadcast(&Mtm->receivers_cv);
	mtm_log(MtmStateDebug, "receive mode set to %s", MtmReceiveModeMnem(mode));
}

/* In what mode we should currently receive from the given node? */
MtmReplicationMode
MtmGetReceiverMode(int nodeId)
{
	uint32 receive_mode = pg_atomic_read_u32(&mtm_state->receive_mode);

	if (receive_mode == RECEIVE_MODE_DISABLED)
		return REPLMODE_DISABLED;
	if (receive_mode == RECEIVE_MODE_NORMAL)
		return REPLMODE_NORMAL;
	if (receive_mode == nodeId)
		return REPLMODE_RECOVERY;
	/* we are in recovery, but this node is not the donor */
	return REPLMODE_DISABLED;
}

/*
 * -----------------------------------
 * Connectivity: who sees who, clique calculation.
 * -----------------------------------
 */

/*
 * Whom I see currently, i.e. have bidirectional dmq connection with.
 * Does *not* include myself.
 */
nodemask_t
MtmGetConnectedMask(bool locked)
{
	nodemask_t res;

	if (!locked)
		LWLockAcquire(mtm_state->connectivity_lock, LW_SHARED);
	Assert(LWLockHeldByMe(mtm_state->connectivity_lock));
	res = mtm_state->dmq_receivers_mask & mtm_state->dmq_senders_mask;
	if (!locked)
		LWLockRelease(mtm_state->connectivity_lock);
	return res;

}

/* MtmGetConnectedMask + me */
nodemask_t
MtmGetConnectedMaskWithMe(bool locked)
{
	int me = Mtm->my_node_id;
	nodemask_t res;

	if (me == MtmInvalidNodeId)
		elog(ERROR, "multimaster is not configured");
	res = MtmGetConnectedMask(locked);
	BIT_SET(res, me - 1);
	return res;
}

void*
MtmOnDmqReceiverConnect(char *node_name)
{
	int			node_id;
	MtmConfig  *cfg = MtmLoadConfig();

	sscanf(node_name, MTM_DMQNAME_FMT, &node_id);

	if (MtmNodeById(cfg, node_id) == NULL)
		mtm_log(FATAL, "[STATE] node %d not found", node_id);
	else
		mtm_log(MtmStateMessage, "[STATE] dmq receiver from node %d connected", node_id);

	/* do not hold lock for mtm.cluster_nodes */
	ResourceOwnerRelease(TopTransactionResourceOwner,
						 RESOURCE_RELEASE_LOCKS,
						 true, true);

	/*
	 * We set dmq_receivers_mask bit not here but on first heartbeat because
	 * 1) dmq calls dmq_receiver_stop_hook *after* releasing its handle
	 *    (which prevents reconnection of the same sender), so there is a
	 *    race -- old dying receiver might clear bit set by new one.
	 * 2) Until the first heartbeat we don't know visibility mask of the node,
	 *    so set bit would not be of much use anyway.
	 */

	return AllocSetContextCreate(TopMemoryContext,
								 "MtmDmqHeartBeatContext",
								 ALLOCSET_DEFAULT_SIZES);
}

void
MtmOnDmqReceiverHeartbeat(char *node_name, StringInfo msg, void *extra)
{
	int			node_id;
	MemoryContext heartbeat_context = (MemoryContext) extra;
	MemoryContext oldcontext;
	MtmHeartbeat *parsed_msg;
	bool changed = false;
	nodemask_t old_connected_mask;

	/*
	 * We could actually make the func alloc-free if MtmMessageUnpack hadn't
	 * palloced...
	 */
	MemoryContextReset(heartbeat_context);
	oldcontext = MemoryContextSwitchTo(heartbeat_context);

	sscanf(node_name, MTM_DMQNAME_FMT, &node_id);

	parsed_msg = (MtmHeartbeat *) MtmMessageUnpack(msg);
	Assert(parsed_msg->tag == T_MtmHeartbeat);

	/* switch into gossiped generation if it is newer */
	MtmConsiderGenSwitch(parsed_msg->current_gen, parsed_msg->donors);

	/* remember neightbour's last_online_in to guide the campaigner */
	pg_atomic_write_u64(&mtm_state->others_last_online_in[node_id - 1],
						parsed_msg->last_online_in);

	/* finally, update connectivity state */
	LWLockAcquire(mtm_state->connectivity_lock, LW_EXCLUSIVE);

	old_connected_mask = MtmGetConnectedMask(true);
	BIT_SET(mtm_state->dmq_receivers_mask, node_id - 1);
	if (old_connected_mask != MtmGetConnectedMask(true))
		changed = true; /* sender is already fine and receiver just emerged */
	if (mtm_state->connectivity_matrix[node_id - 1] != parsed_msg->connected_mask)
		changed = true; /* neighbour's connectivity mask changed */
	mtm_state->connectivity_matrix[node_id - 1] = parsed_msg->connected_mask;

	LWLockRelease(mtm_state->connectivity_lock);

	if (changed)
		CampaignerWake();

	MemoryContextSwitchTo(oldcontext);
}

/*
 * dmq receiver dies, unset the bit and ping the campaigner -- probably it is
 * time to change the generation.
 */
void
MtmOnDmqReceiverDisconnect(char *node_name)
{
	int			node_id;
	nodemask_t old_connected_mask;
	bool changed = false;
	pid_t walreceiver_pid;

	sscanf(node_name, MTM_DMQNAME_FMT, &node_id);

	LWLockAcquire(mtm_state->connectivity_lock, LW_EXCLUSIVE);

	old_connected_mask = MtmGetConnectedMask(true);
	BIT_CLEAR(mtm_state->dmq_receivers_mask, node_id - 1);
	if (old_connected_mask != MtmGetConnectedMask(true))
		changed = true;
	mtm_state->connectivity_matrix[node_id - 1] = 0;

	LWLockRelease(mtm_state->connectivity_lock);
	if (changed)
		CampaignerWake();

	mtm_log(MtmStateMessage, "[STATE] dmq receiver from node %d disconnected", node_id);

	/*
	 * dmq receiver services heartbeats so it should control the lifetime of
	 * walreceiver: e.g. if packets to the node are silently dropped, dmq
	 * receiver will spot that pretty soon and terminate our walreceiver (who
	 * doesn't even obey wal_receiver_timeout and might hang infinitely or,
	 * more likely, until the kernel gives up sending the feedback ~ 15
	 * minutes by default)
	 *
	 */
	LWLockAcquire(Mtm->lock, LW_SHARED);
	walreceiver_pid = Mtm->peers[node_id - 1].walreceiver_pid;
	LWLockRelease(Mtm->lock);
	if (walreceiver_pid != InvalidPid)
		kill(walreceiver_pid, SIGTERM);

}

void
MtmOnDmqSenderConnect(char *node_name)
{
	int			node_id;
	nodemask_t old_connected_mask;
	bool changed = false;

	sscanf(node_name, MTM_DMQNAME_FMT, &node_id);

	LWLockAcquire(mtm_state->connectivity_lock, LW_EXCLUSIVE);
	old_connected_mask = MtmGetConnectedMask(true);
	BIT_SET(mtm_state->dmq_senders_mask, node_id - 1);
	if (old_connected_mask != MtmGetConnectedMask(true))
		changed = true;
	LWLockRelease(mtm_state->connectivity_lock);

	if (changed)
		CampaignerWake();

	mtm_log(MtmStateMessage, "[STATE] dmq sender to node %d connected", node_id);
}

/* send stuff MtmOnDmqReceiverHeartbeat wants to see */
void MtmOnDmqSenderHeartbeat(char *node_name, StringInfo buf)
{
	MtmHeartbeat msg;
	StringInfo	packed_msg;
	msg.tag = T_MtmHeartbeat;

	LWLockAcquire(mtm_state->gen_lock, LW_SHARED);
	msg.current_gen.num = pg_atomic_read_u64(&mtm_state->current_gen_num);
	msg.current_gen.members = mtm_state->current_gen_members;
	msg.current_gen.configured = mtm_state->current_gen_configured;
	msg.donors = mtm_state->donors;
	msg.last_online_in = mtm_state->last_online_in;
	LWLockRelease(mtm_state->gen_lock);

	LWLockAcquire(mtm_state->connectivity_lock, LW_SHARED);
	msg.connected_mask = MtmGetConnectedMask(true);
	LWLockRelease(mtm_state->connectivity_lock);

	/* again, MtmMessagePack running its own buffer is not too nice here */
	packed_msg = MtmMessagePack((MtmMessage *) &msg);
	appendBinaryStringInfo(buf, packed_msg->data, packed_msg->len);
	pfree(packed_msg->data);
	pfree(packed_msg);
}

void
MtmOnDmqSenderDisconnect(char *node_name)
{
	int			node_id;
	nodemask_t	old_connected_mask;
	bool changed = false;

	sscanf(node_name, MTM_DMQNAME_FMT, &node_id);

	LWLockAcquire(mtm_state->connectivity_lock, LW_EXCLUSIVE);
	old_connected_mask = MtmGetConnectedMask(true);
	BIT_CLEAR(mtm_state->dmq_senders_mask, node_id - 1);
	if (old_connected_mask != MtmGetConnectedMask(true))
		changed = true;
	LWLockRelease(mtm_state->connectivity_lock);

	if (changed)
		CampaignerWake();

	mtm_log(MtmStateMessage, "[STATE] dmq sender to node %d disconnected", node_id);
}

/*
 * Do all nodes from mask see each other? The clique is not neccesarily
 * maximal.
 */
static bool
MtmIsConnectivityClique(nodemask_t mask)
{
	int i, j;
	nodemask_t connected_mask;

	Assert(LWLockHeldByMe(mtm_state->connectivity_lock));
	connected_mask = MtmGetConnectedMask(true);
	for (i = 0; i < MTM_MAX_NODES; i++)
	{
		if (!BIT_CHECK(mask, i))
			continue;
		for (j = 0; j < MTM_MAX_NODES; j++)
		{
			if (i == j)
				continue;
			if (i + 1 == Mtm->my_node_id)
			{
				if (!BIT_CHECK(connected_mask, j))
					return false;
			}
			else
			{
				if (!BIT_CHECK(mtm_state->connectivity_matrix[i], j))
					return false;
			}
		}
	}
	return true;
}

/*
 * The largest subset of nodes where each member sees each other.
 */
static nodemask_t
MtmGetConnectivityClique(bool locked)
{
	nodemask_t	matrix[MTM_MAX_NODES];
	nodemask_t	clique;
	int i;
	int j;
	int clique_size;
	int me = Mtm->my_node_id;

	/* can be called from backends for monitoring purposes, so better check */
	if (me == MtmInvalidNodeId)
		elog(ERROR, "multimaster is not configured");

	LWLockAcquire(mtm_state->connectivity_lock, LW_SHARED);
	memcpy(matrix, mtm_state->connectivity_matrix, sizeof(nodemask_t) * MTM_MAX_NODES);
	matrix[me - 1] = MtmGetConnectedMaskWithMe(true);
	LWLockRelease(mtm_state->connectivity_lock);

	/* make matrix symmetric, required by Bron–Kerbosch algorithm */
	for (i = 0; i < MTM_MAX_NODES; i++)
	{
		for (j = 0; j < i; j++)
		{
			/* act conservatively, leaving edge iff both nodes see each other */
			if (!((matrix[j] >> i) & 1) || !((matrix[i] >> j) & 1))
			{
				BIT_CLEAR(matrix[i], j);
				BIT_CLEAR(matrix[j], i);
			}
		}
		/* and set self-loops along the way, required by Bron-Kerbosch algorithm */
		BIT_SET(matrix[i], i);
	}

	clique = MtmFindMaxClique(matrix, MTM_MAX_NODES, &clique_size);
	/*
	 * BKB requires self loops and we feed it matrix of all node ids, whether
	 * configured or not, so in the absence of at least two nodes clique the
	 * result is always 1, while the cluster could e.g contain the only node
	 * 3. Overwrite it with our node id in this case. A bit ugly.
	 */
	if (popcount(clique) == 1)
	{
		clique = 0;
		BIT_SET(clique, me - 1);
	}
	return clique;
}


/*
 * -----------------------------------
 * Monitoring UDFs
 * -----------------------------------
 */

/*
 * We regard as enabled all nodes who are online in current gen according
 * to our knowledge.
 * TODO: s/enabled/online
 */
nodemask_t
MtmGetEnabledNodeMask(bool locked)
{
	nodemask_t	enabled = 0;
	int	i;
	int me = Mtm->my_node_id;
	MtmGeneration curr_gen;

	if (me == MtmInvalidNodeId)
		elog(ERROR, "multimaster is not configured");

	if (!locked)
		LWLockAcquire(mtm_state->gen_lock, LW_SHARED);

	curr_gen = MtmGetCurrentGen(true);
	for (i = 0; i < MTM_MAX_NODES; i++)
	{
		if (!BIT_CHECK(curr_gen.configured, i))
			continue; /* consider only configured nodes */

		if (Mtm->my_node_id == i + 1)
		{
			if (MtmGetCurrentStatusInGen() == MTM_GEN_ONLINE)
				BIT_SET(enabled, i);
		}
		else
		{
			uint64 loi = pg_atomic_read_u64(&mtm_state->others_last_online_in[i]);
			if (loi == curr_gen.num)
				BIT_SET(enabled, i);
		}
	}

	if (!locked)
		LWLockRelease(mtm_state->gen_lock);

	return enabled;
}

/* Compatibility with scheduler */
nodemask_t
MtmGetDisabledNodeMask()
{
	nodemask_t	enabled;
	MtmGeneration curr_gen;

	LWLockAcquire(mtm_state->gen_lock, LW_SHARED);

	curr_gen = MtmGetCurrentGen(true);
	enabled = MtmGetEnabledNodeMask(true);

	LWLockRelease(mtm_state->gen_lock);

	return curr_gen.configured & (~enabled);
}

/*  XXX: During evaluation of (mtm.node_info(id)).* this function called */
/*  once each columnt for every row. So may be just rewrite to SRF. */
/*  probably worth adding loi, is_member, is_donor generation-related fields */
Datum
mtm_node_info(PG_FUNCTION_ARGS)
{
	int			node_id = PG_GETARG_INT32(0);
	TupleDesc	desc;
	Datum		values[Natts_mtm_node_info];
	bool		nulls[Natts_mtm_node_info] = {false};
	bool		enabled;
	nodemask_t	connected = MtmGetConnectedMaskWithMe(false);

	LWLockAcquire(mtm_state->gen_lock, LW_SHARED);

	if (node_id == Mtm->my_node_id)
		enabled = MtmGetCurrentStatusInGen() == MTM_GEN_ONLINE;
	else
	{
		uint64 loi = pg_atomic_read_u64(&mtm_state->others_last_online_in[node_id - 1]);
		enabled = MtmGetCurrentGenNum() == loi;
	}

	values[Anum_mtm_node_info_enabled - 1] = BoolGetDatum(enabled);
	values[Anum_mtm_node_info_connected - 1] =
		BoolGetDatum(BIT_CHECK(connected, node_id - 1));

	if (Mtm->peers[node_id - 1].walsender_pid != InvalidPid)
	{
		values[Anum_mtm_node_info_sender_pid - 1] =
			Int32GetDatum(Mtm->peers[node_id - 1].walsender_pid);
	}
	else
	{
		nulls[Anum_mtm_node_info_sender_pid - 1] = true;
	}

	if (Mtm->peers[node_id - 1].walreceiver_pid != InvalidPid)
	{
		values[Anum_mtm_node_info_receiver_pid - 1] =
			Int32GetDatum(Mtm->peers[node_id - 1].walreceiver_pid);
		values[Anum_mtm_node_info_n_workers - 1] =
			Int32GetDatum(Mtm->pools[node_id - 1].nWorkers);
		values[Anum_mtm_node_info_receiver_mode - 1] =
			CStringGetTextDatum(MtmReplicationModeMnem[Mtm->peers[node_id - 1].receiver_mode]);
	}
	else
	{
		nulls[Anum_mtm_node_info_receiver_pid - 1] = true;
		nulls[Anum_mtm_node_info_n_workers - 1] = true;
		nulls[Anum_mtm_node_info_receiver_mode - 1] = true;
	}

	LWLockRelease(mtm_state->gen_lock);

	get_call_result_type(fcinfo, NULL, &desc);
	PG_RETURN_DATUM(HeapTupleGetDatum(heap_form_tuple(desc, values, nulls)));
}

/* returns palloc'ed array of node ids in the given mask */
static ArrayType *
MaskToArray(nodemask_t mask)
{
	Datum	   *arrayelems;
	int			narrayelems;
	int			i;

	arrayelems = (Datum *) palloc(MTM_MAX_NODES * sizeof(Datum));
	narrayelems = 0;
	for (i = 0; i < MTM_MAX_NODES; i++)
	{
		if (BIT_CHECK(mask, i))
			arrayelems[narrayelems++] = Int32GetDatum(i + 1);
	}

	/* Construct array, using hardwired knowledge about int4 type */
	return construct_array(arrayelems, narrayelems,
						   INT4OID,
						   sizeof(int32), true, 'i');
}

Datum
mtm_status(PG_FUNCTION_ARGS)
{
	TupleDesc	desc;
	Datum		values[Natts_mtm_status];
	bool		nulls[Natts_mtm_status] = {false};
	MtmGeneration curr_gen;
	nodemask_t	connected = MtmGetConnectedMaskWithMe(false);

	LWLockAcquire(mtm_state->gen_lock, LW_SHARED);
	LWLockAcquire(mtm_state->vote_lock, LW_SHARED);

	values[Anum_mtm_status_node_id - 1] = Int32GetDatum(Mtm->my_node_id);
	values[Anum_mtm_status_status - 1] =
		CStringGetTextDatum(MtmNodeStatusMnem[MtmGetCurrentStatus(true, true)]);
	curr_gen = MtmGetCurrentGen(true);

	values[Anum_mtm_status_connected - 1] =
		PointerGetDatum(MaskToArray(connected));

	values[Anum_mtm_status_gen_num - 1] = UInt64GetDatum(curr_gen.num);
	values[Anum_mtm_status_gen_members - 1] =
		PointerGetDatum(MaskToArray(curr_gen.members));
	values[Anum_mtm_status_gen_members_online - 1] =
		PointerGetDatum(MaskToArray(MtmGetEnabledNodeMask(true)));
	values[Anum_mtm_status_gen_configured - 1] =
		PointerGetDatum(MaskToArray(curr_gen.configured));

	LWLockRelease(mtm_state->vote_lock);
	LWLockRelease(mtm_state->gen_lock);

	get_call_result_type(fcinfo, NULL, &desc);
	PG_RETURN_DATUM(HeapTupleGetDatum(heap_form_tuple(desc, values, nulls)));
}


/*
 * -----------------------------------
 * Prepare barrier
 * -----------------------------------
 */

/* Exclude all holders */
void
AcquirePBByPreparer(void)
{
	Assert(!pb_preparers_incremented);
	for (;;)
	{
		SpinLockAcquire(&mtm_state->cb_lock);
		if (mtm_state->n_prepare_holders == 0)
		{
			mtm_state->n_committers += 1;
			pb_preparers_incremented = true;
		}
		SpinLockRelease(&mtm_state->cb_lock);

		if (pb_preparers_incremented)
			break;

		ConditionVariableSleep(&mtm_state->commit_barrier_cv, PG_WAIT_EXTENSION);
	}
	ConditionVariableCancelSleep();
}

/*
 * Exclude all preparers. Note that there is no protection against multiple
 * concurrent holders, but there must be no need in it.
 */
static void
AcquirePBByHolder(void)
{
	Assert(!pb_holders_incremented);
	/* Holder has the priority, so prevent new committers immediately */
	SpinLockAcquire(&mtm_state->cb_lock);
	mtm_state->n_prepare_holders += 1;
	SpinLockRelease(&mtm_state->cb_lock);

	for (;;)
	{
		SpinLockAcquire(&mtm_state->cb_lock);
		if (mtm_state->n_committers == 0)
			pb_holders_incremented = true;
		SpinLockRelease(&mtm_state->cb_lock);

		if (pb_holders_incremented)
			break;

		ConditionVariableSleep(&mtm_state->commit_barrier_cv, PG_WAIT_EXTENSION);
	}
	ConditionVariableCancelSleep();
}

/* Release prepare barrier. No-op, if not acquired. */
void
ReleasePB(void)
{
	Assert(!(pb_holders_incremented && pb_preparers_incremented));
	if (pb_preparers_incremented)
	{
		SpinLockAcquire(&mtm_state->cb_lock);
		mtm_state->n_committers -= 1;
		SpinLockRelease(&mtm_state->cb_lock);
		ConditionVariableBroadcast(&mtm_state->commit_barrier_cv);
		pb_preparers_incremented = false;
	} else if (pb_holders_incremented)
	{
		SpinLockAcquire(&mtm_state->cb_lock);
		mtm_state->n_prepare_holders -= 1;
		SpinLockRelease(&mtm_state->cb_lock);
		ConditionVariableBroadcast(&mtm_state->commit_barrier_cv);
		pb_holders_incremented = false;
	}
}


/*
 * -----------------------------------
 * State serialization support, mostly borrowed from snapbuild.c.
 * We use plain file, not custom table to avoid messing up with transactions:
 *   1) PREPARE and gen switch must exclude each other and table lookup
 *      from commit.c is definitely not a good idea, so locking must survive
 *      transaction, which makes LWLocks inapplicable. Yes, we already use
 *      non-transactional crutch of spinlock and condvars, but currently it
 *      spans only backends, and probably could be removed altogether.
 *      session-level advisory locks might be an option, but most probably they
 *      themselves can't be acquired without xact and state accesses must be
 *      checked in the view of this.
 *   2) We don't do currently, but we might want to MtmConsiderGenSwitch in
 *      receiver on PREPARE handling. This would require autonomous xact.
 * State is primitive anyway.
 * -----------------------------------
 */

typedef struct MtmStateOnDisk
{
	/* first part of this struct needs to be version independent */

	/* data not covered by checksum */
	uint32		magic;
	pg_crc32c	checksum;

	/* data covered by checksum */
	uint32		version;

	/* version dependent part */
	MtmGeneration current_gen;
	nodemask_t donors;
	uint64 last_online_in;
	MtmGeneration last_vote;
} MtmStateOnDisk;

#define MtmStateOnDiskConstantSize \
	offsetof(MtmStateOnDisk, current_gen)
#define MtmStateOnDiskNotChecksummedSize \
	offsetof(MtmStateOnDisk, version)

#define MTMSTATE_MAGIC 0xC6068767
#define MTMSTATE_VERSION 1

/*
 * Save persistent part of MtmState.
 */
static void
MtmStateSave(void)
{
	MtmStateOnDisk ondisk;
	char		path[] = "pg_mtm/state";
	char		tmppath[] = "pg_mtm/state.tmp";
	int			fd;

	/*
	 * We already updated current gen num in shmem, so backends/receivers
	 * could have noticed it and decided they don't need to switch gen --
	 * thus failing mid the way is not allowed.
	 * Obviously we could work around this by first fsyncing tmp state and
	 * pushing it to shmem afterwards, but this seems like too much fuss for
	 * too little benefit.
	 */
	START_CRIT_SECTION();

	MemSet(&ondisk, '\0', sizeof(MtmStateOnDisk));
	ondisk.magic = MTMSTATE_MAGIC;
	ondisk.version = MTMSTATE_VERSION;

	ondisk.current_gen.num = pg_atomic_read_u64(&mtm_state->current_gen_num);
	ondisk.current_gen.members = mtm_state->current_gen_members;
	ondisk.current_gen.configured = mtm_state->current_gen_configured;
	ondisk.donors = mtm_state->donors;
	ondisk.last_online_in = mtm_state->last_online_in;
	ondisk.last_vote = mtm_state->last_vote;

	INIT_CRC32C(ondisk.checksum);
	COMP_CRC32C(ondisk.checksum,
				((char *) &ondisk) + MtmStateOnDiskNotChecksummedSize,
				sizeof(MtmStateOnDisk) - MtmStateOnDiskNotChecksummedSize);
	FIN_CRC32C(ondisk.checksum);

	mkdir("pg_mtm", S_IRWXU);
	fd = OpenTransientFile(tmppath,
						   O_CREAT | O_TRUNC | O_WRONLY | PG_BINARY);
	if (fd < 0)
		ereport(ERROR,
				(errcode_for_file_access(),
				 errmsg("could not open file \"%s\": %m", tmppath)));

	errno = 0;
	if ((write(fd, &ondisk, sizeof(MtmStateOnDisk))) != sizeof(MtmStateOnDisk))
	{
		int			save_errno = errno;

		CloseTransientFile(fd);

		/* if write didn't set errno, assume problem is no disk space */
		errno = save_errno ? save_errno : ENOSPC;
		ereport(ERROR,
				(errcode_for_file_access(),
				 errmsg("could not write to file \"%s\": %m", tmppath)));
	}

	/*
	 * fsync the file before renaming so that even if we crash after this we
	 * have either a fully valid file or nothing.
	 * kinda paranoia as the whole struct is < 512 bytes
	 */
	if (pg_fsync(fd) != 0)
	{
		int			save_errno = errno;

		CloseTransientFile(fd);
		errno = save_errno;
		ereport(ERROR,
				(errcode_for_file_access(),
				 errmsg("could not fsync file \"%s\": %m", tmppath)));
	}

	if (CloseTransientFile(fd) != 0)
		ereport(ERROR,
				(errcode_for_file_access(),
				 errmsg("could not close file \"%s\": %m", tmppath)));

	fsync_fname("pg_mtm", true);

	if (rename(tmppath, path) != 0)
	{
		ereport(ERROR,
				(errcode_for_file_access(),
				 errmsg("could not rename file \"%s\" to \"%s\": %m",
						tmppath, path)));
	}

	/* make sure we persist */
	fsync_fname(path, false);
	fsync_fname("pg_mtm", true);

	END_CRIT_SECTION();

	/* xxx: this pallocs */
	mtm_log(MtmStateMessage, "saved state: current_gen_num=" UINT64_FORMAT ", current_gen_members=%s, current_gen_configured=%s, donors=%s, last_online_in=" UINT64_FORMAT ", last_vote.num=" UINT64_FORMAT ", last_vote.members=%s",
			pg_atomic_read_u64(&mtm_state->current_gen_num),
			maskToString(mtm_state->current_gen_members),
			maskToString(mtm_state->current_gen_configured),
			maskToString(mtm_state->donors),
			mtm_state->last_online_in,
			mtm_state->last_vote.num,
			maskToString(mtm_state->last_vote.members));
}

/*
 * Load persistent part of MtmState, if it exists. If not, it must mean
 * multimaster is not configured.
 *
 * What to do with errors here? PANIC might be suitable as we can't sanely
 * function without gen state. However, before state is loaded current gen
 * num is MtmInvalidGenNum, so no xacts would be allowed anyway, and having
 * the instance up might be useful for investigating/recovering.
 */
static void
MtmStateLoad(void)
{
	MtmStateOnDisk ondisk;
	char		path[] = "pg_mtm/state";
	int			fd;
	int			readBytes;
	pg_crc32c	checksum;

	fd = OpenTransientFile(path, O_RDONLY | PG_BINARY);

	/* this is called from monitor: at this point serialized state must exist */
	if (fd < 0)
		ereport(ERROR,
				(errcode_for_file_access(),
				 errmsg("could not open mtm state file \"%s\": %m", path)));
	/*
	 * Make sure the data had been stored safely to disk
	 */
	fsync_fname(path, false);
	fsync_fname("pg_mtm", true);


	readBytes = read(fd, &ondisk, MtmStateOnDiskConstantSize);
	if (readBytes != MtmStateOnDiskConstantSize)
	{
		int			save_errno = errno;

		CloseTransientFile(fd);

		if (readBytes < 0)
		{
			errno = save_errno;
			ereport(ERROR,
					(errcode_for_file_access(),
					 errmsg("could not read file \"%s\": %m", path)));
		}
		else
			ereport(ERROR,
					(errcode(ERRCODE_DATA_CORRUPTED),
					 errmsg("could not read file \"%s\": read %d of %zu",
							path, readBytes,
							(Size) MtmStateOnDiskConstantSize)));
	}

	if (ondisk.magic != MTMSTATE_MAGIC)
		ereport(ERROR,
				(errcode(ERRCODE_DATA_CORRUPTED),
				 errmsg("mtm state file \"%s\" has wrong magic number: %u instead of %u",
						path, ondisk.magic, MTMSTATE_MAGIC)));

	if (ondisk.version != MTMSTATE_VERSION)
		ereport(ERROR,
				(errcode(ERRCODE_DATA_CORRUPTED),
				 errmsg("mtm state file \"%s\" has unsupported version: %u instead of %u",
						path, ondisk.version, MTMSTATE_VERSION)));

	readBytes = read(fd, &ondisk.current_gen,
					 sizeof(MtmStateOnDisk) - MtmStateOnDiskConstantSize);

	if (readBytes != (sizeof(MtmStateOnDisk) - MtmStateOnDiskConstantSize))
	{
		int			save_errno = errno;

		CloseTransientFile(fd);

		if (readBytes < 0)
		{
			errno = save_errno;
			ereport(ERROR,
					(errcode_for_file_access(),
					 errmsg("could not read file \"%s\": %m", path)));
		}
		else
			ereport(ERROR,
					(errcode(ERRCODE_DATA_CORRUPTED),
					 errmsg("could not read file \"%s\": read %d of %zu",
							path, readBytes,
							sizeof(MtmStateOnDisk) - MtmStateOnDiskConstantSize)));
	}

	INIT_CRC32C(checksum);
	COMP_CRC32C(checksum,
				((char *) &ondisk) + MtmStateOnDiskNotChecksummedSize,
				sizeof(MtmStateOnDisk) - MtmStateOnDiskNotChecksummedSize);
	FIN_CRC32C(checksum);

	/* verify checksum of what we've read */
	if (!EQ_CRC32C(checksum, ondisk.checksum))
		ereport(ERROR,
				(errcode(ERRCODE_DATA_CORRUPTED),
				 errmsg("checksum mismatch for mtm state file \"%s\": is %u, should be %u",
						path, checksum, ondisk.checksum)));

	if (CloseTransientFile(fd) != 0)
		ereport(ERROR,
				(errcode_for_file_access(),
				 errmsg("could not close file \"%s\": %m", path)));

	pg_atomic_write_u64(&mtm_state->current_gen_num, ondisk.current_gen.num);
	mtm_state->current_gen_members = ondisk.current_gen.members;
	mtm_state->current_gen_configured = ondisk.current_gen.configured;
	mtm_state->donors = ondisk.donors;
	mtm_state->last_online_in = ondisk.last_online_in;
	mtm_state->last_vote = ondisk.last_vote;

	mtm_log(MtmStateMessage, "loaded state: current_gen_num=" UINT64_FORMAT ", current_gen_members=%s, current_gen_configured=%s, donors=%s, last_online_in=" UINT64_FORMAT ", last_vote.num=" UINT64_FORMAT ", last_vote.members=%s",
			pg_atomic_read_u64(&mtm_state->current_gen_num),
			maskToString(mtm_state->current_gen_members),
			maskToString(mtm_state->current_gen_configured),
			maskToString(mtm_state->donors),
			mtm_state->last_online_in,
			mtm_state->last_vote.num,
			maskToString(mtm_state->last_vote.members));
}


/*****************************************************************************
 *
 * Mtm monitor
 *
 *****************************************************************************/


#include "storage/latch.h"
#include "postmaster/bgworker.h"
#include "utils/guc.h"
#include "pgstat.h"

void		MtmMonitor(Datum arg);

bool		MtmIsMonitorWorker;

void
MtmMonitorStart(Oid db_id, Oid user_id)
{
	BackgroundWorker worker;
	BackgroundWorkerHandle *handle;

	MemSet(&worker, 0, sizeof(BackgroundWorker));
	worker.bgw_flags = BGWORKER_SHMEM_ACCESS | BGWORKER_BACKEND_DATABASE_CONNECTION;
	worker.bgw_start_time = BgWorkerStart_ConsistentState;
	worker.bgw_restart_time = 1;
	worker.bgw_main_arg = Int32GetDatum(0);

	memcpy(worker.bgw_extra, &db_id, sizeof(Oid));
	memcpy(worker.bgw_extra + sizeof(Oid), &user_id, sizeof(Oid));

	sprintf(worker.bgw_library_name, "multimaster");
	sprintf(worker.bgw_function_name, "MtmMonitor");
	snprintf(worker.bgw_name, BGW_MAXLEN, "mtm-monitor");

	if (!RegisterDynamicBackgroundWorker(&worker, &handle))
		elog(ERROR, "Failed to start monitor worker");
}

/*
 * Workhorse for handle_1a when PREPARE exists: change its state and fill
 * resp appropriately.
 */
static void
handle_1a_gtx(MtmTxRequest *msg, GlobalTx *gtx, MtmTxStatusResponse *resp)
{
	/*
	 * Check whether paxos 1a handling rules allow us to
	 * process xact.
	 */
	if (term_cmp(msg->term, gtx->state.proposal) > 0)
	{
		bool		done;
		char	   *sstate;
		MemoryContext oldcontext = CurrentMemoryContext;

		sstate = serialize_gtx_state(gtx->state.status,
									 msg->term,
									 gtx->state.accepted);
		StartTransactionCommand();
		done = SetPreparedTransactionState(gtx->gid, sstate,
										   false);
		CommitTransactionCommand();
		MemoryContextSwitchTo(oldcontext);

		if (!done)
			Assert(false);
		gtx->state.proposal = msg->term;
		mtm_log(MtmTxTrace, "TXTRACE: processed 1a, set state %s", GlobalTxToString(gtx));

		resp->state = (GTxState) {
			.proposal = gtx->state.proposal,
			.accepted = gtx->state.accepted,
			.status = gtx->state.status
		};
	}
}

typedef struct {
	char	gid[GIDSIZE];
	MtmTxRequest *req;
	/*
	 * if GetLoggedPreparedXactState can find the outcome, it writes it
	 * directly to resp->state.status
	 */
	MtmTxStatusResponse resp;
	bool	prepare_seen;
	int		dest_node_id; /* for logging */
	int		dest_id;
} txset_entry;

/* Reply to 1A of paxos */
static void handle_1a(txset_entry *txse, HTAB *txset, bool *wal_scanned,
					  uint64 *last_online_in)
{
	MtmTxRequest *msg = txse->req;
	MtmTxStatusResponse *resp = &txse->resp; /* shorter lines */
	GlobalTx   *gtx;
	StringInfo	packed_msg;

	/* fast check before digging WAL */
	gtx = GlobalTxAcquire(msg->gid, false);
	if (gtx != NULL)
	{
		handle_1a_gtx(msg, gtx, resp);
		GlobalTxRelease(gtx);
		goto reply_1a;
	}

	/*
	 * We don't have PREPARED xact; time to dig in WAL, probably we've already
	 * committed|aborted it. But before doing this remember last_online_in to
	 * reply with direct ABORT for transactions which can't ever be committed
	 * -- this prevents recovery deadlocks as explained below
	 */
	if (!(*wal_scanned))
	{
		LWLockAcquire(mtm_state->gen_lock, LW_SHARED);
		*last_online_in = mtm_state->last_online_in;
		LWLockRelease(mtm_state->gen_lock);

		GetLoggedPreparedXactState(txset);
		*wal_scanned = true;
	}

	if (resp->state.status != GTXCommitted &&
		resp->state.status != GTXAborted)
	{
		uint64 xact_gen_num;

		/*
		 * So there is no gtx and no CP|AP. Probably we've never seen xact yet
		 * (prepare not applied), or we could have already finished (or
		 * perhaps skipped, if this is abort) it and purged WAL. We can't just
		 * ignore request in this case because resolving and recovery might
		 * thus deadlock each other, at least with >= 5 nodes. e.g.
		 * - 1 prepares T1 on 1, 2, 3
		 * - 4 prepares conflicting T2 on 4, 5
		 * - Everyone fails, and only 2, 3, 5 go up. They can't recover
		 *   without resolving at least one of xacts first because T1 and T2
		 *   conflict; and 'my orphaned xact which never got PRECOMMITted can
		 *   be aborted directly' doesn't help here as no T1 neigher T2
		 *   authored by live nodes.
		 *
		 * To mitigate this, previously infrastructure was developed to vote
		 * for xacts before getting PREPARE (state was stored in special local
		 * table). That worked, but was very cumbersome and ugly, especially
		 * in the face of batch WAL scan. Here is a better idea: we can use
		 * generations to abort one of xacts conflicting in the example above.
		 * Namely, we can reply with direct abort iff
		 * 1) such P can't appear anymore (we are online in later generation)
		 * 2) we don't have CP|AP in WAL (if there had been CP previously,
		 *    we must have already streamed it and thus our answer doesn't
		 *    matter)
		 * 3) P doesn't exist currently and there is no P in WAL
		 *
		 * Informal liveness rationale, i.e. why all xacts will be eventually
		 * resolved in this way: assume we have live majority. It will elect
		 * generation n containing all its members. Why any generation <n xact
		 * will be eventually resolved? See, one of the nodes will be donor in
		 * n. It has all committable <n xacts. Any other (conflicting or not)
		 * <n xact will be aborted by this donor. Any other node in this
		 * majority will eventually get all these committable xacts, 1) as
		 * said above, all other <n xacts are aborted 2) xacts of n can't be
		 * prepared before getting all prepares of n. After that, resolving
		 * proceeds normally as majority has PREPAREs.
		 */

		/* xact obviously could appear after fast path check */
		gtx = GlobalTxAcquire(msg->gid, false);
		if (gtx != NULL)
		{
			handle_1a_gtx(msg, gtx, resp);
			GlobalTxRelease(gtx);
			goto reply_1a;
		}

		xact_gen_num = MtmGidParseGenNum(msg->gid);
		/*
		 * prepare_seen check is needed as xact could have been quickly
		 * finished after WAL reading but before GlobalTxAcquire above. We
		 * could re-scan WAL again in this case, but it is simpler to just
		 * ignore it currently and resolve on the next request.
		 */
		if (!txse->prepare_seen &&
			*last_online_in > xact_gen_num)
		{
			resp->state.status = GTXAborted;
			mtm_log(StatusRequest, "replying directly abort %s as my last_online_in=" UINT64_FORMAT ", xact gen_num=" UINT64_FORMAT " and there is no data for it",
					msg->gid,
					*last_online_in,
					xact_gen_num);
		}
		else
			mtm_log(StatusRequest, "can't participate in xact %s resolution: there is no PREPARE but it might appear in the future",
					msg->gid);
	}

reply_1a:
	packed_msg = MtmMessagePack((MtmMessage *) resp);
	mtm_log(StatusRequest, "replying to 1a from node %d with %s",
			txse->dest_node_id,
			MtmMesageToString((MtmMessage *) resp));
	dmq_push_buffer(txse->dest_id, "txresp", packed_msg->data, packed_msg->len);
}

/* process and send response for each 1a message in the accumulated batch */
static void
handle_1a_batch(HTAB *txset)
{
	HASH_SEQ_STATUS txset_seq;
	txset_entry *txse;
	/*
	 * scan wal lazily, only on first request needing this. e.g. in referee
	 * mode this is never needed.
	 */
	bool wal_scanned = false;
	uint64		last_online_in;

	if (hash_get_num_entries(txset) == 0)
		return; /* prevent dummy logging */
	mtm_log(StatusRequest, "got batch of %ld 1a messages to process",
				hash_get_num_entries(txset));
	hash_seq_init(&txset_seq, txset);
	while ((txse = hash_seq_search(&txset_seq)) != NULL)
	{
		handle_1a(txse, txset, &wal_scanned, &last_online_in);
	}
}

/* reply to 2a of paxos */
static void
handle_2a(MtmTxRequest *msg, int dest_id, int dest_node_id)
{
	GlobalTx   *gtx;
	StringInfo	packed_msg;
	/*
	 * Similarly to handle_1a, always reply something.
	 */
	Mtm2AResponse resp = (Mtm2AResponse) {
		T_Mtm2AResponse,
		Mtm->my_node_id,
		GTXInvalid,
		InvalidGTxTerm,
		ERRCODE_SUCCESSFUL_COMPLETION,
		"",
		msg->gid
	};

	gtx = GlobalTxAcquire(msg->gid, false);
	/*
	 * finalized statuses are sent on 1a, no need to bother with this here
	 */
	if (!gtx)
		goto reply_2a;

	/*
	 * Some parts of various papers use proposal term here, some
	 * accepted, some strong equality -- either variant is fine.
	 */
	if (term_cmp(msg->term, gtx->state.proposal) >= 0)
	{
		GlobalTxStatus new_status;
		bool		done;
		char	   *sstate;
		MemoryContext oldcontext = CurrentMemoryContext;

		new_status = msg->type == MTReq_Precommit ?
			GTXPreCommitted : GTXPreAborted;


		sstate = serialize_gtx_state(new_status,
									 msg->term,
									 msg->term);
		StartTransactionCommand();
		done = SetPreparedTransactionState(gtx->gid, sstate,
										   false);
		if (!done)
			Assert(false);
		CommitTransactionCommand();
		/* transaction knocked down old ctx*/
		MemoryContextSwitchTo(oldcontext);

		gtx->state.proposal = msg->term;
		gtx->state.accepted = msg->term;
		gtx->state.status = new_status;
		mtm_log(MtmTxTrace, "TXTRACE: processed 2a, set state %s", GlobalTxToString(gtx));

		resp.status = gtx->state.status;
		resp.accepted_term = msg->term;
	}
	GlobalTxRelease(gtx);

reply_2a:
	mtm_log(StatusRequest, "replying to 2a from node %d with %s",
			dest_node_id,
			MtmMesageToString((MtmMessage *) &resp));
	packed_msg = MtmMessagePack((MtmMessage *) &resp);
	dmq_push_buffer(dest_id, "txresp", packed_msg->data,
					packed_msg->len);

}

static void
check_status_requests(MtmConfig *mtm_cfg, bool *job_pending)
{
	int8 sender_mask_pos;
	StringInfoData packed_msg;
	bool		wait;
	HASHCTL		txset_hash_ctl;
	HTAB	   *txset;
	/*
	 * this hopefully will resolve all xacts of node who returned after being
	 * offline for a long time in one wal scan
	 */
	int			max_batch_size = mtm_cfg->n_nodes * MaxBackends + 100;
	int			n_received_msgs = 0;

	/*
	 * Initial resolution requests (1a message) handling often requires to
	 * scan WAL to learn status of already finished xact. This might be very
	 * slow e.g. if some node was offline for a long time and others
	 * accumulated a lot of WAL for it. To alleviate (we'd better have
	 * specificially tailored gid->outcome map, but ENOINFRASTRUCTURE) this,
	 * read batch of 1a requests and process (search WAL for needed entries)
	 * them at once. Other messages are processed immediately.
	 *
	 * XXX possibility of blocking generation election by long WAL scan still
	 * doesn't look appealing. It would be probably better to split tx
	 * resolution responder to yet another process.
	 */
	MemSet(&txset_hash_ctl, 0, sizeof(txset_hash_ctl));
	txset_hash_ctl.keysize = GIDSIZE;
	txset_hash_ctl.entrysize = sizeof(txset_entry);
	txset_hash_ctl.hcxt = CurrentMemoryContext;
	txset = hash_create("txset", max_batch_size, &txset_hash_ctl,
						HASH_ELEM | HASH_CONTEXT);

	while (dmq_pop_nb(&sender_mask_pos, &packed_msg, MtmGetConnectedMask(false), &wait))
	{
		MtmMessage *raw_msg = MtmMessageUnpack(&packed_msg);
		int			sender_node_id;
		int			dest_id;

		sender_node_id = sender_mask_pos + 1;
		LWLockAcquire(Mtm->lock, LW_SHARED);
		dest_id = Mtm->peers[sender_node_id - 1].dmq_dest_id;
		LWLockRelease(Mtm->lock);
		Assert(dest_id >= 0);

		if (raw_msg->tag == T_MtmTxRequest)
		{
			MtmTxRequest *msg = (MtmTxRequest *) raw_msg;

			mtm_log(StatusRequest, "got '%s' from %d",
					MtmMesageToString(raw_msg), sender_node_id);

			if (msg->type == MTReq_Status) /* 1a */
			{
				txset_entry *txse;

				txse = hash_search(txset, msg->gid, HASH_ENTER, NULL);
				txse->req = msg;
				/*
				 * We always respond something -- it will be useful if we ever
				 * switch to synchronous resolving instead of that 'resolve
				 * during 3 seconds or I'll start a new ballot'. If by Paxos
				 * rules we must ignore the message, we reply with invalid
				 * terms and status -- resolver won't regard it as ack.
				 */
				txse->resp = (MtmTxStatusResponse)
				{
					.tag = T_MtmTxStatusResponse,
					.node_id = Mtm->my_node_id,
					.state = {
						.proposal = InvalidGTxTerm,
						.accepted = InvalidGTxTerm,
						.status = GTXInvalid
					},
					.gid = msg->gid
				};
				txse->prepare_seen = false;
				txse->dest_node_id = sender_node_id;
				txse->dest_id = dest_id;
			}
			else if (msg->type == MTReq_Preabort || /* 2a */
					 msg->type == MTReq_Precommit)
			{
				handle_2a(msg, dest_id, sender_node_id);
			}
			else /* commit|abort */
			{
				GlobalTx   *gtx;
				MemoryContext oldcontext = CurrentMemoryContext;

				Assert(msg->type == MTReq_Abort || msg->type == MTReq_Commit);

				gtx = GlobalTxAcquire(msg->gid, false);
				if (!gtx)
					goto got_message;

				StartTransactionCommand();
				FinishPreparedTransaction(gtx->gid,
										  msg->type == MTReq_Commit,
										  false);
				mtm_log(MtmTxFinish, "TXFINISH: %s %s via MTReq", msg->gid,
					msg->type == MTReq_Commit ? "committed" : "aborted");
				CommitTransactionCommand();
				MemoryContextSwitchTo(oldcontext);

				gtx->state.status = msg->type == MTReq_Commit ?
					GTXCommitted : GTXAborted;
				GlobalTxRelease(gtx);
			}
		}
		else if (raw_msg->tag == T_MtmGenVoteRequest)
		{
			HandleGenVoteRequest(mtm_cfg, (MtmGenVoteRequest *) raw_msg,
								 sender_node_id, dest_id);
		}
		else
		{
			Assert(false);
		}

got_message:
		n_received_msgs++;
		/*
		 * prevent potential infinite spinning in this function; monitor has
		 * other jobs to do
		 */
		if (n_received_msgs >= max_batch_size)
		{
			*job_pending = true; /* probably there are more messages */
			break;
		}
	}
	/*
	 * if we ended loop not because of WOULDBLOCK but due to one of
	 * counterparties failing, get back here without waiting on latch
	 */
	if (sender_mask_pos != -1)
		*job_pending = true;

	handle_1a_batch(txset);
}


static bool
slot_exists(char *name)
{
	int			i;
	bool		exists = false;

	LWLockAcquire(ReplicationSlotControlLock, LW_SHARED);
	for (i = 0; i < max_replication_slots; i++)
	{
		ReplicationSlot *s = &ReplicationSlotCtl->replication_slots[i];

		if (s->in_use && strcmp(name, NameStr(s->data.name)) == 0)
		{
			exists = true;
			break;
		}
	}
	LWLockRelease(ReplicationSlotControlLock);

	return exists;
}

static bool
is_basebackuped(MtmConfig *mtm_cfg)
{
	int			i;
	int			n_missing_slots = 0;

	StartTransactionCommand();
	for (i = 0; i < mtm_cfg->n_nodes; i++)
	{
		char	   *slot_name = psprintf(MULTIMASTER_SLOT_PATTERN,
										 mtm_cfg->nodes[i].node_id);

		if (mtm_cfg->nodes[i].init_done && !slot_exists(slot_name))
			n_missing_slots++;
	}
	CommitTransactionCommand();

	if (n_missing_slots == 0)
		return false;
	else if (n_missing_slots == mtm_cfg->n_nodes)
		return true;
	else
		mtm_log(ERROR, "Missing %d replication slots out of %d",
				n_missing_slots, mtm_cfg->n_nodes);
}

static void
start_node_workers(int node_id, MtmConfig *new_cfg, Datum arg)
{
	BackgroundWorkerHandle **receivers = (BackgroundWorkerHandle **) arg;
	LogicalDecodingContext *ctx;
	DmqDestinationId dest;
	char	   *dmq_connstr,
			   *slot,
			   *filter_slot,
			   *dmq_my_name,
			   *dmq_node_name;
	MemoryContext old_context;

	/*
	 * Transaction is needed for logical slot and replication origin creation.
	 * Also it clean ups psprintfs.
	 */
	StartTransactionCommand();

	dmq_connstr = psprintf("%s application_name=%s",
						   MtmNodeById(new_cfg, node_id)->conninfo,
						   MULTIMASTER_BROADCAST_SERVICE);
	slot = psprintf(MULTIMASTER_SLOT_PATTERN, node_id);
	filter_slot = psprintf(MULTIMASTER_FILTER_SLOT_PATTERN, node_id);
	dmq_my_name = psprintf(MTM_DMQNAME_FMT, new_cfg->my_node_id);
	dmq_node_name = psprintf(MTM_DMQNAME_FMT, node_id);

	if (MtmNodeById(new_cfg, node_id)->init_done)
	{
		if (!slot_exists(filter_slot))
			mtm_log(ERROR, "can't find recovery slot for node%d", node_id);

		if (!slot_exists(slot))
			mtm_log(ERROR, "can't find replication slot for node%d", node_id);
	}

	if (!MtmNodeById(new_cfg, node_id)->init_done)
	{
		/*
		 * Create filter slot to filter out already applied changes since the
		 * last syncpoint during replication start
		 */
		ReplicationSlotCreate(filter_slot, false, RS_PERSISTENT);
		ReplicationSlotReserveWal();
		/* Write this slot to disk */
		ReplicationSlotMarkDirty();
		ReplicationSlotSave();
		ReplicationSlotRelease();
	}

	/* Add dmq destination */
	dest = dmq_destination_add(dmq_connstr, dmq_my_name, dmq_node_name,
							   node_id - 1, MtmHeartbeatRecvTimeout);

	LWLockAcquire(Mtm->lock, LW_EXCLUSIVE);
	Mtm->peers[node_id - 1].dmq_dest_id = dest;
	LWLockRelease(Mtm->lock);

	/* Attach receiver so we can collect tx requests */
	dmq_attach_receiver(dmq_node_name, node_id - 1);

	/*
	 * Finally start receiver. bgw handle should be allocated in TopMcxt.
	 *
	 * Start receiver before logical slot creation, as during start after a
	 * basebackup logical stot creation will wait for all in-progress
	 * transactions to finish (including prepared ones). And to finish them we
	 * need to start receiver.
	 */
	old_context = MemoryContextSwitchTo(TopMemoryContext);
	receivers[node_id - 1] = MtmStartReceiver(node_id, MyDatabaseId,
											  GetUserId(), MyProcPid);
	MemoryContextSwitchTo(old_context);

	if (!MtmNodeById(new_cfg, node_id)->init_done)
	{
		char	   *query;
		int			rc;

		/* Create logical slot for our publication to this neighbour */
		ReplicationSlotCreate(slot, true, RS_EPHEMERAL);
		ctx = CreateInitDecodingContext(MULTIMASTER_NAME, NIL,
										false,	/* do not build snapshot */
										logical_read_local_xlog_page, NULL, NULL,
										NULL);
		DecodingContextFindStartpoint(ctx);
		FreeDecodingContext(ctx);
		ReplicationSlotPersist();
		ReplicationSlotRelease();

		/*
		 * Mark this node as init_done, so at next boot we won't try to create
		 * slots again.
		 */
		if (SPI_connect() != SPI_OK_CONNECT)
			mtm_log(ERROR, "could not connect using SPI");
		PushActiveSnapshot(GetTransactionSnapshot());

		query = psprintf("insert into mtm.nodes_init_done values (%d, true) ",
						 node_id);
		rc = SPI_execute(query, false, 0);
		if (rc < 0 || rc != SPI_OK_INSERT)
			mtm_log(ERROR, "failed to insert in mtm.nodes_init_done node%d", node_id);

		if (SPI_finish() != SPI_OK_FINISH)
			mtm_log(ERROR, "could not finish SPI");
		PopActiveSnapshot();
	}

	CommitTransactionCommand();

	mtm_log(NodeMgmt, "started workers for node %d", node_id);
}

static void
stop_node_workers(int node_id, MtmConfig *new_cfg, Datum arg)
{
	BackgroundWorkerHandle **receivers = (BackgroundWorkerHandle **) arg;
	char	   *dmq_name;
	char	   *logical_slot;
	char	   *filter_slot_name;

	Assert(!IsTransactionState());

	mtm_log(LOG, "dropping node %d", node_id);

	StartTransactionCommand();

	dmq_name = psprintf(MTM_DMQNAME_FMT, node_id);
	logical_slot = psprintf(MULTIMASTER_SLOT_PATTERN, node_id);
	filter_slot_name = psprintf(MULTIMASTER_FILTER_SLOT_PATTERN, node_id);

	/* detach incoming queues from this node */
	dmq_detach_receiver(dmq_name);

	/*
	 * Disable this node by terminating receiver. It shouldn't came back
	 * online as dmq-receiver check node_id presense in mtm.cluster_nodes.
	 */
	dmq_terminate_receiver(dmq_name);

	/* do not try to connect this node by dmq */
	dmq_destination_drop(dmq_name);

	LWLockAcquire(Mtm->lock, LW_EXCLUSIVE);
	Mtm->peers[node_id - 1].dmq_dest_id = -1;
	LWLockRelease(Mtm->lock);

	/*
	 * Stop corresponding receiver. Also await for termination, so that we can
	 * drop slots and origins that were acquired by receiver.
	 */
	TerminateBackgroundWorker(receivers[node_id - 1]);
	WaitForBackgroundWorkerShutdown(receivers[node_id - 1]);
	pfree(receivers[node_id - 1]);
	receivers[node_id - 1] = NULL;

	/* delete recovery slot, was acquired by receiver */
	ReplicationSlotDrop(filter_slot_name, true);

	/* delete replication origin, was acquired by receiver */
	replorigin_drop(replorigin_by_name(logical_slot, false), true);

	/*
	 * Delete logical slot. It is aquired by walsender, so call with nowait =
	 * false and wait for walsender exit.
	 */
	ReplicationSlotDrop(logical_slot, false);

	CommitTransactionCommand();

	mtm_log(NodeMgmt, "stopped workers for node %d", node_id);
}

static void
pubsub_change_cb(Datum arg, int cacheid, uint32 hashvalue)
{
	config_valid = false;
}

void
MtmMonitor(Datum arg)
{
	Oid			db_id,
				user_id;
	MtmConfig  *mtm_cfg = NULL;
	BackgroundWorkerHandle *receivers[MTM_MAX_NODES];
	BackgroundWorkerHandle *resolver = NULL;
	BackgroundWorkerHandle *campaigner = NULL;
	MemoryContext mon_loop_ctx = AllocSetContextCreate(TopMemoryContext,
													   "MonitorContext",
													   ALLOCSET_DEFAULT_SIZES);
	bool	job_pending;
	TimestampTz last_start_time = 0;

	memset(receivers, '\0', MTM_MAX_NODES * sizeof(BackgroundWorkerHandle *));

	pqsignal(SIGTERM, die);
	pqsignal(SIGHUP, PostgresSigHupHandler);

	MtmBackgroundWorker = true;
	MtmIsMonitorWorker = true;

	/* We're now ready to receive signals */
	BackgroundWorkerUnblockSignals();

	/* Connect to a database */
	memcpy(&db_id, MyBgworkerEntry->bgw_extra, sizeof(Oid));
	memcpy(&user_id, MyBgworkerEntry->bgw_extra + sizeof(Oid), sizeof(Oid));
	BackgroundWorkerInitializeConnectionByOid(db_id, user_id, 0);

	/*
	 * Online upgrade.
	 */
	{
		int			rc;

		StartTransactionCommand();
		if (SPI_connect() != SPI_OK_CONNECT)
			mtm_log(ERROR, "could not connect using SPI");
		PushActiveSnapshot(GetTransactionSnapshot());

		/* Add new column to mtm.syncpoints */
		rc = SPI_execute("select relnatts from pg_class where relname='syncpoints';",
						 true, 0);
		if (rc < 0 || rc != SPI_OK_SELECT)
			mtm_log(ERROR, "Failed to find syncpoints relation");
		if (SPI_processed > 0)
		{
			TupleDesc	tupdesc = SPI_tuptable->tupdesc;
			HeapTuple	tup = SPI_tuptable->vals[0];
			bool		isnull;
			int			relnatts;

			relnatts = DatumGetInt32(SPI_getbinval(tup, tupdesc, 1, &isnull));
			if (relnatts == 3)
			{
				rc = SPI_execute("ALTER TABLE mtm.syncpoints ADD COLUMN restart_lsn bigint DEFAULT 0 NOT NULL",
								 false, 0);
				if (rc < 0 || rc != SPI_OK_UTILITY)
					mtm_log(ERROR, "Failed to alter syncpoints relation");

				mtm_log(LOG, "Altering syncpoints to newer schema");
			}
		}

		/* set_temp_schema */
		rc = SPI_execute("select proname from pg_proc where proname='set_temp_schema';",
						 true, 0);
		if (rc < 0 || rc != SPI_OK_SELECT)
			mtm_log(ERROR, "Failed to query pg_proc");
		if (SPI_processed == 0)
		{
			rc = SPI_execute("CREATE FUNCTION mtm.set_temp_schema(nsp text) RETURNS void "
							 "AS '$libdir/multimaster','mtm_set_temp_schema' "
							 "LANGUAGE C; ", false, 0);
			if (rc < 0 || rc != SPI_OK_UTILITY)
				mtm_log(ERROR, "Failed to create mtm.set_temp_schema()");

			mtm_log(LOG, "Creating mtm.set_temp_schema()");
		}

		SPI_finish();
		PopActiveSnapshot();
		CommitTransactionCommand();
	}

	/*
	 * During mtm_init_cluster() our worker is started from transaction that
	 * created mtm config, so we can get here before this transaction is
	 * committed, so we won't see config yet. Just wait for it to became
	 * visible.
	 */
	mtm_cfg = MtmLoadConfig();
	while (mtm_cfg->n_nodes == 0)
	{
		pfree(mtm_cfg);
		MtmSleep(USECS_PER_SEC);
		mtm_cfg = MtmLoadConfig();
	}
	/* check config sanity */
	if (mtm_cfg->my_node_id == MtmInvalidNodeId)
		elog(ERROR, "multimaster is not configured");
	if (mtm_cfg->n_nodes > 1 && IS_REFEREE_ENABLED())
		ereport(ERROR,
				(errmsg("referee mode supports only 2 nodes, but %d configured",
					mtm_cfg->n_nodes + 1),
				 errhint("Unset multimaster.referee_connstring config value or reinitialize cluster with 2 nodes")));

	/*
	 * XXX to handle reinits gracefully, before (re)initting mtm we should
	 * kill monitor, who should on exit wait for all bgws deaths. Thus bgws
	 * would safely assume that my_node_id is set and constant without locks.
	 */
	Mtm->my_node_id = mtm_cfg->my_node_id;

	/* now that we know our node id, restore generation state */
	MtmStateStartup();

	StartTransactionCommand();
	GlobalTxLoadAll();
	CommitTransactionCommand();

	/*
	 * Ok, we are starting from a basebackup. Delete neighbors from
	 * mtm.cluster_nodes so we don't start receivers using wrong my_node_id.
	 * mtm.join_cluster() should create proper info in mtm.cluster_nodes.
	 */
	if (is_basebackuped(mtm_cfg))
	{
		int			rc;

		mtm_log(LOG, "basebackup detected");

		/*
		 * We are not really the member of last_online_in gen inherited from
		 * donor (others may commit without waiting for us), so reset it.
		 */
		LWLockAcquire(mtm_state->gen_lock, LW_EXCLUSIVE);
		mtm_state->last_online_in = MtmInvalidGenNum;
		MtmStateSave();
		LWLockRelease(mtm_state->gen_lock);

		StartTransactionCommand();
		if (SPI_connect() != SPI_OK_CONNECT)
			mtm_log(ERROR, "could not connect using SPI");
		PushActiveSnapshot(GetTransactionSnapshot());

		rc = SPI_execute("select pg_replication_origin_drop(name) from "
						 "(select 'mtm_slot_' || id as name from " MTM_NODES " where is_self = 'f') names;",
						 false, 0);
		if (rc < 0 || rc != SPI_OK_SELECT)
			mtm_log(ERROR, "Failed to clean up replication origins after a basebackup");

		rc = SPI_execute("delete from " MTM_NODES, false, 0);
		if (rc < 0 || rc != SPI_OK_DELETE)
			mtm_log(ERROR, "Failed to clean up nodes after a basebackup");

		rc = SPI_execute("delete from mtm.nodes_init_done" , false, 0);
		if (rc < 0 || rc != SPI_OK_DELETE)
			mtm_log(ERROR, "Failed to clean up nodes_init_done after a basebackup");

		SPI_finish();
		PopActiveSnapshot();
		CommitTransactionCommand();

		proc_exit(0);
	}

	/*
	 * Reset mtm_cfg, as it need to be NULL during first call of
	 * MtmReloadConfig to properly fire on_node_create callbacks.
	 */
	pfree(mtm_cfg);
	mtm_cfg = NULL;

	/*
	 * Keep us informed about subscription changes, so we can react on node
	 * addition or deletion.
	 */
	CacheRegisterSyscacheCallback(SUBSCRIPTIONNAME,
								  pubsub_change_cb,
								  (Datum) 0);

	/*
	 * Keep us informed about publication changes. This is used to stop mtm
	 * after our node was dropped.
	 */
	CacheRegisterSyscacheCallback(PUBLICATIONNAME,
								  pubsub_change_cb,
								  (Datum) 0);

	dmq_stream_subscribe("mon"); /* use single stream for xact and gen reqs */

	/* Launch resolver */
	Assert(resolver == NULL);
	resolver = ResolverStart(db_id, user_id);
	campaigner = CampaignerStart(db_id, user_id);
	mtm_log(MtmStateMessage, "MtmMonitor started");

	for (;;)
	{
		int			rc;
		int			i;
		pid_t		pid;
		long		wait_time = 2000; /* milliseconds */
		TimestampTz	now;
		bool		restart_workers;
		long		next_restart_after = 0;

		job_pending = false;
		CHECK_FOR_INTERRUPTS();

		if (ConfigReloadPending)
		{
			ConfigReloadPending = false;
			ProcessConfigFile(PGC_SIGHUP);
		}

		/* check wheter we need to update config */
		AcceptInvalidationMessages();
		if (!config_valid)
		{
			mtm_cfg = MtmReloadConfig(mtm_cfg, start_node_workers,
									  stop_node_workers, (Datum) receivers);

			/* we were excluded from cluster */
			if (mtm_cfg->my_node_id == MtmInvalidNodeId)
			{
				int			i;
				int			rc;

				for (i = 0; i < MTM_MAX_NODES; i++)
				{
					if (receivers[i] != NULL)
						stop_node_workers(i + 1, NULL, (Datum) receivers);
				}
				TerminateBackgroundWorker(resolver);
				TerminateBackgroundWorker(campaigner);

				Mtm->my_node_id = MtmInvalidNodeId;

				StartTransactionCommand();
				if (SPI_connect() != SPI_OK_CONNECT)
					mtm_log(ERROR, "could not connect using SPI");
				PushActiveSnapshot(GetTransactionSnapshot());

				rc = SPI_execute("delete from " MTM_NODES, false, 0);
				if (rc < 0 || rc != SPI_OK_DELETE)
					mtm_log(ERROR, "Failed delete nodes");

				if (SPI_finish() != SPI_OK_FINISH)
					mtm_log(ERROR, "could not finish SPI");
				PopActiveSnapshot();
				CommitTransactionCommand();

				/* XXX: kill myself somehow? */
				proc_exit(0);
			}

			config_valid = true;
		}

		/*
		 * Check and restart resolver, campaigner and receivers if they were
		 * stopped by any error.
		 */
		now = GetCurrentTimestamp();
		/*
		 * Limit the start retry to once a wal_retrieve_retry_interval. Usage
		 * of this GUC is slightly weird, but we follow vanilla LR here.
		 *
		 * We are using single last_start_time for all workers; thus restart
		 * may grab worker for which the interval hasn't passed yet along with
		 * one for which it did happen, making restarts possibly more frequent
		 * than wal_retrieve_retry_interval. OTOH, if immediately after worker
		 * 1 start worker 2 fails, we would need to wait for the interval to
		 * pass before restarting it. That's all shouldn't matter much.
		 *
		 * TODO: these 3 copies of identic code ask for unification.
		 */
		restart_workers = TimestampDifferenceExceeds(last_start_time, now,
													 wal_retrieve_retry_interval);
		if (!restart_workers)
			next_restart_after = last_start_time / 1000 + wal_retrieve_retry_interval - now / 1000;
		if (GetBackgroundWorkerPid(resolver, &pid) == BGWH_STOPPED)
		{
			if (restart_workers)
			{
				mtm_log(MtmStateMessage, "resolver is dead, restarting it");
				pfree(resolver);
				resolver = ResolverStart(db_id, user_id);
				last_start_time = now;
			}
			/*
			 * if we should restart but the interval hasn't passed yet, wake
			 * up when it does
			 */
			else
			{
				wait_time = Min(wait_time, next_restart_after);
			}
		}
		if (GetBackgroundWorkerPid(campaigner, &pid) == BGWH_STOPPED)
		{
			if (restart_workers)
			{
				mtm_log(MtmStateMessage, "campaigner is dead, restarting it");
				pfree(campaigner);
				campaigner = CampaignerStart(db_id, user_id);
				last_start_time = now;
			}
			else
			{
				wait_time = Min(wait_time, next_restart_after);
			}
		}

		for (i = 0; i < MTM_MAX_NODES; i++)
		{
			if (receivers[i] == NULL)
				continue;

			if (GetBackgroundWorkerPid(receivers[i], &pid) == BGWH_STOPPED)
			{
				if (restart_workers ||
					/*
					 * Restart worker immediately if his last reported mode is
					 * not the one he should be currently working in; e.g. we
					 * don't want to have wait interval in recovery->normal
					 * transition.
					 */
					Mtm->peers[i].receiver_mode != MtmGetReceiverMode(i + 1))
				{
					mtm_log(MtmStateMessage,
							"receiver for node %d is dead, restarting it", i + 1);
					/* Receiver has finished by some kind of mistake. Start it. */
					pfree(receivers[i]);
					receivers[i] = MtmStartReceiver(i + 1, MyDatabaseId,
													GetUserId(), MyProcPid);
					last_start_time = now;
				}
				else
				{
					wait_time = Min(wait_time, next_restart_after);
				}
			}
		}

		/* reset once per monitor loop, mainly for messages pack/unpack */
		MemoryContextReset(mon_loop_ctx);
		MemoryContextSwitchTo(mon_loop_ctx);
		check_status_requests(mtm_cfg, &job_pending);
		MemoryContextSwitchTo(TopMemoryContext);

		if (!job_pending)
		{
			rc = WaitLatch(MyLatch,
						   WL_LATCH_SET | WL_TIMEOUT | WL_POSTMASTER_DEATH,
						   wait_time, PG_WAIT_EXTENSION);

			/* Emergency bailout if postmaster has died */
			if (rc & WL_POSTMASTER_DEATH)
				proc_exit(1);

			if (rc & WL_LATCH_SET)
				ResetLatch(MyLatch);
		}
	}
}

/*
 * Lsn can point to the end of the record, which is not necessarily the
 * beginning of the next record, if the previous record happens to end at
 * a page boundary. Skip over the page header in that case to find the next
 * record.
 */
static void
lsn_bump(XLogRecPtr *lsn)
{
	if (*lsn != InvalidXLogRecPtr && *lsn % XLOG_BLCKSZ == 0)
	{
		if (XLogSegmentOffset(*lsn, wal_segment_size) == 0)
			*lsn += SizeOfXLogLongPHD;
		else
			*lsn += SizeOfXLogShortPHD;
	}
}

/* ars: this is horrible */
static void
GetLoggedPreparedXactState(HTAB *txset)
{
	XLogRecord *record;
	XLogReaderState *xlogreader;
	char *errormsg;
	XLogRecPtr start_lsn;
	XLogRecPtr lsn;
	TimeLineID timeline;
	XLogRecPtr end_wal_lsn = GetFlushRecPtr();
	XLogRecPtr end_lsn = end_wal_lsn;
	int		   n_trans = hash_get_num_entries(txset);

	GetOldestRestartPoint(&start_lsn, &timeline);
	if (start_lsn != InvalidXLogRecPtr)
	{
		MemoryContext memctx = CurrentMemoryContext;
		xlogreader = XLogReaderAllocate(wal_segment_size, &read_local_xlog_page, NULL);
		if (!xlogreader)
			ereport(ERROR,
					(errcode(ERRCODE_OUT_OF_MEMORY),
					 errmsg("out of memory"),
					 errdetail("Failed while allocating a WAL reading processor.")));

		PG_TRY();
		{
			/*
			 * If checkpoint interval is large enough it may be more efficient
			 * to start scanning from last WAL segment
			 */
			XLogSegNoOffsetToRecPtr(end_lsn / wal_segment_size, 0, wal_segment_size, lsn);
			lsn_bump(&lsn);
			lsn = XLogFindNextRecord(xlogreader, lsn);
			if (lsn != InvalidXLogRecPtr && lsn > start_lsn)
				start_lsn = lsn;

			/* loop over segments */
			while (start_lsn != InvalidXLogRecPtr)
			{
				lsn = start_lsn;
				/* loop over records in the segment */
				do
				{
					lsn_bump(&lsn);
					record = XLogReadRecord(xlogreader, lsn, &errormsg);
					if (record == NULL)
						break;
					lsn = InvalidXLogRecPtr; /* continue after the record */
					if (XLogRecGetRmid(xlogreader) == RM_XACT_ID)
					{
						uint32 info = XLogRecGetInfo(xlogreader);
						bool	found;
						txset_entry *txse;

						switch (info & XLOG_XACT_OPMASK)
						{
						case XLOG_XACT_PREPARE:
						{
							TwoPhaseFileHeader *hdr = (TwoPhaseFileHeader *)XLogRecGetData(xlogreader);
							char *xact_gid = (char *)hdr + MAXALIGN(sizeof(TwoPhaseFileHeader));

							txse = hash_search(txset, xact_gid, HASH_FIND, &found);
							if (found)
							{
								Assert(TransactionIdIsValid(hdr->xid));
								txse->prepare_seen = true;
								/*
								 * we could also pick up hdr->state_3pc here
								 * if we wanted, but we don't
								 */
							}
							break;
						}
						case XLOG_XACT_COMMIT_PREPARED:
						{
							xl_xact_commit *xlrec;
							xl_xact_parsed_commit parsed;

							xlrec = (xl_xact_commit *)XLogRecGetData(xlogreader);
							ParseCommitRecord(info, xlrec, &parsed);
							txse = hash_search(txset, parsed.twophase_gid, HASH_FIND, &found);
							if (found)
							{
								Assert(TransactionIdIsValid(parsed.twophase_xid));
								txse->resp.state.status = GTXCommitted;
								n_trans--;
							}
							break;
						}

						case XLOG_XACT_ABORT_PREPARED:
						{
							xl_xact_abort *xlrec;
							xl_xact_parsed_abort parsed;

							xlrec = (xl_xact_abort *)XLogRecGetData(xlogreader);
							ParseAbortRecord(info, xlrec, &parsed);
							txse = hash_search(txset, parsed.twophase_gid, HASH_FIND, &found);
							if (found)
							{
								Assert(TransactionIdIsValid(parsed.twophase_xid));
								txse->resp.state.status = GTXAborted;
								n_trans--;
							}
							break;
						}
						default:
							break;
						}
					}
				} while (n_trans > 0 && xlogreader->EndRecPtr < end_lsn);

				if (n_trans == 0)
					break;

				end_lsn = start_lsn;
				/* Get LSN of first record in the current segment */
				XLogSegNoOffsetToRecPtr(end_lsn / wal_segment_size, 0, wal_segment_size, start_lsn);
				lsn_bump(&start_lsn);
				start_lsn = XLogFindNextRecord(xlogreader, start_lsn);
				/*
				 * If we didn't start from the beginning of segment, then restart
				 * scan from the beginning of segment
				 */
				if (start_lsn == end_lsn)
				{
					/* ... otherwise check if it is not the first segment */
					if (end_lsn <= wal_segment_size * 2)
						break;
					/* ... and if not: shift to previous segment */
					XLogSegNoOffsetToRecPtr(end_lsn / wal_segment_size - 1, 0, wal_segment_size, start_lsn);
					/* ... and check that pending segment is actually exists */
					if (start_lsn / wal_segment_size <= XLogGetLastRemovedSegno())
						break;
					lsn_bump(&start_lsn);
					start_lsn = XLogFindNextRecord(xlogreader, start_lsn);
				}
			}
		}
		PG_CATCH();
		{
			/* Catch access to unexisted WAL segment */
			/*
			 * XXX ars: we need to understand better how we end up
			 * here. Generally this function must not silently eat errors: if
			 * it failed to determine the xact outcome, we expect that the
			 * outcome indeed doesn't exist, or status responder will
			 * misbehave. However, if this is a failure to read recycled WAL,
			 * and the whole WAL up to the flush point on the moment of scan
			 * start has already been either scanned or recycled that's fine.
			 *
			 * I tried to make it safer by ignoring the result in case of
			 * error (peers resolvers retry their requests), but that once led
			 * to 2 minutes of continious errors (during which xacts to be
			 * resolved hanged of course), after which they suddently
			 * disappeared -- and I'm pretty sure slots weren't advanced
			 * during that period.
			 */
			EmitErrorReport();
			FlushErrorState();
		}
		PG_END_TRY();
		MemoryContextSwitchTo(memctx);
		XLogReaderFree(xlogreader);
	}
	return;
}

/* SQL wrapper for debugging/testing/investigating */
Datum
mtm_get_logged_prepared_xact_state(PG_FUNCTION_ARGS)
{
	char	   *gid = text_to_cstring(PG_GETARG_TEXT_P(0));
	char	   *res;
	HASHCTL		txset_hash_ctl;
	HTAB	   *txset;
	txset_entry *txse;

	MemSet(&txset_hash_ctl, 0, sizeof(txset_hash_ctl));
	txset_hash_ctl.keysize = GIDSIZE;
	txset_hash_ctl.entrysize = sizeof(txset_entry);
	txset_hash_ctl.hcxt = CurrentMemoryContext;
	txset = hash_create("txset", 1, &txset_hash_ctl,
						HASH_ELEM | HASH_CONTEXT);

	txse = hash_search(txset, gid, HASH_ENTER, NULL);
	txse->resp.state.status = GTXInvalid;
	txse->prepare_seen = false;
	GetLoggedPreparedXactState(txset);

	if (txse->resp.state.status == GTXCommitted)
		res = "committed";
	else if (txse->resp.state.status == GTXAborted)
		res = "aborted";
	else
	{
		Assert(txse->resp.state.status == GTXInvalid);
		res = "notfound";
	}
	PG_RETURN_TEXT_P(cstring_to_text(res));
}
