#ifndef STATE_H
#define STATE_H

/*
 * Generation is a uniquely numbered subset of configured nodes allowed to
 * commit transactions. Each xact is stamped with generation it belongs
 * to. Transaction must be PREPAREd on *all* generation members before commit;
 * this provides recovery -> normal work transition without risk of reordering
 * xacts.
 *
 * The two main properties of generations are
 *   - At each node all prepares of generation n who might ever be committed
 *     lie strictly before all such prepares of generation n+1.
 *   - Node which is MTM_GEN_ONLINE in generation n holds all committable
 *     xacts of all generations < n.
 * See generations2.md and MtmGenerations.tla for details.
 *
 * Normal (making xacts) generation contains at least majority
 * members. However, we allow to elect generation with less members as a sort
 * of mark that its members are recovered enough to be included in the
 * following normal generations. It allows nodes always add *only myself* (but
 * remove anyone else) when campaigning for new generations; thus only node
 * itself decides when it is recovered enough to force others wait for it,
 * which simplifies reasoning who should be next gen members.
 */
typedef struct MtmGeneration
{
	uint64 num; /* logical clock aka term number aka ballot */
	uint64 members; /* xxx extract nodemask.h and use it here */
	/*
	 * Generation has fixed set of configured nodes, which helps consistent
	 * xact resolving with dynamic add/rm of nodes.
	 */
	uint64 configured; /* xxx extract nodemask.h and use it here */
} MtmGeneration;

#define MtmInvalidGenNum 0
#define EQUAL_GENS(g1, g2) \
	((g1).num == (g2).num && (g1).members == (g2).members && (g1).configured == (g2).configured)
/*
 * Referee is enabled only with 2 nodes and single member gen is ever proposed
 * as referee one (requiring referee vote and allowing to be online this
 * single node), so instead of separate flag use this check.
 *
 * First condition is important as single node cluster shouldn't access
 * referee; also, with > 2 nodes there is at least theoretical possibility of
 * electing single-node generation after two consecutive minority gen
 * elections.
 */
#define IS_REFEREE_GEN(members, configured) \
	(popcount(configured) == 2 && popcount(members) == 1)

typedef enum
{
	MTM_GEN_DEAD,		/* can't ever be online in this gen */
	MTM_GEN_RECOVERY,	/* need to pull in recovery latest xacts before */
						/* starting making my own and receiving normally */
	MTM_GEN_ONLINE		/* participating normally */
} MtmStatusInGen;

typedef enum
{
	/*
	 * We were not excluded to the best of our knowledge, but we don't see all
	 * peers from current generation, so commits will likely fail.
	 */
	MTM_ISOLATED,

	/*
	 * We were excluded and definitely need recovery, but not yet sure from
	 * whom as we don't see majority.
	 */
	MTM_DISABLED,

	/*
	 * We are catching up, eating changes committed without us participating.
	 * Other nodes don't wait for us yet, so this doesn't freeze the cluster.
	 */
	MTM_CATCHUP,

	/*
	 * Generation with us was elected and others started waiting for us, but
	 * we need to eat the latest changes in recovery mode to participate
	 * normally.
	 */
	MTM_RECOVERY,

	/*
	 * It's Twelve O'clock and All's Well.
	 */
	MTM_ONLINE,

	MTM_RECOVERED /* REMOVEME */
} MtmNodeStatus;

extern char const *const MtmNodeStatusMnem[];

extern void MtmStateInit(void);
extern void MtmStateShmemStartup(void);
extern void MtmStateStartup(void);

/* generation management */
extern uint64 MtmGetCurrentGenNum(void);
extern MtmGeneration MtmGetCurrentGen(bool locked);
extern void MtmConsiderGenSwitch(MtmGeneration gen, nodemask_t donors);
extern bool MtmHandleParallelSafe(MtmGeneration ps_gen, nodemask_t ps_donors,
								  bool is_recovery, XLogRecPtr end_lsn);
extern MtmStatusInGen MtmGetCurrentStatusInGen(void);
extern MtmStatusInGen MtmGetCurrentStatusInGenNotLocked(void);
extern MtmNodeStatus MtmGetCurrentStatus(bool gen_locked, bool vote_locked);

/* receiver bits */
extern void MtmReportReceiverCaughtup(int node_id);
/* we should recover, but not not sure from whom yet */
#define RECEIVE_MODE_DISABLED (~(uint32)0)
/* all receivers work normally */
#define RECEIVE_MODE_NORMAL   0
#define IS_RECEIVE_MODE_DONOR(rcv_mode) ((rcv_mode) != RECEIVE_MODE_NORMAL && \
										 ((rcv_mode) != RECEIVE_MODE_DISABLED))
extern MtmReplicationMode MtmGetReceiverMode(int nodeId);

/* connectivity */
extern nodemask_t MtmGetConnectedMask(bool locked);
extern nodemask_t MtmGetConnectedMaskWithMe(bool locked);
extern void *MtmOnDmqReceiverConnect(char *node_name);
extern void MtmOnDmqReceiverHeartbeat(char *node_name, StringInfo msg, void *extra);
extern void MtmOnDmqReceiverDisconnect(char *node_name);
extern void MtmOnDmqSenderConnect(char *node_name);
extern void MtmOnDmqSenderHeartbeat(char *node_name, StringInfo buf);
extern void MtmOnDmqSenderDisconnect(char *node_name);

extern void AcquirePBByPreparer(void);
extern void ReleasePB(void);

extern void MtmMonitorStart(Oid db_id, Oid user_id);

/* not cleaned up yet */
extern void MtmRefreshClusterStatus(void);
extern nodemask_t MtmGetDisabledNodeMask(void);
extern nodemask_t MtmGetEnabledNodeMask(bool ignore_disabled);

#endif
