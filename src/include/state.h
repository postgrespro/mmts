#ifndef STATE_H
#define STATE_H

typedef enum
{
	MTM_NEIGHBOR_CLIQUE_DISABLE,
	MTM_NEIGHBOR_WAL_RECEIVER_START,
	MTM_NEIGHBOR_WAL_RECEIVER_ERROR,
	MTM_NEIGHBOR_WAL_SENDER_START_RECOVERY,
	MTM_NEIGHBOR_WAL_SENDER_START_RECOVERED,
	MTM_NEIGHBOR_RECOVERY_CAUGHTUP,
	MTM_NEIGHBOR_WAL_SENDER_STOP
} MtmNeighborEvent;

typedef enum
{
	MTM_REMOTE_DISABLE,
	MTM_CLIQUE_DISABLE,
	MTM_CLIQUE_MINORITY,
	MTM_ARBITER_RECEIVER_START,
	MTM_RECOVERY_START1,
	MTM_RECOVERY_START2,
	MTM_RECOVERY_FINISH1,
	MTM_RECOVERY_FINISH2,
	MTM_NONRECOVERABLE_ERROR
} MtmEvent;

typedef enum
{
	MTM_DISABLED,				/* Node disabled */
	MTM_RECOVERY,				/* Node is in recovery process */
	MTM_RECOVERED,				/* Node is recovered by is not yet switched to
								 * ONLINE because not all sender/receivers are
								 * restarted */
	MTM_ONLINE					/* Ready to receive client's queries */
} MtmNodeStatus;

extern char const *const MtmNodeStatusMnem[];

extern void MtmStateInit(void);
extern void MtmStateShmemStartup(void);
extern void MtmStateLoad(MtmConfig *cfg);

extern void MtmMonitorStart(Oid db_id, Oid user_id);

extern void MtmStateProcessNeighborEvent(int node_id, MtmNeighborEvent ev, bool locked);
extern void MtmStateProcessEvent(MtmEvent ev, bool locked);

extern void MtmOnNodeDisconnect(char *node_name);
extern void MtmOnNodeConnect(char *node_name);
extern void MtmOnDmqSenderConnect(char *node_name);
extern void MtmOnDmqSenderDisconnect(char *node_name);

extern void MtmRefreshClusterStatus(void);

extern int countZeroBits(nodemask_t mask, int nNodes);

extern MtmReplicationMode MtmGetReplicationMode(int nodeId);

extern MtmNodeStatus MtmGetCurrentStatus(void);
extern nodemask_t MtmGetDisabledNodeMask(void);
extern nodemask_t MtmGetConnectedNodeMask(void);
extern nodemask_t MtmGetEnabledNodeMask(void);
extern int MtmGetRecoveryCount(void);
extern int MtmGetNodeDisableCount(int node_id);

extern nodemask_t MtmGetDisabledNodeMask(void);

#endif
