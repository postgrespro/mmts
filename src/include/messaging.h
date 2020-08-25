
/*****************************************************************************
 *
 * Messaging
 *
 *****************************************************************************/
#ifndef MESSAGING_H
#define MESSAGING_H

#include "global_tx.h"
#include "state.h"

/*
 * All messages are stamped with MtmMessageTag that should came before the rest
 * of the message. That is used upon receival as typecasting criterion.
 */
typedef enum
{
	T_MtmPrepareResponse = 0,
	T_Mtm2AResponse,
	T_MtmTxRequest,
	T_MtmTxStatusResponse,
	T_MtmHeartbeat,
	T_MtmGenVoteRequest,
	T_MtmGenVoteResponse
} MtmMessageTag;

typedef struct MtmMessage
{
	MtmMessageTag		tag;
} MtmMessage;

#define messageTag(msgptr)		(((const MtmMessage *)(msgptr))->tag)

/* Response to PREPARE by apply worker */
typedef struct
{
	MtmMessageTag		tag;
	int					node_id;
	/* for PREPARE we care only about, well, prepare success */
	bool				prepared;
	int32				errcode;
	const char		   *errmsg;
	TransactionId	   	xid; /* identifies the message */
} MtmPrepareResponse;

/*
 * Response to 2A msg by apply worker or by replier (during resolving).
 * This could be named just 2B, ha.
 * It is also abused for COMMIT PREPARED ack (with .status = GTXCommitted).
 */
typedef struct
{
	MtmMessageTag		tag;
	int					node_id;
	/*
	 * Our prevVote in terms of the Part-Time Parliament paper. Actually there
	 * is no need to carry the decree (status) itself, ballot (term) is
	 * enough, but it is kept for convenience.
	 */
	GlobalTxStatus		status;
	GlobalTxTerm		accepted_term;
	int32				errcode;
	const char		   *errmsg;
	const char		   *gid; /* identifies the message */
} Mtm2AResponse;

/*
 * Response on MtmLastTermRequest request, holds last proposal value.
 */
typedef struct
{
	MtmMessageTag		tag;
	GlobalTxTerm		term;
} MtmLastTermResponse;

/*
 * Request to change transaction state. This messages are duplicate of
 * corresponding WAL records, but we need them during transaction resolution
 * upon recovery as WAL receiver may be blocked by a transaction that we
 * are actually resolving.
 *
 * Sent from mtm-resolver to mtm-status worker.
 */
typedef enum
{
	MTReq_Abort = 0,
	MTReq_Commit,
	MTReq_Precommit,  /* 2a with value commit */
	MTReq_Preabort,   /* 2a with value abort */
	MTReq_Status	  /* 1a */
} MtmTxRequestValue;

typedef struct
{
	MtmMessageTag		tag;
	MtmTxRequestValue	type;
	GlobalTxTerm		term;
	const char		   *gid;
	XLogRecPtr			coordinator_end_lsn; /* matters for 1a */
} MtmTxRequest;

extern char const * const MtmTxRequestValueMnem[];

/*
 * Status response, phase 1b of paxos on a given transaction result.
 * Sent from mtm-status to mtm-resolver worker.
 */
typedef struct
{
	MtmMessageTag		tag;
	int					node_id;
	GTxState			state;
	const char		   *gid;
} MtmTxStatusResponse;

/*
 * Data sent in dmq heartbeats.
 */
typedef struct
{
	MtmMessageTag		tag;
	MtmGeneration		current_gen;
	uint64				donors; /* xxx nodemask_t */
	uint64				last_online_in;
	uint64				connected_mask; /* xxx nodemask_t */
} MtmHeartbeat;

/*
 * Request to vote for new generation.
 */
typedef struct
{
	MtmMessageTag		tag;
	MtmGeneration		gen;
} MtmGenVoteRequest;

/*
 * Reply to new generation vote request.
 */
typedef struct
{
	MtmMessageTag		tag;
	uint64				gen_num; /* identifies the message */
	uint8				vote_ok;
	/* last_online_in of replier on the moment of voting, determines donors */
	uint64				last_online_in;
	/*
	 * if vote_ok is false this might be a valid gen number showing that
	 * replier couldn't vote because its last_vote is higher.
	 */
	uint64				last_vote_num;
} MtmGenVoteResponse;


StringInfo MtmMessagePack(MtmMessage *anymsg);
MtmMessage *MtmMessageUnpack(StringInfo s);
char *MtmMesageToString(MtmMessage *anymsg);

#endif							/* MESSAGING_H */
