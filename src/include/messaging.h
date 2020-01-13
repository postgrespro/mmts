
/*****************************************************************************
 *
 * Messaging
 *
 *****************************************************************************/
#ifndef MESSAGING_H
#define MESSAGING_H

#include "global_tx.h"

/*
 * All messages are stamped with MtmMessageTag that should came before the rest
 * of the message. That is used upon receival as typecasting criterion.
 */
typedef enum
{
	T_MtmTxResponse = 0,
	T_MtmTxRequest,
	T_MtmTxStatusResponse,
	T_MtmLastTermRequest,
	T_MtmLastTermResponse
} MtmMessageTag;

typedef struct
{
	MtmMessageTag		tag;
} MtmMessage;

// char const *const MtmMessageTagMnem[] =
// {
// 	"MtmTxResponse",
// 	"MtmTxRequest",
// 	"MtmTxStatusResponse",
// 	"MtmLastTermRequest",
// 	"MtmLastTermResponse"
// };

extern char const *const MtmMessageTagMnem[];

#define messageTag(msgptr)		(((const MtmMessage *)(msgptr))->tag)

/*
 * Responses upon transaction action execution at receiver side.
 * Can be sent from apply worker to originating backend in a failure-free case
 * or from mtm-status worker to mtm-resolver during transaction resolution
 * process.
 */
typedef struct
{
	MtmMessageTag		tag;
	int					node_id;
	GlobalTxStatus		status;
	GlobalTxTerm		term;
	int32				errcode;
	const char		   *errmsg;
	const char		   *gid;
} MtmTxResponse;

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
} MtmTxRequest;

// char const * const MtmTxRequestValueMnem[] = {
// 	"MTReq_Abort",
// 	"MTReq_Commit",
// 	"MTReq_Precommit",
// 	"MTReq_Preabort",
// 	"MTReq_Status"
// };

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

StringInfo MtmMessagePack(MtmMessage *anymsg);
MtmMessage *MtmMessageUnpack(StringInfo s);
char *MtmMesageToString(MtmMessage *anymsg);

#endif							/* MESSAGING_H */
