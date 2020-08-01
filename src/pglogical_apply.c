#include <unistd.h>
#include "postgres.h"

#include "funcapi.h"
#include "libpq-fe.h"
#include "miscadmin.h"
#include "pgstat.h"

#include "access/htup_details.h"
#include "access/relscan.h"
#include "access/xact.h"
#include "access/clog.h"
#include "access/tuptoaster.h"

#include "catalog/catversion.h"
#include "catalog/dependency.h"
#include "catalog/index.h"
#include "catalog/heap.h"
#include "catalog/namespace.h"
#include "catalog/pg_subscription.h"
#include "catalog/pg_type.h"

#include "executor/spi.h"
#include "commands/vacuum.h"
#include "commands/tablecmds.h"
#include "commands/tablespace.h"
#include "commands/defrem.h"
#include "commands/sequence.h"
#include "parser/parse_utilcmd.h"

#include "libpq/pqformat.h"

#include "mb/pg_wchar.h"

#include "parser/parse_type.h"
#include "parser/parse_relation.h"

#include "replication/message.h"
#include "replication/logical.h"
#include "replication/origin.h"

#include "storage/ipc.h"
#include "storage/lmgr.h"
#include "storage/lwlock.h"
#include "storage/bufmgr.h"
#include "storage/proc.h"

#include "tcop/pquery.h"
#include "tcop/tcopprot.h"
#include "tcop/utility.h"

#include "utils/array.h"
#include "utils/datum.h"
#include "utils/tqual.h"
#include "utils/builtins.h"
#include "utils/datetime.h"
#include "utils/lsyscache.h"
#include "utils/memutils.h"
#include "utils/snapmgr.h"
#include "utils/syscache.h"
#include "utils/inval.h"

#include "multimaster.h"
#include "ddd.h"
#include "pglogical_relid_map.h"
#include "spill.h"
#include "state.h"
#include "logger.h"
#include "ddl.h"
#include "receiver.h"
#include "syncpoint.h"
#include "commit.h"
#include "global_tx.h"
#include "messaging.h"

typedef struct TupleData
{
	Datum		values[MaxTupleAttributeNumber];
	bool		isnull[MaxTupleAttributeNumber];
	bool		changed[MaxTupleAttributeNumber];
} TupleData;

static bool query_cancel_allowed;

static Relation read_rel(StringInfo s, LOCKMODE mode);
static void read_tuple_parts(StringInfo s, Relation rel, TupleData *tup);
static EState *create_rel_estate(Relation rel);
static void process_remote_begin(StringInfo s,
								 MtmReceiverWorkerContext *rwctx);
static bool process_remote_message(StringInfo s,
								   MtmReceiverWorkerContext *rwctx);
static void process_remote_commit(StringInfo s,
								  MtmReceiverWorkerContext *rwctx);
static void process_remote_insert(StringInfo s, Relation rel);
static void process_remote_update(StringInfo s, Relation rel);
static void process_remote_delete(StringInfo s, Relation rel);

#if 0
/*
 * Handle critical errors while applying transaction at replica.
 * Such errors should cause shutdown of this cluster node to allow other nodes to continue serving client requests.
 * Other error will be just reported and ignored
 *
 * XXX this kind of artificial intelligence is not used now.
 */
static void
MtmHandleApplyError(void)
{
	ErrorData  *edata = CopyErrorData();

	switch (edata->sqlerrcode)
	{
		case ERRCODE_DISK_FULL:
		case ERRCODE_INSUFFICIENT_RESOURCES:
		case ERRCODE_IO_ERROR:
		case ERRCODE_DATA_CORRUPTED:
		case ERRCODE_INDEX_CORRUPTED:

			/*
			 * Should we really treate this errors as fatal? case
			 * ERRCODE_SYSTEM_ERROR: case ERRCODE_INTERNAL_ERROR: case
			 * ERRCODE_OUT_OF_MEMORY:
			 */
			((void) 42);
	}
	FreeErrorData(edata);
}
#endif

/*
 * Handler of receiver worker for SIGINT and SIGHUP signals
 */
void
ApplyCancelHandler(SIGNAL_ARGS)
{
	int			save_errno = errno;

	/*
	 * Don't joggle the elbow of proc_exit
	 */
	if (!proc_exit_inprogress && query_cancel_allowed)
	{
		InterruptPending = true;
		QueryCancelPending = true;
	}

	/* If we're still here, waken anything waiting on the process latch */
	SetLatch(MyLatch);

	errno = save_errno;
}

static EState *
create_rel_estate(Relation rel)
{
	EState	   *estate;
	ResultRelInfo *resultRelInfo;
	RangeTblEntry *rte;

	estate = CreateExecutorState();

	resultRelInfo = makeNode(ResultRelInfo);
	InitResultRelInfo(resultRelInfo, rel, 1, NULL, 0);

	estate->es_result_relations = resultRelInfo;
	estate->es_num_result_relations = 1;
	estate->es_result_relation_info = resultRelInfo;
	estate->es_output_cid = GetCurrentCommandId(true);

	/* Triggers might need a slot */
	if (resultRelInfo->ri_TrigDesc)
		estate->es_trig_tuple_slot = ExecInitExtraTupleSlot(estate, NULL);

	rte = makeNode(RangeTblEntry);
	rte->rtekind = RTE_RELATION;
	rte->relid = RelationGetRelid(rel);
	rte->relkind = rel->rd_rel->relkind;
	estate->es_range_table = list_make1(rte);

	/* Prepare to catch AFTER triggers. */
	AfterTriggerBeginQuery();

	return estate;
}

static void
process_remote_begin(StringInfo s, MtmReceiverWorkerContext *rwctx)
{
	/* there is no need to send this, but since we do, check its sanity */
	int sender_node_id = pq_getmsgint(s, 4);
	Assert(rwctx->sender_node_id == sender_node_id);
	rwctx->origin_xid = pq_getmsgint64(s);

	InterruptPending = false;
	QueryCancelPending = false;
	query_cancel_allowed = true;

	pq_getmsgint64(s);
//XXX:snapshot
		pq_getmsgint64(s);
//XXX:participantsMask

	SetCurrentStatementStartTimestamp();

	StartTransactionCommand();

	/* ddd */
	rwctx->my_xid = GetCurrentTransactionId();
	// MtmDeadlockDetectorAddXact(rwctx->my_xid, gtid);

	suppress_internal_consistency_checks = true;
}

static void
process_syncpoint(MtmReceiverWorkerContext *rwctx, const char *msg, XLogRecPtr received_end_lsn)
{
	int			rc;
	int			origin_node;
	XLogRecPtr	origin_lsn;
	XLogRecPtr	receiver_lsn;

	Assert(MtmIsReceiver && !MtmIsPoolWorker);

	mtm_log(SyncpointApply, "syncpoint: await for parallel workers to finish");
	/*
	 * Await for our pool workers to finish what they are currently doing.
	 */
	rwctx->txlist_pos = txl_store(&BGW_POOL_BY_NODE_ID(rwctx->sender_node_id)->txlist,
								  2);
	txl_wait_sphead(&BGW_POOL_BY_NODE_ID(rwctx->sender_node_id)->txlist,
					rwctx->txlist_pos);

	/*
	 * Postgres decoding API doesn't disclose origin info about logical
	 * messages, so we have to work around it. Any receiver of original
	 * message writes it in slighly different format (prefixed with 'F' and
	 * origin info) so the readers of forwarded messages can distinguish them
	 * from original messages and set proper node_id and origin_lsn.
	 *
	 * (note that origin info still of course exists in the message and
	 * decoding core uses it for filtering, so we don't get non-sender sps
	 * during normal working)
	 */
	if (msg[0] == 'F')
	{
		Assert(rwctx->mode == REPLMODE_RECOVERY);

		/* forwarded, parse and save as is */
		rc = sscanf(msg, "F_%d_%" INT64_MODIFIER "X",
					&origin_node, &origin_lsn);
		if (rc != 2)
		{
			Assert(false);
			mtm_log(PANIC, "unexpected forwarded syncpoint message format");
		}

		/* skip our own syncpoints */
		if (origin_node == Mtm->my_node_id)
			return;

		/*
		 * TODO: log syncpoint, but then we must 1) filter it out in
		 * MtmFilterTransaction to prevent loops. Recovery loops are hardly
		 * possible, but the danger nevertheless exists. 2) configure
		 * proper origin session.
		 */
		receiver_lsn = GetXLogInsertRecPtr();
	}
	else
	{
		char	   *new_msg;

		/* direct, save with it lsn and 'F' prefix */
		origin_lsn = received_end_lsn;
		origin_node = rwctx->sender_node_id;
		Assert(origin_node != Mtm->my_node_id);

		new_msg = psprintf("F_%d_%" INT64_MODIFIER "X",
						   origin_node, origin_lsn);
		receiver_lsn = LogLogicalMessage("S", new_msg, strlen(new_msg) + 1, false);
		pfree(new_msg);
	}

	Assert(rwctx->mode == REPLMODE_RECOVERY ||
		   rwctx->sender_node_id == origin_node);

	/*
	 * wear the hat of right filter slot if we are in recovery and thus
	 * pulling syncpoints of various origins
	 */
	if (rwctx->mode == REPLMODE_RECOVERY)
	{
		ReplicationSlotAcquire(psprintf(MULTIMASTER_FILTER_SLOT_PATTERN,
										origin_node),
							   true);
	}
	/*
	 * And note that info at which LSN we have processed the syncpoint is
	 * *our* change which should be broadcast to all nodes, so reset
	 * replorigin session.
	 */
	MtmEndSession(42, false);
	SyncpointRegister(origin_node, origin_lsn, receiver_lsn);
	if (rwctx->mode == REPLMODE_RECOVERY)
	{
		ReplicationSlotRelease();
		/*
		 * if we are in recovery, ping non-donor receivers that they might
		 * succeed in advancing the slot.
		 */
		ConditionVariableBroadcast(&Mtm->receivers_cv);
	}
}

/* TODO: make messaging layer for logical messages like existing dmq one */
static void
UnpackGenAndDonors(char const *msg, int len, MtmGeneration *gen, nodemask_t *donors)
{
	StringInfoData s;

	gen->num = MtmInvalidGenNum;
	s.data = (char *) msg;
	s.len = len;
	s.cursor = 0;

	gen->num = pq_getmsgint64(&s);
	gen->members = pq_getmsgint64(&s);
	gen->configured = pq_getmsgint64(&s);
	*donors = pq_getmsgint64(&s);
	Assert(gen->num != MtmInvalidGenNum);
}

/*  XXX: process messages that should run in receiver itself in separate */
/*  function in receiver */
static bool
process_remote_message(StringInfo s, MtmReceiverWorkerContext *rwctx)
{
	char		action = pq_getmsgbyte(s);
	XLogRecPtr	record_lsn = pq_getmsgint64(s);
	int			messageSize = pq_getmsgint(s, 4);
	char const *messageBody = pq_getmsgbytes(s, messageSize);
	bool		standalone = false;

	/* XXX: during recovery this is wrong, as we receive forwarded txes */
	MtmBeginSession(rwctx->sender_node_id);

	switch (action)
	{
		case 'C':
			{
				char *activity = psprintf("non-tx ddl %s", messageBody);
				pgstat_report_activity(STATE_RUNNING, activity);
				pfree(activity);

				mtm_log(MtmApplyMessage, "Executing non-tx DDL message %s", messageBody);
				SetCurrentStatementStartTimestamp();
				StartTransactionCommand();
				MtmApplyDDLMessage(messageBody, false);
				CommitTransactionCommand();

				pgstat_report_activity(STATE_RUNNING, NULL);
				standalone = true;
				break;
			}
		case 'D':
			{
				char *activity = psprintf("tx ddl %s", messageBody);
				pgstat_report_activity(STATE_RUNNING, activity);
				pfree(activity);
				mtm_log(MtmApplyMessage, "Executing tx DDL message %s", messageBody);
				MtmApplyDDLMessage(messageBody, true);
				pgstat_report_activity(STATE_RUNNING, NULL);
				break;
			}
		case 'L':
			{
				/*
				 * XXX: new syncpoints machinery can block receiver, so that
				 * we
				 */
				/*
				 * won't be able to process deadlock messages. If all nodes
				 * are doing
				 */
				/*
				 * syncpoint simultaneously and deadlock happens exactly in
				 * this time
				 */
				/*
				 * we will not be able to resolve it. Proper solution is to
				 * move DDD
				 */
				/* messages to dmq. */
				// mtm_log(MtmApplyMessage, "Executing deadlock message from %d",
				//		rwctx->sender_node_id);
				// MtmUpdateLockGraph(MtmReplicationNodeId, messageBody, messageSize);
				standalone = true;
				break;
			}
		case 'P':
			{
				MtmGeneration ps_gen;
				nodemask_t ps_donors;
				bool reconnect;

				UnpackGenAndDonors(messageBody, messageSize, &ps_gen, &ps_donors);
				mtm_log(MtmApplyMessage, "Processing parallel-safe message from %d: gen.num=" UINT64_FORMAT,
						rwctx->sender_node_id, ps_gen.num);
				reconnect = MtmHandleParallelSafe(ps_gen, ps_donors,
												  rwctx->mode == REPLMODE_RECOVERY,
												  record_lsn);
				if (reconnect)
				{
					proc_exit(0);
				}

				standalone = true;
				break;
			}
			/* create syncpoint */
		case 'S':
			{
				mtm_log(MtmApplyMessage, "executing syncpoint message from %d: '%s'",
						rwctx->sender_node_id, messageBody);

				process_syncpoint(rwctx, messageBody, record_lsn);

				/* XXX: clear filter_map */
				standalone = true;
				break;
			}
		case 'B':
			{
				/*
				 * This xact ends with plain COMMIT, so remember we can't
				 * ignore apply failure.
				 *
				 * An attentive pedant might notice that receiver might catch
				 * ERROR (OOM of whatever) in the short window after begin
				 * message but before this flag is set and still go ahead
				 * applying after PG_CATCH. Well, that's really unlikely. If
				 * desired, we could fix that by forcing receiver death
				 * whenever it fails in that window between begin and first
				 * change.
				 *
				 */
				rwctx->bdr_like = true;
				break;
			}
		default:
			Assert(false);
	}

	MtmEndSession(42, false);

	return standalone;
}

static void
read_tuple_parts(StringInfo s, Relation rel, TupleData *tup)
{
	TupleDesc	desc = RelationGetDescr(rel);
	int			i;
	int			rnatts;
	char		action;

	action = pq_getmsgbyte(s);

	if (action != 'T')
		mtm_log(ERROR, "expected TUPLE, got %c", action);

	memset(tup->isnull, 1, sizeof(tup->isnull));
	memset(tup->changed, 1, sizeof(tup->changed));

	rnatts = pq_getmsgint(s, 2);

	if (desc->natts < rnatts)
		mtm_log(ERROR, "tuple natts mismatch, %u vs %u", desc->natts, rnatts);

	/* FIXME: unaligned data accesses */

	for (i = 0; i < desc->natts; i++)
	{
		Form_pg_attribute att = TupleDescAttr(desc, i);
		char		kind;
		const char *data;
		int			len;

		if (att->atttypid == InvalidOid)
			continue;

		kind = pq_getmsgbyte(s);

		switch (kind)
		{
			case 'n':			/* null */
				/* already marked as null */
				tup->values[i] = PointerGetDatum(NULL);
				break;
			case 'u':			/* unchanged column */
				tup->isnull[i] = true;
				tup->changed[i] = false;
				tup->values[i] = PointerGetDatum(NULL);
				break;

			case 'b':			/* binary format */
				tup->isnull[i] = false;
				len = pq_getmsgint(s, 4);	/* read length */

				data = pq_getmsgbytes(s, len);

				/* and data */
				if (att->attbyval)
					tup->values[i] = fetch_att(data, true, len);
				else
					tup->values[i] = PointerGetDatum(data);
				break;

			case 't':			/* text format */
				{
					Oid			typinput;
					Oid			typioparam;

					tup->isnull[i] = false;
					len = pq_getmsgint(s, 4);	/* read length */

					getTypeInputInfo(att->atttypid, &typinput, &typioparam);
					/* and data */
					data = (char *) pq_getmsgbytes(s, len);
					tup->values[i] = OidInputFunctionCall(
														  typinput, (char *) data, typioparam, att->atttypmod);
				}
				break;
			default:
				mtm_log(ERROR, "unknown column type '%c'", kind);
		}

		if (att->attisdropped && !tup->isnull[i])
			mtm_log(ERROR, "data for dropped column");
	}
}

static void
tuple_to_slot(EState *estate, Relation rel, TupleData *tuple, TupleTableSlot *slot)
{
	TupleDesc	tupDesc = RelationGetDescr(rel);
	int			natts = slot->tts_tupleDescriptor->natts;
	HeapTuple	tup;
	MemoryContext oldctx;

	ExecClearTuple(slot);

	/*
	 * RelationFindReplTupleByIndex() expects virtual tuple, so let's set
	 * them. We stick here to HeapTuple instead of VirtualTuple, because we
	 * don't need to create own version of heap_modify_tuple().
	 */
	memcpy(slot->tts_values, tuple->values, natts * sizeof(Datum));
	memcpy(slot->tts_isnull, tuple->isnull, natts * sizeof(bool));

	oldctx = MemoryContextSwitchTo(GetPerTupleMemoryContext(estate));
	tup = heap_form_tuple(tupDesc, tuple->values, tuple->isnull);
	// ExecStoreHeapTuple(tup, slot, false);
	ExecStoreTuple(tup, slot, InvalidBuffer, false);
	MemoryContextSwitchTo(oldctx);
}

static void
close_rel(Relation rel)
{
	if (rel != NULL)
	{
		heap_close(rel, NoLock);
	}
}

static Relation
read_rel(StringInfo s, LOCKMODE mode)
{
	int			relnamelen;
	int			nspnamelen;
	RangeVar   *rv;
	Oid			remote_relid = pq_getmsgint(s, 4);
	Oid			local_relid;
	MemoryContext old_context;

	local_relid = pglogical_relid_map_get(remote_relid);
	if (local_relid == InvalidOid)
	{
		rv = makeNode(RangeVar);

		nspnamelen = pq_getmsgbyte(s);
		rv->schemaname = (char *) pq_getmsgbytes(s, nspnamelen);

		relnamelen = pq_getmsgbyte(s);
		rv->relname = (char *) pq_getmsgbytes(s, relnamelen);

		local_relid = RangeVarGetRelidExtended(rv, mode, 0, NULL, NULL);
		old_context = MemoryContextSwitchTo(TopMemoryContext);
		pglogical_relid_map_put(remote_relid, local_relid);
		MemoryContextSwitchTo(old_context);
		return heap_open(local_relid, NoLock);
	}
	else
	{
		nspnamelen = pq_getmsgbyte(s);
		s->cursor += nspnamelen;
		relnamelen = pq_getmsgbyte(s);
		s->cursor += relnamelen;
		return heap_open(local_relid, mode);
	}
}

/*
 * Send response to coordinator after PREPARE or ABORT.
 *
 * Stream name is xid not GID based because we may switch to a chunked
 * transaction decoding and GID will not be known in advance. Besides, gids
 * might be longer than reasonable dmq stream name.
 */
static void
mtm_send_prepare_reply(TransactionId xid, int dst_node_id,
					   bool prepared, ErrorData *edata)
{
	DmqDestinationId dest_id;
	StringInfo	packed_msg;
	MtmPrepareResponse msg;

	LWLockAcquire(Mtm->lock, LW_SHARED);
	dest_id = Mtm->peers[dst_node_id - 1].dmq_dest_id;
	LWLockRelease(Mtm->lock);

	Assert(dest_id >= 0);

	msg.tag = T_MtmPrepareResponse;
	msg.node_id = Mtm->my_node_id;
	msg.prepared = prepared;
	msg.errcode = edata ? edata->sqlerrcode : ERRCODE_SUCCESSFUL_COMPLETION;
	msg.errmsg = edata ? edata->message : "";
	msg.xid = xid;
	packed_msg = MtmMessagePack((MtmMessage *) &msg);

	dmq_push_buffer(dest_id, psprintf("xid" XID_FMT, xid),
					packed_msg->data, packed_msg->len);

	mtm_log(MtmApplyTrace,
			"MtmFollowerSendReply: " XID_FMT " to node%d (dest %d), prepared=%d",
			xid, dst_node_id, dest_id, prepared);

	pfree(packed_msg->data);
	pfree(packed_msg);
}

/*
 * Send response to coordinator after paxos 2a msg.
 * The same stream name as in mtm_send_prepare_reply is used to make
 * coordinators life eaiser.
 * COMMIT PREPARED ack is also sent from here.
 */
static void
mtm_send_2a_reply(char *gid, GlobalTxStatus status,
				  GlobalTxTerm accepted_term, int dst_node_id)
{
	DmqDestinationId dest_id;
	StringInfo	packed_msg;
	Mtm2AResponse msg;
	TransactionId xid = MtmGidParseXid(gid);

	LWLockAcquire(Mtm->lock, LW_SHARED);
	dest_id = Mtm->peers[dst_node_id - 1].dmq_dest_id;
	LWLockRelease(Mtm->lock);

	Assert(dest_id >= 0);

	msg.tag = T_Mtm2AResponse;
	msg.node_id = Mtm->my_node_id;
	msg.status = status;
	msg.accepted_term = accepted_term;
	msg.errcode = ERRCODE_SUCCESSFUL_COMPLETION;
	msg.errmsg = "";
	msg.gid = gid;
	packed_msg = MtmMessagePack((MtmMessage *) &msg);

	dmq_push_buffer(dest_id, psprintf("xid" XID_FMT, xid),
					packed_msg->data, packed_msg->len);
	mtm_log(MtmApplyTrace,
			"MtmFollowerSendReply: 2b for %s to node%d (dest %d), status %d",
			gid, dst_node_id, dest_id, status);

	pfree(packed_msg->data);
	pfree(packed_msg);
}

static void
process_remote_commit(StringInfo in,
					  MtmReceiverWorkerContext *rwctx)
{
	uint8		event;
	XLogRecPtr	end_lsn;
	XLogRecPtr	origin_lsn;
	int			origin_node;
	char		gid[GIDSIZE];

	gid[0] = '\0';

	/* read event */
	event = pq_getmsgbyte(in);
	/* this must be equal to rwctx->sender_node_id */
	pq_getmsgbyte(in);

	/* read fields */
	pq_getmsgint64(in);			/* commit_lsn */
	end_lsn = pq_getmsgint64(in);	/* end_lsn */
	replorigin_session_origin_timestamp = pq_getmsgint64(in);	/* commit_time */

	origin_node = pq_getmsgbyte(in);
	Assert(origin_node >= 1);
	origin_lsn = pq_getmsgint64(in);

	replorigin_session_origin_lsn =
		origin_node == rwctx->sender_node_id ? end_lsn : origin_lsn;
	Assert(replorigin_session_origin == InvalidRepOriginId);

	suppress_internal_consistency_checks = false;

	switch (event)
	{
		case PGLOGICAL_PREPARE_PHASE2A:
			{
				const char *state_3pc;
				GlobalTxStatus msg_status;
				GlobalTxTerm _msg_prop, msg_term;
				GlobalTxStatus reply_status = GTXInvalid;
				GlobalTxTerm reply_acc = InvalidGTxTerm;

				Assert(!IsTransactionState());
				Assert(!query_cancel_allowed);
				strncpy(gid, pq_getmsgstring(in), sizeof gid);
				state_3pc = pq_getmsgstring(in);
				parse_gtx_state(state_3pc, &msg_status, &_msg_prop, &msg_term);
				/*
				 * coordinator ever sends PRECOMMIT via walsender; PREABORTs
				 * is the territory of the resolver who operates via dmq.
				 */
				Assert(msg_status == GTXPreCommitted);

				rwctx->gtx = GlobalTxAcquire(gid, false);
				if (!rwctx->gtx)
				{
					/*
					 * Xact was already finished. Still reply to coordinator
					 * so he doesn't wait for us.
					 */
					if (rwctx->mode == REPLMODE_NORMAL)
						mtm_send_2a_reply(gid, GTXInvalid, InvalidGTxTerm, origin_node);
					break;
				}

				/*
				 * give vote iff paxos rules allow it.
				 */
				if (term_cmp(msg_term, rwctx->gtx->state.proposal) >= 0)
				{
					bool		done = false;
					char	   *sstate;

					MtmBeginSession(origin_node);
					StartTransactionCommand();

					sstate = serialize_gtx_state(msg_status, msg_term, msg_term);
					done = SetPreparedTransactionState(gid, sstate, false);
					if (!done)
						Assert(false);

					CommitTransactionCommand();
					MemoryContextSwitchTo(MtmApplyContext);
					rwctx->gtx->state.proposal = msg_term;
					rwctx->gtx->state.accepted = msg_term;
					rwctx->gtx->state.status = msg_status;
					reply_status = msg_status;
					reply_acc = msg_term;

					mtm_log(MtmTxFinish, "TXFINISH: %s precommitted", gid);

					MtmEndSession(origin_node, true);
				}

				if (rwctx->mode == REPLMODE_NORMAL)
					mtm_send_2a_reply(gid, reply_status,
									  reply_acc, origin_node);

				GlobalTxRelease(rwctx->gtx);
				rwctx->gtx = NULL;
				break;
			}
		case PGLOGICAL_COMMIT:
			{
				/*
				 * Plain commits are used for bdr-like broadcast of syncpoint
				 * table to provide nodes with apply progress info of each
				 * other. Such modifications never conflict (node creates and
				 * deletes only its own records, with minor exception of
				 * drop_node) so full commit procedure is not needed.
				 *
				 * Empty commits are also streamed if xact modified only local
				 * tables.
				 */
				Assert(IsTransactionState());

				MtmBeginSession(origin_node);
				CommitTransactionCommand();
				mtm_log(MtmTxFinish, "finished plain commit %d-" XID_FMT " at %X/%X, replorigin_session_lsn=%X/%X, origin_node=%d",
						origin_node, rwctx->origin_xid,
						(uint32) (XactLastCommitEnd >> 32),
						(uint32) (XactLastCommitEnd),
						(uint32) (replorigin_session_origin_lsn >> 32),
						(uint32) replorigin_session_origin_lsn,
						origin_node
					);

				MtmEndSession(origin_node, true);

				rwctx->origin_xid = InvalidTransactionId;
				rwctx->my_xid = InvalidTransactionId;

				/* xxx why we need this */
				InterruptPending = false;
				QueryCancelPending = false;
				query_cancel_allowed = false;

				break;
			}
		case PGLOGICAL_PREPARE:
			{
				ListCell   *cur_item,
						   *prev_item;
				TransactionId xid = GetCurrentTransactionIdIfAny();
				uint64 prepare_gen_num;
				bool latch_set = false;

				Assert(IsTransactionState() && TransactionIdIsValid(xid));
				Assert(xid == rwctx->my_xid);

				strncpy(gid, pq_getmsgstring(in), sizeof gid);
				prepare_gen_num = MtmGidParseGenNum(gid); /* xxx user 2pc */

				/*
				 * Before applying, make sure our node had switched to gen of
				 * PREPARE or higher one. Normally this would be no-op or
				 * imperceptible amount of time (until the next heartbeat).
				 *
				 * We could remove the waiting altogether if we carried the
				 * full gen info in xact itself (members, donors), but that
				 * seems like gid size bloating for no reason.
				 */
				while(unlikely(MtmGetCurrentGenNum() < prepare_gen_num))
				{
					int			rc;

					CHECK_FOR_INTERRUPTS();
					rc = WaitLatch(MyLatch,
								   WL_LATCH_SET | WL_TIMEOUT | WL_POSTMASTER_DEATH,
								   100.0,
								   PG_WAIT_EXTENSION);
					if (rc & WL_LATCH_SET)
					{
						latch_set = true;
						ResetLatch(&MyProc->procLatch);
					}
					if (rc & WL_POSTMASTER_DEATH)
						proc_exit(1);
				}
				/* restore the latch cock if we've swallowed it above */
				if (latch_set)
					SetLatch(&MyProc->procLatch);

				MtmBeginSession(origin_node);

				/* Exclude concurrent gen switchers, c.f. AcquirePBByHolder call site */
				AcquirePBByPreparer(); /* xxx release it on shmem exit in workers */

				/*
				 * With lock acquired, check again that our apply mode makes
				 * sense: applying normally when we are in recovery is
				 * unacceptable as we might get xact out of order. Applying in
				 * recovery when we are online is also not ok because we would
				 * miss reply to coordinator; though we hardly would ever end
				 * up doing this.
				 *
				 * As for the second check, under extremely unlikely
                 * circumstances due to slow wareceivers convergence we might
                 * get P of gen where we are in RECOVERY before corresponding
                 * ParallelSafe, if we happen to have working walreceiver in
                 * REPLMODE_RECOVERY from non-donor gen member. We could
                 * safely enable us right away in this case, but for that we
                 * need to re-acquire exclusive gen lock, recheck that gen
                 * hadn't changed... seems simpler to just restart as we will
                 * learn the real donor with ParallelSafe eventually.
				 */
				if (unlikely((rwctx->mode != MtmGetReceiverMode(rwctx->sender_node_id)) ||
							 (MtmGetCurrentStatusInGen() == MTM_GEN_RECOVERY &&
							  prepare_gen_num == MtmGetCurrentGenNum())))
				{
					ReleasePB();
					proc_exit(0);
				}

				/*
				 * refuse to prepare xacts from older gens in normal mode
				 * (but continue applying, there is no need to restart receiver)
				 */
				if (rwctx->mode == REPLMODE_NORMAL &&
					prepare_gen_num < MtmGetCurrentGenNum())
				{
					ereport(ERROR,
							(errcode(ERRCODE_INTERNAL_ERROR),
							 MTM_ERRMSG("refusing transaction %s due to generation switch: prepare_gen_num=" UINT64_FORMAT ", current_gen_num=" UINT64_FORMAT,
										gid, prepare_gen_num, MtmGetCurrentGenNum())));
				}

				/* fine, apply the prepare */

				/*
				 * prepare TBLOCK_INPROGRESS state for
				 * PrepareTransactionBlock()
				 */
				BeginTransactionBlock(false);
				CommitTransactionCommand();
				StartTransactionCommand();

				rwctx->gtx = GlobalTxAcquire(gid, true);
				/*
				 * it must be brand new gtx, we don't do anything before P
				 */
				Assert(!rwctx->gtx->prepared);
				Assert(term_cmp(rwctx->gtx->state.proposal, InitialGTxTerm) == 0);

				PrepareTransactionBlock(gid);
				AllowTempIn2PC = true;
				/* PREPARE itself */
				CommitTransactionCommand();
				MemoryContextSwitchTo(MtmApplyContext);
				ReleasePB();
				rwctx->gtx->prepared = true; /* now we have WAL record */
				mtm_log(MtmTxFinish, "TXFINISH: %s prepared (local_xid=" XID_FMT ")", gid, xid);

				GlobalTxRelease(rwctx->gtx);
				rwctx->gtx = NULL;

				MemoryContextSwitchTo(MtmApplyContext);

				InterruptPending = false;
				QueryCancelPending = false;
				query_cancel_allowed = false;

				if (rwctx->mode == REPLMODE_NORMAL)
				{
					mtm_send_prepare_reply(rwctx->origin_xid,
										   rwctx->sender_node_id,
										   true, NULL);
				}

				/*
				 * Clean this after CommitTransactionCommand(), as it may
				 * throw an error that we should propagate to the originating
				 * node.
				 */
				rwctx->origin_xid = InvalidTransactionId;
				rwctx->my_xid = InvalidTransactionId;
				// MtmDeadlockDetectorRemoveXact(xid);

				/*
				 * Reset on_commit_actions.
				 *
				 * Temp schema is shared between pool workers so we can try to
				 * truncate tables that were delete by other workers, but
				 * still exist is pg_on_commit_actions which is static. So
				 * clean it up: as sata is not replicated for temp tables
				 * there is nothing to delete anyway.
				 */
				prev_item = NULL;
				cur_item = list_head(pg_on_commit_actions);
				while (cur_item != NULL)
				{
					void	   *oc = (void *) lfirst(cur_item);

					pg_on_commit_actions = list_delete_cell(pg_on_commit_actions, cur_item, prev_item);
					pfree(oc);
					if (prev_item)
						cur_item = lnext(prev_item);
					else
						cur_item = list_head(pg_on_commit_actions);
				}
				Assert(pg_on_commit_actions == NIL);

				MtmEndSession(origin_node, true);

				mtm_log(MtmApplyTrace,
						"Prepare transaction %s event=%d origin=(%d, " LSN_FMT ")",
						gid, event, origin_node,
						origin_node == rwctx->sender_node_id ? end_lsn : origin_lsn);

				break;
			}
		case PGLOGICAL_COMMIT_PREPARED:
			{
				Assert(!query_cancel_allowed);
				pq_getmsgint64(in); /* csn */
				strncpy(gid, pq_getmsgstring(in), sizeof gid);

				rwctx->gtx = GlobalTxAcquire(gid, false);

				/* normal path: we have PREPARE, finish it */
				if (rwctx->gtx)
				{
					StartTransactionCommand();
					MtmBeginSession(origin_node);

					FinishPreparedTransaction(gid, true, false);
					CommitTransactionCommand();
					rwctx->gtx->state.status = GTXCommitted;
					GlobalTxRelease(rwctx->gtx);
					rwctx->gtx = NULL;
					MtmEndSession(origin_node, true);
					mtm_log(MtmTxFinish, "TXFINISH: %s committed", gid);
				}
				/*
				 * A subtle moment. We want to prevent getting CP before P,
				 * otherwise something like
				 *
				 * 1) A makes P and then PC on AB
				 *    (AB is generation, i.e. their votes are enough for commit)
				 * 2) B initiates resolving and with A resolves to CP,
				 *    making its own CP record.
				 * 3) C gets and acks CP from B, but it still handn't got P
				 *    (it might not yet realized it should recover from A or B).
				 * 4) A also acks CP from B.
				 * 5) C gets and acks P from A or B, so B purges WAL with P
				 *    and CP.
				 * 6) BC is the new generation.
				 * 7) Now C having nothing but P asks B to resolve, who
				 *    doesn't have any state for the xact anymore; so it is
				 *    like 'ho, I don't have such gtx, this is an xact from
				 *    an older gen than I am online in and I don't see CP in
				 *    WAL, so if there was CP it must had already been
				 *    streamed => reply with direct ABORT'
				 *
				 * this (extremely unlikely but at least theoretically) is
				 * possible. Luckily there is an easy way to detect such
				 * out-of-order CP: it can happen only when we are still
				 * applying in normal mode while in fact we must recover in
				 * this xact's gen. Because normal apply is allowed only when
				 * we are member of the gen (and recovered in it), and member
				 * of the gen can't get CP before P by definition of
				 * generations; or we are ONLINE in higher gen, which means we
				 * already got all committable prepares of this one.
				 *
				 * Alternatively we could persist CP outcome somewhere, but
				 * that's much more cumbersome and it is not clear how to
				 * obtain PREPARE's end_lsn here to know when we can purge the
				 * entry.
				 */
				else if (rwctx->mode == REPLMODE_NORMAL)
				{
					uint64 prepare_gen_num = MtmGidParseGenNum(gid);

					/*
					 * Could wait until we switch into it, but bailing out
					 * here seems like incredibly rare situation.
					 */
					if (unlikely(MtmGetCurrentGenNum() < prepare_gen_num))
					{
						ereport(ERROR,
								(errcode(ERRCODE_INTERNAL_ERROR),
								 MTM_ERRMSG("refusing COMMIT PREPARED of transaction %s due to generation switch: prepare_gen_num=" UINT64_FORMAT ", current_gen_num=" UINT64_FORMAT,
											gid, prepare_gen_num, MtmGetCurrentGenNum())));
					}

					if (unlikely((rwctx->mode !=
								  MtmGetReceiverMode(rwctx->sender_node_id))))
					{
						proc_exit(0);
					}
				}

				/* restore ctx after CommitTransaction */
				MemoryContextSwitchTo(MtmApplyContext);

				/* send CP ack */
				if (rwctx->mode == REPLMODE_NORMAL)
					mtm_send_2a_reply(gid, GTXCommitted,
									  InvalidGTxTerm, origin_node);

				break;
			}
		case PGLOGICAL_ABORT_PREPARED:
			{
				strncpy(gid, pq_getmsgstring(in), sizeof gid);

				/*
				 * Unlike CP handling, there is no need to persist
				 * out-of-order abort anywhere: if we ask node about status of
				 * xact for which it had purged WAL with finalization record,
				 * she'd reply with direct abort -- because if there was
				 * commit, we must have already received it and the answer
				 * doesn't matter.
				 *
				 * However, we have another ordering issue: if client abruptly
				 * disconnects before PRECOMMIT, backend aborts immediately.
				 * P and AP in healthy cluster (on online nodes) will be
				 * executed concurrently and possibly reorder. This results in
				 * hanged PREPARED xact: until node reboots or receiver dies
				 * there is no reason to resolve it. To prevent this, avoid
				 * reordering by waiting till all xacts before this AP are
				 * applied. This bears some performance penalty but 1) aborts
				 * shouldn't be frequent 2) if really needed, we could
				 * optimize by waiting for our specific P instead of all
				 * previous queue 3) the only alternative I've in mind
				 * currently -- HOLD_INTERRUPTS during PREPARE round -- is
				 * much less appealing and not even bullet-proof safe
				 * (e.g. commit sequence interruption might be raised by
				 * internal error, OOM or whatever)
				 */
				if (rwctx->txlist_pos != -1)
				{
					txl_wait_txhead(&BGW_POOL_BY_NODE_ID(rwctx->sender_node_id)->txlist,
									rwctx->txlist_pos);
				}

				rwctx->gtx = GlobalTxAcquire(gid, false);
				if (!rwctx->gtx)
				{
					mtm_log(MtmApplyTrace, "skipping ABORT PREPARED of %s as there is no xact", gid);
					break;
				}

				MtmBeginSession(origin_node);
				StartTransactionCommand();

				FinishPreparedTransaction(gid, false, false);
				CommitTransactionCommand();
				rwctx->gtx->state.status = GTXAborted;
				GlobalTxRelease(rwctx->gtx);
				rwctx->gtx = NULL;

				mtm_log(MtmTxFinish, "TXFINISH: %s aborted", gid);
				MemoryContextSwitchTo(MtmApplyContext);
				MtmEndSession(origin_node, true);
				mtm_log(MtmApplyTrace, "PGLOGICAL_ABORT_PREPARED %s", gid);
				break;
			}
		default:
			Assert(false);
	}

	Assert(replorigin_session_origin == InvalidRepOriginId);
	MaybeLogSyncpoint();
}

static int
pq_peekmsgbyte(StringInfo msg)
{
	return (msg->cursor < msg->len) ? (unsigned char) msg->data[msg->cursor] : EOF;
}

#define MAX_BUFFERED_TUPLES 1024
#define MAX_BUFFERED_TUPLES_SIZE 0x10000

static void
process_remote_insert(StringInfo s, Relation rel)
{
	EState	   *estate;
	TupleData	new_tuple;
	TupleTableSlot *newslot;
	ResultRelInfo *relinfo;
	int			i;
	TupleDesc	tupDesc = RelationGetDescr(rel);

	PushActiveSnapshot(GetTransactionSnapshot());
	estate = create_rel_estate(rel);
	newslot = ExecInitExtraTupleSlot(estate, tupDesc);

	ExecOpenIndices(estate->es_result_relation_info, false);
	relinfo = estate->es_result_relation_info;

	read_tuple_parts(s, rel, &new_tuple);

	if (pq_peekmsgbyte(s) == 'I')
	{
		/* Use bulk insert */
		BulkInsertState bistate = GetBulkInsertState();
		HeapTuple	bufferedTuples[MAX_BUFFERED_TUPLES];
		MemoryContext oldcontext;
		int			nBufferedTuples = 1;
		size_t		bufferedTuplesSize;
		CommandId	mycid = GetCurrentCommandId(true);

		bufferedTuples[0] = heap_form_tuple(tupDesc, new_tuple.values, new_tuple.isnull);
		bufferedTuplesSize = bufferedTuples[0]->t_len;

		while (nBufferedTuples < MAX_BUFFERED_TUPLES && bufferedTuplesSize < MAX_BUFFERED_TUPLES_SIZE)
		{
			if (pq_getmsgbyte(s) != 'I')
			{
				Assert(false);
				mtm_log(PANIC, "unexpected insert message format");
			}
			read_tuple_parts(s, rel, &new_tuple);
			bufferedTuples[nBufferedTuples] = heap_form_tuple(tupDesc,
															  new_tuple.values, new_tuple.isnull);
			bufferedTuplesSize += bufferedTuples[nBufferedTuples++]->t_len;
			if (pq_peekmsgbyte(s) != 'I')
				break;
		}

		/*
		 * heap_multi_insert leaks memory, so switch to short-lived memory
		 * context before calling it.
		 */
		oldcontext = MemoryContextSwitchTo(GetPerTupleMemoryContext(estate));
		heap_multi_insert(rel,
						  bufferedTuples,
						  nBufferedTuples,
						  mycid,
						  0,
						  bistate);
		MemoryContextSwitchTo(oldcontext);


		for (i = 0; i < nBufferedTuples; i++)
		{
			/*
			 * If there are any indexes, update them for all the inserted tuples,
			 * and run AFTER ROW INSERT triggers.
			 */
			if (relinfo->ri_NumIndices > 0)
			{
				List	   *recheckIndexes;

				ExecStoreTuple(bufferedTuples[i], newslot, InvalidBuffer, false);
				recheckIndexes =
					ExecInsertIndexTuples(newslot, &bufferedTuples[i]->t_self,
										  estate, false, NULL, NIL);

				ExecARInsertTriggers(estate, relinfo, bufferedTuples[i],
										 recheckIndexes, NULL);

				list_free(recheckIndexes);
			}
			/*
			 * There's no indexes, but see if we need to run AFTER ROW INSERT
			 * triggers anyway.
			 */
			else if (relinfo->ri_TrigDesc != NULL &&
							(relinfo->ri_TrigDesc->trig_insert_after_row ||
								relinfo->ri_TrigDesc->trig_insert_new_table))
			{
					ExecARInsertTriggers(estate, relinfo, bufferedTuples[i],
															NIL, NULL);
			}
		}

		FreeBulkInsertState(bistate);
	}
	else
	{
		TupleTableSlot *newslot;

		newslot = ExecInitExtraTupleSlot(estate, tupDesc);
		tuple_to_slot(estate, rel, &new_tuple, newslot);

		ExecSimpleRelationInsert(estate, newslot);
	}
	ExecCloseIndices(estate->es_result_relation_info);
	if (ActiveSnapshotSet())
		PopActiveSnapshot();

	/* Handle queued AFTER triggers. */
	AfterTriggerEndQuery(estate);

	/* XXX: maybe just insert it during extension creation? */
	if (strcmp(RelationGetRelationName(rel), MULTIMASTER_LOCAL_TABLES_TABLE) == 0 &&
		strcmp(get_namespace_name(RelationGetNamespace(rel)), MULTIMASTER_SCHEMA_NAME) == 0)
	{
		MtmMakeTableLocal((char *) DatumGetPointer(new_tuple.values[0]), (char *) DatumGetPointer(new_tuple.values[1]), false);
	}

	ExecResetTupleTable(estate->es_tupleTable, true);
	FreeExecutorState(estate);

	CommandCounterIncrement();
}

static void
process_remote_update(StringInfo s, Relation rel)
{
	char		action;
	EState	   *estate;
	TupleTableSlot *remoteslot;
	TupleTableSlot *localslot;
	bool		has_oldtup;
	bool		found;
	TupleData	old_tuple;
	TupleData	new_tuple;
	Oid			idxoid = InvalidOid;
	TupleDesc	tupDesc = RelationGetDescr(rel);
	EPQState	epqstate;

	estate = create_rel_estate(rel);
	remoteslot = ExecInitExtraTupleSlot(estate, tupDesc);
	localslot = ExecInitExtraTupleSlot(estate, tupDesc);
	EvalPlanQualInit(&epqstate, estate, NULL, NIL, -1);

	action = pq_getmsgbyte(s);

	/* old key present, identifying key changed */
	if (action != 'K' && action != 'N')
		mtm_log(ERROR, "expected action 'N' or 'K', got %c",
				action);

	if (action == 'K')
	{
		has_oldtup = true;
		read_tuple_parts(s, rel, &old_tuple);
		action = pq_getmsgbyte(s);
	}
	else
		has_oldtup = false;

	/* check for new tuple */
	if (action != 'N')
		mtm_log(ERROR, "expected action 'N', got %c",
				action);

	if (rel->rd_rel->relkind != RELKIND_RELATION)
		mtm_log(ERROR, "unexpected relkind '%c' rel \"%s\"",
				rel->rd_rel->relkind, RelationGetRelationName(rel));

	/* read new tuple */
	read_tuple_parts(s, rel, &new_tuple);

	idxoid = RelationGetReplicaIndex(rel);
	if (!OidIsValid(idxoid))
		idxoid = RelationGetPrimaryKeyIndex(rel);

	PushActiveSnapshot(GetTransactionSnapshot());
	ExecOpenIndices(estate->es_result_relation_info, false);

	tuple_to_slot(estate, rel, has_oldtup ? &old_tuple : &new_tuple, remoteslot);

	if (OidIsValid(idxoid))
	{
		found = RelationFindReplTupleByIndex(rel, idxoid,
											 LockTupleExclusive,
											 remoteslot, localslot);
	}
	else
	{
		found = RelationFindReplTupleSeq(rel, LockTupleExclusive,
										 remoteslot, localslot);
	}

	ExecClearTuple(remoteslot);

	if (found)
	{
		HeapTuple	remote_tuple = NULL;

		remote_tuple = heap_modify_tuple(localslot->tts_tuple,
										 tupDesc,
										 new_tuple.values,
										 new_tuple.isnull,
										 new_tuple.changed);
		ExecStoreTuple(remote_tuple, remoteslot, InvalidBuffer, false);

		EvalPlanQualSetSlot(&epqstate, remoteslot);
		ExecSimpleRelationUpdate(estate, &epqstate, localslot, remoteslot);
	}
	else
	{
		ereport(ERROR,
				(errcode(ERRCODE_NO_DATA_FOUND),
				 MTM_ERRMSG("Record with specified key can not be located at this node"),
				 errdetail("Most likely we have DELETE-UPDATE conflict")));
	}

	ExecCloseIndices(estate->es_result_relation_info);
	PopActiveSnapshot();

	/* Handle queued AFTER triggers. */
	AfterTriggerEndQuery(estate);

	EvalPlanQualEnd(&epqstate);
	ExecResetTupleTable(estate->es_tupleTable, false);
	FreeExecutorState(estate);

	CommandCounterIncrement();
}

static void
process_remote_delete(StringInfo s, Relation rel)
{
	EState	   *estate;
	TupleData	deltup;
	EPQState	epqstate;
	TupleTableSlot *localslot;
	TupleTableSlot *remoteslot;
	Oid			idxoid = InvalidOid;
	bool		found;

	estate = create_rel_estate(rel);
	remoteslot = ExecInitExtraTupleSlot(estate,
										RelationGetDescr(rel));
	localslot = ExecInitExtraTupleSlot(estate,
										RelationGetDescr(rel));

	read_tuple_parts(s, rel, &deltup);
	tuple_to_slot(estate, rel, &deltup, remoteslot);

	EvalPlanQualInit(&epqstate, estate, NULL, NIL, -1);

	PushActiveSnapshot(GetTransactionSnapshot());
	ExecOpenIndices(estate->es_result_relation_info, false);

	idxoid = RelationGetReplicaIndex(rel);
	if (!OidIsValid(idxoid))
		idxoid = RelationGetPrimaryKeyIndex(rel);

	if (OidIsValid(idxoid))
	{
		found = RelationFindReplTupleByIndex(rel, idxoid,
											 LockTupleExclusive,
											 remoteslot, localslot);
	}
	else
	{
		found = RelationFindReplTupleSeq(rel, LockTupleExclusive,
										 remoteslot, localslot);
	}


	if (found)
	{
		EvalPlanQualSetSlot(&epqstate, localslot);
		ExecSimpleRelationDelete(estate, &epqstate, localslot);
	}
	else
	{
		ereport(ERROR,
				(errcode(ERRCODE_NO_DATA_FOUND),
				 MTM_ERRMSG("Record with specified key can not be located at this node"),
				 errdetail("Most likely we have DELETE-DELETE conflict")));
	}

	ExecCloseIndices(estate->es_result_relation_info);
	PopActiveSnapshot();

	/* Handle queued AFTER triggers. */
	AfterTriggerEndQuery(estate);

	EvalPlanQualEnd(&epqstate);
	ExecResetTupleTable(estate->es_tupleTable, true);
	FreeExecutorState(estate);

	CommandCounterIncrement();
}

void
MtmExecutor(void *work, size_t size, MtmReceiverWorkerContext *rwctx)
{
	StringInfoData s;
	Relation	rel = NULL;
	int			spill_file = -1;
	int			save_cursor = 0;
	int			save_len = 0;
	MemoryContext old_context;

	rwctx->origin_xid = InvalidTransactionId;
	rwctx->my_xid = InvalidTransactionId;
	rwctx->bdr_like = false;

	s.data = work;
	s.len = size;
	s.maxlen = -1;
	s.cursor = 0;

	if (MtmApplyContext == NULL)
		MtmApplyContext = AllocSetContextCreate(TopMemoryContext,
												"ApplyContext",
												ALLOCSET_DEFAULT_SIZES);
	replorigin_session_origin = InvalidRepOriginId;

	PG_TRY();
	{
		bool		inside_transaction = true;

		AcceptInvalidationMessages();

		/* Clear authorization settings */
		StartTransactionCommand();
		SetPGVariable("session_authorization", NIL, false);
		ResetAllOptions();
		CommitTransactionCommand();

		if (!receiver_mtm_cfg_valid)
		{
			if (receiver_mtm_cfg)
				pfree(receiver_mtm_cfg);
			receiver_mtm_cfg = MtmLoadConfig();
			if (receiver_mtm_cfg->my_node_id == 0 ||
				MtmNodeById(receiver_mtm_cfg, rwctx->sender_node_id) == NULL)
				//XXX
					proc_exit(0);

			receiver_mtm_cfg_valid = true;
		}

		/*
		 * context which is reset per message, containing whatever hasn't got
		 * into transaction ctxses (most things will; xxx are there any of
		 * such allocations at all apart from ErrorData?)
		 */
		old_context = MemoryContextSwitchTo(MtmApplyContext);
		do
		{
			char		action = pq_getmsgbyte(&s);

			MemoryContextResetAndDeleteChildren(MtmApplyContext);
			mtm_log(MtmApplyTrace, "got action '%c'", action);

			switch (action)
			{
					/* BEGIN */
				case 'B':
					process_remote_begin(&s, rwctx);
					inside_transaction = true;
					break;
					/* COMMIT */
				case 'C':
					close_rel(rel);
					process_remote_commit(&s, rwctx);
					inside_transaction = false;
					break;
					/* INSERT */
				case 'I':
					Assert(rel);
					process_remote_insert(&s, rel);
					break;
					/* UPDATE */
				case 'U':
					Assert(rel);
					process_remote_update(&s, rel);
					break;
					/* DELETE */
				case 'D':
					Assert(rel);
					process_remote_delete(&s, rel);
					break;
				case 'R':
					close_rel(rel);
					rel = read_rel(&s, RowExclusiveLock);
					break;
				case 'F':
					{
						int			node_id = pq_getmsgint(&s, 4);
						int			file_id = pq_getmsgint(&s, 4);

						Assert(spill_file < 0);
						spill_file = MtmOpenSpillFile(node_id, file_id);
						break;
					}
				case '(': /* read chunk from spill file */
					{
						size_t		size = pq_getmsgint(&s, 4);

						s.data = MemoryContextAlloc(TopMemoryContext, size);
						save_cursor = s.cursor;
						save_len = s.len;
						s.cursor = 0;
						s.len = size;
						MtmReadSpillFile(spill_file, s.data, size);
						break;
					}
				case ')': /* end of chunk in spill file */
					pfree(s.data);
					s.data = work;
					s.cursor = save_cursor;
					s.len = save_len;
					break;
				case 'N':
					{
						int64		next;
						Oid			relid;

						Assert(rel != NULL);
						relid = RelationGetRelid(rel);
						close_rel(rel);
						rel = NULL;
						next = pq_getmsgint64(&s);
						AdjustSequence(relid, next);
						break;
					}
				case '0':
					Assert(rel != NULL);
					heap_truncate_one_rel(rel);
					break;
				case 'M':
					close_rel(rel);
					rel = NULL;
					inside_transaction = !process_remote_message(&s, rwctx);
					break;
				case 'Z':
					{
						int			rc;

						Assert(!IsTransactionState());

						if (receiver_mtm_cfg->backup_node_id > 0)
						{
							StartTransactionCommand();

							if (SPI_connect() != SPI_OK_CONNECT)
								mtm_log(ERROR, "could not connect using SPI");
							PushActiveSnapshot(GetTransactionSnapshot());

							rc = SPI_execute("delete from mtm.config where key='basebackup'", false, 0);
							if (rc != SPI_OK_DELETE)
								mtm_log(ERROR, "Failed to clean basebackup info");

							if (SPI_finish() != SPI_OK_FINISH)
								mtm_log(ERROR, "could not finish SPI");
							PopActiveSnapshot();

							/*
							 * xxx a really rude way to force config update in
							 * all procs, but this is done very rarely
							 */
							CacheInvalidateCatalog(SubscriptionRelationId);
							CommitTransactionCommand();

							receiver_mtm_cfg_valid = false;
						}

						/* tell campaigner we are caught up */
						MtmReportReceiverCaughtup(rwctx->sender_node_id);

						inside_transaction = false;
						break;
					}
				default:
					mtm_log(ERROR, "unknown action of type %c", action);
			}
		} while (inside_transaction);
	}
	PG_CATCH();
	{
		ErrorData  *edata;

		/* log error immediately, before the cleanup */
		MemoryContextSwitchTo(MtmApplyContext);
		edata = CopyErrorData();
		EmitErrorReport();
		FlushErrorState();

		pgstat_report_activity(STATE_RUNNING, NULL);
		txl_remove(&BGW_POOL_BY_NODE_ID(rwctx->sender_node_id)->txlist,
				   rwctx->txlist_pos);
		rwctx->txlist_pos = -1;
		query_cancel_allowed = false;

		/*
		 * handle only prepare errors here
		 * XXX reply to 2a also, though its failure is unlikely
		 */
		if (TransactionIdIsValid(rwctx->origin_xid))
		{
			// MtmDeadlockDetectorRemoveXact(current_gtid.my_xid);
			if (rwctx->mode == REPLMODE_NORMAL)
				mtm_send_prepare_reply(rwctx->origin_xid,
									   rwctx->sender_node_id,
									   false, edata);
		}

		/*
		 * If we are in recovery, there is no excuse for refusing the
		 * transaction. Die and restart the recovery then.
		 *
		 * This should never happen under normal circumstances. Yeah, you
		 * might imagine something like e.g. evil user making local xact which
		 * makes our xact violating constraint or out of disk space error, but
		 * in testing this most probably means a bug, so put additional
		 * warning.
		 *
		 * Also die instead of continuing applying if that was whatever but
		 * not xact itself (ending with PREPARE) -- ERROR in these cases is
		 * not something normal. In particular, if we proceed after failure of
		 * COMMIT PREPARED apply (and ack its receival), theoretically sender
		 * might purge WAL with it and later respond with 'dunno, this is an
		 * old xact and if there was a commit you should had already got it,
		 * so just abort it'.
		 *
		 * There is one more scenario where we can't ignore the error and
		 * continue applying even if we applied xact in normal mode: it is
		 * bdr-like transaction on which 3PC is not performed -- it is already
		 * committed at origin. Currently only syncpoint table records are
		 * broadcast in this way.
		 */
		if (rwctx->mode == REPLMODE_RECOVERY ||
			!TransactionIdIsValid(rwctx->origin_xid) ||
			rwctx->bdr_like)
		{
			if (rwctx->mode == REPLMODE_RECOVERY)
				mtm_log(WARNING, "got ERROR while applying in recovery, origin_xid=" XID_FMT,
						rwctx->origin_xid);
			/*
			 * We already did FlushErrorState, so EmitErrorReport in
			 * bgworker.c would confuse elog machinery. Die on our own instead
			 * of PG_RE_THROW.
			 */
			proc_exit(1);
		}

		ReleasePB();
		if (rwctx->gtx != NULL)
		{
			GlobalTxRelease(rwctx->gtx);
			rwctx->gtx = NULL;
		}
		MtmEndSession(42, false);

		MtmDDLResetApplyState();

		mtm_log(MtmApplyError, "abort transaction origin_xid=" XID_FMT,
				rwctx->origin_xid);

		FreeErrorData(edata);
		AbortCurrentTransaction();
	}
	PG_END_TRY();

	txl_remove(&BGW_POOL_BY_NODE_ID(rwctx->sender_node_id)->txlist,
			   rwctx->txlist_pos);
	rwctx->txlist_pos = -1;
	if (s.data != work)
		pfree(s.data);
	MemoryContextSwitchTo(old_context);
}
