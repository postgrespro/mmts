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
static bool find_pkey_tuple(ScanKey skey, Relation rel, Relation idxrel,
				TupleTableSlot *slot, bool lock, LockTupleMode mode);
static bool find_heap_tuple(TupleData *tup, Relation rel, TupleTableSlot *slot, bool lock);
static bool build_index_scan_key(ScanKey skey, Relation rel, Relation idxrel, TupleData *tup);
static void UserTableUpdateOpenIndexes(EState *estate, TupleTableSlot *slot);
static void UserTableUpdateIndexes(EState *estate, TupleTableSlot *slot);
static bool process_remote_begin(StringInfo s, volatile GlobalTransactionId *gtid,
								 MtmReceiverWorkerContext *rwctx);
static bool process_remote_message(StringInfo s,
								   MtmReceiverWorkerContext *rwctx);
static void process_remote_commit(StringInfo s,
								  volatile GlobalTransactionId *current_gtid,
								  MtmReceiverWorkerContext *rwctx);
static void process_remote_insert(StringInfo s, Relation rel);
static void process_remote_update(StringInfo s, Relation rel);
static void process_remote_delete(StringInfo s, Relation rel);

/*
 * Handle critical errors while applying transaction at replica.
 * Such errors should cause shutdown of this cluster node to allow other nodes to continue serving client requests.
 * Other error will be just reported and ignored
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

static bool
find_heap_tuple(TupleData *tup, Relation rel, TupleTableSlot *slot, bool lock)
{
	HeapTuple	tuple;
	HeapScanDesc scan;
	SnapshotData snap;
	TransactionId xwait;
	TupleDesc	tupDesc = RelationGetDescr(rel);
	int			natts = tupDesc->natts;
	Datum	   *values = (Datum *) palloc(natts * sizeof(Datum));
	bool	   *nulls = (bool *) palloc(natts * sizeof(bool));
	bool		found = false;
	int			i;

	InitDirtySnapshot(snap);
	scan = heap_beginscan(rel, &snap, 0, NULL);
retry:
	while ((tuple = heap_getnext(scan, ForwardScanDirection)) != NULL)
	{
		CHECK_FOR_INTERRUPTS();
		heap_deform_tuple(tuple, tupDesc, values, nulls);

		for (i = 0; i < natts; i++)
		{
			Form_pg_attribute att = TupleDescAttr(tupDesc, i);

			if (att->attisdropped)
				/* The case of ALTER TABLE ... DROP ... COLUMN */
				continue;

			if (nulls[i] && tup->isnull[i]) /* both nulls */
				continue;

			else if (nulls[i] ^ tup->isnull[i]) /* one is null and one is not
												 * null */
				break;

			else if (!(att->attlen == -1
					   ? datumIsEqual(PointerGetDatum(heap_tuple_untoast_attr((struct varlena *) DatumGetPointer(tup->values[i]))),
									  PointerGetDatum(heap_tuple_untoast_attr((struct varlena *) DatumGetPointer(values[i]))), att->attbyval, -1)
					   : datumIsEqual(tup->values[i], values[i], att->attbyval, att->attlen)))
				/* Corresponding attributes are not identical */
				break;
		}

		if (i == natts)
		{
			/* FIXME: Improve TupleSlot to not require copying the whole tuple */
			ExecStoreTuple(tuple, slot, InvalidBuffer, false);
			ExecMaterializeSlot(slot);

			xwait = TransactionIdIsValid(snap.xmin) ?
				snap.xmin : snap.xmax;

			if (TransactionIdIsValid(xwait))
			{
				XactLockTableWait(xwait, NULL, NULL, XLTW_None);
				heap_rescan(scan, NULL);
				goto retry;
			}
			found = true;

			if (lock)
			{
				Buffer		buf;
				HeapUpdateFailureData hufd;
				HTSU_Result res;
				HeapTupleData locktup;

				ItemPointerCopy(&slot->tts_tuple->t_self, &locktup.t_self);

				PushActiveSnapshot(GetLatestSnapshot());

				res = heap_lock_tuple(rel, &locktup, GetCurrentCommandId(false), LockTupleExclusive,
									  false /* wait */ ,
									  false /* don't follow updates */ ,
									  &buf, &hufd);
				/* the tuple slot already has the buffer pinned */
				ReleaseBuffer(buf);

				PopActiveSnapshot();

				switch (res)
				{
					case HeapTupleMayBeUpdated:
						break;
					case HeapTupleUpdated:
						/* XXX: Improve handling here */
						ereport(LOG,
								(errcode(ERRCODE_T_R_SERIALIZATION_FAILURE),
								 MTM_ERRMSG("concurrent update, retrying")));
						heap_rescan(scan, NULL);
						goto retry;
					default:
						mtm_log(ERROR, "unexpected HTSU_Result after locking: %u", res);
						break;
				}
			}
			break;
		}
	}
	heap_endscan(scan);
	pfree(values);
	pfree(nulls);

	return found;
}

/*
 * Search the index 'idxrel' for a tuple identified by 'skey' in 'rel'.
 *
 * If a matching tuple is found setup 'tid' to point to it and return true,
 * false is returned otherwise.
 */
static bool
find_pkey_tuple(ScanKey skey, Relation rel, Relation idxrel,
				TupleTableSlot *slot, bool lock, LockTupleMode mode)
{
	HeapTuple	scantuple;
	bool		found;
	IndexScanDesc scan;
	SnapshotData snap;
	TransactionId xwait;

	InitDirtySnapshot(snap);
	scan = index_beginscan(rel, idxrel,
						   &snap,
						   IndexRelationGetNumberOfKeyAttributes(idxrel),
						   0);

retry:
	found = false;

	index_rescan(scan, skey, IndexRelationGetNumberOfKeyAttributes(idxrel), NULL, 0);

	if ((scantuple = index_getnext(scan, ForwardScanDirection)) != NULL)
	{
		found = true;
		/* FIXME: Improve TupleSlot to not require copying the whole tuple */
		ExecStoreTuple(scantuple, slot, InvalidBuffer, false);
		ExecMaterializeSlot(slot);

		xwait = TransactionIdIsValid(snap.xmin) ?
			snap.xmin : snap.xmax;

		if (TransactionIdIsValid(xwait))
		{
			XactLockTableWait(xwait, NULL, NULL, XLTW_None);
			goto retry;
		}
	}

	if (lock && found)
	{
		Buffer		buf;
		HeapUpdateFailureData hufd;
		HTSU_Result res;
		HeapTupleData locktup;

		ItemPointerCopy(&slot->tts_tuple->t_self, &locktup.t_self);

		PushActiveSnapshot(GetLatestSnapshot());

		res = heap_lock_tuple(rel, &locktup, GetCurrentCommandId(false), mode,
							  false /* wait */ ,
							  false /* don't follow updates */ ,
							  &buf, &hufd);
		/* the tuple slot already has the buffer pinned */
		ReleaseBuffer(buf);

		PopActiveSnapshot();

		switch (res)
		{
			case HeapTupleMayBeUpdated:
				break;
			case HeapTupleUpdated:
				/* XXX: Improve handling here */
				ereport(LOG,
						(errcode(ERRCODE_T_R_SERIALIZATION_FAILURE),
						 MTM_ERRMSG("concurrent update, retrying")));
				goto retry;
			default:
				mtm_log(ERROR, "unexpected HTSU_Result after locking: %u", res);
				break;
		}
	}

	index_endscan(scan);

	return found;
}

/*
 * Setup a ScanKey for a search in the relation 'rel' for a tuple 'key' that
 * is setup to match 'rel' (*NOT* idxrel!).
 *
 * Returns whether any column contains NULLs.
 */
static bool
build_index_scan_key(ScanKey skey, Relation rel, Relation idxrel, TupleData *tup)
{
	int			attoff;
	bool		isnull;
	Datum		indclassDatum;
	oidvector  *opclass;
	int2vector *indkey = &idxrel->rd_index->indkey;
	bool		hasnulls = false;

	Assert(RelationGetReplicaIndex(rel) == RelationGetRelid(idxrel));

	indclassDatum = SysCacheGetAttr(INDEXRELID, idxrel->rd_indextuple,
									Anum_pg_index_indclass, &isnull);
	Assert(!isnull);
	opclass = (oidvector *) DatumGetPointer(indclassDatum);

	/* Build scankey for every attribute in the index. */
	for (attoff = 0; attoff < IndexRelationGetNumberOfKeyAttributes(idxrel); attoff++)
	{
		Oid			operator;
		Oid			opfamily;
		RegProcedure regop;
		int			pkattno = attoff + 1;
		int			mainattno = indkey->values[attoff];
		Oid			optype = get_opclass_input_type(opclass->values[attoff]);

		/*
		 * Load the operator info.  We need this to get the equality operator
		 * function for the scan key.
		 */
		opfamily = get_opclass_family(opclass->values[attoff]);

		operator = get_opfamily_member(opfamily, optype,
									   optype,
									   BTEqualStrategyNumber);
		if (!OidIsValid(operator))
			elog(ERROR, "missing operator %d(%u,%u) in opfamily %u",
				 BTEqualStrategyNumber, optype, optype, opfamily);

		regop = get_opcode(operator);

		/* Initialize the scankey. */
		ScanKeyInit(&skey[attoff],
					pkattno,
					BTEqualStrategyNumber,
					regop,
					tup->values[mainattno - 1]);

		/* Check for null value. */
		if (tup->isnull[mainattno - 1])
		{
			hasnulls = true;
			skey[attoff].sk_flags |= SK_ISNULL;
		}
	}

	return hasnulls;
}

static void
UserTableUpdateIndexes(EState *estate, TupleTableSlot *slot)
{
	/* HOT update does not require index inserts */
	if (HeapTupleIsHeapOnly(slot->tts_tuple))
		return;

	ExecOpenIndices(estate->es_result_relation_info, false);
	UserTableUpdateOpenIndexes(estate, slot);
	ExecCloseIndices(estate->es_result_relation_info);
}

static void
UserTableUpdateOpenIndexes(EState *estate, TupleTableSlot *slot)
{
	List	   *recheckIndexes = NIL;

	/* HOT update does not require index inserts */
	if (HeapTupleIsHeapOnly(slot->tts_tuple))
		return;

	if (estate->es_result_relation_info->ri_NumIndices > 0)
	{
		ExecInsertIndexTuples(slot, &slot->tts_tuple->t_self,
							  estate, false, NULL, NIL);
	}

	/* FIXME: recheck the indexes */
	list_free(recheckIndexes);
}

static EState *
create_rel_estate(Relation rel)
{
	EState	   *estate;
	ResultRelInfo *resultRelInfo;

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

	/* Prepare to catch AFTER triggers. */
	AfterTriggerBeginQuery();

	return estate;
}

static bool
process_remote_begin(StringInfo s, volatile GlobalTransactionId *gtid,
					 MtmReceiverWorkerContext *rwctx)
{
	/* there is no need to send this, but since we do, check its sanity */
	gtid->node = pq_getmsgint(s, 4);
	Assert(rwctx->sender_node_id == gtid->node);
	gtid->xid = pq_getmsgint64(s);

	InterruptPending = false;
	QueryCancelPending = false;
	query_cancel_allowed = true;

	pq_getmsgint64(s);
//XXX:snapshot
		pq_getmsgint64(s);
//XXX:participantsMask
		Assert(gtid->node > 0);

	SetCurrentStatementStartTimestamp();

	StartTransactionCommand();

	/* ddd */
	gtid->my_xid = GetCurrentTransactionId();
	// MtmDeadlockDetectorAddXact(gtid->my_xid, gtid);

	suppress_internal_consistency_checks = true;

	return true;
}

static void
process_syncpoint(MtmReceiverWorkerContext *rwctx, const char *msg, XLogRecPtr received_end_lsn)
{
	int			rc;
	int			origin_node;
	XLogRecPtr	origin_lsn,
				local_lsn,
				restart_lsn = InvalidXLogRecPtr,
				trim_lsn;

	Assert(MtmIsReceiver && !MtmIsPoolWorker);

	mtm_log(SyncpointApply, "Syncpoint: await for parallel workers to finish");

	/*
	 * Await for our pool workers to finish what they are currently doing.
	 *
	 * XXX: that is quite performance-sensitive. Much better solution would be
	 * a barried before processing prepare/commit which will delay all
	 * transaction with bigger lsn that this syncpoint but will allow previous
	 * transactions to proceed. This way we will not delay application of
	 * transaction bodies, just prepare record itself.
	 */
	LWLockAcquire(&Mtm->pools[rwctx->sender_node_id - 1].lock, LW_EXCLUSIVE);
	for (;;)
	{
		int			ntasks;

		if (Mtm->pools[rwctx->sender_node_id - 1].nWorkers <= 0)
		{
			LWLockRelease(&Mtm->pools[rwctx->sender_node_id - 1].lock);
			break;
		}

		ntasks = Mtm->pools[rwctx->sender_node_id - 1].active +
			Mtm->pools[rwctx->sender_node_id - 1].pending;
		ConditionVariablePrepareToSleep(&Mtm->pools[rwctx->sender_node_id - 1].syncpoint_cv);
		LWLockRelease(&Mtm->pools[rwctx->sender_node_id - 1].lock);

		Assert(ntasks >= 0);
		if (ntasks == 0)
			break;

		ConditionVariableSleep(&Mtm->pools[rwctx->sender_node_id - 1].syncpoint_cv,
							   PG_WAIT_EXTENSION);
		LWLockAcquire(&Mtm->pools[rwctx->sender_node_id - 1].lock, LW_EXCLUSIVE);
	}
	ConditionVariableCancelSleep();

	/*
	 * Postgres decoding API doesn't disclose origin info about logical
	 * messages, so we have to work around it. Any receiver of original
	 * message writes it in slighly different format (prefixed with 'F' and
	 * origin info) so the readers of forwarded messages can distinguish them
	 * from original messages and set proper node_id and origin_lsn.
	 */
	if (msg[0] == 'F')
	{
		Assert(rwctx->mode == REPLMODE_RECOVERY);

		/* forwarded, parse and save as is */
		rc = sscanf(msg, "F_%d_" UINT64_FORMAT "_" UINT64_FORMAT, &origin_node,
					&origin_lsn, &trim_lsn);
		if (rc != 3)
		{
			Assert(false);
			mtm_log(PANIC, "wrong syncpoint message format");
		}

		/* skip our own syncpoints */
		if (origin_node == Mtm->my_node_id)
			return;

		local_lsn = GetXLogInsertRecPtr();
	}
	else
	{
		char	   *new_msg;

		/* direct, save with it lsn and 'F' prefix */
		origin_lsn = received_end_lsn;
		origin_node = rwctx->sender_node_id;
		rc = sscanf(msg, UINT64_FORMAT, &trim_lsn);
		Assert(rc == 1);
		Assert(origin_node != Mtm->my_node_id);

		/* load restart_lsn before taking local_lsn */
		{
			char	   *slot_name = psprintf(MULTIMASTER_SLOT_PATTERN, origin_node);
			int			i;

			LWLockAcquire(ReplicationSlotControlLock, LW_SHARED);
			for (i = 0; i < max_replication_slots; i++)
			{
				ReplicationSlot *s = &ReplicationSlotCtl->replication_slots[i];

				if (s->in_use && strcmp(slot_name, NameStr(s->data.name)) == 0)
					restart_lsn = s->data.restart_lsn;
			}
			LWLockRelease(ReplicationSlotControlLock);

			pfree(slot_name);
		}

		new_msg = psprintf("F_%d_" UINT64_FORMAT "_" UINT64_FORMAT,
						   origin_node, origin_lsn, trim_lsn);
		local_lsn = LogLogicalMessage("S", new_msg, strlen(new_msg) + 1, false);
		pfree(new_msg);
	}

	Assert(rwctx->mode == REPLMODE_RECOVERY || rwctx->sender_node_id == origin_node);

	SyncpointRegister(origin_node, origin_lsn, local_lsn, restart_lsn, trim_lsn);

	MtmUpdateLsnMapping(origin_node, origin_lsn);
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
				mtm_log(MtmApplyMessage, "Executing non-tx DDL message %s", messageBody);
				SetCurrentStatementStartTimestamp();
				StartTransactionCommand();
				MtmApplyDDLMessage(messageBody, false);
				CommitTransactionCommand();
				//XXX
					standalone = true;
				break;
			}
		case 'D':
			{
				mtm_log(MtmApplyMessage, "Executing tx DDL message %s", messageBody);
				MtmApplyDDLMessage(messageBody, true);
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
				mtm_log(MtmApplyMessage, "Executing syncpoint message from %d: %s",
						rwctx->sender_node_id, messageBody);

				/*
				 * XXX: during recovery end_lsn is wrong, as we receive
				 * forwarded txes
				 */
				process_syncpoint(rwctx, messageBody, record_lsn);

				/* XXX: clear filter_map */
				standalone = true;
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
 * Xact finish can also be sent from here once we get it back.
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
					  volatile  GlobalTransactionId *current_gtid,
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

				rwctx->gtx = GlobalTxAcquire(gid, false, NULL);
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
				 * give vote iff we actually have PREPARE, final outcome is
				 * not known yet and paxos rules allow it.
				 */
				if (rwctx->gtx->prepared &&
					rwctx->gtx->state.status != GTXCommitted &&
					rwctx->gtx->state.status != GTXAborted &&
					term_cmp(msg_term, rwctx->gtx->state.proposal) >= 0)
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
				Assert(false);
				break;
			}
		case PGLOGICAL_PREPARE:
			{
				ListCell   *cur_item,
						   *prev_item;
				TransactionId xid = GetCurrentTransactionIdIfAny();
				uint64 prepare_gen_num;
				bool latch_set = false;
				bool gtx_created;
				char   *sstate;


				Assert(IsTransactionState() && TransactionIdIsValid(xid));
				Assert(xid == current_gtid->my_xid);

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

				rwctx->gtx = GlobalTxAcquire(gid, true, &gtx_created);

				/*
				 * TODO: we could check 'proposal' (aka nextBal in The
				 * Part-Time Parliament) term here: if it is not {1, 0}, we
				 * already started concurrent resolving to abort xact, and it
				 * might be better to tell coordinator to abort immediately
				 * while he can rather than risk exiting later with 'unclear
				 * xact status'.
				 */

				/*
				 * Be careful not to lose vote we could have given/obtained
				 * while xact lived as gtx_proposals entry.
				 *
				 * So we need to 1) write state_3pc, if any, into PREPARE 2)
				 * purge gtx_proposals entry ASAP -- otherwise, we might
				 * proceed, commit/abort xact, crash-reboot and use old stale
				 * state from table (we could have given new votes since state
				 * migrated to PREPARE). This is problematic as we can't both
				 * write PREPARE and delete from table atomically. Purging
				 * gtx_proposals here might fail (it really shouldn't, but we
				 * can't rely on that), so we achieve 2) by maintaining
				 * separate in_table flag and forbidding to finish xact while
				 * it has both in-table entry and prepare record. Resolver
				 * worker periodically searches for such xacts and removes
				 * gtx_entries for them.
				 *
				 */
				sstate = NULL;
				if (!gtx_created)
				{
					sstate = serialize_gtx_state(rwctx->gtx->state.status,
												 rwctx->gtx->state.proposal,
												 rwctx->gtx->state.accepted);
					mtm_log(MtmApplyTrace,
							"migrating transaction %s state %s to PREPARE",
							gid, sstate);
				}
				PrepareTransactionBlockWithState3PC(gid, sstate);
				AllowTempIn2PC = true;
				/* PREPARE itself */
				CommitTransactionCommand();
				MemoryContextSwitchTo(MtmApplyContext);
				ReleasePB();
				rwctx->gtx->prepared = true; /* now we have WAL record */
				rwctx->gtx->coordinator_end_lsn = replorigin_session_origin_lsn;
				mtm_log(MtmTxFinish, "TXFINISH: %s prepared (local_xid=" XID_FMT ")", gid, xid);

				if (!gtx_created)
				{
					Assert(rwctx->gtx->in_table);

					/* prevent logging the same origin_lsn twice */
					MtmEndSession(42, true);
					GlobalTxDeleteFromTable(gid);
					rwctx->gtx->in_table = false;

					/* c.f. GlobalTxRelease why we can't have final states here */
					Assert(rwctx->gtx->state.status != GTXCommitted &&
						   rwctx->gtx->state.status != GTXAborted);
				}

				GlobalTxRelease(rwctx->gtx);
				rwctx->gtx = NULL;

				MemoryContextSwitchTo(MtmApplyContext);

				InterruptPending = false;
				QueryCancelPending = false;
				query_cancel_allowed = false;

				if (rwctx->mode == REPLMODE_NORMAL)
				{
					mtm_send_prepare_reply(current_gtid->xid,
										   rwctx->sender_node_id,
										   true, NULL);
				}

				/*
				 * Clean this after CommitTransactionCommand(), as it may
				 * throw an error that we should propagate to the originating
				 * node.
				 */
				current_gtid->node = 0;
				current_gtid->xid = InvalidTransactionId;
				current_gtid->my_xid = InvalidTransactionId;
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
						"Prepare transaction %s event=%d origin=(%d, " LSN_FMT ")", gid, event,
						origin_node, origin_node == rwctx->sender_node_id ? end_lsn : origin_lsn);

				break;
			}
		case PGLOGICAL_COMMIT_PREPARED:
			{
				Assert(!query_cancel_allowed);
				pq_getmsgint64(in); /* csn */
				strncpy(gid, pq_getmsgstring(in), sizeof gid);

				rwctx->gtx = GlobalTxAcquire(gid, false, NULL);

				/* normal path: we have PREPARE, finish it */
				if (rwctx->gtx && rwctx->gtx->prepared)
				{
					/*
					 * c.f. PREPARE handling why finishing xact while
					 * gtx_proposals entry exists is unsafe
					 */
					if (rwctx->gtx->in_table)
					{
						ereport(ERROR,
								(errcode(ERRCODE_INTERNAL_ERROR),
								 MTM_ERRMSG("can't apply COMMIT PREPARED of transaction %s as its gtx_proposals entry is not deleted yet", gid)));
					}

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
				 * 6) Now C having nothing but P asks B to resolve, who don't
				 *    have any state for the xact anymore; together they
				 *    resolve to aborted.
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
				 * Alternatively we could persist CP outcome in gtx_proposals
				 * table before acking it, but that would bloat the table (and
				 * shmem), and it is not clear how to obtain PREPARE's end_lsn
				 * here to know when we can purge the entry.
				 */
				else if (rwctx->mode == REPLMODE_NORMAL)
				{
					uint64 prepare_gen_num = MtmGidParseGenNum(gid);

					/*
					 * Could wait until we switch into it, but bailing out
					 * here seems like incredibly rare situtation.
					 */
					if (unlikely(MtmGetCurrentGenNum() < prepare_gen_num))
					{
						ereport(ERROR,
								(errcode(ERRCODE_INTERNAL_ERROR),
								 MTM_ERRMSG("refusing COMMIT PREPARED of transaction %s due to generation switch: prepare_gen_num=" UINT64_FORMAT ", current_gen_num=" UINT64_FORMAT,
											gid, prepare_gen_num, MtmGetCurrentGenNum())));
					}

					if (unlikely((rwctx->mode != MtmGetReceiverMode(rwctx->sender_node_id))))
					{
						proc_exit(0);
					}
				}

				/*
				 * Having gtx here means we have in-table state for this xact
				 * instead of PREPARE, but the check above hasn't thrown us
				 * out, so we still managed to get CP out of order somehow.
				 */
				Assert(rwctx->gtx == NULL);

				/* restore ctx after CommitTransaction */
				MemoryContextSwitchTo(MtmApplyContext);

				// if (receiver_ctx->parallel_allowed)
				// {
				// 	mtm_send_gid_reply(gtx, origin_node);
				// }

				break;
			}
		case PGLOGICAL_ABORT_PREPARED:
			{
				strncpy(gid, pq_getmsgstring(in), sizeof gid);

				/*
				 * Unlike CP handling, there is no need to persist
				 * out-of-order abort in gtx_proposals table: if we ask node
				 * about status of xact for which it had purged WAL with
				 * finalization record, she'd reply with direct abort --
				 * because if there was commit, we must have already received
				 * it and the answer doesn't matter.
				 */
				rwctx->gtx = GlobalTxAcquire(gid, false, NULL);
				if (!rwctx->gtx || !rwctx->gtx->prepared)
				{
					/*
					 * We have in-table state for this xact. We could update
					 * it with 'abort', but again, there is no need to.
					 */
					if (rwctx->gtx)
					{
						GlobalTxRelease(rwctx->gtx);
						rwctx->gtx = NULL;
					}
					break;
				}

				/*
				 * c.f. PREPARE handling why finishing xact while
				 * gtx_proposals entry exists is unsafe
				 */
				if (rwctx->gtx->in_table)
				{
					ereport(ERROR,
							(errcode(ERRCODE_INTERNAL_ERROR),
							 MTM_ERRMSG("can't apply ABORT PREPARED of transaction %s as its gtx_proposals entry is not deleted yet", gid)));
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

	if (rwctx->mode == REPLMODE_NORMAL)
	{
		Assert(replorigin_session_origin == InvalidRepOriginId);
		MaybeLogSyncpoint();
	}
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
	HeapTuple	tup;

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
			int			action = pq_getmsgbyte(s);

			if (action != 'I')
				Assert(false);
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

		/*
		 * If there are any indexes, update them for all the inserted tuples,
		 * and run AFTER ROW INSERT triggers.
		 */
		if (relinfo->ri_NumIndices > 0)
		{
			for (i = 0; i < nBufferedTuples; i++)
			{
				List	   *recheckIndexes;

				ExecStoreTuple(bufferedTuples[i], newslot, InvalidBuffer, false);
				recheckIndexes =
					ExecInsertIndexTuples(newslot, &bufferedTuples[i]->t_self,
										  estate, false, NULL, NIL);

				/* AFTER ROW INSERT Triggers */
				if (strcmp(get_namespace_name(RelationGetNamespace(rel)), MULTIMASTER_SCHEMA_NAME) == 0)
				{
					ExecARInsertTriggers(estate, relinfo, bufferedTuples[i],
										 recheckIndexes, NULL);
				}

				list_free(recheckIndexes);
			}
		}

		FreeBulkInsertState(bistate);
	}
	else
	{
		MemoryContext oldctx;
		TriggerDesc *tmp;

		/* Insert single tuple */
		oldctx = MemoryContextSwitchTo(GetPerTupleMemoryContext(estate));
		tup = heap_form_tuple(tupDesc, new_tuple.values, new_tuple.isnull);
		ExecStoreTuple(tup, newslot, InvalidBuffer, true);
		MemoryContextSwitchTo(oldctx);

		tmp = estate->es_result_relation_info->ri_TrigDesc;
		estate->es_result_relation_info->ri_TrigDesc = NULL;
		ExecSimpleRelationInsert(estate, newslot);
		estate->es_result_relation_info->ri_TrigDesc = tmp;

		/* AFTER ROW INSERT Triggers */
		if (strcmp(get_namespace_name(RelationGetNamespace(rel)),
				   MULTIMASTER_SCHEMA_NAME) == 0)
		{
			ExecARInsertTriggers(estate, relinfo, newslot->tts_tuple,
								 NIL, NULL);
		}
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
	TupleTableSlot *newslot;
	TupleTableSlot *oldslot;
	bool		pkey_sent;
	bool		found_tuple;
	TupleData	old_tuple;
	TupleData	new_tuple;
	Oid			idxoid = InvalidOid;
	Relation	idxrel = NULL;
	TupleDesc	tupDesc = RelationGetDescr(rel);
	ScanKeyData skey[INDEX_MAX_KEYS];
	HeapTuple	remote_tuple = NULL;

	action = pq_getmsgbyte(s);

	/* old key present, identifying key changed */
	if (action != 'K' && action != 'N')
		mtm_log(ERROR, "expected action 'N' or 'K', got %c",
				action);

	estate = create_rel_estate(rel);
	oldslot = ExecInitExtraTupleSlot(estate, tupDesc);
	newslot = ExecInitExtraTupleSlot(estate, tupDesc);

	if (action == 'K')
	{
		pkey_sent = true;
		read_tuple_parts(s, rel, &old_tuple);
		action = pq_getmsgbyte(s);
	}
	else
		pkey_sent = false;

	/* check for new  tuple */
	if (action != 'N')
		mtm_log(ERROR, "expected action 'N', got %c",
				action);

	if (rel->rd_rel->relkind != RELKIND_RELATION)
		mtm_log(ERROR, "unexpected relkind '%c' rel \"%s\"",
				rel->rd_rel->relkind, RelationGetRelationName(rel));

	/* read new tuple */
	read_tuple_parts(s, rel, &new_tuple);

	/* lookup index to build scankey */
	if (rel->rd_indexvalid == 0)
		RelationGetIndexList(rel);
	idxoid = rel->rd_replidindex;

	PushActiveSnapshot(GetTransactionSnapshot());

	if (!OidIsValid(idxoid))
		found_tuple = find_heap_tuple(pkey_sent ? &old_tuple : &new_tuple, rel, oldslot, true);
	else
	{
		/* open index, so we can build scan key for row */
		idxrel = index_open(idxoid, RowExclusiveLock);

		Assert(idxrel->rd_index->indisunique);

		/* Use columns from the new tuple if the key didn't change. */
		build_index_scan_key(skey, rel, idxrel,
							 pkey_sent ? &old_tuple : &new_tuple);

		/* look for tuple identified by the (old) primary key */
		found_tuple = find_pkey_tuple(skey, rel, idxrel, oldslot, true,
									  pkey_sent ? LockTupleExclusive : LockTupleNoKeyExclusive);

	}
	if (found_tuple)
	{
		remote_tuple = heap_modify_tuple(oldslot->tts_tuple,
										 tupDesc,
										 new_tuple.values,
										 new_tuple.isnull,
										 new_tuple.changed);

		ExecStoreTuple(remote_tuple, newslot, InvalidBuffer, true);

#ifdef VERBOSE_UPDATE
		{
			StringInfoData o;

			initStringInfo(&o);
			tuple_to_stringinfo(&o, tupDesc, oldslot->tts_tuple, false);
			appendStringInfo(&o, " to");
			tuple_to_stringinfo(&o, tupDesc, remote_tuple, false);
			mtm_log(LOG, "%lu: UPDATE: %s", GetCurrentTransactionId(), o.data);
			resetStringInfo(&o);
		}
#endif

		simple_heap_update(rel, &oldslot->tts_tuple->t_self, newslot->tts_tuple);
		UserTableUpdateIndexes(estate, newslot);
	}
	else
	{
		ereport(ERROR,
				(errcode(ERRCODE_NO_DATA_FOUND),
				 MTM_ERRMSG("Record with specified key can not be located at this node"),
				 errdetail("Most likely we have DELETE-UPDATE conflict")));
	}

	/* release locks upon commit */
	if (OidIsValid(idxoid))
		index_close(idxrel, NoLock);

	PopActiveSnapshot();

	/* Handle queued AFTER triggers. */
	AfterTriggerEndQuery(estate);

	ExecResetTupleTable(estate->es_tupleTable, true);
	FreeExecutorState(estate);

	CommandCounterIncrement();
}

static void
process_remote_delete(StringInfo s, Relation rel)
{
	EState	   *estate;
	TupleData	oldtup;
	TupleTableSlot *oldslot;
	Oid			idxoid = InvalidOid;
	Relation	idxrel = NULL;
	TupleDesc	tupDesc = RelationGetDescr(rel);
	ScanKeyData skey[INDEX_MAX_KEYS];
	bool		found_old;

	estate = create_rel_estate(rel);
	oldslot = ExecInitExtraTupleSlot(estate, NULL);
	ExecSetSlotDescriptor(oldslot, tupDesc);

	read_tuple_parts(s, rel, &oldtup);

	/* lookup index to build scankey */
	if (rel->rd_indexvalid == 0)
		RelationGetIndexList(rel);
	idxoid = rel->rd_replidindex;

	PushActiveSnapshot(GetTransactionSnapshot());

	if (!OidIsValid(idxoid))
		found_old = find_heap_tuple(&oldtup, rel, oldslot, true);
	else
	{
		/* Now open the primary key index */
		idxrel = index_open(idxoid, RowExclusiveLock);

		if (rel->rd_rel->relkind != RELKIND_RELATION)
			mtm_log(ERROR, "unexpected relkind '%c' rel \"%s\"",
					rel->rd_rel->relkind, RelationGetRelationName(rel));

#ifdef VERBOSE_DELETE
		{
			HeapTuple	tup;

			tup = heap_form_tuple(tupDesc,
								  oldtup.values, oldtup.isnull);
			ExecStoreTuple(tup, oldslot, InvalidBuffer, true);
		}
		log_tuple("DELETE old-key:%s", tupDesc, oldslot->tts_tuple);
#endif

		build_index_scan_key(skey, rel, idxrel, &oldtup);

		/* try to find tuple via a (candidate|primary) key */
		found_old = find_pkey_tuple(skey, rel, idxrel, oldslot, true, LockTupleExclusive);
	}
	if (found_old)
	{
		simple_heap_delete(rel, &oldslot->tts_tuple->t_self);

		/* AFTER ROW DELETE Triggers */
		if (strcmp(get_namespace_name(RelationGetNamespace(rel)), MULTIMASTER_SCHEMA_NAME) == 0)
		{
			ExecARDeleteTriggers(estate, estate->es_result_relation_info,
								 &oldslot->tts_tuple->t_self, NULL, NULL);
		}
	}
	else
	{
		ereport(ERROR,
				(errcode(ERRCODE_NO_DATA_FOUND),
				 MTM_ERRMSG("Record with specified key can not be located at this node"),
				 errdetail("Most likely we have DELETE-DELETE conflict")));
	}

	PopActiveSnapshot();

	/* Handle queued AFTER triggers. */
	AfterTriggerEndQuery(estate);

	if (OidIsValid(idxoid))
		index_close(idxrel, NoLock);

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
	MemoryContext top_context;
	volatile GlobalTransactionId current_gtid = {0, InvalidTransactionId, InvalidTransactionId};

	s.data = work;
	s.len = size;
	s.maxlen = -1;
	s.cursor = 0;

	if (MtmApplyContext == NULL)
		MtmApplyContext = AllocSetContextCreate(TopMemoryContext,
												"ApplyContext",
												ALLOCSET_DEFAULT_SIZES);

	top_context = MemoryContextSwitchTo(MtmApplyContext);
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

		do
		{
			char		action = pq_getmsgbyte(&s);

			old_context = MemoryContextSwitchTo(MtmApplyContext);
			mtm_log(MtmApplyTrace, "got action '%c'", action);

			switch (action)
			{
					/* BEGIN */
				case 'B':
					inside_transaction = process_remote_begin(&s, &current_gtid,
															  rwctx);
					break;
					/* COMMIT */
				case 'C':
					close_rel(rel);
					process_remote_commit(&s, &current_gtid, rwctx);
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
				case '(':
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
				case ')':
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
			MemoryContextSwitchTo(old_context);
			MemoryContextResetAndDeleteChildren(MtmApplyContext);
		} while (inside_transaction);
	}
	PG_CATCH();
	{
		ErrorData  *edata;

		query_cancel_allowed = false;

		old_context = MemoryContextSwitchTo(MtmApplyContext);
		MtmHandleApplyError();
		edata = CopyErrorData();

		/*
		 * handle only prepare errors here
		 * XXX reply to 2a also, though its failure is unlikely
		 */
		if (TransactionIdIsValid(current_gtid.my_xid))
		{
			// MtmDeadlockDetectorRemoveXact(current_gtid.my_xid);
			if (rwctx->mode == REPLMODE_NORMAL)
				mtm_send_prepare_reply(current_gtid.xid,
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
		 */
		if (rwctx->mode == REPLMODE_RECOVERY ||
			!TransactionIdIsValid(current_gtid.my_xid))
		{
			if (rwctx->mode == REPLMODE_RECOVERY)
				mtm_log(WARNING, "got ERROR while applying in recovery, xid=" XID_FMT,
					current_gtid.my_xid);
			PG_RE_THROW();
		}

		MemoryContextSwitchTo(old_context);
		EmitErrorReport();
		FlushErrorState();
		ReleasePB();
		if (rwctx->gtx != NULL)
		{
			GlobalTxRelease(rwctx->gtx);
			rwctx->gtx = NULL;
		}
		MtmEndSession(42, false);

		MtmDDLResetApplyState();

		mtm_log(MtmApplyError, "Receiver: abort transaction " XID_FMT,
				current_gtid.xid);

		FreeErrorData(edata);
		AbortCurrentTransaction();
	}
	PG_END_TRY();

	if (s.data != work)
		pfree(s.data);
	MemoryContextSwitchTo(top_context);
	MemoryContextResetAndDeleteChildren(MtmApplyContext);
}
