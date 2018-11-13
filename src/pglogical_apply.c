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

#include "catalog/catversion.h"
#include "catalog/dependency.h"
#include "catalog/index.h"
#include "catalog/heap.h"
#include "catalog/namespace.h"
#include "catalog/pg_type.h"

#include "executor/spi.h"
#include "commands/vacuum.h"
#include "commands/tablespace.h"
#include "commands/defrem.h"
#include "commands/sequence.h"
#include "parser/parse_utilcmd.h"

#include "libpq/pqformat.h"

#include "mb/pg_wchar.h"

#include "parser/parse_type.h"

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
#include "parser/parse_relation.h"

#include "multimaster.h"
#include "mm.h"
#include "ddd.h"
#include "pglogical_relid_map.h"
#include "spill.h"
#include "state.h"
#include "logger.h"
#include "ddl.h"
#include "receiver.h"

typedef struct TupleData
{
	Datum		values[MaxTupleAttributeNumber];
	bool		isnull[MaxTupleAttributeNumber];
	bool		changed[MaxTupleAttributeNumber];
} TupleData;


static Relation read_rel(StringInfo s, LOCKMODE mode);
static void read_tuple_parts(StringInfo s, Relation rel, TupleData *tup);
static EState* create_rel_estate(Relation rel);
static bool find_pkey_tuple(ScanKey skey, Relation rel, Relation idxrel,
                            TupleTableSlot *slot, bool lock, LockTupleMode mode);
static bool find_heap_tuple(TupleData *tup, Relation rel, TupleTableSlot *slot, bool lock);
static void build_index_scan_keys(EState *estate, ScanKey *scan_keys, TupleData *tup);
static bool build_index_scan_key(ScanKey skey, Relation rel, Relation idxrel, TupleData *tup);
static void UserTableUpdateOpenIndexes(EState *estate, TupleTableSlot *slot);
static void UserTableUpdateIndexes(EState *estate, TupleTableSlot *slot);

static bool process_remote_begin(StringInfo s, GlobalTransactionId *gtid);
static bool process_remote_message(StringInfo s, MtmReceiverContext *receiver_ctx);
static void process_remote_commit(StringInfo s, GlobalTransactionId *current_gtid, MtmReceiverContext *receiver_ctx);
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
	ErrorData *edata = CopyErrorData();

	switch (edata->sqlerrcode) {
		case ERRCODE_DISK_FULL:
		case ERRCODE_INSUFFICIENT_RESOURCES:
		case ERRCODE_IO_ERROR:
		case ERRCODE_DATA_CORRUPTED:
		case ERRCODE_INDEX_CORRUPTED:
		  /* Should we really treate this errors as fatal?
		case ERRCODE_SYSTEM_ERROR:
		case ERRCODE_INTERNAL_ERROR:
		case ERRCODE_OUT_OF_MEMORY:
		  */
			MtmStateProcessEvent(MTM_NONRECOVERABLE_ERROR, false);
	}
	FreeErrorData(edata);
}


bool find_heap_tuple(TupleData *tup, Relation rel, TupleTableSlot *slot, bool lock)
{
	HeapTuple	  tuple;
	HeapScanDesc  scan;
	SnapshotData  snap;
	TransactionId xwait;
	TupleDesc     tupDesc =  RelationGetDescr(rel);
	int           natts = tupDesc->natts;
	Datum*        values = (Datum*)palloc(natts*sizeof(Datum));
	bool*         nulls = (bool*)palloc(natts*sizeof(bool));
	bool		  found = false;
	int           i;

	InitDirtySnapshot(snap);
	scan = heap_beginscan(rel,
						  &snap,
						  0,
						  NULL);
retry:
	while ((tuple = heap_getnext(scan, ForwardScanDirection)) != NULL)
	{
		CHECK_FOR_INTERRUPTS();
		heap_deform_tuple(tuple, tupDesc, values, nulls);

		for (i = 0; i < natts; i++)
		{
			Form_pg_attribute att = TupleDescAttr(tupDesc, i);
			if (nulls[i] && tup->isnull[i]) /* both nulls */
				continue;
			else if (nulls[i] ^ tup->isnull[i]) /* one is null and one is not null */
				break;
			else if (!(att->attlen == -1
					   ? datumIsEqual(PointerGetDatum(PG_DETOAST_DATUM_PACKED(tup->values[i])), PointerGetDatum(PG_DETOAST_DATUM_PACKED(values[i])), att->attbyval, -1)
					   : datumIsEqual(tup->values[i], values[i], att->attbyval, att->attlen)))
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
				Buffer buf;
				HeapUpdateFailureData hufd;
				HTSU_Result res;
				HeapTupleData locktup;

				ItemPointerCopy(&slot->tts_tuple->t_self, &locktup.t_self);

				PushActiveSnapshot(GetLatestSnapshot());

				res = heap_lock_tuple(rel, &locktup, GetCurrentCommandId(false), LockTupleExclusive,
									  false /* wait */,
									  false /* don't follow updates */,
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
bool
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
		Buffer buf;
		HeapUpdateFailureData hufd;
		HTSU_Result res;
		HeapTupleData locktup;

		ItemPointerCopy(&slot->tts_tuple->t_self, &locktup.t_self);

		PushActiveSnapshot(GetLatestSnapshot());

		res = heap_lock_tuple(rel, &locktup, GetCurrentCommandId(false), mode,
							  false /* wait */,
							  false /* don't follow updates */,
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

static void
build_index_scan_keys(EState *estate, ScanKey *scan_keys, TupleData *tup)
{
	ResultRelInfo *relinfo;
	int i;

	relinfo = estate->es_result_relation_info;

	/* build scankeys for each index */
	for (i = 0; i < relinfo->ri_NumIndices; i++)
	{
		IndexInfo  *ii = relinfo->ri_IndexRelationInfo[i];

		/*
		 * Only unique indexes are of interest here, and we can't deal with
		 * expression indexes so far. FIXME: predicates should be handled
		 * better.
		 */
		if (!ii->ii_Unique || ii->ii_Expressions != NIL)
		{
			scan_keys[i] = NULL;
			continue;
		}

		scan_keys[i] = palloc(ii->ii_NumIndexAttrs * sizeof(ScanKeyData));

		/*
		 * Only return index if we could build a key without NULLs.
		 */
		if (build_index_scan_key(scan_keys[i],
								  relinfo->ri_RelationDesc,
								  relinfo->ri_IndexRelationDescs[i],
								  tup))
		{
			pfree(scan_keys[i]);
			scan_keys[i] = NULL;
			continue;
		}
	}
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
	Datum		indclassDatum;
	Datum		indkeyDatum;
	bool		isnull;
	oidvector  *opclass;
	int2vector  *indkey;
	bool		hasnulls = false;

	indclassDatum = SysCacheGetAttr(INDEXRELID, idxrel->rd_indextuple,
									Anum_pg_index_indclass, &isnull);
	Assert(!isnull);
	opclass = (oidvector *) DatumGetPointer(indclassDatum);

	indkeyDatum = SysCacheGetAttr(INDEXRELID, idxrel->rd_indextuple,
									Anum_pg_index_indkey, &isnull);
	Assert(!isnull);
	indkey = (int2vector *) DatumGetPointer(indkeyDatum);


	for (attoff = 0; attoff < IndexRelationGetNumberOfKeyAttributes(idxrel); attoff++)
	{
		Oid			operator;
		Oid			opfamily;
		RegProcedure regop;
		int			pkattno = attoff + 1;
		int			mainattno = indkey->values[attoff];
		Oid			atttype = attnumTypeId(rel, mainattno);
		Oid			optype = get_opclass_input_type(opclass->values[attoff]);

		opfamily = get_opclass_family(opclass->values[attoff]);

		operator = get_opfamily_member(opfamily, optype,
									   optype,
									   BTEqualStrategyNumber);

		if (!OidIsValid(operator))
			mtm_log(ERROR,
				 "could not lookup equality operator for type %u, optype %u in opfamily %u",
				 atttype, optype, opfamily);

		regop = get_opcode(operator);

		/* FIXME: convert type? */
		ScanKeyInit(&skey[attoff],
					pkattno,
					BTEqualStrategyNumber,
					regop,
					tup->values[mainattno - 1]);

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
		recheckIndexes = ExecInsertIndexTuples(slot,
											   &slot->tts_tuple->t_self,
											   estate, false, NULL, NIL);

		if (recheckIndexes != NIL)
			ereport(ERROR,
					(errcode(ERRCODE_FEATURE_NOT_SUPPORTED),
					 MTM_ERRMSG("bdr doesn't support index rechecks")));
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
	resultRelInfo->ri_RangeTableIndex = 1;		/* dummy */
	resultRelInfo->ri_RelationDesc = rel;
	resultRelInfo->ri_TrigInstrument = NULL;

	estate->es_result_relations = resultRelInfo;
	estate->es_num_result_relations = 1;
	estate->es_result_relation_info = resultRelInfo;

	return estate;
}

static bool
process_remote_begin(StringInfo s, GlobalTransactionId *gtid)
{
	TransactionId xid;

	gtid->node = pq_getmsgint(s, 4);
	gtid->xid = pq_getmsgint64(s);

	// XXX: get rid of MtmReplicationNodeId
	MtmReplicationNodeId = gtid->node;

	pq_getmsgint64(s); // XXX: snapshot
	pq_getmsgint64(s); // XXX: participantsMask
	Assert(gtid->node > 0);

	MtmResetTransaction();
	SetCurrentStatementStartTimestamp();

	StartTransactionCommand();

	/* ddd */
	xid = GetCurrentTransactionId();
	MtmDeadlockDetectorAddXact(xid, gtid);

	return true;
}

static bool
process_remote_message(StringInfo s, MtmReceiverContext *receiver_ctx)
{
	char action = pq_getmsgbyte(s);
	int messageSize = pq_getmsgint(s, 4);
	char const* messageBody = pq_getmsgbytes(s, messageSize);
	bool standalone = false;

	switch (action)
	{
		case 'C':
		{
			mtm_log(MtmApplyMessage, "Executing non-tx DDL message %s", messageBody);
			SetCurrentStatementStartTimestamp();
			MtmResetTransaction();
			StartTransactionCommand();
			MtmApplyDDLMessage(messageBody);
			CommitTransactionCommand(); // XXX
			standalone = true;
			break;
		}
		case 'D':
		{
			mtm_log(MtmApplyMessage, "Executing tx DDL message %s", messageBody);
			MtmApplyDDLMessage(messageBody);
			break;
		}
		case 'L':
		{
			mtm_log(MtmApplyMessage, "Executing deadlock message from %d", MtmReplicationNodeId);
			MtmUpdateLockGraph(MtmReplicationNodeId, messageBody, messageSize);
			standalone = true;
			break;
		}
		case 'P':
		{
			int node_id = -1;

			sscanf(messageBody, "%d", &node_id);

			Assert(node_id > 0);
			// XXX assert that it is receiver itself

			if (!receiver_ctx->is_recovery && node_id == MtmNodeId)
			{
				Assert(!receiver_ctx->parallel_allowed);
				Assert(receiver_ctx->node_id > 0);
				Assert(receiver_ctx->node_id == MtmReplicationNodeId);
				Assert(receiver_ctx->node_id != node_id);

				receiver_ctx->parallel_allowed = true;
				MtmStateProcessNeighborEvent(MtmReplicationNodeId, MTM_NEIGHBOR_WAL_RECEIVER_START, false);
			}
			standalone = true;
			break;
		}
		default:
			Assert(false);
 	}
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

		if (att->atttypid == InvalidOid) { 
			continue;
		}

		kind = pq_getmsgbyte(s);

		switch (kind)
		{
			case 'n': /* null */
				/* already marked as null */
				tup->values[i] = 0xdeadbeef;
				break;
			case 'u': /* unchanged column */
				tup->isnull[i] = true;
				tup->changed[i] = false;
				tup->values[i] = 0xdeadbeef; /* make bad usage more obvious */
				break;

			case 'b': /* binary format */
				tup->isnull[i] = false;
				len = pq_getmsgint(s, 4); /* read length */

				data = pq_getmsgbytes(s, len);

				/* and data */
				if (att->attbyval)
					tup->values[i] = fetch_att(data, true, len);
				else
					tup->values[i] = PointerGetDatum(data);
				break;
			case 's': /* send/recv format */
				{
					Oid typreceive;
					Oid typioparam;
					StringInfoData buf;

					tup->isnull[i] = false;
					len = pq_getmsgint(s, 4); /* read length */

					getTypeBinaryInputInfo(att->atttypid,
										   &typreceive, &typioparam);

					/* create StringInfo pointing into the bigger buffer */
					initStringInfo(&buf);
					/* and data */
					buf.data = (char *) pq_getmsgbytes(s, len);
					buf.len = len;
					tup->values[i] = OidReceiveFunctionCall(
						typreceive, &buf, typioparam, att->atttypmod);

					if (buf.len != buf.cursor)
						ereport(ERROR,
								(errcode(ERRCODE_INVALID_BINARY_REPRESENTATION),
								 MTM_ERRMSG("incorrect binary data format")));
					break;
				}
			case 't': /* text format */
				{
					Oid typinput;
					Oid typioparam;

					tup->isnull[i] = false;
					len = pq_getmsgint(s, 4); /* read length */

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
	RangeVar*	rv;
	Oid			remote_relid = pq_getmsgint(s, 4);
	Oid         local_relid;
	MemoryContext old_context;

	local_relid = pglogical_relid_map_get(remote_relid);
	if (local_relid == InvalidOid) { 
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
	} else { 
		nspnamelen = pq_getmsgbyte(s);
		s->cursor += nspnamelen;
		relnamelen = pq_getmsgbyte(s);
		s->cursor += relnamelen;
		return heap_open(local_relid, mode);
	}
}

/* send response to coordinator after PREPARE */
static void
mtm_send_reply(TransactionId xid, int node_id, MtmMessageCode msg_code)
{
	DmqDestinationId dest_id = Mtm->nodes[node_id-1].destination_id;
	MtmArbiterMessage msg;

	MtmInitMessage(&msg, msg_code);
	msg.dxid = xid;
	Assert(MtmReplicationNodeId == node_id);
	Assert(TransactionIdIsValid(xid));

	dmq_push_buffer(dest_id, psprintf("xid" XID_FMT, msg.dxid),
					&msg, sizeof(MtmArbiterMessage));

	mtm_log(MtmApplyTrace, "MtmFollowerSendReply: %s to node%d (dest %d)", msg.gid, node_id, dest_id);
}

static void
process_remote_commit(StringInfo in, GlobalTransactionId *current_gtid, MtmReceiverContext *receiver_ctx)
{
	uint8 		event;
	lsn_t       end_lsn;
	lsn_t       origin_lsn;
	lsn_t       commit_lsn;
	int         origin_node;
	char        gid[GIDSIZE];

	gid[0] = '\0';

	/* read event */
	event = pq_getmsgbyte(in);
	MtmReplicationNodeId = pq_getmsgbyte(in);

	/* read fields */
	commit_lsn = pq_getmsgint64(in); /* commit_lsn */
	end_lsn = pq_getmsgint64(in); /* end_lsn */
	replorigin_session_origin_timestamp = pq_getmsgint64(in); /* commit_time */

	origin_node = pq_getmsgbyte(in);
	origin_lsn = pq_getmsgint64(in);

	replorigin_session_origin_lsn = origin_node == MtmReplicationNodeId ? end_lsn : origin_lsn;
	Assert(replorigin_session_origin == InvalidRepOriginId);

	switch (event)
	{
		case PGLOGICAL_PRECOMMIT_PREPARED:
		{
			strncpy(gid, pq_getmsgstring(in), sizeof gid);
			MtmBeginSession(origin_node);

			if (!IsTransactionState()) {
				MtmResetTransaction();
				StartTransactionCommand();
				SetPreparedTransactionState(gid, MULTIMASTER_PRECOMMITTED);
				CommitTransactionCommand();
			} else {
				SetPreparedTransactionState(gid, MULTIMASTER_PRECOMMITTED);
			}
			mtm_log(MtmTxFinish, "TXFINISH: %s precommitted", gid);

			if (receiver_ctx->parallel_allowed)
			{
				TransactionId origin_xid = MtmGidParseXid(gid);
				mtm_send_reply(origin_xid, origin_node, MSG_PRECOMMITTED);
			}

			MtmEndSession(origin_node, true);

			mtm_log(MtmApplyTrace, "PGLOGICAL_PRECOMMIT %s", gid);
			return;
		}
		case PGLOGICAL_COMMIT:
		{
			// XXX: investigate
			if (IsTransactionState())
			{
				MtmBeginSession(origin_node);
				CommitTransactionCommand();
				MtmEndSession(origin_node, true);
			}
			mtm_log(LOG, "PGLOGICAL_COMMIT (%llx,%llx,%llx)", commit_lsn, end_lsn, origin_lsn);
			break;
		}
		case PGLOGICAL_PREPARE:
		{
			bool res;
			TransactionId xid = GetCurrentTransactionIdIfAny();

			Assert(IsTransactionState() && TransactionIdIsValid(xid));

			strncpy(gid, pq_getmsgstring(in), sizeof gid);

			MtmBeginSession(origin_node);

			/* prepare TBLOCK_INPROGRESS state for PrepareTransactionBlock() */
			BeginTransactionBlock(false);
			CommitTransactionCommand();
			StartTransactionCommand();

			/* PREPARE itself */
			res = PrepareTransactionBlock(gid);
			mtm_log(MtmTxFinish, "TXFINISH: %s prepared", gid);
			CommitTransactionCommand();

			MtmDeadlockDetectorRemoveXact(xid);

			if (receiver_ctx->parallel_allowed)
			{
				TransactionId origin_xid = MtmGidParseXid(gid);
				mtm_send_reply(origin_xid, origin_node, res ? MSG_PREPARED : MSG_ABORTED);
			}

			MtmEndSession(origin_node, true);

			mtm_log(MtmApplyTrace, "PGLOGICAL_PREPARE %s", gid);

			current_gtid->node = 0;
			current_gtid->xid = InvalidTransactionId;
			break;
		}
		case PGLOGICAL_COMMIT_PREPARED:
		{
			Assert(!TransactionIdIsValid(MtmGetCurrentTransactionId()));
			pq_getmsgint64(in); /* csn */
			strncpy(gid, pq_getmsgstring(in), sizeof gid);
			MtmResetTransaction();
			StartTransactionCommand();
			MtmBeginSession(origin_node);
			MtmSetCurrentTransactionCSN();
			MtmSetCurrentTransactionGID(gid, origin_node);
			FinishPreparedTransaction(gid, true, false);
			mtm_log(MtmTxFinish, "TXFINISH: %s committed", gid);
			CommitTransactionCommand();

			if (receiver_ctx->parallel_allowed)
			{
				TransactionId origin_xid = MtmGidParseXid(gid);
				mtm_send_reply(origin_xid, origin_node, MSG_COMMITTED);
			}

			MtmEndSession(origin_node, true);

			mtm_log(MtmApplyTrace, "PGLOGICAL_COMMIT_PREPARED %s", gid);
			break;
		}
		case PGLOGICAL_ABORT_PREPARED:
		{
			strncpy(gid, pq_getmsgstring(in), sizeof gid);

			StartTransactionCommand();
			FinishPreparedTransaction(gid, false, true);
			mtm_log(MtmTxFinish, "TXFINISH: %s aborted", gid);
			CommitTransactionCommand();
			mtm_log(MtmApplyTrace, "PGLOGICAL_ABORT_PREPARED %s", gid);
			break;
		}
		default:
			Assert(false);
	}

	if (!receiver_ctx->parallel_allowed)
	{
		// XXX
		elog(LOG, "Recover transaction %s event=%d", gid, event);
	}

	MtmUpdateLsnMapping(MtmReplicationNodeId, end_lsn);
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
	EState *estate;
	TupleData new_tuple;
	TupleTableSlot *newslot;
	TupleTableSlot *oldslot;
	ResultRelInfo *relinfo;
	ScanKey	*index_keys;
	int	i;
	TupleDesc tupDesc = RelationGetDescr(rel);
	HeapTuple tup;

	PushActiveSnapshot(GetTransactionSnapshot());

	estate = create_rel_estate(rel);
	newslot = ExecInitExtraTupleSlot(estate, NULL);
	oldslot = ExecInitExtraTupleSlot(estate, NULL);
	ExecSetSlotDescriptor(newslot, tupDesc);
	ExecSetSlotDescriptor(oldslot, tupDesc);

	ExecOpenIndices(estate->es_result_relation_info, false);
	relinfo = estate->es_result_relation_info;

	read_tuple_parts(s, rel, &new_tuple);

	if (pq_peekmsgbyte(s) == 'I')
	{
		/* Use bulk insert */
		BulkInsertState bistate = GetBulkInsertState();
		HeapTuple bufferedTuples[MAX_BUFFERED_TUPLES];
		MemoryContext oldcontext;
		int nBufferedTuples = 1;
		size_t bufferedTuplesSize;
		CommandId	mycid = GetCurrentCommandId(true);

		bufferedTuples[0] = heap_form_tuple(tupDesc, new_tuple.values, new_tuple.isnull);
		bufferedTuplesSize = bufferedTuples[0]->t_len;

		while (nBufferedTuples < MAX_BUFFERED_TUPLES && bufferedTuplesSize < MAX_BUFFERED_TUPLES_SIZE)
		{
			int action = pq_getmsgbyte(s);
			Assert(action == 'I');
			read_tuple_parts(s, rel, &new_tuple);
			bufferedTuples[nBufferedTuples] = heap_form_tuple(tupDesc,
															  new_tuple.values, new_tuple.isnull);
			bufferedTuplesSize += bufferedTuples[nBufferedTuples++]->t_len;
			if (pq_peekmsgbyte(s) != 'I')
				break;
		}
		/*
		 * heap_multi_insert leaks memory, so switch to short-lived memory context
		 * before calling it.
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
		 * If there are any indexes, update them for all the inserted tuples, and
		 * run AFTER ROW INSERT triggers.
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
				list_free(recheckIndexes);
			}
		}

	    FreeBulkInsertState(bistate);
	} else {
        /* Insert single tuple */

	    tup = heap_form_tuple(tupDesc,
							  new_tuple.values, new_tuple.isnull);
		ExecStoreTuple(tup, newslot, InvalidBuffer, true);

		// if (rel->rd_rel->relkind != RELKIND_RELATION) // RELKIND_MATVIEW
		// 	mtm_log(ERROR, "unexpected relkind '%c' rel \"%s\"",
		// 		 rel->rd_rel->relkind, RelationGetRelationName(rel));

		/* debug output */
#ifdef VERBOSE_INSERT
		log_tuple("INSERT:%s", tupDesc, newslot->tts_tuple);
#endif

		/*
		 * Search for conflicting tuples.
		 */
		index_keys = palloc0(relinfo->ri_NumIndices * sizeof(ScanKeyData*));

		build_index_scan_keys(estate, index_keys, &new_tuple);

		/* do a SnapshotDirty search for conflicting tuples */
		for (i = 0; i < relinfo->ri_NumIndices; i++)
		{
			IndexInfo  *ii = relinfo->ri_IndexRelationInfo[i];
			bool found = false;

			/*
			 * Only unique indexes are of interest here, and we can't deal with
			 * expression indexes so far. FIXME: predicates should be handled
			 * better.
			 *
			 * NB: Needs to match expression in build_index_scan_key
			 */
			if (!ii->ii_Unique || ii->ii_Expressions != NIL)
				continue;

			if (index_keys[i] == NULL)
				continue;

			Assert(ii->ii_Expressions == NIL);

			/* if conflict: wait */
			found = find_pkey_tuple(index_keys[i],
									rel, relinfo->ri_IndexRelationDescs[i],
									oldslot, true, LockTupleExclusive);
			/* alert if there's more than one conflicting unique key */
			if (found)
			{
				/* TODO: Report tuple identity in log */
				ereport(ERROR,
						(errcode(ERRCODE_UNIQUE_VIOLATION),
						 MTM_ERRMSG("Unique constraints violated by remotely INSERTed tuple in %s", RelationGetRelationName(rel)),
						 errdetail("Cannot apply transaction because remotely INSERTed tuple conflicts with a local tuple on UNIQUE constraint and/or PRIMARY KEY")));
			}
			CHECK_FOR_INTERRUPTS();
		}

		simple_heap_insert(rel, newslot->tts_tuple);
		UserTableUpdateOpenIndexes(estate, newslot);

	}
	ExecCloseIndices(estate->es_result_relation_info);
	if (ActiveSnapshotSet())
		PopActiveSnapshot();

	// XXX: maybe just insert it during extension creation?
	if (strcmp(RelationGetRelationName(rel), MULTIMASTER_LOCAL_TABLES_TABLE) == 0 &&
		strcmp(get_namespace_name(RelationGetNamespace(rel)), MULTIMASTER_SCHEMA_NAME) == 0)
	{
		MtmMakeTableLocal((char*)DatumGetPointer(new_tuple.values[0]), (char*)DatumGetPointer(new_tuple.values[1]));
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
	bool		    pkey_sent;
	bool		    found_tuple;
	TupleData       old_tuple;
	TupleData       new_tuple;
	Oid			    idxoid = InvalidOid;
	Relation	    idxrel;
	TupleDesc	    tupDesc = RelationGetDescr(rel);
	ScanKeyData     skey[INDEX_MAX_KEYS];
	HeapTuple	    remote_tuple = NULL;

	action = pq_getmsgbyte(s);

	/* old key present, identifying key changed */
	if (action != 'K' && action != 'N')
		mtm_log(ERROR, "expected action 'N' or 'K', got %c",
			 action);

	estate = create_rel_estate(rel);
	oldslot = ExecInitExtraTupleSlot(estate, NULL);
	ExecSetSlotDescriptor(oldslot, tupDesc);
	newslot = ExecInitExtraTupleSlot(estate, NULL);
	ExecSetSlotDescriptor(newslot, tupDesc);

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
	{
		found_tuple = find_heap_tuple(pkey_sent ? &old_tuple : &new_tuple, rel, oldslot, true);
	}
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
			tuple_to_stringinfo(&o,tupDesc, oldslot->tts_tuple, false);
			appendStringInfo(&o, " to");
			tuple_to_stringinfo(&o,tupDesc, remote_tuple, false);
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
	ExecResetTupleTable(estate->es_tupleTable, true);
	FreeExecutorState(estate);

	CommandCounterIncrement();
}

static void
process_remote_delete(StringInfo s, Relation rel)
{
	EState	   *estate;
	TupleData   oldtup;
	TupleTableSlot *oldslot;
	Oid			idxoid;
	Relation	idxrel;
	TupleDesc   tupDesc = RelationGetDescr(rel);
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
	{
		found_old = find_heap_tuple(&oldtup, rel, oldslot, true);
	}
	else
	{
		/* Now open the primary key index */
		idxrel = index_open(idxoid, RowExclusiveLock);

		if (rel->rd_rel->relkind != RELKIND_RELATION)
			mtm_log(ERROR, "unexpected relkind '%c' rel \"%s\"",
					 rel->rd_rel->relkind, RelationGetRelationName(rel));

#ifdef VERBOSE_DELETE
		{
			HeapTuple tup;
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
	}
	else
	{
        ereport(ERROR,
                (errcode(ERRCODE_NO_DATA_FOUND),
                 MTM_ERRMSG("Record with specified key can not be located at this node"),
                 errdetail("Most likely we have DELETE-DELETE conflict")));
	}

	PopActiveSnapshot();

	if (OidIsValid(idxoid))
		index_close(idxrel, NoLock);

	ExecResetTupleTable(estate->es_tupleTable, true);
	FreeExecutorState(estate);

	CommandCounterIncrement();
}

void
MtmExecutor(void* work, size_t size, MtmReceiverContext *receiver_ctx)
{
    StringInfoData s;
    Relation rel = NULL;
	int spill_file = -1;
	int save_cursor = 0;
	int save_len = 0;
	MemoryContext old_context;
	MemoryContext top_context;
	GlobalTransactionId current_gtid = {0, InvalidTransactionId};

    s.data = work;
    s.len = size;
    s.maxlen = -1;
	s.cursor = 0;
	
    if (MtmApplyContext == NULL) {
        MtmApplyContext = AllocSetContextCreate(TopMemoryContext,
												"ApplyContext",
												ALLOCSET_DEFAULT_SIZES);
    }
	top_context = MemoryContextSwitchTo(MtmApplyContext);
	replorigin_session_origin = InvalidRepOriginId;

	StartTransactionCommand();
	SetPGVariable("session_authorization", NIL, false);
	ResetAllOptions();
	CommitTransactionCommand();

    PG_TRY();
    {    
		bool inside_transaction = true;
        do { 
            char action = pq_getmsgbyte(&s);
			old_context = MemoryContextSwitchTo(MtmApplyContext);

			mtm_log(MtmApplyTrace, "got action '%c'", action);

			switch (action)
			{
                /* BEGIN */
            case 'B':
			    inside_transaction = process_remote_begin(&s, &current_gtid);
				break;
                /* COMMIT */
            case 'C':
  			    close_rel(rel);
				process_remote_commit(&s, &current_gtid, receiver_ctx);
				inside_transaction = false;
                break;
                /* INSERT */
            case 'I':
			    process_remote_insert(&s, rel);
                break;
                /* UPDATE */
            case 'U':
                process_remote_update(&s, rel);
                break;
                /* DELETE */
            case 'D':
                process_remote_delete(&s, rel);
                break;
            case 'R':
  			    close_rel(rel);
                rel = read_rel(&s, RowExclusiveLock);
                break;
			case 'F':
			{
				int node_id = pq_getmsgint(&s, 4);
				int file_id = pq_getmsgint(&s, 4);
				Assert(spill_file < 0);
				spill_file = MtmOpenSpillFile(node_id, file_id);
				break;
			}
 		    case '(':
			{
			    size_t size = pq_getmsgint(&s, 4);    
				s.data = MemoryContextAlloc(TopMemoryContext, size);
				save_cursor = s.cursor;
				save_len = s.len;
				s.cursor = 0;
				s.len = size;
				MtmReadSpillFile(spill_file, s.data, size);
				break;
			}
  		    case ')':
			{
  			    pfree(s.data);
				s.data = work;
  			    s.cursor = save_cursor;
				s.len = save_len;
				break;
			}
			case 'N':
			{
				int64 next;
				Oid relid;
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
			{
  			    close_rel(rel);
				rel = NULL;
				inside_transaction = !process_remote_message(&s, receiver_ctx);
				break;
			}
			case 'Z':
			{
				MtmStateProcessEvent(MTM_RECOVERY_FINISH2, false);
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

		old_context = MemoryContextSwitchTo(MtmApplyContext);
		MtmHandleApplyError();
		MemoryContextSwitchTo(old_context);
		EmitErrorReport();
		FlushErrorState();
		MtmEndSession(MtmReplicationNodeId, false);

		MtmDDLResetApplyState();

		mtm_log(MtmApplyError, "Receiver: abort transaction " XID_FMT,
				current_gtid.xid);

		/* handle only prepare errors here */
		if (TransactionIdIsValid(current_gtid.xid))
		{
			MtmDeadlockDetectorRemoveXact(GetCurrentTransactionIdIfAny());
			if (receiver_ctx->parallel_allowed)
				mtm_send_reply(current_gtid.xid, current_gtid.node, MSG_ABORTED);
		}

		AbortCurrentTransaction();
	}
	PG_END_TRY();

	// Assert(s.cursor == s.len);
	// only non-error scenario
	// Assert(s.data == work);

	if (s.data != work)
		pfree(s.data);
	MemoryContextSwitchTo(top_context);
	MemoryContextResetAndDeleteChildren(MtmApplyContext);
}
