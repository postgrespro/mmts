#include "postgres.h"

#include "miscadmin.h"

#include "pgstat.h"

#include "pglogical_output.h"
#include "replication/origin.h"

#include "access/sysattr.h"
#include "access/tuptoaster.h"
#include "access/xact.h"
#include "access/clog.h"

#include "catalog/catversion.h"
#include "catalog/index.h"

#include "catalog/namespace.h"
#include "catalog/pg_class.h"
#include "catalog/pg_database.h"
#include "catalog/pg_namespace.h"
#include "catalog/pg_type.h"

#include "commands/dbcommands.h"

#include "executor/spi.h"

#include "libpq/pqformat.h"

#include "mb/pg_wchar.h"

#include "utils/builtins.h"
#include "utils/lsyscache.h"
#include "utils/memutils.h"
#include "utils/rel.h"
#include "utils/syscache.h"
#include "utils/timestamp.h"
#include "utils/typcache.h"
#include "utils/snapmgr.h"

#include "storage/ipc.h"

#include "replication/message.h"

#include "pglogical_relid_map.h"

#include "multimaster.h"
#include "state.h"
#include "ddl.h"
#include "logger.h"


static int MtmTransactionRecords;
static bool MtmIsFilteredTxn;
static TransactionId MtmCurrentXid;
static bool DDLInProgress = false;
static Oid MtmSenderTID; /* transaction identifier for WAL sender */
static Oid MtmLastRelId; /* last relation ID sent to the receiver in this transaction */

static void pglogical_write_rel(StringInfo out, PGLogicalOutputData *data, Relation rel);

static void pglogical_write_begin(StringInfo out, PGLogicalOutputData *data,
							ReorderBufferTXN *txn);
static void pglogical_write_commit(StringInfo out,PGLogicalOutputData *data,
							ReorderBufferTXN *txn, XLogRecPtr commit_lsn);

static void pglogical_write_insert(StringInfo out, PGLogicalOutputData *data,
							Relation rel, HeapTuple newtuple);
static void pglogical_write_update(StringInfo out, PGLogicalOutputData *data,
							Relation rel, HeapTuple oldtuple,
							HeapTuple newtuple);
static void pglogical_write_delete(StringInfo out, PGLogicalOutputData *data,
							Relation rel, HeapTuple oldtuple);

static void pglogical_write_tuple(StringInfo out, PGLogicalOutputData *data,
								  Relation rel, HeapTuple tuple);
static char decide_datum_transfer(Form_pg_attribute att,
								  Form_pg_type typclass,
								  bool allow_internal_basetypes,
								  bool allow_binary_basetypes);

static void pglogical_write_caughtup(StringInfo out, PGLogicalOutputData *data,
									 XLogRecPtr wal_end_ptr);


/*
 * Write relation description to the output stream.
 */
static void
pglogical_write_rel(StringInfo out, PGLogicalOutputData *data, Relation rel)
{
	const char *nspname;
	uint8		nspnamelen;
	const char *relname;
	uint8		relnamelen;
	Oid         relid;
	Oid         tid;

	if (MtmIsFilteredTxn)
	{
		mtm_log(ProtoTraceFilter, "pglogical_write_rel filtered");
		return;
	}
	if (DDLInProgress)
	{
		mtm_log(ProtoTraceFilter, "pglogical_write_rel filtered DDLInProgress");
		return;
	}

	relid = RelationGetRelid(rel);

	if (relid == MtmLastRelId) { 
		return;
	}
	MtmLastRelId = relid;

	pq_sendbyte(out, 'R');		/* sending RELATION */	
	pq_sendint(out, relid, sizeof relid); /* use Oid as relation identifier */
	
	Assert(MtmSenderTID != InvalidOid);
	tid = pglogical_relid_map_get(relid);
	if (tid == MtmSenderTID) { /* this relation was already sent in this transaction */
		pq_sendbyte(out, 0); /* do not need to send relation namespace and name in this case */
		pq_sendbyte(out, 0);
	} else { 
		pglogical_relid_map_put(relid, MtmSenderTID);
		nspname = get_namespace_name(rel->rd_rel->relnamespace);
		if (nspname == NULL)
			elog(ERROR, "cache lookup failed for namespace %u",
				 rel->rd_rel->relnamespace);
		nspnamelen = strlen(nspname) + 1;
		
		relname = NameStr(rel->rd_rel->relname);
		relnamelen = strlen(relname) + 1;
		
		pq_sendbyte(out, nspnamelen);		/* schema name length */
		pq_sendbytes(out, nspname, nspnamelen);
		
		pq_sendbyte(out, relnamelen);		/* table name length */
		pq_sendbytes(out, relname, relnamelen);
	}
}

/*
 * Write BEGIN to the output stream.
 */
static void
pglogical_write_begin(StringInfo out, PGLogicalOutputData *data,
					  ReorderBufferTXN *txn)
{
	MtmDecoderPrivate *hooks_data = (MtmDecoderPrivate *) data->hooks.hooks_private_data;

	Assert(hooks_data->is_recovery || txn->origin_id == InvalidRepOriginId);

	if (++MtmSenderTID == InvalidOid) { 
		pglogical_relid_map_reset();
		MtmSenderTID += 1; /* skip InvalidOid */
	}
	MtmLastRelId = InvalidOid;
	MtmCurrentXid = txn->xid;
	MtmIsFilteredTxn = false;
	DDLInProgress = false;

	pq_sendbyte(out, 'B');		/* BEGIN */
	pq_sendint(out, hooks_data->cfg->my_node_id, 4);
	pq_sendint64(out, txn->xid);
	pq_sendint64(out, 42);
	pq_sendint64(out, 142);

	MtmTransactionRecords = 0;

	mtm_log(ProtoTraceSender, "pglogical_write_begin xid=" XID_FMT " gid=%s",
			txn->xid, txn->gid);
}

static void pglogical_seq_nextval(StringInfo out, LogicalDecodingContext *ctx, MtmSeqPosition* pos)
{
	Relation rel = heap_open(pos->seqid, NoLock);
	pglogical_write_rel(out, ctx->output_plugin_private, rel);
	heap_close(rel, NoLock);
	pq_sendbyte(out, 'N');
	pq_sendint64(out, pos->next);
}

static void pglogical_broadcast_table(StringInfo out, LogicalDecodingContext *ctx, MtmCopyRequest* copy)
{
	if (BIT_CHECK(copy->targetNodes, MtmReplicationNodeId-1)) { 
		HeapScanDesc scandesc;
		HeapTuple	 tuple;
		Relation     rel;
		
		rel = heap_open(copy->sourceTable, ShareLock);
		
		pglogical_write_rel(out, ctx->output_plugin_private, rel);

		pq_sendbyte(out, '0');

		scandesc = heap_beginscan(rel, GetTransactionSnapshot(), 0, NULL);
		while ((tuple = heap_getnext(scandesc, ForwardScanDirection)) != NULL)
		{
			MtmOutputPluginPrepareWrite(ctx, false, false);
			pq_sendbyte(out, 'I');		/* action INSERT */
			pglogical_write_tuple(out, ctx->output_plugin_private, rel, tuple);
			MtmOutputPluginWrite(ctx, false, false);
		}
		heap_endscan(scandesc);
		heap_close(rel, ShareLock);
	}
}

static void
pglogical_write_message(StringInfo out, LogicalDecodingContext *ctx,
						XLogRecPtr end_lsn,
						const char *prefix, Size sz, const char *message)
{
	PGLogicalOutputData* data = (PGLogicalOutputData*)ctx->output_plugin_private;
	MtmDecoderPrivate *hooks_data = (MtmDecoderPrivate *) data->hooks.hooks_private_data;

	MtmLastRelId = InvalidOid;

	switch (*prefix)
	{
	case 'L':
		if (hooks_data->is_recovery)
		{
			return;
		}
		mtm_log(ProtoTraceMessage, "Sent deadlock message to node %d",
				MtmReplicationNodeId);
		break;

	case 'D':
		if (MtmIsFilteredTxn)
		{
			mtm_log(ProtoTraceFilter, "pglogical_write_message filtered");
			return;
		}
		DDLInProgress = true;
		mtm_log(ProtoTraceMessage, "Sent tx DDL message to node %d: %s",
				MtmReplicationNodeId, message);
		break;

	case 'C':
		mtm_log(ProtoTraceMessage, "Sent non-tx DDL message to node %d: %s",
				MtmReplicationNodeId, message);
		break;

	case 'E':
		DDLInProgress = false;
		/*
		 * we use End message only as indicator of DDL transaction finish,
		 * so no need to send that to replicas.
		 */
		return;

	case 'B':
		pglogical_broadcast_table(out, ctx, (MtmCopyRequest*)message);
		return;


	case 'N':
		pglogical_seq_nextval(out, ctx, (MtmSeqPosition*)message);
		mtm_log(ProtoTraceMessage, "Sent nextval message to node %d",
				MtmReplicationNodeId);
		return;
	}

	pq_sendbyte(out, 'M');
	pq_sendbyte(out, *prefix);
	pq_sendint64(out, end_lsn);
	pq_sendint(out, sz, 4);
	pq_sendbytes(out, message, sz);
}

/* 
 * WAL sender caught up 
 */
void pglogical_write_caughtup(StringInfo out, PGLogicalOutputData *data,
							  XLogRecPtr wal_end_ptr)
{
	MtmDecoderPrivate *hooks_data = (MtmDecoderPrivate *) data->hooks.hooks_private_data;

	Assert(hooks_data->is_recovery);
	/* sending CAUGHT-UP */
	pq_sendbyte(out, 'Z');
	MtmStateProcessNeighborEvent(MtmReplicationNodeId, MTM_NEIGHBOR_RECOVERY_CAUGHTUP, false);
}

/*
 * Write INSERT to the output stream.
 */
static void
pglogical_write_insert(StringInfo out, PGLogicalOutputData *data,
						Relation rel, HeapTuple newtuple)
{

	elog(ProtoTraceSender, "pglogical_write_insert %d %d",
		 MtmIsFilteredTxn, DDLInProgress);

	if (MtmIsFilteredTxn)
	{
		mtm_log(ProtoTraceFilter, "pglogical_write_insert filtered");
		return;
	}
	if (DDLInProgress)
	{
		mtm_log(ProtoTraceFilter, "pglogical_write_insert filtered DDLInProgress");
		return;
	}

	MtmTransactionRecords += 1;
	pq_sendbyte(out, 'I');		/* action INSERT */
	pglogical_write_tuple(out, data, rel, newtuple);

}

/*
 * Write UPDATE to the output stream.
 */
static void
pglogical_write_update(StringInfo out, PGLogicalOutputData *data,
						Relation rel, HeapTuple oldtuple, HeapTuple newtuple)
{
	if (MtmIsFilteredTxn)
	{
		mtm_log(ProtoTraceFilter, "pglogical_write_update filtered");
		return;
	}
	if (DDLInProgress)
	{
		mtm_log(ProtoTraceFilter, "pglogical_write_update filtered DDLInProgress");
		return;
	}

	MtmTransactionRecords += 1;

	pq_sendbyte(out, 'U');		/* action UPDATE */
	/* FIXME support whole tuple (O tuple type) */
	if (oldtuple != NULL)
	{
		pq_sendbyte(out, 'K');	/* old key follows */
		pglogical_write_tuple(out, data, rel, oldtuple);
	}

	pq_sendbyte(out, 'N');		/* new tuple follows */
	pglogical_write_tuple(out, data, rel, newtuple);
}
	
/*
 * Write DELETE to the output stream.
 */
static void
pglogical_write_delete(StringInfo out, PGLogicalOutputData *data,
						Relation rel, HeapTuple oldtuple)
{
	if (MtmIsFilteredTxn)
	{
		mtm_log(ProtoTraceFilter, "pglogical_write_delete filtered");
		return;
	}
	if (DDLInProgress)
	{
		mtm_log(ProtoTraceFilter, "pglogical_write_delete filtered DDLInProgress");
		return;
	}

	MtmTransactionRecords += 1;
	pq_sendbyte(out, 'D');		/* action DELETE */
	pglogical_write_tuple(out, data, rel, oldtuple);
}

/*
 * Most of the brains for startup message creation lives in
 * pglogical_config.c, so this presently just sends the set of key/value pairs.
 */
static void
write_startup_message(StringInfo out, List *msg)
{
}

static void
send_node_id(StringInfo out, ReorderBufferTXN *txn, MtmDecoderPrivate *private)
{
	if (txn->origin_id != InvalidRepOriginId)
	{
		int i;
		for (i = 0; i < private->cfg->n_nodes; i++)
		{
			if (private->cfg->nodes[i].origin_id == txn->origin_id)
			{
				pq_sendbyte(out, private->cfg->nodes[i].node_id);
				return;
			}
		}
		mtm_log(WARNING, "Failed to map origin %d", txn->origin_id);
		Assert(false);
	}
	else
	{
		pq_sendbyte(out, private->cfg->my_node_id);
	}
}


/*
 * Write PREPARE/PRECOMMIT to the output stream.
 */
void
pglogical_write_prepare(StringInfo out, PGLogicalOutputData *data,
					   ReorderBufferTXN *txn, XLogRecPtr lsn)
{
	MtmDecoderPrivate *hooks_data = (MtmDecoderPrivate *) data->hooks.hooks_private_data;
	uint8 event = *txn->state_3pc ? PGLOGICAL_PRECOMMIT_PREPARED : PGLOGICAL_PREPARE;

	/* Ensure that we reset DDLInProgress */
	Assert(!DDLInProgress);

	/* COMMIT and PREPARE are preceded by BEGIN, which set MtmIsFilteredTxn flag */
	if (MtmIsFilteredTxn && event == PGLOGICAL_PREPARE)
		return;

	/* send the event fields */
	pq_sendbyte(out, 'C');
	pq_sendbyte(out, event);
	pq_sendbyte(out, hooks_data->cfg->my_node_id);

	/* send fixed fields */
	pq_sendint64(out, lsn);
	pq_sendint64(out, txn->end_lsn);
	pq_sendint64(out, txn->commit_time);

	send_node_id(out, txn, hooks_data);
	pq_sendint64(out, txn->origin_lsn);

	pq_sendstring(out, txn->gid);

	mtm_log(ProtoTraceSender, "XXX: pglogical_write_prepare %s", txn->gid);
}

/*
 * Write COMMIT PREPARED to the output stream.
 */
void
pglogical_write_commit_prepared(StringInfo out, PGLogicalOutputData *data,
					   ReorderBufferTXN *txn, XLogRecPtr lsn)
{
	MtmDecoderPrivate *hooks_data = (MtmDecoderPrivate *) data->hooks.hooks_private_data;

	Assert(hooks_data->is_recovery || txn->origin_id == InvalidRepOriginId);

	/* send the event fields */
	pq_sendbyte(out, 'C');
	pq_sendbyte(out, PGLOGICAL_COMMIT_PREPARED);
	pq_sendbyte(out, hooks_data->cfg->my_node_id);

	/* send fixed fields */
	pq_sendint64(out, lsn);
	pq_sendint64(out, txn->end_lsn);
	pq_sendint64(out, txn->commit_time);

	send_node_id(out, txn, hooks_data);
	pq_sendint64(out, txn->origin_lsn);

	/* only for commit prepared */
	pq_sendint64(out, 42);//MtmGetTransactionCSN(txn->xid));

	pq_sendstring(out, txn->gid);
}

/*
 * Write ABORT PREPARED to the output stream.
 */
void
pglogical_write_abort_prepared(StringInfo out, PGLogicalOutputData *data,
					   ReorderBufferTXN *txn, XLogRecPtr lsn)
{
	MtmDecoderPrivate *hooks_data = (MtmDecoderPrivate *) data->hooks.hooks_private_data;

	Assert(hooks_data->is_recovery || txn->origin_id == InvalidRepOriginId);

	/* send the event fields */
	pq_sendbyte(out, 'C');
	pq_sendbyte(out, PGLOGICAL_ABORT_PREPARED);
	pq_sendbyte(out, hooks_data->cfg->my_node_id);

	/* send fixed fields */
	pq_sendint64(out, lsn);
	pq_sendint64(out, txn->end_lsn);
	pq_sendint64(out, txn->commit_time);

	send_node_id(out, txn, hooks_data);
	pq_sendint64(out, txn->origin_lsn);

	/* skip CSN */

	pq_sendstring(out, txn->gid);
}

static void
pglogical_write_commit(StringInfo out, PGLogicalOutputData *data,
					   ReorderBufferTXN *txn, XLogRecPtr lsn)
{
	MtmDecoderPrivate *hooks_data = (MtmDecoderPrivate *) data->hooks.hooks_private_data;
	uint8 event = PGLOGICAL_COMMIT;

	if (MtmIsFilteredTxn)
		return;

	/* send fixed fields */
	pq_sendbyte(out, 'C');
	pq_sendbyte(out, event);
	pq_sendbyte(out, hooks_data->cfg->my_node_id);

	/* send fixed fields */
	pq_sendint64(out, lsn);
	pq_sendint64(out, txn->end_lsn);
	pq_sendint64(out, txn->commit_time);

	send_node_id(out, txn, hooks_data);
	pq_sendint64(out, txn->origin_lsn);
}

/*
 * Write a tuple to the outputstream, in the most efficient format possible.
 */
static void
pglogical_write_tuple(StringInfo out, PGLogicalOutputData *data,
					   Relation rel, HeapTuple tuple)
{
	TupleDesc	desc;
	Datum		values[MaxTupleAttributeNumber];
	bool		isnull[MaxTupleAttributeNumber];
	int			i;
	uint16		nliveatts = 0;

	if (MtmIsFilteredTxn)
	{
		mtm_log(ProtoTraceFilter, "pglogical_write_tuple filtered");
		return;
	}
	if (DDLInProgress)
	{
		mtm_log(ProtoTraceFilter, "pglogical_write_tuple filtered DDLInProgress");
		return;
	}

	desc = RelationGetDescr(rel);

	pq_sendbyte(out, 'T');			/* sending TUPLE */

	for (i = 0; i < desc->natts; i++)
	{
		if (TupleDescAttr(desc, i)->attisdropped)
			continue;
		nliveatts++;
	}
	pq_sendint(out, nliveatts, 2);

	/* try to allocate enough memory from the get go */
	enlargeStringInfo(out, tuple->t_len +
					  nliveatts * (1 + 4));

	/*
	 * XXX: should this prove to be a relevant bottleneck, it might be
	 * interesting to inline heap_deform_tuple() here, we don't actually need
	 * the information in the form we get from it.
	 */
	heap_deform_tuple(tuple, desc, values, isnull);

	for (i = 0; i < desc->natts; i++)
	{
		HeapTuple	typtup;
		Form_pg_type typclass;
		Form_pg_attribute att = TupleDescAttr(desc, i);
		char		transfer_type;

		/* skip dropped columns */
		if (att->attisdropped)
			continue;

		if (isnull[i])
		{
			pq_sendbyte(out, 'n');	/* null column */
			continue;
		}
		else if (att->attlen == -1 && VARATT_IS_EXTERNAL_ONDISK(values[i]))
		{
			pq_sendbyte(out, 'u');	/* unchanged toast column */
			continue;
		}

		typtup = SearchSysCache1(TYPEOID, ObjectIdGetDatum(att->atttypid));
		if (!HeapTupleIsValid(typtup))
			elog(ERROR, "cache lookup failed for type %u", att->atttypid);
		typclass = (Form_pg_type) GETSTRUCT(typtup);

		transfer_type = decide_datum_transfer(att, typclass,
											  data->allow_internal_basetypes,
											  data->allow_binary_basetypes);
			
        pq_sendbyte(out, transfer_type);
		switch (transfer_type)
		{
			case 'b':	/* internal-format binary data follows */

				/* pass by value */
				if (att->attbyval)
				{
					pq_sendint(out, att->attlen, 4); /* length */

					enlargeStringInfo(out, att->attlen);
					store_att_byval(out->data + out->len, values[i],
									att->attlen);
					out->len += att->attlen;
					out->data[out->len] = '\0';
				}
				/* fixed length non-varlena pass-by-reference type */
				else if (att->attlen > 0)
				{
					pq_sendint(out, att->attlen, 4); /* length */

					appendBinaryStringInfo(out, DatumGetPointer(values[i]),
										   att->attlen);
				}
				/* varlena type */
				else if (att->attlen == -1)
				{
					char *data = DatumGetPointer(values[i]);

					/* send indirect datums inline */
					if (VARATT_IS_EXTERNAL_INDIRECT(values[i]))
					{
						struct varatt_indirect redirect;
						VARATT_EXTERNAL_GET_POINTER(redirect, data);
						data = (char *) redirect.pointer;
					}

					Assert(!VARATT_IS_EXTERNAL(data));

					pq_sendint(out, VARSIZE_ANY(data), 4); /* length */

					appendBinaryStringInfo(out, data, VARSIZE_ANY(data));
				}
				else
					elog(ERROR, "unsupported tuple type");

				break;

			case 's': /* binary send/recv data follows */
				{
					bytea	   *outputbytes;
					int			len;

					outputbytes = OidSendFunctionCall(typclass->typsend,
													  values[i]);

					len = VARSIZE(outputbytes) - VARHDRSZ;
					pq_sendint(out, len, 4); /* length */
					pq_sendbytes(out, VARDATA(outputbytes), len); /* data */
					pfree(outputbytes);
				}
				break;

			default:
				{
					char   	   *outputstr;
					int			len;

					outputstr =	OidOutputFunctionCall(typclass->typoutput,
													  values[i]);
					len = strlen(outputstr) + 1;
					pq_sendint(out, len, 4); /* length */
					appendBinaryStringInfo(out, outputstr, len); /* data */
					pfree(outputstr);
				}
		}

		ReleaseSysCache(typtup);
	}
}

/*
 * Make the executive decision about which protocol to use.
 */
static char
decide_datum_transfer(Form_pg_attribute att, Form_pg_type typclass,
					  bool allow_internal_basetypes,
					  bool allow_binary_basetypes)
{
	/*
	 * Use the binary protocol, if allowed, for builtin & plain datatypes.
	 */
	if (allow_internal_basetypes &&
		typclass->typtype == 'b' &&
		att->atttypid < FirstNormalObjectId &&
		typclass->typelem == InvalidOid)
	{
		return 'b';
	}
	/*
	 * Use send/recv, if allowed, if the type is plain or builtin.
	 *
	 * XXX: we can't use send/recv for array or composite types for now due to
	 * the embedded oids.
	 */
	else if (allow_binary_basetypes &&
			 OidIsValid(typclass->typreceive) &&
			 (att->atttypid < FirstNormalObjectId || typclass->typtype != 'c') &&
			 (att->atttypid < FirstNormalObjectId || typclass->typelem == InvalidOid))
	{
		return 's';
	}

	return 't';
}

static void
MtmReplicationStartupHook(struct PGLogicalStartupHookArgs* args)
{
	ListCell *param;
	MtmDecoderPrivate   *hooks_data;

	hooks_data = (MtmDecoderPrivate *) palloc0(sizeof(MtmDecoderPrivate));
	args->private_data = hooks_data;
	hooks_data->session_id = 0;
	hooks_data->recovery_done = false;
	hooks_data->is_recovery = false;
	hooks_data->cfg = MtmLoadConfig();
	hooks_data->recovery_count = MtmGetRecoveryCount();
	hooks_data->counterpart_disable_count = MtmGetNodeDisableCount(MtmReplicationNodeId);

	if (!BIT_CHECK(MtmGetConnectedNodeMask(), MtmReplicationNodeId - 1))
	{
		mtm_log(ERROR, "Walsender to node %d exits as dmq connection is not yet fully established", MtmReplicationNodeId);
	}

	foreach(param, args->in_params)
	{
		DefElem	   *elem = lfirst(param);
		if (strcmp("mtm_replication_mode", elem->defname) == 0)
		{
			if (elem->arg != NULL && strVal(elem->arg) != NULL)
			{
				if (strcmp(strVal(elem->arg), "recovery") == 0)
				{
					hooks_data->is_recovery = true;
				}
				else if (strcmp(strVal(elem->arg), "recovered") == 0)
				{
					hooks_data->is_recovery = false;
				}
				else
				{
					mtm_log(ERROR, "Illegal recovery mode %s", strVal(elem->arg));
				}
			}
			else
			{
				mtm_log(ERROR, "Replication mode is not specified");
			}
		}
		else if (strcmp("mtm_session_id", elem->defname) == 0)
		{
			if (elem->arg != NULL && strVal(elem->arg) != NULL)
			{
				int64 session_id = 0;
				sscanf(strVal(elem->arg), INT64_FORMAT, &session_id);

				if (session_id == 0)
					mtm_log(ERROR, "Illegal mtm_session_id");

				hooks_data->session_id = session_id;
			}
			else
			{
				mtm_log(ERROR, "mtm_session_id is not specified");
			}
		}
	}

	if (hooks_data->session_id == 0)
		mtm_log(ERROR, "mtm_session_id is not specified");

	/*
	 * Set proper originId mappings.
	 *
	 * This is copypasted from receiver. Better to have normal init method
	 * to setup all stuff in shared memory. But seems that there is no such
	 * callback in vanilla pg and adding one will require some carefull thoughts.
	 */
	LWLockAcquire(Mtm->lock, LW_EXCLUSIVE);
	Mtm->peers[MtmReplicationNodeId - 1].sender_pid = MyProcPid;
	LWLockRelease(Mtm->lock);

	if (hooks_data->is_recovery)
	{
		mtm_log(ProtoTraceMode,
				"Walsender starts in recovery mode to node %d",
				MtmReplicationNodeId);

		Assert(MyReplicationSlot != NULL);
		MtmStateProcessNeighborEvent(MtmReplicationNodeId, MTM_NEIGHBOR_WAL_SENDER_START_RECOVERY, false);
	}
	else
	{
		mtm_log(ProtoTraceMode,
				"Walsender starting in recovered mode to node %d",
				MtmReplicationNodeId);

		/*
		 * Indicate receiver that after this point in wal it is safe to send
		 * transaction to the pool of workers. Before this point in wal (or in other
		 * words before we processed MTM_NEIGHBOR_WAL_SENDER_START_RECOVERY event
		 * and enabled this node in our disabledNodeMask) our backends are not waiting
		 * for prepare confirmations from this node, so receiver can get precommit
		 * before it will finish prepare. This will leave receiver with lots of
		 * prepared transactions that will never be commited as precommit and commit
		 * already happend before prepare.
		 *
		 * To ensure that all transactions ended after this message had seen right
		 * disabledNodeMask we took MtmCommitBarrier in exclusive mode to await
		 * finish of all transactions with potentially old disabledNodeMask.
		 */
		if (!hooks_data->is_recovery)
		{
			XLogRecPtr msg_xptr;
			char *session_id = psprintf(INT64_FORMAT, hooks_data->session_id);

			SpinLockAcquire(&Mtm->cb_lock);
			Mtm->n_commit_holders += 1;
			SpinLockRelease(&Mtm->cb_lock);

			PG_TRY();
			{
				for (;;)
				{
					bool done = false;
					SpinLockAcquire(&Mtm->cb_lock);
					if (Mtm->n_committers == 0)
						done = true;
					SpinLockRelease(&Mtm->cb_lock);

					if (done)
						break;

					ConditionVariableSleep(&Mtm->commit_barrier_cv, PG_WAIT_EXTENSION);
				}
				ConditionVariableCancelSleep();

				MtmStateProcessNeighborEvent(MtmReplicationNodeId, MTM_NEIGHBOR_WAL_SENDER_START_RECOVERED, false);
				msg_xptr = LogLogicalMessage("P", session_id, strlen(session_id) + 1, false);
			}
			PG_CATCH();
			{
				SpinLockAcquire(&Mtm->cb_lock);
				Mtm->n_commit_holders -= 1;
				SpinLockRelease(&Mtm->cb_lock);
				ConditionVariableBroadcast(&Mtm->commit_barrier_cv);
				PG_RE_THROW();
			}
			PG_END_TRY();

			SpinLockAcquire(&Mtm->cb_lock);
			Mtm->n_commit_holders -= 1;
			SpinLockRelease(&Mtm->cb_lock);
			ConditionVariableBroadcast(&Mtm->commit_barrier_cv);

			XLogFlush(msg_xptr);
		}

		mtm_log(ProtoTraceMode,
				"Walsender started in recovered mode to node %d",
				MtmReplicationNodeId);
	}


}

static void
MtmReplicationShutdownHook(struct PGLogicalShutdownHookArgs* args)
{
	Assert(MtmReplicationNodeId >= 0);

	LWLockAcquire(Mtm->lock, LW_EXCLUSIVE);
	Mtm->peers[MtmReplicationNodeId - 1].sender_pid = InvalidPid;
	LWLockRelease(Mtm->lock);

	MtmStateProcessNeighborEvent(MtmReplicationNodeId,
								 MTM_NEIGHBOR_WAL_SENDER_STOP, false);

	MtmReplicationNodeId = -1;

	mtm_log(ProtoTraceMode, "Walsender to node %d exiting",
			MtmReplicationNodeId);
}

/*
 * Filter transactions which should be replicated to other nodes.
 * This filter is applied at sender side (WAL sender).
 * Final filtering is also done at destination side by MtmFilterTransaction function.
 */
static bool
MtmReplicationTxnFilterHook(struct PGLogicalTxnFilterArgs* args)
{
	MtmDecoderPrivate *hooks_data = (MtmDecoderPrivate *) args->private_data;

	/* Do not replicate any transactions in recovery mode (because we should apply
	 * changes sent to us rather than send our own pending changes)
	 * and transactions received from other nodes
	 * (originId should be non-zero in this case)
	 * unless we are performing recovery of disabled node
	 * (in this case all transactions should be sent)
	 */
	bool res = (args->origin_id == InvalidRepOriginId ||
				hooks_data->is_recovery);

	return res;
}

/*
 * Filter record corresponding to local (non-distributed) tables
 */
static bool
MtmReplicationRowFilterHook(struct PGLogicalRowFilterArgs* args)
{
	bool isDistributed;

	/*
	 * We have several built-in local tables that shouldn't be replicated.
	 * It is hard to insert them into MtmLocalTables properly on extension
	 * creation so we just list them here.
	 */
	if (strcmp(args->changed_rel->rd_rel->relname.data, "referee_decision") == 0)
		return false;

	/*
	 * Check in shared hash of local tables.
	 */
	isDistributed = !MtmIsRelationLocal(args->changed_rel);

	return isDistributed;
}

static void
MtmSetupReplicationHooks(struct PGLogicalHooks* hooks)
{
	hooks->startup_hook = MtmReplicationStartupHook;
	hooks->shutdown_hook = MtmReplicationShutdownHook;
	hooks->txn_filter_hook = MtmReplicationTxnFilterHook;
	hooks->row_filter_hook = MtmReplicationRowFilterHook;
}

PGLogicalProtoAPI *
pglogical_init_api(PGLogicalProtoType typ)
{
    PGLogicalProtoAPI* res = palloc0(sizeof(PGLogicalProtoAPI));
	sscanf(MyReplicationSlot->data.name.data, MULTIMASTER_SLOT_PATTERN, &MtmReplicationNodeId);
    res->write_rel = pglogical_write_rel;
    res->write_begin = pglogical_write_begin;
	res->write_message = pglogical_write_message;
    res->write_commit = pglogical_write_commit;
    res->write_insert = pglogical_write_insert;
    res->write_update = pglogical_write_update;
    res->write_delete = pglogical_write_delete;
    res->write_caughtup = pglogical_write_caughtup;
	res->setup_hooks = MtmSetupReplicationHooks;
    res->write_startup_message = write_startup_message;
    return res;
}
