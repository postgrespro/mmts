/*-------------------------------------------------------------------------
 *
 * pglogical_output.c
 *		  Logical Replication output plugin
 *
 * Portions Copyright (c) 2015-2021, Postgres Professional
 * Portions Copyright (c) 2015-2020, PostgreSQL Global Development Group
 *
 * IDENTIFICATION
 *		  pglogical_output.c
 *
 *-------------------------------------------------------------------------
 */
#include "postgres.h"

#include "pglogical_config.h"
#include "pglogical_output.h"
#include "pglogical_proto.h"
#include "pglogical_hooks.h"

#include "access/hash.h"
#include "access/sysattr.h"
#include "access/xact.h"

#include "catalog/pg_class.h"
#include "catalog/pg_proc.h"
#include "catalog/pg_type.h"

#include "mb/pg_wchar.h"

#include "nodes/parsenodes.h"

#include "parser/parse_func.h"

#include "replication/output_plugin.h"
#include "replication/logical.h"
#include "replication/message.h"
#include "replication/origin.h"

#include "storage/ipc.h"

#include "tcop/tcopprot.h"

#include "utils/builtins.h"
#include "utils/catcache.h"
#include "utils/guc.h"
#include "utils/int8.h"
#include "utils/inval.h"
#include "utils/lsyscache.h"
#include "utils/memutils.h"
#include "utils/rel.h"
#include "utils/relcache.h"
#include "utils/syscache.h"
#include "utils/typcache.h"
#include "miscadmin.h"

#include "multimaster.h"
#include "logger.h"
#include "state.h"
#include "mtm_utils.h"

extern void _PG_output_plugin_init(OutputPluginCallbacks *cb);

/* These must be available to pg_dlsym() */
static void pg_decode_startup(LogicalDecodingContext *ctx,
				  OutputPluginOptions *opt, bool is_init);
static void pg_decode_shutdown(LogicalDecodingContext *ctx);
static void pg_decode_begin_txn(LogicalDecodingContext *ctx,
					ReorderBufferTXN *txn);
static void pg_decode_commit_txn(LogicalDecodingContext *ctx,
					 ReorderBufferTXN *txn, XLogRecPtr commit_lsn);

static void pg_decode_prepare_txn(LogicalDecodingContext *ctx, ReorderBufferTXN *txn,
					  XLogRecPtr lsn);
static void pg_decode_commit_prepared_txn(LogicalDecodingContext *ctx, ReorderBufferTXN *txn,
							  XLogRecPtr lsn);
static void pg_decode_abort_prepared_txn(LogicalDecodingContext *ctx, ReorderBufferTXN *txn,
							 XLogRecPtr lsn);

static bool pg_filter_prepare(LogicalDecodingContext *ctx, ReorderBufferTXN *txn,
				  TransactionId xid, const char *gid);

static void pg_decode_abort_txn(LogicalDecodingContext *ctx, ReorderBufferTXN *txn,
					XLogRecPtr abort_lsn);

static void pg_decode_change(LogicalDecodingContext *ctx,
				 ReorderBufferTXN *txn, Relation rel,
				 ReorderBufferChange *change);

static bool pg_decode_origin_filter(LogicalDecodingContext *ctx,
						RepOriginId origin_id);

static void pg_decode_message(LogicalDecodingContext *ctx,
				  ReorderBufferTXN *txn, XLogRecPtr message_lsn,
				  bool transactional, const char *prefix,
				  Size sz, const char *message);
static void pg_decode_caughtup(LogicalDecodingContext *ctx);

static void send_startup_message(LogicalDecodingContext *ctx,
					 PGLogicalOutputData *data, bool last_message);

static bool startup_message_sent = false;

#define OUTPUT_BUFFER_SIZE (16*1024*1024)

void
MtmOutputPluginWrite(LogicalDecodingContext *ctx, bool last_write, bool flush)
{
	if (flush)
		OutputPluginWrite(ctx, last_write);
}

void
MtmOutputPluginPrepareWrite(LogicalDecodingContext *ctx, bool last_write, bool flush)
{
	if (!ctx->prepared_write)
		OutputPluginPrepareWrite(ctx, last_write);
	else if (flush || ctx->out->len > OUTPUT_BUFFER_SIZE)
	{
		OutputPluginWrite(ctx, false);
		OutputPluginPrepareWrite(ctx, last_write);
	}
}


/* specify output plugin callbacks */
void
_PG_output_plugin_init(OutputPluginCallbacks *cb)
{
	AssertVariableIsOfType(&_PG_output_plugin_init, LogicalOutputPluginInit);

	cb->startup_cb = pg_decode_startup;
	cb->begin_cb = pg_decode_begin_txn;
	cb->change_cb = pg_decode_change;
	cb->commit_cb = pg_decode_commit_txn;
	cb->abort_cb = pg_decode_abort_txn;

	cb->filter_prepare_cb = pg_filter_prepare;
	cb->prepare_cb = pg_decode_prepare_txn;
	cb->commit_prepared_cb = pg_decode_commit_prepared_txn;
	cb->abort_prepared_cb = pg_decode_abort_prepared_txn;

	cb->filter_by_origin_cb = pg_decode_origin_filter;
	cb->shutdown_cb = pg_decode_shutdown;
	cb->message_cb = pg_decode_message;
	cb->caughtup_cb = pg_decode_caughtup;

	MtmDisableTimeouts();
}

#if 0
static bool
check_binary_compatibility(PGLogicalOutputData *data)
{
	if (data->client_binary_basetypes_major_version != PG_VERSION_NUM / 100)
		return false;

	if (data->client_binary_bigendian_set
		&& data->client_binary_bigendian != server_bigendian())
	{
		elog(DEBUG1, "Binary mode rejected: Server and client endian mismatch");
		return false;
	}

	if (data->client_binary_sizeofdatum != 0
		&& data->client_binary_sizeofdatum != sizeof(Datum))
	{
		elog(DEBUG1, "Binary mode rejected: Server and client endian sizeof(Datum) mismatch");
		return false;
	}

	if (data->client_binary_sizeofint != 0
		&& data->client_binary_sizeofint != sizeof(int))
	{
		elog(DEBUG1, "Binary mode rejected: Server and client endian sizeof(int) mismatch");
		return false;
	}

	if (data->client_binary_sizeoflong != 0
		&& data->client_binary_sizeoflong != sizeof(long))
	{
		elog(DEBUG1, "Binary mode rejected: Server and client endian sizeof(long) mismatch");
		return false;
	}

	if (data->client_binary_float4byval_set
		&& data->client_binary_float4byval != server_float4_byval())
	{
		elog(DEBUG1, "Binary mode rejected: Server and client endian float4byval mismatch");
		return false;
	}

	if (data->client_binary_float8byval_set
		&& data->client_binary_float8byval != server_float8_byval())
	{
		elog(DEBUG1, "Binary mode rejected: Server and client endian float8byval mismatch");
		return false;
	}

	if (data->client_binary_intdatetimes_set
		&& data->client_binary_intdatetimes != server_integer_datetimes())
	{
		elog(DEBUG1, "Binary mode rejected: Server and client endian integer datetimes mismatch");
		return false;
	}

	return true;
}
#endif

/* initialize this plugin */
static void
pg_decode_startup(LogicalDecodingContext *ctx, OutputPluginOptions *opt,
				  bool is_init)
{
	PGLogicalOutputData *data = palloc0(sizeof(PGLogicalOutputData));

	data->context = AllocSetContextCreate(TopMemoryContext,
										  "pglogical conversion context",
										  ALLOCSET_DEFAULT_SIZES);

	ctx->output_plugin_private = data;

	/*
	 * This is replication start and not slot initialization.
	 *
	 * Parse and validate options passed by the client.
	 */
	if (!is_init)
	{
		int			params_format;

		/*
		 * Ideally we'd send the startup message immediately. That way it'd
		 * arrive before any error we emit if we see incompatible options sent
		 * by the client here. That way the client could possibly adjust its
		 * options and reconnect. It'd also make sure the client gets the
		 * startup message in a timely way if the server is idle, since
		 * otherwise it could be a while before the next callback.
		 *
		 * The decoding plugin API doesn't let us write to the stream from
		 * here, though, so we have to delay the startup message until the
		 * first change processed on the stream, in a begin callback.
		 *
		 * If we ERROR there, the startup message is buffered but not sent
		 * since the callback didn't finish. So we'd have to send the startup
		 * message, finish the callback and check in the next callback if we
		 * need to ERROR.
		 *
		 * That's a bit much hoop jumping, so for now ERRORs are immediate. A
		 * way to emit a message from the startup callback is really needed to
		 * change that.
		 */
		startup_message_sent = false;

		/*
		 * Now parse the rest of the params and ERROR if we see any we don't
		 * recognise
		 */
		params_format = process_parameters(ctx->output_plugin_options, data);

		if (params_format != 1)
			ereport(ERROR,
					(errcode(ERRCODE_FEATURE_NOT_SUPPORTED),
					 MTM_ERRMSG("client sent startup parameters in format %d but we only support format 1",
								params_format)));

		if (data->client_min_proto_version > PG_LOGICAL_PROTO_VERSION_NUM)
			ereport(ERROR,
					(errcode(ERRCODE_FEATURE_NOT_SUPPORTED),
					 MTM_ERRMSG("client sent min_proto_version=%d but we only support protocol %d or lower",
								data->client_min_proto_version, PG_LOGICAL_PROTO_VERSION_NUM)));

		if (data->client_max_proto_version < PG_LOGICAL_PROTO_MIN_VERSION_NUM)
			ereport(ERROR,
					(errcode(ERRCODE_FEATURE_NOT_SUPPORTED),
					 MTM_ERRMSG("client sent max_proto_version=%d but we only support protocol %d or higher",
								data->client_max_proto_version, PG_LOGICAL_PROTO_MIN_VERSION_NUM)));

		/*
		 * Set correct protocol format.
		 *
		 * This is the output plugin protocol format, this is different from
		 * the individual fields binary vs textual format.
		 */
		if (data->client_protocol_format != NULL
			&& strcmp(data->client_protocol_format, "json") == 0)
		{
			data->api = pglogical_init_api(PGLogicalProtoJson);
			opt->output_type = OUTPUT_PLUGIN_TEXTUAL_OUTPUT;
		}
		else if ((data->client_protocol_format != NULL
				  && strcmp(data->client_protocol_format, "native") == 0)
				 /* yeah, that's what mm really uses */
				 || data->client_protocol_format == NULL)
		{
			data->api = pglogical_init_api(PGLogicalProtoNative);
			/* nobody cares about this value */
			opt->output_type = OUTPUT_PLUGIN_BINARY_OUTPUT;

			if (data->client_no_txinfo)
			{
				elog(WARNING, "no_txinfo option ignored for protocols other than json");
				data->client_no_txinfo = false;
			}
		}
		else
		{
			ereport(ERROR,
					(errcode(ERRCODE_FEATURE_NOT_SUPPORTED),
					 MTM_ERRMSG("client requested protocol %s but only \"json\" or \"native\" are supported",
								data->client_protocol_format)));
		}

		/* check for encoding match if specific encoding demanded by client */
		if (data->client_expected_encoding != NULL
			&& strlen(data->client_expected_encoding) != 0)
		{
			int			wanted_encoding = pg_char_to_encoding(data->client_expected_encoding);

			if (wanted_encoding == -1)
				ereport(ERROR,
						(errcode(ERRCODE_INVALID_PARAMETER_VALUE),
						 MTM_ERRMSG("unrecognized encoding name %s passed to expected_encoding",
									data->client_expected_encoding)));

			if (opt->output_type == OUTPUT_PLUGIN_TEXTUAL_OUTPUT)
			{
				/*
				 * datum encoding must match assigned client_encoding in text
				 * proto, since everything is subject to client_encoding
				 * conversion.
				 */
				if (wanted_encoding != pg_get_client_encoding())
					ereport(ERROR,
							(errcode(ERRCODE_INVALID_PARAMETER_VALUE),
							 MTM_ERRMSG("expected_encoding must be unset or match client_encoding in text protocols")));
			}
			else
			{
				/*
				 * currently in the binary protocol we can only emit encoded
				 * datums in the server encoding. There's no support for
				 * encoding conversion.
				 */
				if (wanted_encoding != GetDatabaseEncoding())
					ereport(ERROR,
							(errcode(ERRCODE_FEATURE_NOT_SUPPORTED),
							 MTM_ERRMSG("encoding conversion for binary datum not supported yet"),
							 errdetail("expected_encoding %s must be unset or match server_encoding %s",
									   data->client_expected_encoding, GetDatabaseEncodingName())));
			}

			data->field_datum_encoding = wanted_encoding;
		}

		/*
		 * Will we forward changesets? We have to if we're on 9.4; otherwise
		 * honour the client's request.
		 */
		if (PG_VERSION_NUM / 100 == 904)
		{
			/*
			 * 9.4 unconditionally forwards changesets due to lack of
			 * replication origins, and it can't ever send origin info for the
			 * same reason.
			 */
			data->forward_changesets = true;
			data->forward_changeset_origins = false;

			if (data->client_forward_changesets_set
				&& !data->client_forward_changesets)
			{
				ereport(DEBUG1,
						(errcode(ERRCODE_FEATURE_NOT_SUPPORTED),
						 MTM_ERRMSG("Cannot disable changeset forwarding on PostgreSQL 9.4")));
			}
		}
		else if (data->client_forward_changesets_set
				 && data->client_forward_changesets)
		{
			/*
			 * Client explicitly asked for forwarding; forward csets and
			 * origins
			 */
			data->forward_changesets = true;
			data->forward_changeset_origins = true;
		}
		else
		{
			/* Default to not forwarding or honour client's request not to fwd */
			data->forward_changesets = false;
			data->forward_changeset_origins = false;
		}

		if (data->hooks_setup_funcname != NIL || data->api->setup_hooks)
		{

			data->hooks_mctxt = AllocSetContextCreate(ctx->context,
													  "pglogical_output hooks context",
													  ALLOCSET_DEFAULT_SIZES);

			load_hooks(data);
			call_startup_hook(data, ctx->output_plugin_options);
		}
	}
}

/*
 * BEGIN callback
 */
void
pg_decode_begin_txn(LogicalDecodingContext *ctx, ReorderBufferTXN *txn)
{
	PGLogicalOutputData *data = (PGLogicalOutputData *) ctx->output_plugin_private;
	bool		send_replication_origin = data->forward_changeset_origins;

	if (!startup_message_sent)
		send_startup_message(ctx, data, false /* can't be last message */ );

	/* If the record didn't originate locally, send origin info */
	send_replication_origin &= txn->origin_id != InvalidRepOriginId;

	if (data->api)
	{
		MtmOutputPluginPrepareWrite(ctx, !send_replication_origin, true);
		data->api->write_begin(ctx->out, data, txn);

		if (send_replication_origin)
		{
			char	   *origin;

			/* Message boundary */
			MtmOutputPluginWrite(ctx, false, false);
			MtmOutputPluginPrepareWrite(ctx, true, false);

			/*
			 * XXX: which behaviour we want here?
			 *
			 * Alternatives: - don't send origin message if origin name not
			 * found (that's what we do now) - throw error - that will break
			 * replication, not good - send some special "unknown" origin
			 */
			if (data->api->write_origin &&
				replorigin_by_oid(txn->origin_id, true, &origin))
				data->api->write_origin(ctx->out, origin, txn->origin_lsn);
		}
		MtmOutputPluginWrite(ctx, true, false);
	}
}

void
pg_decode_caughtup(LogicalDecodingContext *ctx)
{
	PGLogicalOutputData *data = (PGLogicalOutputData *) ctx->output_plugin_private;
	MtmDecoderPrivate *hooks_data = (MtmDecoderPrivate *) data->hooks.hooks_private_data;

	/*
	 * MtmOutputPluginPrepareWrite send some bytes to downstream, so we must
	 * avoid calling it in normal (non-recovery) situation.
	 */
	if (data->api && hooks_data->is_recovery)
	{
		MtmOutputPluginPrepareWrite(ctx, true, true);
		data->api->write_caughtup(ctx->out, data, ctx->reader->EndRecPtr);
		MtmOutputPluginWrite(ctx, true, true);
	}
}


/*
 * COMMIT callback
 */
void
pg_decode_commit_txn(LogicalDecodingContext *ctx, ReorderBufferTXN *txn,
					 XLogRecPtr commit_lsn)
{
	PGLogicalOutputData *data = (PGLogicalOutputData *) ctx->output_plugin_private;

	if (data->api)
	{
		MtmOutputPluginPrepareWrite(ctx, true, true);
		data->api->write_commit(ctx->out, data, txn, commit_lsn);
		MtmOutputPluginWrite(ctx, true, true);
	}
}

void
pg_decode_prepare_txn(LogicalDecodingContext *ctx, ReorderBufferTXN *txn,
					  XLogRecPtr lsn)
{
	PGLogicalOutputData *data = (PGLogicalOutputData *) ctx->output_plugin_private;

	MtmOutputPluginPrepareWrite(ctx, true, true);
	pglogical_write_prepare(ctx->out, data, txn, lsn);
	MtmOutputPluginWrite(ctx, true, true);
}

void
pg_decode_commit_prepared_txn(LogicalDecodingContext *ctx, ReorderBufferTXN *txn,
							  XLogRecPtr lsn)
{
	PGLogicalOutputData *data = (PGLogicalOutputData *) ctx->output_plugin_private;

	MtmOutputPluginPrepareWrite(ctx, true, true);
	pglogical_write_commit_prepared(ctx->out, data, txn, lsn);
	MtmOutputPluginWrite(ctx, true, true);
}

void
pg_decode_abort_prepared_txn(LogicalDecodingContext *ctx, ReorderBufferTXN *txn,
							 XLogRecPtr lsn)
{
	PGLogicalOutputData *data = (PGLogicalOutputData *) ctx->output_plugin_private;

	MtmOutputPluginPrepareWrite(ctx, true, true);
	pglogical_write_abort_prepared(ctx->out, data, txn, lsn);
	MtmOutputPluginWrite(ctx, true, true);
}

void
pg_decode_change(LogicalDecodingContext *ctx, ReorderBufferTXN *txn,
				 Relation relation, ReorderBufferChange *change)
{
	PGLogicalOutputData *data = ctx->output_plugin_private;
	MemoryContext old;

	/* First check the table filter */
	if (!call_row_filter_hook(data, txn, relation, change) || data->api == NULL)
		return;

	/* Avoid leaking memory by using and resetting our own context */
	old = MemoryContextSwitchTo(data->context);

	/* TODO: add caching (send only if changed) */
	if (data->api->write_rel)
	{
		MtmOutputPluginPrepareWrite(ctx, false, false);
		data->api->write_rel(ctx->out, data, relation);
		MtmOutputPluginWrite(ctx, false, false);
	}

	/* Send the data */
	switch (change->action)
	{
		case REORDER_BUFFER_CHANGE_INSERT:
			MtmOutputPluginPrepareWrite(ctx, true, false);
			data->api->write_insert(ctx->out, data, relation,
									&change->data.tp.newtuple->tuple);
			MtmOutputPluginWrite(ctx, true, false);
			break;
		case REORDER_BUFFER_CHANGE_UPDATE:
			{
				HeapTuple	oldtuple = change->data.tp.oldtuple ?
				&change->data.tp.oldtuple->tuple : NULL;

				MtmOutputPluginPrepareWrite(ctx, true, false);
				data->api->write_update(ctx->out, data, relation, oldtuple,
										&change->data.tp.newtuple->tuple);
				MtmOutputPluginWrite(ctx, true, false);
				break;
			}
		case REORDER_BUFFER_CHANGE_DELETE:
			if (change->data.tp.oldtuple)
			{
				MtmOutputPluginPrepareWrite(ctx, true, false);
				data->api->write_delete(ctx->out, data, relation,
										&change->data.tp.oldtuple->tuple);
				MtmOutputPluginWrite(ctx, true, false);
			}
			else
				elog(DEBUG1, "didn't send DELETE change because of missing oldtuple");
			break;
		default:
			Assert(false);
	}

	/* Cleanup */
	MemoryContextSwitchTo(old);
	MemoryContextReset(data->context);
}

/*
 * Decide if the whole transaction with specific origin should be filtered out.
 */
static bool
pg_decode_origin_filter(LogicalDecodingContext *ctx,
						RepOriginId origin_id)
{
	PGLogicalOutputData *data = ctx->output_plugin_private;

	if (!call_txn_filter_hook(data, origin_id))
	{
		return true;
	}

	if (!data->forward_changesets && origin_id != InvalidRepOriginId)
	{
		return true;
	}

	return false;
}

static void
pg_decode_message(LogicalDecodingContext *ctx,
				  ReorderBufferTXN *txn, XLogRecPtr lsn, bool transactional,
				  const char *prefix, Size sz, const char *message)
{
	PGLogicalOutputData *data = (PGLogicalOutputData *) ctx->output_plugin_private;

	MtmOutputPluginPrepareWrite(ctx, true, !transactional);
	data->api->write_message(ctx->out, ctx, lsn, prefix, sz, message);
	MtmOutputPluginWrite(ctx, true, !transactional);
}

static void
send_startup_message(LogicalDecodingContext *ctx,
					 PGLogicalOutputData *data, bool last_message)
{
	List	   *msg;

	Assert(!startup_message_sent);

	msg = prepare_startup_message(data);

	/*
	 * We could free the extra_startup_params DefElem list here, but it's
	 * pretty harmless to just ignore it, since it's in the decoding memory
	 * context anyway, and we don't know if it's safe to free the defnames or
	 * not.
	 */

	if (data->api)
	{
		MtmOutputPluginPrepareWrite(ctx, last_message, true);
		data->api->write_startup_message(ctx->out, msg);
		MtmOutputPluginWrite(ctx, last_message, true);
	}

	pfree(msg);

	startup_message_sent = true;
}

static void
pg_decode_shutdown(LogicalDecodingContext *ctx)
{
	PGLogicalOutputData *data = (PGLogicalOutputData *) ctx->output_plugin_private;

	call_shutdown_hook(data);

	if (data->hooks_mctxt != NULL)
	{
		MemoryContextDelete(data->hooks_mctxt);
		data->hooks_mctxt = NULL;
	}
}

/* Filter out unnecessary two-phase transactions */
static bool
pg_filter_prepare(LogicalDecodingContext *ctx, ReorderBufferTXN *txn,
				  TransactionId xid, const char *gid)
{
	/* PGLogicalOutputData *data = ctx->output_plugin_private; */

	/*
	 * decode all 2PC transactions
	 */
	return false;
}

/*
 * ABORT callback, it can be called if prepared transaction was aborted during
 * decoding.
 */
static void
pg_decode_abort_txn(LogicalDecodingContext *ctx, ReorderBufferTXN *txn,
					XLogRecPtr abort_lsn)
{
	PGLogicalOutputData *data = (PGLogicalOutputData *) ctx->output_plugin_private;

	MtmOutputPluginPrepareWrite(ctx, true, true);
	pglogical_write_abort(ctx->out, data, txn, abort_lsn);
	MtmOutputPluginWrite(ctx, true, true);
}
