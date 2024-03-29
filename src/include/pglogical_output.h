/*-------------------------------------------------------------------------
 *
 * pglogical_output.h
 *		pglogical output plugin
 *
 * Copyright (c) 2015, PostgreSQL Global Development Group
 * Portions Copyright (c) 2021, Postgres Professional
 *
 * IDENTIFICATION
 *		pglogical_output.h
 *
 *-------------------------------------------------------------------------
 */
#ifndef PG_LOGICAL_OUTPUT_H
#define PG_LOGICAL_OUTPUT_H

#include "nodes/parsenodes.h"

#include "replication/logical.h"
#include "replication/output_plugin.h"

#include "storage/lock.h"

#include "pglogical_output/hooks.h"

#include "pglogical_proto.h"

#include "multimaster.h"

#define PG_LOGICAL_PROTO_VERSION_NUM 1
#define PG_LOGICAL_PROTO_MIN_VERSION_NUM 1

/*
 * The name of a hook function. This is used instead of the usual List*
 * because can serve as a hash key.
 *
 * Must be zeroed on allocation if used as a hash key since padding is
 * *not* ignored on compare.
 */
typedef struct HookFuncName
{
	/* funcname is more likely to be unique, so goes first */
	char		function[NAMEDATALEN];
	char		schema[NAMEDATALEN];
}			HookFuncName;

typedef struct MtmDecoderPrivate
{
	int			receiver_node_id;
	bool		is_recovery;
	MtmConfig  *cfg;
} MtmDecoderPrivate;

typedef struct PGLogicalOutputData
{
	MemoryContext context;

	PGLogicalProtoAPI *api;

	/* protocol */
	bool		allow_internal_basetypes;
	bool		allow_binary_basetypes;
	bool		forward_changesets;
	bool		forward_changeset_origins;
	int			field_datum_encoding;

	/*
	 * client info
	 *
	 * Lots of this should move to a separate shorter-lived struct used only
	 * during parameter reading, since it contains what the client asked for.
	 * Once we've processed this during startup we don't refer to it again.
	 */
	uint32		client_pg_version;
	uint32		client_max_proto_version;
	uint32		client_min_proto_version;
	const char *client_expected_encoding;
	const char *client_protocol_format;
	uint32		client_binary_basetypes_major_version;
	bool		client_want_internal_basetypes_set;
	bool		client_want_internal_basetypes;
	bool		client_want_binary_basetypes_set;
	bool		client_want_binary_basetypes;
	bool		client_binary_bigendian_set;
	bool		client_binary_bigendian;
	uint32		client_binary_sizeofdatum;
	uint32		client_binary_sizeofint;
	uint32		client_binary_sizeoflong;
	bool		client_binary_float4byval_set;
	bool		client_binary_float4byval;
	bool		client_binary_float8byval_set;
	bool		client_binary_float8byval;
	bool		client_binary_intdatetimes_set;
	bool		client_binary_intdatetimes;
	bool		client_forward_changesets_set;
	bool		client_forward_changesets;
	bool		client_no_txinfo;

	/* hooks */
	List	   *hooks_setup_funcname;
	struct PGLogicalHooks hooks;
	MemoryContext hooks_mctxt;

	/* DefElem<String> list populated by startup hook */
	List	   *extra_startup_params;
} PGLogicalOutputData;

typedef struct PGLogicalTupleData
{
	Datum		values[MaxTupleAttributeNumber];
	bool		nulls[MaxTupleAttributeNumber];
	bool		changed[MaxTupleAttributeNumber];
}			PGLogicalTupleData;

extern void MtmOutputPluginWrite(LogicalDecodingContext *ctx, bool last_write, bool flush);
extern void MtmOutputPluginPrepareWrite(LogicalDecodingContext *ctx, bool last_write, bool flush);

#endif							/* PG_LOGICAL_OUTPUT_H */
