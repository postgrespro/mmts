/*-------------------------------------------------------------------------
 *
 * syncpoint.h
 *
 * Portions Copyright (c) 1996-2018, PostgreSQL Global Development Group
 * Portions Copyright (c) 1994, Regents of the University of California
 *
 *-------------------------------------------------------------------------
 */
#ifndef SYNCPOINT_H
#define SYNCPOINT_H

#include "access/xlogdefs.h"
#include "libpq-fe.h"
#include "utils/hsearch.h"
#include "replication/walsender.h"

typedef struct
{
	XLogRecPtr	origin_lsn;
	XLogRecPtr	local_lsn;
} Syncpoint;

/*
 * Used as a hashkey in recovery filter.
 *
 * NB: make sure to memset this structure to zeroes before using as hashkey
 * because it contains 4-byte padding hole in the middle.
 */
typedef struct
{
	int			node_id;
	XLogRecPtr	origin_lsn;
} FilterEntry;

extern void MaybeLogSyncpoint(void);
extern void SyncpointRegister(int node_id, XLogRecPtr origin_lsn, XLogRecPtr local_lsn,
							  XLogRecPtr restart_lsn, XLogRecPtr trim_lsn);
extern Syncpoint SyncpointGetLatest(int node_id);
extern Syncpoint *SyncpointGetAllLatest(void);
extern XLogRecPtr QueryRecoveryHorizon(PGconn *conn, int node_id, Syncpoint *local_spvector);
extern HTAB *RecoveryFilterLoad(int filter_node_id, Syncpoint *spvector, MtmConfig *mtm_cfg);

#endif							/* SYNCPOINT_H */
