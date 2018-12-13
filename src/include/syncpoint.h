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

typedef struct
{
	int64		node_id;
	XLogRecPtr	origin_lsn;
} FilterEntry;

extern void MaybeLogSyncpoint(void);
extern void SyncpointRegister(int node_id, XLogRecPtr origin_lsn,
							  XLogRecPtr local_lsn, XLogRecPtr trim_lsn);
extern Syncpoint SyncpointGetLatest(int node_id);
extern Syncpoint *SyncpointGetAllLatest(void);
extern XLogRecPtr QueryRecoveryHorizon(PGconn *conn, int node_id, Syncpoint *local_spvector);
extern HTAB *RecoveryFilterLoad(int filter_node_id, Syncpoint *spvector);

#endif							/* SYNCPOINT_H */