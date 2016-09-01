/*-------------------------------------------------------------------------
 *
 * receiver_raw.c
 *		Receive and apply logical changes generated by decoder_raw. This
 *		creates some basics for a multi-master cluster using vanilla
 *		PostgreSQL without modifying its code.
 *
 * Copyright (c) 1996-2015, PostgreSQL Global Development Group
 *
 * IDENTIFICATION
 *		receiver_raw/receiver_raw.c
 *
 *-------------------------------------------------------------------------
 */

/* Some general headers for custom bgworker facility */
#include <unistd.h>
#include "postgres.h"
#include "fmgr.h"
#include "miscadmin.h"
#include "libpq-fe.h"
#include "pqexpbuffer.h"
#include "access/xact.h"
#include "access/clog.h"
#include "access/transam.h"
#include "lib/stringinfo.h"
#include "libpq/pqformat.h"
#include "pgstat.h"
#include "postmaster/bgworker.h"
#include "storage/ipc.h"
#include "storage/latch.h"
#include "storage/proc.h"
#include "utils/guc.h"
#include "utils/snapmgr.h"
#include "utils/memutils.h"
#include "executor/spi.h"
#include "replication/origin.h"

#include "multimaster.h"
#include "spill.h"

#define ERRCODE_DUPLICATE_OBJECT_STR  "42710"
#define RECEIVER_SUSPEND_TIMEOUT (1*USECS_PER_SEC)

/* Signal handling */
static volatile sig_atomic_t got_sigterm = false;
static volatile sig_atomic_t got_sighup = false;

/* GUC variables */
static int receiver_idle_time = 0;
static bool receiver_sync_mode = true; /* We need sync mode to have up-to-date values of catalog_xmin in replication slots */

/* Worker name */
static char worker_proc[BGW_MAXLEN];

/* Lastly written positions */
static XLogRecPtr output_written_lsn = InvalidXLogRecPtr;

/* Stream functions */
static void fe_sendint64(int64 i, char *buf);
static int64 fe_recvint64(char *buf);

static void
receiver_raw_sigterm(SIGNAL_ARGS)
{
	int save_errno = errno;
	got_sigterm = true;
	if (MyProc)
		SetLatch(&MyProc->procLatch);
	errno = save_errno;
}

static void
receiver_raw_sighup(SIGNAL_ARGS)
{
	int save_errno = errno;
	got_sighup = true;
	if (MyProc)
		SetLatch(&MyProc->procLatch);
	errno = save_errno;
}

/*
 * Send a Standby Status Update message to server.
 */
static bool
sendFeedback(PGconn *conn, int64 now, int node_id)
{
	char		replybuf[1 + 8 + 8 + 8 + 8 + 1];
	int		 len = 0;
	XLogRecPtr output_applied_lsn = output_written_lsn;
	XLogRecPtr output_flushed_lsn = MtmGetFlushPosition(node_id);

	replybuf[len] = 'r';
	len += 1;
	fe_sendint64(output_written_lsn, &replybuf[len]);   /* write */
	len += 8;
	fe_sendint64(output_flushed_lsn, &replybuf[len]);	/* flush */
	len += 8;
	fe_sendint64(output_applied_lsn, &replybuf[len]);	/* apply */
	len += 8;
	fe_sendint64(now, &replybuf[len]);  /* sendTime */
	len += 8;

	/* No reply requested from server */
	replybuf[len] = 0;
	len += 1;

	if (PQputCopyData(conn, replybuf, len) <= 0 || PQflush(conn))
	{
		ereport(LOG, (errmsg("%s: could not send feedback packet: %s",
							 worker_proc, PQerrorMessage(conn))));
		return false;
	}

	return true;
}

/*
 * Converts an int64 to network byte order.
 */
static void
fe_sendint64(int64 i, char *buf)
{
	uint32	  n32;

	/* High order half first, since we're doing MSB-first */
	n32 = (uint32) (i >> 32);
	n32 = htonl(n32);
	memcpy(&buf[0], &n32, 4);

	/* Now the low order half */
	n32 = (uint32) i;
	n32 = htonl(n32);
	memcpy(&buf[4], &n32, 4);
}

/*
 * Converts an int64 from network byte order to native format.
 */
static int64
fe_recvint64(char *buf)
{
	int64	   result;
	uint32	  h32;
	uint32	  l32;

	memcpy(&h32, buf, 4);
	memcpy(&l32, buf + 4, 4);
	h32 = ntohl(h32);
	l32 = ntohl(l32);

	result = h32;
	result <<= 32;
	result |= l32;

	return result;
}

static int64
feGetCurrentTimestamp(void)
{
	int64	   result;
	struct timeval tp;

	gettimeofday(&tp, NULL);

	result = (int64) tp.tv_sec -
		((POSTGRES_EPOCH_JDATE - UNIX_EPOCH_JDATE) * SECS_PER_DAY);

	result = (result * USECS_PER_SEC) + tp.tv_usec;

	return result;
}

static void
feTimestampDifference(int64 start_time, int64 stop_time,
					  long *secs, int *microsecs)
{
	int64	   diff = stop_time - start_time;

	if (diff <= 0)
	{
		*secs = 0;
		*microsecs = 0;
	}
	else
	{
		*secs = (long) (diff / USECS_PER_SEC);
		*microsecs = (int) (diff % USECS_PER_SEC);
	}
}

static char const* const MtmReplicationModeName[] = 
{
	"exit",
	"recovered", /* recovery of node is completed so drop old slot and restart replication from the current position in WAL */
	"recovery",  /* perform recorvery of the node by applying all data from theslot from specified point */
	"normal"     /* normal mode: use existed slot or create new one and start receiving data from it from the specified position */
};

static void
pglogical_receiver_main(Datum main_arg)
{
	int nodeId = DatumGetInt32(main_arg);
	/* Variables for replication connection */
	PQExpBuffer query;
	PGconn *conn;
	PGresult *res;
	MtmReplicationMode mode;

    ByteBuffer buf;
	RepOriginId originId;
	char* originName;
	/* Buffer for COPY data */
	char	*copybuf = NULL;
	int spill_file = -1;
	StringInfoData spill_info;
	char *slotName;
	char* connString = psprintf("replication=database %s", Mtm->nodes[nodeId-1].con.connStr);
	slotName = psprintf(MULTIMASTER_SLOT_PATTERN, MtmNodeId);

	MtmIsLogicalReceiver = true;

	initStringInfo(&spill_info);

	/* Register functions for SIGTERM/SIGHUP management */
	pqsignal(SIGHUP, receiver_raw_sighup);
	pqsignal(SIGTERM, receiver_raw_sigterm);

	MtmCreateSpillDirectory(nodeId);

	Mtm->nodes[nodeId-1].receiverPid = MyProcPid;
	Mtm->nodes[nodeId-1].receiverStartTime = MtmGetSystemTime();

    sprintf(worker_proc, "mtm_pglogical_receiver_%d_%d", MtmNodeId, nodeId);

	/* We're now ready to receive signals */
	BackgroundWorkerUnblockSignals();

	/* Connect to a database */
	BackgroundWorkerInitializeConnection(MtmDatabaseName, NULL);

	/* This is main loop of logical replication.
	 * In case of errors we will try to reestablish connection.
	 * Also reconnet is forced when node is switch to recovery mode
	 */
	while (!got_sigterm)
	{ 
		int  count;
		ConnStatusType status;
		XLogRecPtr originStartPos = InvalidXLogRecPtr;
		int timeline;

		/* 
		 * Determine when and how we should open replication slot.
		 * Druing recovery we need to open only one replication slot from which node should receive all transactions.
		 * Slots at other nodes should be removed 
		 */
		mode = MtmGetReplicationMode(nodeId, &got_sigterm);	
		if (mode == REPLMODE_EXIT) 
		{ 
			break;
		}
		timeline = Mtm->nodes[nodeId-1].timeline;
		count = Mtm->recoveryCount;
		
		/* Establish connection to remote server */
		conn = PQconnectdb_safe(connString);
		status = PQstatus(conn);
		if (status != CONNECTION_OK)
		{
			ereport(WARNING, (errmsg("%s: Could not establish connection to remote server (%s), status = %d, error = %s",
									 worker_proc, connString, status, PQerrorMessage(conn))));
			goto OnError;
		}
		
		query = createPQExpBuffer();
		if (mode == REPLMODE_NORMAL && timeline != Mtm->nodes[nodeId-1].timeline) { /* recreate slot */
			appendPQExpBuffer(query, "DROP_REPLICATION_SLOT \"%s\"", slotName);
			res = PQexec(conn, query->data);
			PQclear(res);
			resetPQExpBuffer(query);
			timeline = Mtm->nodes[nodeId-1].timeline;
		}
		/* My original assumption was that we can perfrom recovery only fromm existed slot, 
		 * but unfortunately looks like slots can "disapear" together with WAL-sender.
		 * So let's try to recreate slot always. */
		/* if (mode != REPLMODE_REPLICATION) */
		{ 
			appendPQExpBuffer(query, "CREATE_REPLICATION_SLOT \"%s\" LOGICAL \"%s\"", slotName, MULTIMASTER_NAME);
			res = PQexec(conn, query->data);
			if (PQresultStatus(res) != PGRES_TUPLES_OK)
			{
				const char *sqlstate = PQresultErrorField(res, PG_DIAG_SQLSTATE);
				if (!sqlstate || strcmp(sqlstate, ERRCODE_DUPLICATE_OBJECT_STR) != 0)
				{
					PQclear(res);
					ereport(ERROR, (errmsg("%s: Could not create logical slot",
										   worker_proc)));
					
					goto OnError;
				}
			}
			PQclear(res);
			resetPQExpBuffer(query);
		}
		
		/* Start logical replication at specified position */
		if (mode == REPLMODE_RECOVERED) {
			originStartPos = Mtm->nodes[nodeId-1].restartLsn;
			MTM_LOG1("Restart replication from node %d from position %lx", nodeId, originStartPos);
		} 
		if (originStartPos == InvalidXLogRecPtr) { 
			StartTransactionCommand();
			originName = psprintf(MULTIMASTER_SLOT_PATTERN, nodeId);
			originId = replorigin_by_name(originName, true);
			if (originId == InvalidRepOriginId) { 
				originId = replorigin_create(originName);
				/* 
				 * We are just creating new replication slot.
				 * It is assumed that state of local and remote nodes is the same at this moment.
				 * Them are either empty, either new node is synchronized using base_backup.
				 * So we assume that LSNs are the same for local and remote node
				 */
				originStartPos = Mtm->status == MTM_RECOVERY ? GetXLogInsertRecPtr() : InvalidXLogRecPtr;
				MTM_LOG1("Start logical receiver at position %lx from node %d", originStartPos, nodeId);
			} else { 
				originStartPos = replorigin_get_progress(originId, false);
				MTM_LOG1("Restart logical receiver at position %lx with origin=%d from node %d", originStartPos, originId, nodeId);
			}
			Mtm->nodes[nodeId-1].originId = originId;
			CommitTransactionCommand();
		}
		
		appendPQExpBuffer(query, "START_REPLICATION SLOT \"%s\" LOGICAL %x/%x (\"startup_params_format\" '1', \"max_proto_version\" '%d',  \"min_proto_version\" '%d', \"forward_changesets\" '1', \"mtm_replication_mode\" '%s')",
						  slotName,
						  (uint32) (originStartPos >> 32),
						  (uint32) originStartPos,
						  MULTIMASTER_MAX_PROTO_VERSION,
						  MULTIMASTER_MIN_PROTO_VERSION,
						  MtmReplicationModeName[mode]
			);
		res = PQexec(conn, query->data);
		if (PQresultStatus(res) != PGRES_COPY_BOTH)
		{
			PQclear(res);
			ereport(WARNING, (errmsg("%s: Could not start logical replication",
								 worker_proc)));
			goto OnError;
		}
		PQclear(res);
		resetPQExpBuffer(query);
		
		MtmReceiverStarted(nodeId);
		ByteBufferAlloc(&buf);
		
		while (!got_sigterm)
		{
			int rc, hdr_len;
			/* Wait necessary amount of time */
			rc = WaitLatch(&MyProc->procLatch,
						   WL_LATCH_SET | WL_TIMEOUT | WL_POSTMASTER_DEATH,
						   receiver_idle_time * 1L);
			ResetLatch(&MyProc->procLatch);
			/* Process signals */
			if (got_sighup)
			{
				/* Process config file */
				ProcessConfigFile(PGC_SIGHUP);
				got_sighup = false;
				ereport(LOG, (errmsg("%s: processed SIGHUP", worker_proc)));
			}
			
			if (got_sigterm)
			{
				/* Simply exit */
				ereport(LOG, (errmsg("%s: processed SIGTERM", worker_proc)));
				proc_exit(0);
			}
			
			/* Emergency bailout if postmaster has died */
			if (rc & WL_POSTMASTER_DEATH)
				proc_exit(1);
			
			if (Mtm->status == MTM_OFFLINE || (Mtm->status == MTM_RECOVERY && Mtm->recoverySlot != nodeId)) 
			{
				ereport(LOG, (errmsg("%s: restart WAL receiver because node was switched to %s mode", worker_proc, MtmNodeStatusMnem[Mtm->status])));
				break;
			}
			if (count != Mtm->recoveryCount) { 
				
				ereport(LOG, (errmsg("%s: restart WAL receiver because node was recovered", worker_proc)));
				break;
			}
			

			/*
			 * Receive data.
			 */
			while (true)
			{
				XLogRecPtr  walEnd;
				char* stmt;
				
				/* Some cleanup */
				if (copybuf != NULL)
				{
					PQfreemem(copybuf);
					copybuf = NULL;
				}
				
				rc = PQgetCopyData(conn, &copybuf, 1);
				if (rc <= 0) {
					break;
				}
				
				/*
				 * Check message received from server:
				 * - 'k', keepalive message
				 * - 'w', check for streaming header
				 */
				if (copybuf[0] == 'k')
				{
					int			pos;
					bool		replyRequested;
					
					/*
					 * Parse the keepalive message, enclosed in the CopyData message.
					 * We just check if the server requested a reply, and ignore the
					 * rest.
					 */
					pos = 1;	/* skip msgtype 'k' */

					/*
					 * In this message is the latest WAL position that server has
					 * considered as sent to this receiver.
					 */
					walEnd = fe_recvint64(&copybuf[pos]);
					pos += 8;	/* read walEnd */
					pos += 8;	/* skip sendTime */
					if (rc < pos + 1)
					{
						ereport(LOG, (errmsg("%s: streaming header too small: %d",
											 worker_proc, rc)));
						goto OnError;
					}
					replyRequested = copybuf[pos];

					/* Update written position */
					output_written_lsn = Max(walEnd, output_written_lsn);

					/*
					 * If the server requested an immediate reply, send one.
					 * If sync mode is sent reply in all cases to ensure that
					 * server knows how far replay has been done.
					 * In recovery mode also always send reply to provide master with more precise information
					 * about recovery progress
					 */
					if (replyRequested || receiver_sync_mode || Mtm->status == MTM_RECOVERY)
					{
						int64 now = feGetCurrentTimestamp();

						/* Leave is feedback is not sent properly */
						MtmUpdateLsnMapping(nodeId, walEnd);
						if (!sendFeedback(conn, now, nodeId)) {
							goto OnError;
						}
					}
					continue;
				}
				else if (copybuf[0] != 'w')
				{
					ereport(LOG, (errmsg("%s: Incorrect streaming header",
										 worker_proc)));
					goto OnError;
				}

				/* Now fetch the data */
				hdr_len = 1;		/* msgtype 'w' */
				fe_recvint64(&copybuf[hdr_len]);
				hdr_len += 8;		/* dataStart */
				walEnd = fe_recvint64(&copybuf[hdr_len]);
				hdr_len += 8;		/* WALEnd */
				hdr_len += 8;		/* sendTime */

				/*ereport(LOG, (errmsg("%s: receive message %c length %d", worker_proc, copybuf[hdr_len], rc - hdr_len)));*/

				Assert(rc >= hdr_len);

				if (rc > hdr_len)
				{
					stmt = copybuf + hdr_len;
					if (mode == REPLMODE_RECOVERED) {
						if (stmt[0] != 'B') {
							output_written_lsn = Max(walEnd, output_written_lsn);
							continue;
						}
						mode = REPLMODE_NORMAL;
					}

					if (buf.used >= MtmTransSpillThreshold*MB) { 
						if (spill_file < 0) {
							int file_id;
							spill_file = MtmCreateSpillFile(nodeId, &file_id);
							pq_sendbyte(&spill_info, 'F');
							pq_sendint(&spill_info, nodeId, 4);
							pq_sendint(&spill_info, file_id, 4);
						}
						ByteBufferAppend(&buf, ")", 1);
						pq_sendbyte(&spill_info, '(');
						pq_sendint(&spill_info, buf.used, 4);
						MtmSpillToFile(spill_file, buf.data, buf.used);
						ByteBufferReset(&buf);
					}
					ByteBufferAppend(&buf, stmt, rc - hdr_len);
					if (stmt[0] == 'C') /* commit */
					{
						if (spill_file >= 0) { 
							ByteBufferAppend(&buf, ")", 1);
							pq_sendbyte(&spill_info, '(');
							pq_sendint(&spill_info, buf.used, 4);
							MtmSpillToFile(spill_file, buf.data, buf.used);
							MtmCloseSpillFile(spill_file);
							MtmExecute(spill_info.data, spill_info.len);
							spill_file = -1;
							resetStringInfo(&spill_info);
						} else { 
							MtmExecute(buf.data, buf.used);
						}
						ByteBufferReset(&buf);
					}
				}
				/* Update written position */
				output_written_lsn = Max(walEnd, output_written_lsn);
			}

			/* No data, move to next loop */
			if (rc == 0)
			{
				/*
				 * In async mode, and no data available. We block on reading but
				 * not more than the specified timeout, so that we can send a
				 * response back to the client.
				 */
				int			r;
				fd_set	  input_mask;
				int64	   message_target = 0;
				int64	   fsync_target = 0;
				struct timeval timeout;
				struct timeval *timeoutptr = NULL;
				int64	   targettime;
				long		secs;
				int		 usecs;
				int64		now;

				FD_ZERO(&input_mask);
				FD_SET(PQsocket(conn), &input_mask);

				/* Now compute when to wakeup. */
				targettime = message_target;

				if (fsync_target > 0 && fsync_target < targettime)
					targettime = fsync_target;
				now = feGetCurrentTimestamp();
				feTimestampDifference(now,
									  targettime,
									  &secs,
									  &usecs);
				if (secs <= 0)
					timeout.tv_sec = 1; /* Always sleep at least 1 sec */
				else
					timeout.tv_sec = secs;
				timeout.tv_usec = usecs;
				timeoutptr = &timeout;

				r = select(PQsocket(conn) + 1, &input_mask, NULL, NULL, timeoutptr);
				if (r == 0)
				{
					int64 now = feGetCurrentTimestamp();
					
					/* Leave is feedback is not sent properly */
					MtmUpdateLsnMapping(nodeId, InvalidXLogRecPtr);
					sendFeedback(conn, now, nodeId);
				}
				else if (r < 0 && errno == EINTR)
				{
					/*
					 * Got a timeout or signal. Continue the loop and either
					 * deliver a status packet to the server or just go back into
					 * blocking.
					 */
					continue;
				}
				else if (r < 0)
				{
					ereport(LOG, (errmsg("%s: Incorrect status received.",
										 worker_proc)));
					
					goto OnError;
				}

				/* Else there is actually data on the socket */
				if (PQconsumeInput(conn) == 0)
				{
					ereport(LOG, (errmsg("%s: Data remaining on the socket.",
										 worker_proc)));
					goto OnError;
				}
				continue;
			}

			/* End of copy stream */
			if (rc == -1)
			{
				ereport(LOG, (errmsg("%s: COPY Stream has abruptly ended...",
									 worker_proc)));
				goto OnError;
			}

			/* Failure when reading copy stream, leave */
			if (rc == -2)
			{
				ereport(LOG, (errmsg("%s: Failure while receiving changes...",
									 worker_proc)));
				goto OnError;
			}
		}
		PQfinish(conn);
		continue;

	  OnError:
		PQfinish(conn);
		MtmSleep(RECEIVER_SUSPEND_TIMEOUT);		
	}
    ByteBufferFree(&buf);
	/* Restart this bgworker */
	proc_exit(1);
}

void MtmStartReceiver(int nodeId, bool dynamic)
{
	BackgroundWorker worker;
	MemoryContext oldContext = MemoryContextSwitchTo(TopMemoryContext);

	MemSet(&worker, 0, sizeof(BackgroundWorker));
    worker.bgw_flags = BGWORKER_SHMEM_ACCESS |  BGWORKER_BACKEND_DATABASE_CONNECTION;
	worker.bgw_start_time = BgWorkerStart_ConsistentState;
	worker.bgw_main = pglogical_receiver_main; 
	worker.bgw_restart_time = MULTIMASTER_BGW_RESTART_TIMEOUT;
	
	/* Worker parameter and registration */
	snprintf(worker.bgw_name, BGW_MAXLEN, "mtm_pglogical_receiver_%d_%d", MtmNodeId, nodeId);
	
	worker.bgw_main_arg = Int32GetDatum(nodeId);
	if (dynamic) { 
		BackgroundWorkerHandle *handle;
		RegisterDynamicBackgroundWorker(&worker, &handle);
	} else {
		RegisterBackgroundWorker(&worker);
	}

	MemoryContextSwitchTo(oldContext);
}

void MtmStartReceivers(void)
{
    int i;
	for (i = 0; i < MtmNodes; i++) {
        if (i+1 != MtmNodeId) {
			MtmStartReceiver(i+1, false);
        }
    }
}

