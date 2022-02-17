/*-------------------------------------------------------------------------
 *
 * mtm_utils.h
 *	Utility functions:
 *	- disable global timeouts settings;
 *	- libpq connect function wrappers.
 *
 *
 * Copyright (c) 2022, Postgres Professional
 *
 *-------------------------------------------------------------------------
 */
#ifndef MTM_UTILS_H
#define MTM_UTILS_H

#include "libpq/pqformat.h"
#include "libpq-fe.h"

extern void MtmDisableTimeouts(void);

extern PostgresPollingStatusType MtmPQconnectPoll(PGconn *conn);
extern PGconn* MtmPQconnectdb(const char *conninfo);

#endif
