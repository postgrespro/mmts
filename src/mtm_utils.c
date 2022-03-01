/*----------------------------------------------------------------------------
 *
 * mtm_utils.c
 *	  Utility functions
 *
 * Copyright (c) 2022, Postgres Professional
 *
 *----------------------------------------------------------------------------
 */

#include "logger.h"
#include "mtm_utils.h"

#include "utils/timeout.h"

/*
 * Disables timeouts on a client side:
 * - statement_timeout;
 * - lock_timeout;
 * - idle_in_transaction_session_timeout;
 * - idle_session_timeout.
 *
 * This timeouts, when set in the postgres config file, affect all process.
 * The multimaster needs his sessions not to be interrupted, so we disable
 * these timeouts.
 *
 * This function raises an error on PQExec failed.
 */
static bool
disable_client_timeouts(PGconn *conn)
{
	PGresult   *res;

	res = PQexec(conn, "SET statement_timeout = 0");
	if (PQresultStatus(res) != PGRES_COMMAND_OK)
		mtm_log(ERROR, "failed to set statement_timeout: %s",
				pchomp(PQerrorMessage(conn)));

	res = PQexec(conn, "SET lock_timeout = 0");
	if (PQresultStatus(res) != PGRES_COMMAND_OK)
		mtm_log(ERROR, "failed to set lock_timeout: %s",
				pchomp(PQerrorMessage(conn)));

	res = PQexec(conn, "SET idle_in_transaction_session_timeout = 0");
	if (PQresultStatus(res) != PGRES_COMMAND_OK)
		mtm_log(ERROR, "failed to set idle_in_transaction_session_timeout: %s",
				pchomp(PQerrorMessage(conn)));

	res = PQexec(conn, "SET idle_session_timeout = 0");
	if (PQresultStatus(res) != PGRES_COMMAND_OK)
		mtm_log(ERROR, "failed to set idle_session_timeout: %s",
				pchomp(PQerrorMessage(conn)));

	return true;
}

/*
 * Disable timeouts for a current process
 * - statement_timeout;
 * - lock_timeout;
 * - idle_in_transaction_session_timeout;
 * - idle_session_timeout.
 *
 * We disable these timeout for the same reason as in the disable_client_timeout()
 */
extern void
MtmDisableTimeouts(void)
{
	if (get_timeout_active(STATEMENT_TIMEOUT))
		disable_timeout(STATEMENT_TIMEOUT, false);
	if (get_timeout_active(LOCK_TIMEOUT))
		disable_timeout(LOCK_TIMEOUT, false);
	if (get_timeout_active(IDLE_IN_TRANSACTION_SESSION_TIMEOUT))
		disable_timeout(IDLE_IN_TRANSACTION_SESSION_TIMEOUT, false);
	if (get_timeout_active(IDLE_SESSION_TIMEOUT))
		disable_timeout(IDLE_SESSION_TIMEOUT, false);
}

/*
 * Wrapper on PQconnectPoll
 *
 * On connect disables timeouts on a client side
 */
PostgresPollingStatusType
MtmPQconnectPoll(PGconn *conn)
{
	PostgresPollingStatusType status;

	status = PQconnectPoll(conn);
	if (status != PGRES_POLLING_OK)
		return status;

	disable_client_timeouts(conn);

	return status;
}

/*
 * Wrapper on PQconnectdb
 *
 * On connect disables timeouts on a client side
 */
PGconn *
MtmPQconnectdb(const char *conninfo)
{
	PGconn *conn;

	conn = PQconnectdb(conninfo);
	if (PQstatus(conn) != CONNECTION_OK)
		return conn;

	disable_client_timeouts(conn);

	return conn;
}

