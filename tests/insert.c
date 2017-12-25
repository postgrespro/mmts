#include <time.h>
#include <stdio.h>
#include <stdarg.h>
#include <stdlib.h>
#include <string.h>
#include <unistd.h>
#include <inttypes.h>
#include <sys/time.h>
#include <pthread.h>
#include <libpq-fe.h>

#define MAX_THREADS 1024
#define TEXTOID 25

char const* connection = "dbname=regression host=localhost port=5432 sslmode=disable";
int n_records = 100;
int n_clients = 100;
int first_tid;
int delay = 0;
long inserts[MAX_THREADS];
volatile int termination;

void* worker(void* arg)
{
	PGconn* con = PQconnectdb(connection);
	PGresult *res;
	ConnStatusType status = PQstatus(con);
	Oid paramTypes[1] = { TEXTOID };
	int id = (size_t)arg;
	int i;

	if (status != CONNECTION_OK)
	{
		fprintf(stderr, "Could not establish connection to server %s, error = %s",
				connection, PQerrorMessage(con));
		exit(1);
	}
	PQprepare(con,
			  "ins",
			  "insert into t values ($1)",
			  1,
			  paramTypes);

	for (i = 0; !termination; i++) {
		char key[64];
		char const* paramValues[] = {key};
		sprintf(key, "%d.%d", first_tid + id, i);
		res = PQexecPrepared(con, "ins", 1, paramValues, NULL, NULL, 0);
		if (PQresultStatus(res) != PGRES_COMMAND_OK) {
			fprintf(stderr, "Insert %d failed: %s\n", i, PQresultErrorMessage(res));
			exit(1);
		}
		if (strcmp(PQcmdTuples(res), "1") != 0) {
			fprintf(stderr, "Insert affect wrong number of tuples: %s\n", PQcmdTuples(res));
			exit(1);
		}
		usleep(delay);
		inserts[id] += 1;
		PQclear(res);
	}
	return NULL;
}



int main (int argc, char* argv[])
{
	int i;
	pthread_t threads[MAX_THREADS];
	long thread_inserts[MAX_THREADS];
	int test_duration = 10;
	time_t finish;
	int iteration = 0;
	int initialize = 0;
	long total;
	PGconn* con;

	if (argc == 1) {
        fprintf(stderr, "Use -h to show usage options\n");
        return 1;
    }

    for (i = 1; i < argc; i++) {
        if (argv[i][0] == '-') {
            switch (argv[i][1]) {
			  case 'c':
				n_clients = atoi(argv[++i]);
				continue;
			  case 't':
				test_duration = atoi(argv[++i]);
				continue;
			  case 'd':
                connection = argv[++i];
                continue;
			  case 'i':
				initialize = 1;
				continue;
			  case 'p':
				delay = atoi(argv[++i]);
				continue;
			  case 'f':
				first_tid = atoi(argv[++i]);
				continue;
			}
        }
		printf("Options:\n"
			   "\t-i\tinitialize database\n"
			   "\t-f\tfirst thread ID (default 0)\n"
			   "\t-p\tdelay (microseconds)\n"
			   "\t-c\tnumber of client (default 100)\n"
			   "\t-t\ttest duration (default 10 sec)\n"
			   "\t-d\tconnection string ('host=localhost port=5432')\n");
        return 1;
    }
	con = PQconnectdb(connection);
	if (initialize) {
		PQexec(con, "drop table if exists t");
	}
	PQexec(con, "create table if not exists t(k text primary key)");

	for (i = 0; i < n_clients; i++) {
		thread_inserts[i] = 0;
		pthread_create(&threads[i], NULL, worker, (void*)(size_t)i);
	}
	finish = time(NULL) + test_duration;
	do {
		total = 0;
		sleep(1);
		for (i = 0; i < n_clients; i++) {
			total += inserts[i] - thread_inserts[i];
			thread_inserts[i] = inserts[i];
		}
		printf("%d: %ld\n", ++iteration, total);
	} while (time(NULL) < finish);

	total = 0;
	for (i = 0; i < n_clients; i++) {
		total += inserts[i];
	}
	printf("Total %ld inserts, %ld inserts per second\n", total, total/test_duration);

	termination = 1;

	for (i = 0; i < n_clients; i++) {
		pthread_join(threads[i], NULL);
	}
	return 0;
}
