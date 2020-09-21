#!/bin/sh

if [ "$1" = 'postgres' ]; then
	mkdir -p "$PGDATA"
	mkdir -p /pg/archive/
	mkdir -p /pg/src/src/test/regress/testtablespace

	# look specifically for PG_VERSION, as it is expected in the DB dir
	if [ ! -s "$PGDATA/PG_VERSION" ]; then
		initdb --nosync

		{ echo; echo "host all all 0.0.0.0/0 trust"; } >> "$PGDATA/pg_hba.conf"
		{ echo; echo "host replication all 0.0.0.0/0 trust"; } >> "$PGDATA/pg_hba.conf"

		cat <<-EOF >> $PGDATA/postgresql.conf
			listen_addresses='*'
			log_line_prefix = '%m [%p] [%a]: '
			archive_mode = on
			archive_command = 'cp %p /pg/archive/%f'

			fsync = on

			max_prepared_transactions = 100
			wal_level = logical
			max_worker_processes = 100
			max_replication_slots = 10
			max_wal_senders = 10
			log_statement = all
			log_connections = true
			log_lock_waits = true

			shared_preload_libraries = 'multimaster'
			multimaster.volkswagen_mode = off
                        multimaster.max_workers = 30

			multimaster.connect_timeout = 10
			# Be careful; tests expect commits on live
			# nodes during others failures, and failure time is ~10s;
			# if we simulate network loss, failure won't be
			# detected until this timeout passes.
			# OTOH, setting it too low might lead to node
			# exclusions on weak machines during normal work.
                        # It was also noticed that if extensive logging is enabled
			# (older, at least pre #6392) journald might not be able
			# to swallow logs time which also provoked exlusions
			# with 2s timeout
			multimaster.heartbeat_recv_timeout = 2000
			multimaster.heartbeat_send_timeout = 200
			wal_sender_timeout = 10min
			wal_receiver_status_interval = 10s
		EOF

		if [ -n "$MAJOR" ]; then
			echo 'multimaster.major_node = on' >> $PGDATA/postgresql.conf
		fi

		if [ -n "$REFEREE" ]; then
			echo 'multimaster.referee = on' >> $PGDATA/postgresql.conf
		fi

		if [ -n "$REFEREE_CONNSTR" ]; then
			echo "multimaster.referee_connstring = '$REFEREE_CONNSTR'" >> $PGDATA/postgresql.conf
		fi

		# internal start of server in order to allow set-up using psql-client
		# does not listen on TCP/IP and waits until start finishes
		pg_ctl -D "$PGDATA" \
			-o "-c listen_addresses=''" \
			-w start

		: ${POSTGRES_USER:=postgres}
		: ${POSTGRES_DB:=$POSTGRES_USER}
		export POSTGRES_USER POSTGRES_DB

		if [ "$POSTGRES_DB" != 'postgres' ]; then
			psql -U `whoami` postgres <<-EOSQL
				CREATE DATABASE "$POSTGRES_DB" ;
			EOSQL
			echo
		fi

		if [ "$POSTGRES_USER" = `whoami` ]; then
			op='ALTER'
		else
			op='CREATE'
		fi

		psql -U `whoami` postgres <<-EOSQL
			$op USER "$POSTGRES_USER" WITH SUPERUSER PASSWORD '';
		EOSQL
		echo

		# psql -U `whoami` $POSTGRES_DB -c 'CREATE EXTENSION multimaster;';
		# psql -U `whoami` $POSTGRES_DB -c "select mtm.init_node($NODE_ID, '{$CONNSTRS}');"

		pg_ctl -D "$PGDATA" -m fast -w stop
	fi
fi

"$@"
