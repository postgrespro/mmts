CURPATH=`pwd`
USER=`whoami`
BASEDIR=$CURPATH/../../..
export PATH=$BASEDIR/tmp_install/usr/local/pgsql/bin/:$PATH
export DESTDIR=$BASEDIR/tmp_install
export PGHOST=127.0.0.1
export LD_LIBRARY_PATH=$BASEDIR/tmp_install/usr/local/pgsql/lib/:$LD_LIBRARY_PATH

n_nodes=3
ulimit -c unlimited
pkill -9 postgres

cd $BASEDIR
make install

cd $BASEDIR/contrib/mmts
make clean && make install

cd $BASEDIR/contrib/referee
make clean && make install

cd $BASEDIR/contrib/mmts/tests

rm -rf tmp_check *.log

###############################################################################

initdb tmp_check/referee
cat <<SQL > tmp_check/referee/postgresql.conf
    listen_addresses='*'
    port = '5440'
SQL
pg_ctl -w -D tmp_check/referee -l referee.log start
createdb -p 5440
psql -p 5440 -c 'create extension referee'

###############################################################################

conn_str=""
sep=""
for ((i=1;i<=n_nodes;i++))
do    
    port=$((5431 + i))
    arbiter_port=$((7000 + i))
    conn_str="$conn_str${sep} dbname=$USER host=127.0.0.1 port=$port arbiter_port=$arbiter_port sslmode=disable"
    sep=","
    initdb tmp_check/node$i
    pg_ctl -w -D tmp_check/node$i -l node$i.log start
    createdb
    pg_ctl -w -D tmp_check/node$i -l node$i.log stop
done

echo "Starting nodes..."

echo Start nodes
for ((i=1;i<=n_nodes;i++))
do
    port=$((5431+i))
    arbiter_port=$((7000 + i))

    cat <<SQL > tmp_check/node$i/postgresql.conf
        listen_addresses='*'
        port = '$port'
        max_prepared_transactions = 300
        fsync = on
        wal_level = logical
        max_worker_processes = 300
        max_replication_slots = 10
        max_wal_senders = 10
        shared_preload_libraries = 'multimaster'
        wal_writer_delay = 5ms

        multimaster.heartbeat_recv_timeout = 2000
        multimaster.heartbeat_send_timeout = 250
        multimaster.conn_strings = '$conn_str'
        multimaster.node_id = $i
        multimaster.arbiter_port = $arbiter_port
        multimaster.max_recovery_lag = 30GB
        multimaster.referee_connstring = 'dbname=$USER host=127.0.0.1 port=5440 sslmode=disable'
        multimaster.monotonic_sequences = on
SQL

    cat <<CONF >> tmp_check/node$i/pg_hba.conf
        host all all 0.0.0.0/0 trust
        host replication all 0.0.0.0/0 trust
CONF

    # cp pg_hba.conf tmp_check/node$i
    pg_ctl -w -D tmp_check/node$i -l node$i.log start
done

sleep 10
psql < regress.sql
psql -c 'create extension multimaster'

echo Done
