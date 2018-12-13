-- complain if script is sourced in psql, rather than via CREATE EXTENSION
\echo Use "CREATE EXTENSION multimaster" to load this file. \quit

-- check that multimaster shared library is really loaded
DO $$
BEGIN
    IF strpos(current_setting('shared_preload_libraries'), 'multimaster') = 0 THEN
        RAISE EXCEPTION 'Multimaster must be loaded via shared_preload_libraries. Refusing to proceed.';
    END IF;
END
$$;


CREATE FUNCTION mtm.dmq_receiver_loop(sender_name text) RETURNS void
AS 'MODULE_PATHNAME','dmq_receiver_loop'
LANGUAGE C;


-- Stop replication to the node. Node is didsabled, If drop_slot is true, then replication slot is dropped and node can be recovered using basebackup and recover_node function.
-- If drop_slot is false and limit for maximal slot gap was not reached, then node can be restarted using resume_node function.
CREATE FUNCTION mtm.stop_node(node integer, drop_slot bool default false) RETURNS void
AS 'MODULE_PATHNAME','mtm_stop_node'
LANGUAGE C;

-- Add new node to the cluster. Number of nodes should not exeed maximal number of nodes in the cluster.
CREATE FUNCTION mtm.add_node(conn_str text) RETURNS void
AS 'MODULE_PATHNAME','mtm_add_node'
LANGUAGE C;

-- Create replication slot for the node which was previously stalled (its replicatoin slot was deleted)
CREATE FUNCTION mtm.recover_node(node integer) RETURNS void
AS 'MODULE_PATHNAME','mtm_recover_node'
LANGUAGE C;

-- Resume previously stopped node with live replication slot. If node was not stopped, this function has no effect.
-- It doesn't create slot and returns error if node is stalled (slot eas dropped)
CREATE FUNCTION mtm.resume_node(node integer) RETURNS void
AS 'MODULE_PATHNAME','mtm_resume_node'
LANGUAGE C;


CREATE TYPE mtm.node_state AS (
    "id" integer,
    "enabled" bool,
    "connected" bool,
    "slot_active" bool,
    "stopped" bool,
    "catch_up" bool,
    "slot_lag" bigint,
    "avg_trans_delay" bigint,
    "last_status_change" timestamp,
    "oldest_snapshot" bigint,
    "sender_pid" integer,
    "sender_start_time" timestamp,
    "receiver_pid" integer,
    "receiver_start_time" timestamp,
    "conn_str" text,
    "connectivity_mask" bigint,
    "n_heartbeats" bigint
);

CREATE FUNCTION mtm.get_nodes_state() RETURNS SETOF mtm.node_state
AS 'MODULE_PATHNAME','mtm_get_nodes_state'
LANGUAGE C;

CREATE TYPE mtm.cluster_state AS (
    "id" integer,
    "status" text,
    "disabled_node_mask" bigint,
    "disconnected_node_mask" bigint,
    "catch_up_node_mask" bigint,
    "live_nodes" integer,
    "all_nodes" integer,
    "n_active_queries" integer,
    "n_pending_queries" integer,
    "queue_size" bigint,
    "trans_count" bigint,
    "time_shift" bigint,
    "recovery_slot" integer,
    "xid_hash_size" bigint,
    "gid_hash_size" bigint,
    "oldest_xid" bigint,
    "config_changes" integer,
    "stalled_node_mask" bigint,
    "stopped_node_mask" bigint,
    "last_status_change" timestamp
);

CREATE TYPE mtm.trans_state AS (
    "status" text,
    "gid" text,
    "xid" bigint,
    "coordinator" integer,
    "gxid" bigint,
    "csn" timestamp,
    "snapshot" timestamp,
    "local" boolean,
    "prepared" boolean,
    "active" boolean,
    "twophase" boolean,
    "voting_completed" boolean,
    "participants" bigint,
    "voted" bigint,
    "config_changes" integer
);


CREATE FUNCTION mtm.get_cluster_state() RETURNS mtm.cluster_state
AS 'MODULE_PATHNAME','mtm_get_cluster_state'
LANGUAGE C;

CREATE FUNCTION mtm.collect_cluster_info() RETURNS SETOF mtm.cluster_state
AS 'MODULE_PATHNAME','mtm_collect_cluster_info'
LANGUAGE C;

CREATE FUNCTION mtm.make_table_local(relation regclass) RETURNS void
AS 'MODULE_PATHNAME','mtm_make_table_local'
LANGUAGE C;

-- CREATE FUNCTION mtm.broadcast_table(source_table regclass) RETURNS void
-- AS 'MODULE_PATHNAME','mtm_broadcast_table'
-- LANGUAGE C;

-- CREATE FUNCTION mtm.copy_table(source_table regclass, target_node_id integer) RETURNS void
-- AS 'MODULE_PATHNAME','mtm_copy_table'
-- LANGUAGE C;

CREATE FUNCTION mtm.dump_lock_graph() RETURNS text
AS 'MODULE_PATHNAME','mtm_dump_lock_graph'
LANGUAGE C;

CREATE FUNCTION mtm.poll_node(node_id integer, no_wait boolean default FALSE) RETURNS boolean
AS 'MODULE_PATHNAME','mtm_poll_node'
LANGUAGE C;

CREATE FUNCTION mtm.check_deadlock(xid bigint) RETURNS boolean
AS 'MODULE_PATHNAME','mtm_check_deadlock'
LANGUAGE C;

CREATE TABLE mtm.local_tables(
    rel_schema name,
    rel_name name,
    primary key(rel_schema, rel_name)
);

CREATE TABLE mtm.referee_decision(
    key text primary key not null,
    node_id int
);

CREATE TABLE mtm.syncpoints(
    node_id int not null,
    origin_lsn bigint not null,
    local_lsn  bigint not null,
    primary key(node_id, origin_lsn)
);

CREATE OR REPLACE FUNCTION mtm.alter_sequences() RETURNS boolean AS
$$
DECLARE
    seq_class record;
    seq_tuple record;
    node_id int;
    max_nodes int;
    new_start bigint;
    altered boolean := false;
BEGIN
    select current_setting('multimaster.max_nodes') into max_nodes;
    select id, "allNodes" into node_id from mtm.get_cluster_state();
    FOR seq_class IN
        SELECT '"' || ns.nspname || '"."' || seq.relname || '"' as seqname FROM pg_namespace ns,pg_class seq WHERE seq.relkind = 'S' and seq.relnamespace=ns.oid
    LOOP
            EXECUTE 'select * from ' || seq_class.seqname INTO seq_tuple;
            IF seq_tuple.increment_by != max_nodes THEN
                altered := true;
                RAISE NOTICE 'Altering step for sequence % to %.', seq_tuple.sequence_name, max_nodes;
                EXECUTE 'ALTER SEQUENCE ' || seq_class.seqname || ' INCREMENT BY ' || max_nodes || ';';
            END IF;
            IF (seq_tuple.last_value % max_nodes) != node_id THEN
                altered := true;
                new_start := (seq_tuple.last_value / max_nodes + 1)*max_nodes + node_id;
                RAISE NOTICE 'Altering start for sequence % to %.', seq_tuple.sequence_name, new_start;
                EXECUTE 'ALTER SEQUENCE ' || seq_class.seqname || ' RESTART WITH ' || new_start || ';';
            END IF;
    END LOOP;
    IF altered = false THEN
        RAISE NOTICE 'All found sequnces have proper params.';
    END IF;
    RETURN true;
END
$$
LANGUAGE plpgsql;

-- select mtm.alter_sequences();
