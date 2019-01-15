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


CREATE FUNCTION mtm.make_table_local(relation regclass) RETURNS void
AS 'MODULE_PATHNAME','mtm_make_table_local'
LANGUAGE C;

CREATE FUNCTION mtm.init_node(node_id integer, connstrs text[]) RETURNS void
AS 'MODULE_PATHNAME','mtm_init_node'
LANGUAGE C;

CREATE FUNCTION mtm.dump_lock_graph() RETURNS text
AS 'MODULE_PATHNAME','mtm_dump_lock_graph'
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

CREATE TABLE mtm.nodes(
    id int primary key not null,
    conninfo text not null,
    is_self bool not null
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
