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

-- message queue receiver, for internal use only
CREATE FUNCTION mtm.dmq_receiver_loop(sender_name text) RETURNS void
AS 'MODULE_PATHNAME','dmq_receiver_loop'
LANGUAGE C;

---
--- Plumbering of node management: internal tables and triggers.
--- Not indended to be used directly by users but rather through add/drop/init
--- functions.
---

CREATE TABLE mtm.nodes(
    id int primary key not null,
    conninfo text not null,
    is_self bool not null,
    init_done bool not null default 'f'
);

CREATE FUNCTION mtm.after_node_create()
RETURNS TRIGGER
AS 'MODULE_PATHNAME','mtm_after_node_create'
LANGUAGE C;

CREATE TRIGGER on_node_create
    AFTER INSERT ON mtm.nodes
    FOR EACH ROW
    EXECUTE FUNCTION mtm.after_node_create();

CREATE FUNCTION mtm.after_node_drop()
RETURNS TRIGGER
AS 'MODULE_PATHNAME','mtm_after_node_drop'
LANGUAGE C;

CREATE TRIGGER on_node_drop
    AFTER DELETE ON mtm.nodes
    FOR EACH ROW
    EXECUTE FUNCTION mtm.after_node_drop();

CREATE FUNCTION mtm.join_node(node_id int)
RETURNS VOID
AS 'MODULE_PATHNAME','mtm_join_node'
LANGUAGE C;

---
--- User facing API for node management.
---

CREATE OR REPLACE FUNCTION mtm.init_node(node_id integer, connstrs text[]) RETURNS void AS
$$
BEGIN
    IF node_id <= 0 OR node_id > least(16, array_length(connstrs, 1)) THEN
        RAISE EXCEPTION 'node_id should be in range [1 .. length(connstrs)]';
    END IF;
    -- XXX
    EXECUTE 'SET mtm.emerging_node_id = ' || node_id || ';';
    INSERT INTO mtm.nodes SELECT
        ordinality::int as id,
        unnest as conninfo,
        ordinality = current_setting('mtm.emerging_node_id')::int as is_self
    FROM
        unnest(connstrs)
    WITH ORDINALITY;
END
$$
LANGUAGE plpgsql;

CREATE OR REPLACE FUNCTION mtm.add_node(connstr text) RETURNS void AS
$$
DECLARE
    new_node_id int;
BEGIN
    -- XXX: add only to a configured mm?

    INSERT INTO mtm.nodes SELECT
        min(unused_ids.id), connstr, 'false'
    FROM (
        SELECT id FROM generate_series(1,16) id
        EXCEPT
        SELECT id FROM mtm.nodes
    ) unused_ids
    RETURNING id INTO new_node_id;

    -- SELECT mtm.node_join(new_node_id, connstr);
END
$$
LANGUAGE plpgsql;

---
--- Various
---

CREATE FUNCTION mtm.make_table_local(relation regclass) RETURNS void
AS 'MODULE_PATHNAME','mtm_make_table_local'
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
