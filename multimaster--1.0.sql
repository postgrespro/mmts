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
CREATE FUNCTION mtm.dmq_receiver_loop(sender_name text, recv_timeout int) RETURNS void
AS 'MODULE_PATHNAME','dmq_receiver_loop'
LANGUAGE C;

---
--- Plumbering of node management: internal tables and triggers.
--- Not indended to be used directly by users but rather through add/drop/init
--- functions.
---

CREATE TABLE mtm.cluster_nodes(
    id int primary key not null,
    conninfo text not null,
    is_self bool not null
);

/*
 * Nodes for which init (slot creation, etc) is done. This could be a bool
 * column in cluster_nodes, however this state is local per node, and
 * cluster_nodes is global table.
 */
CREATE TABLE mtm.nodes_init_done (
    id int primary key not null,
    init_done bool
);

CREATE FUNCTION mtm.after_node_create()
RETURNS TRIGGER
AS 'MODULE_PATHNAME','mtm_after_node_create'
LANGUAGE C;

CREATE TRIGGER on_node_create
    AFTER INSERT ON mtm.cluster_nodes
    FOR EACH ROW
    EXECUTE FUNCTION mtm.after_node_create();
ALTER TABLE mtm.cluster_nodes ENABLE ALWAYS TRIGGER on_node_create;

CREATE FUNCTION mtm.after_node_drop()
RETURNS TRIGGER
AS 'MODULE_PATHNAME','mtm_after_node_drop'
LANGUAGE C;

CREATE TRIGGER on_node_drop
    AFTER DELETE ON mtm.cluster_nodes
    FOR EACH ROW
    EXECUTE FUNCTION mtm.after_node_drop();
ALTER TABLE mtm.cluster_nodes ENABLE ALWAYS TRIGGER on_node_drop;

/*
 * Remove syncpoints of node on its drop.
 * XXX: Syncpoints where dropped node is origin will be automatically removed
 * in cleanup_old_syncpoints: we must adhere to the rule 'row is deleted by
 * node who inserted id' in bdr-like tables to avoid conflicts. However,
 * rows inserted *by* dropped node are problematic: obviously dropped node
 * won't remove them, and deleting it here might cause transaction to abort
 * as not all such rows necessarily had arrived to all nodes before dropped
 * node went down. We probably should add 'don't stream the following changes'
 * logical message and thus make this cleanup entirely local per-node.
 */
CREATE FUNCTION mtm.after_node_drop_plpgsql() RETURNS TRIGGER AS $$
BEGIN
  -- DELETE FROM mtm.syncpoints WHERE receiver_node_id = OLD.id;
  RETURN NULL; -- result is ignored since this is an AFTER trigger
END
$$ LANGUAGE plpgsql;

CREATE TRIGGER on_node_drop_plpgsql
    AFTER DELETE ON mtm.cluster_nodes
    FOR EACH ROW
    EXECUTE FUNCTION mtm.after_node_drop_plpgsql();


CREATE FUNCTION mtm.node_info(id int)
RETURNS mtm.node_info
AS 'MODULE_PATHNAME','mtm_node_info'
LANGUAGE C;

CREATE TYPE mtm.node_info AS (
    "enabled" bool,
    "connected" bool,
    "sender_pid" int,
    "receiver_pid" int,
    "n_workers" int,
    "receiver_mode" text
);

CREATE TYPE mtm.node AS (
    "id" int,
    "conninfo" text,
    "is_self" bool,
    "enabled" bool,
    "connected" bool,
    "sender_pid" int,
    "receiver_pid" int,
    "n_workers" int,
    "receiver_mode" text
);

---
--- User facing API for node info and management.
---

CREATE OR REPLACE FUNCTION mtm.init_cluster(my_conninfo text, peers_conninfo text[])
RETURNS VOID
AS 'MODULE_PATHNAME','mtm_init_cluster'
LANGUAGE C;

CREATE OR REPLACE FUNCTION mtm.state_create(node_ids int[])
RETURNS VOID
AS 'MODULE_PATHNAME','mtm_state_create'
LANGUAGE C;

CREATE TYPE mtm.cluster_status AS (
    "my_node_id" int,
    "status" text,
    "connected" int[],
    "gen_num" int8, -- xxx pg doesn't have unsigned int8
    "gen_members" int[],
    "gen_members_online" int[],
    "gen_configured" int[]
);

CREATE FUNCTION mtm.status()
RETURNS mtm.cluster_status
AS 'MODULE_PATHNAME','mtm_status'
LANGUAGE C;

CREATE OR REPLACE FUNCTION mtm.nodes() RETURNS SETOF mtm.node AS
$$
    SELECT id, conninfo, is_self, (mtm.node_info(id)).*
    FROM mtm.cluster_nodes
    ORDER BY id;
$$
LANGUAGE sql;

CREATE OR REPLACE FUNCTION mtm.add_node(connstr text) RETURNS int AS
$$
DECLARE
    new_node_id int;
BEGIN
    INSERT INTO mtm.cluster_nodes SELECT
        min(unused_ids.id), connstr, 'false'
    FROM (
        SELECT id FROM generate_series(1,16) id
        EXCEPT
        SELECT id FROM mtm.cluster_nodes
    ) unused_ids
    RETURNING id INTO new_node_id;
    RETURN new_node_id;
END
$$
LANGUAGE plpgsql;

CREATE OR REPLACE FUNCTION mtm.drop_node(node_id int) RETURNS void AS
$$
DELETE FROM mtm.cluster_nodes WHERE id = $1;
$$
LANGUAGE sql;

CREATE FUNCTION mtm.join_node(node_id int, backup_end_lsn pg_lsn)
RETURNS VOID
AS 'MODULE_PATHNAME','mtm_join_node'
LANGUAGE C;

---
--- Various
---

CREATE FUNCTION mtm.make_table_local(relation regclass) RETURNS void
AS 'MODULE_PATHNAME','mtm_make_table_local'
LANGUAGE C;

-- CREATE FUNCTION mtm.dump_lock_graph() RETURNS text
-- AS 'MODULE_PATHNAME','mtm_dump_lock_graph'
-- LANGUAGE C;

-- CREATE FUNCTION mtm.check_deadlock(xid bigint) RETURNS boolean
-- AS 'MODULE_PATHNAME','mtm_check_deadlock'
-- LANGUAGE C;

CREATE FUNCTION mtm.set_temp_schema(nsp text) RETURNS void
AS 'MODULE_PATHNAME','mtm_set_temp_schema'
LANGUAGE C;

CREATE TABLE mtm.local_tables(
    rel_schema name,
    rel_name name,
    primary key(rel_schema, rel_name)
) WITH (user_catalog_table=true);

/* don't broadcast init_done state which is local per node */
INSERT INTO mtm.local_tables VALUES ('mtm', 'nodes_init_done');
/* same for mtm.config */
INSERT INTO mtm.local_tables VALUES ('mtm', 'config');

-- possible tuples:
--   'basebackup' : source node_id and end lsn of basebackup
--   XXX: move my_node_id here?
CREATE TABLE mtm.config(
    key text primary key not null,
    value jsonb
);


CREATE TABLE mtm.syncpoints(
    receiver_node_id int not null,
    origin_node_id int not null,
    origin_lsn pg_lsn not null,
    receiver_lsn pg_lsn not null
);
-- The column and sort order is important for queries optimization
-- (latest_syncpoints, translate_syncpoint, cleanup_old_syncpoints);
-- unfortunately, pg doesn't allow to specify sort ordering (DESC) in primary
-- key, so create separate idx and use it as replica identity.
-- Note that we first log sp and then register it, thus it is impossible to
-- register the same sp twice -- so it is safe not to include receiver_lsn in
-- unique idx (OTOH it is possible but unlikely to skip registering some sp at
-- all).
CREATE UNIQUE INDEX syncpoints_idx ON mtm.syncpoints
  (receiver_node_id, origin_node_id, origin_lsn DESC);
ALTER TABLE mtm.syncpoints REPLICA IDENTITY USING INDEX syncpoints_idx;

COMMENT ON TABLE mtm.syncpoints IS
'a row means node receiver_node_id has
 1) all changes of origin_node_id with end_lsn <= origin_lsn lying before
    receiver_lsn locally;
 2) all changes of origin_node_id with start lsn >= origin_lsn will be
    landed at >= receiver_lsn locally -- this is used by other nodes to report
    origin changes to receiver node';

CREATE FUNCTION update_recovery_horizons() returns void
  AS 'MODULE_PATHNAME','update_recovery_horizons'
  LANGUAGE C;

CREATE FUNCTION mtm.syncpoints_trigger_f() RETURNS trigger AS $$
BEGIN
  IF (TG_OP = 'DELETE') THEN
    -- RAISE LOG 'deleting syncpoint row: %', OLD;
    RETURN OLD;
  ELSE
    -- RAISE LOG 'inserting syncpoint row: %', NEW;
    -- others info about syncpoints apply allow to advance our horizons, so
    -- it makes sense to update them here, not only sp apply
    PERFORM mtm.update_recovery_horizons();
    RETURN NEW;
  END IF;
END
$$ LANGUAGE plpgsql;

CREATE TRIGGER syncpoints_trigger BEFORE INSERT OR UPDATE OR DELETE
    ON mtm.syncpoints FOR EACH ROW EXECUTE FUNCTION mtm.syncpoints_trigger_f();
ALTER TABLE mtm.syncpoints ENABLE ALWAYS TRIGGER syncpoints_trigger;

/*
 * If we ever switch to non-blocking syncpoints implementation we would have
 * two different types of
 *   <origin_node, origin_lsn, receiver_node, receiver_lsn>
 * syncpoint tuples (and thus two tables) instead of current
 * (meaning hard border) one:
 * 1) absorbpoint: means receiver got all origin transactions with end_lsn <=
 *    origin_lsn; so he must request streaming these changes since origin_lsn
 *    (translated into appropriate LSNs, if pulling from non-origin) and fill the
 *    filter since receiver_lsn of emitpoint with origin_lsn <= absorbpoint's
 *    origin_lsn to spot all already applied transactions. In fact,
 *    receiver_lsn (local curr WAL pt on the moment of absorbpoint creation),
 *    is useless for absorbpoints, though could be recorded. Absorbpoint is
 *    just a mark that node have applied everything up to origin_lsn.
 * 2) emitpoint: means all origin_node transactions with start_lsn >= origin_lsn
 *    at receiver have or will have start_lsn >= receiver_lsn. So recovering
 *    node should translate origin_lsn it needs to the appropriate emitpoint of
 *    donor and request streaming since it (and ack it in its reports).
 * In the current blocking implementation single record represents both
 * absorbpoint and emitpoint. To make future development easier, hints are put
 * in places where the distinction would matter.
 */

CREATE FUNCTION mtm.my_node_id() RETURNS int AS $$
    SELECT id FROM mtm.cluster_nodes WHERE is_self;
$$ LANGUAGE sql STABLE;

-- .emitter_lsn is since which LSN we must request streaming from
-- emitter_node_id to get origin_node_id changes with start_lsn >= origin_lsn.
-- This is the same LSN since which recovering emitter_node_id must scan its WAL
-- to fill the filter.
-- .origin_lsn is the origin_lsn of the emitpoint; it might be slightly less
-- than passed origin_lsn (reading since emitter_lsn must provide *at least*
-- all changes with start_lsn >= passed origin_lsn, but might give more)
-- Returns NULL if no suitable syncpoint is found.
-- would select emitpoints in non-blocking implementation
CREATE FUNCTION mtm.translate_syncpoint(origin_node_id int, origin_lsn pg_lsn,
                                        emitter_node_id int,
					out origin_lsn pg_lsn, out emitter_lsn pg_lsn)
RETURNS record AS $$
    SELECT s.origin_lsn, s.receiver_lsn FROM mtm.syncpoints s
    WHERE s.origin_node_id = translate_syncpoint.origin_node_id AND
          s.receiver_node_id = emitter_node_id AND
          s.origin_lsn <= translate_syncpoint.origin_lsn
    ORDER BY origin_lsn DESC LIMIT 1
$$ LANGUAGE sql STABLE;

-- Latest syncpoint to and from each node: up to which origin_lsn
-- receiver_node_id applied *all* transactions of origin_node_id?
-- If some syncpoint doesn't exist (possible immediately after startup),
-- origin_lsn is 0/0 for this pair of nodes.
-- would be latest_absorbpoints in non-blocking implementation
CREATE or replace VIEW mtm.latest_syncpoints AS
    SELECT node_id_pairs.receiver_node_id,
           node_id_pairs.origin_node_id,
           COALESCE(ls.origin_lsn, '0/0'::pg_lsn) origin_lsn,
	   -- Since which local lsn receiver_node_id should fill the filter?
	   -- The answer is since the corresponding emitpoint, though in
	   -- current blocking implementation this just trivially returns
	   -- receiver_lsn of the row.
	   COALESCE((mtm.translate_syncpoint(node_id_pairs.origin_node_id,
	                                    ls.origin_lsn,
					    node_id_pairs.receiver_node_id)).emitter_lsn,
		    -- If there is no syncpoint yet and receiver is me, fetch
		    -- curr position of filter slot directly. This is important
		    -- in case of node reboot before first syncpoint: we must
		    -- fill the filter with already applied xacts. We attempt
		    -- generic query first as it allows to get fill_filter_since
		    -- for other nodes which is probably useful for monitoring.
                    CASE WHEN node_id_pairs.receiver_node_id = mtm.my_node_id() THEN
		      (SELECT restart_lsn FROM pg_replication_slots
		       WHERE slot_name = format('mtm_filter_slot_%s',
		         node_id_pairs.origin_node_id))
		    END
		   ) fill_filter_since
    FROM
      -- Generate pairs of all nodes and left join them with the actual sp table
      -- to set origin_lsn 0/0 if syncpoint doesn't exist yet (and ignore
      -- dropped nodes at the same time)
      (SELECT n1.id origin_node_id, n2.id receiver_node_id
       FROM mtm.cluster_nodes n1, mtm.cluster_nodes n2
       WHERE n1.id != n2.id) node_id_pairs LEFT OUTER JOIN
      (SELECT DISTINCT ON (receiver_node_id, origin_node_id) *
       FROM mtm.syncpoints
       ORDER BY receiver_node_id, origin_node_id, origin_lsn DESC) ls
      ON (node_id_pairs.origin_node_id = ls.origin_node_id AND
          node_id_pairs.receiver_node_id = ls.receiver_node_id)
    ORDER BY receiver_node_id, origin_node_id;

-- Which LSN our node can safely confirm as flushed to sender_node_id?
-- Returns NULL if there is no sender_node_id node; returns 0/0 if there is no
-- suitable syncpoint (yet) for at least one node (possible immediately after init)
CREATE FUNCTION mtm.get_recovery_horizon(sender_node_id int) RETURNS pg_lsn AS $$
  SELECT min(CASE
                 WHEN origin_node_id = sender_node_id THEN origin_lsn
                 ELSE COALESCE((mtm.translate_syncpoint(origin_node_id,
                                                       origin_lsn,
                                                       sender_node_id)).emitter_lsn,
                               '0/0'::pg_lsn)
                 END)
  FROM mtm.latest_syncpoints
  WHERE receiver_node_id = mtm.my_node_id();
$$ LANGUAGE sql STABLE;

CREATE FUNCTION mtm.get_recovery_horizons(OUT node_id int, OUT horizon pg_lsn)
RETURNS SETOF record AS $$
  SELECT n.id, mtm.get_recovery_horizon(n.id)
  FROM mtm.cluster_nodes n
  WHERE n.id != mtm.my_node_id();
$$ LANGUAGE sql STABLE;


/*
 * old syncpoints cleanup support
 */

-- Since which origin_lsn changes of origin_node_id must be still retained,
-- i.e. which LSN is still needed by the node who lags applying them the most?
-- Returns 0/0 if some node syncpoint doesn't exist yet at all.
-- would select oldest_absorbpoints in non-blocking implementation
CREATE FUNCTION mtm.oldest_syncpoint(origin_node_id int) RETURNS pg_lsn AS $$
    SELECT min(origin_lsn)
    FROM mtm.latest_syncpoints ls
    WHERE ls.origin_node_id = oldest_syncpoint.origin_node_id;
$$ LANGUAGE sql STABLE;

-- Remove obsolete mtm.syncpoint entries. To avoid reordering conflicts in
-- this bdr-like table we follow the simple 'only the guy who inserted the row
-- deletes it' rule, i.e. purge only own records.
CREATE FUNCTION mtm.cleanup_old_syncpoints() RETURNS void AS $$
DECLARE
    o_node_id int;
    kept_ep pg_lsn;
    kept_ap pg_lsn;
BEGIN
    -- compute once conditions by which <origin_node_id, receiver_node_id>
    -- records are pruned for efficiency
    FOR o_node_id IN SELECT id FROM mtm.cluster_nodes WHERE id != mtm.my_node_id() LOOP
        -- Find oldest origin_lsn still needed by me or someone else to get
        -- origin_node_id changes. We must retain the emitpoint after which all
        -- xacts >= oldest origin_lsn go: if oldest absorbpoint owner is me,
        -- I must fill recovery filter since this point; if oldest absorbpoint
        -- owner is someone else, we must stream to him since this point.
        kept_ep := (mtm.translate_syncpoint(o_node_id,
                                           mtm.oldest_syncpoint(o_node_id),
                                           mtm.my_node_id())).origin_lsn;
        -- don't remove (my own) latest absorbpoint -- it determines the LSN we
        -- report/request streaming from.
        -- (currently that's redundant as kept_ep clause always keeps at
        -- least one syncpoint for each pair of nodes, but that's the condition
        -- by which absorbpoints would be pruned in non-blocking implementation)
        kept_ap := (SELECT ls.origin_lsn FROM mtm.latest_syncpoints ls
                    WHERE ls.origin_node_id = o_node_id AND
                          ls.receiver_node_id = mtm.my_node_id());

        DELETE FROM mtm.syncpoints s WHERE
          (s.receiver_node_id, s.origin_node_id, s.origin_lsn) IN
          (SELECT s.receiver_node_id, s.origin_node_id, s.origin_lsn
           FROM mtm.syncpoints s
           WHERE
             s.receiver_node_id = mtm.my_node_id() AND -- purge only own records
             s.origin_node_id = o_node_id AND
             s.origin_lsn < kept_ep AND
             s.origin_lsn < kept_ap
             -- Several receivers might attempt to concurrently cleanup the same
             -- rows; we would like to avoid fighting between them, especially
             -- since by default SPI seems to use RR, so receiver ERRORs out on
             -- conflict.
           FOR UPDATE SKIP LOCKED
          );
     END LOOP;
     -- Also cleanup syncpoints created by me about changes of dropped nodes.
     -- (we also have some issues with syncpoints created *by* dropped nodes, see
     -- after_node_drop_plpgsql)
     DELETE FROM mtm.syncpoints s WHERE
       receiver_node_id = mtm.my_node_id() AND
       origin_node_id NOT IN (SELECT id FROM mtm.cluster_nodes);
END
$$ LANGUAGE plpgsql;

CREATE OR REPLACE FUNCTION mtm.alter_sequences() RETURNS boolean AS
$$
DECLARE
    seq_class record;
    seq_tuple record;
    seq_rel record;
    node_id int;
    max_nodes int;
    new_start bigint;
    altered boolean := false;
    saved_remotes text;
BEGIN
    -- with sparse node_id's max(node_id) can be bigger then n_nodes
    select max(id) into max_nodes from mtm.nodes();
    select current_setting('multimaster.remote_functions') into saved_remotes;
    set multimaster.remote_functions to 'mtm.alter_sequences';
    select my_node_id into node_id from mtm.status();
    FOR seq_class IN
        SELECT
            '"' || ns.nspname || '"."' || seq.relname || '"' as seqname,
            seq.oid as oid,
            seq.relname as name
        FROM pg_namespace ns, pg_class seq
        WHERE seq.relkind = 'S' and seq.relnamespace = ns.oid and seq.relpersistence != 't'
    LOOP
            EXECUTE 'select * from ' || seq_class.seqname INTO seq_rel;
            EXECUTE 'select * from pg_sequence where seqrelid = ' || seq_class.oid INTO seq_tuple;
            IF seq_tuple.seqincrement != max_nodes THEN
                altered := true;
                RAISE NOTICE 'Altering step for sequence % to %.', seq_class.name, max_nodes;
                EXECUTE 'ALTER SEQUENCE ' || seq_class.seqname || ' INCREMENT BY ' || max_nodes || ';';
            END IF;
            IF (seq_rel.last_value % max_nodes) != (node_id % max_nodes) THEN
                altered := true;
                new_start := (seq_rel.last_value / max_nodes + 1)*max_nodes + node_id;
                RAISE NOTICE 'Altering start for sequence % to %.', seq_class.name, new_start;
                EXECUTE 'ALTER SEQUENCE ' || seq_class.seqname || ' RESTART WITH ' || new_start || ';';
            END IF;
    END LOOP;
    EXECUTE 'set multimaster.remote_functions to ''' || saved_remotes || '''';
    IF altered = false THEN
        RAISE NOTICE 'All found sequences have proper params.';
    END IF;
    RETURN true;
END
$$
LANGUAGE plpgsql;

CREATE TYPE bgwpool_result AS (nWorkers INT, Active INT, Pending INT, Size INT,
								Head INT, Tail INT, ReceiverName TEXT);
CREATE FUNCTION mtm.node_bgwpool_stat() RETURNS bgwpool_result
AS 'MODULE_PATHNAME','mtm_get_bgwpool_stat'
LANGUAGE C;

CREATE VIEW mtm.stat_bgwpool AS
	SELECT	nWorkers,
			Active,
			Pending,
			Size,
			Head,
			Tail,
			ReceiverName
	FROM mtm.node_bgwpool_stat();

-- select mtm.alter_sequences();

CREATE FUNCTION mtm.get_logged_prepared_xact_state(gid text) RETURNS text
  AS 'MODULE_PATHNAME','mtm_get_logged_prepared_xact_state'
  LANGUAGE C;

CREATE FUNCTION mtm.ping() RETURNS bool AS 'MODULE_PATHNAME','mtm_ping'
LANGUAGE C;

CREATE FUNCTION mtm.check_query(query TEXT) RETURNS BOOL
AS 'MODULE_PATHNAME','mtm_check_query'
LANGUAGE C;

CREATE FUNCTION mtm.hold_backends() RETURNS VOID
AS 'MODULE_PATHNAME','mtm_hold_backends'
LANGUAGE C;

CREATE FUNCTION mtm.release_backends() RETURNS VOID
AS 'MODULE_PATHNAME','mtm_release_backends'
LANGUAGE C;
