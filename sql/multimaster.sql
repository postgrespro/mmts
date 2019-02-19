-- set connection strings to nodes
select conninfo as node1 from mtm.nodes() where id = 1 \gset
select conninfo as node2 from mtm.nodes() where id = 2 \gset
select conninfo as node3 from mtm.nodes() where id = 3 \gset


-- check that implicit empty transactions works fine
create table t (a int, b text);
create or replace function f1() returns trigger as $$begin raise notice 'b: %', new.b; return NULL; end$$ language plpgsql;
create trigger tr1 before insert on t for each row execute procedure f1();
insert into t values (1, 'asdf');
copy t from stdout;
1	baz
\.


-- test mixed temp table and persistent write
create table t_tempddl_mix(id int primary key);
insert into t_tempddl_mix values(1);
begin;
insert into t_tempddl_mix values(42);
create temp table tempddl(id int);
commit;

table t;

\c :node2
table t;

-- test CTA replication inside explain
EXPLAIN ANALYZE create table explain_cta as select 42;

--- test schemas
create user user1;
create schema user1;
alter schema user1 owner to user1;

\c :node1
create table user1.test(i int primary key);
create table user1.test2(i int primary key);

\c :node3
table test;

\c :node1


--- scheduler example with secdefs and triggers
CREATE TABLE aaa (
    id   int primary key,
    text text
);
CREATE TABLE aaa_copy (LIKE aaa);
ALTER  TABLE aaa_copy ADD submit_time timestamp NOT NULL DEFAULT now();
ALTER  TABLE aaa_copy ADD submitter text NOT NULL DEFAULT session_user;
ALTER  TABLE aaa_copy ADD version_id SERIAL NOT NULL;
ALTER  TABLE aaa_copy ADD PRIMARY KEY (id, version_id);

CREATE FUNCTION add_aaa(
  aid   integer
) RETURNS integer AS
$BODY$
DECLARE
  nid integer;
BEGIN
  INSERT INTO aaa (id, text) VALUES (aid, 'zzz') RETURNING id INTO nid;
  RETURN nid;
END
$BODY$
LANGUAGE plpgsql SECURITY DEFINER;

CREATE FUNCTION drop_aaa(
  aid   integer
) RETURNS integer AS
$BODY$
BEGIN
  DELETE FROM aaa WHERE id = aid;
  RETURN aid;
END
$BODY$
LANGUAGE plpgsql SECURITY DEFINER;

CREATE OR REPLACE FUNCTION on_aaa_update() RETURNS TRIGGER
AS $BODY$
DECLARE
  aaa_id integer;
BEGIN
  aaa_id := NEW.id;
  INSERT INTO aaa_copy VALUES (NEW.*);
  IF TG_OP = 'UPDATE' THEN
    INSERT INTO aaa_copy VALUES (NEW.*);
  END IF;
  RETURN OLD;
END
$BODY$ LANGUAGE plpgsql;

CREATE OR REPLACE FUNCTION on_aaa_delete() RETURNS TRIGGER
AS $BODY$
DECLARE
  aaa_id INTEGER;
BEGIN
  aaa_id := OLD.id;
  DELETE FROM aaa_copy WHERE id = aaa_id;
  RETURN OLD;
END
$BODY$ LANGUAGE plpgsql;

CREATE TRIGGER aaa_update_trigger
AFTER UPDATE OR INSERT ON aaa
  FOR EACH ROW EXECUTE PROCEDURE on_aaa_update();

CREATE TRIGGER aaa_delete_trigger
BEFORE DELETE ON aaa
  FOR EACH ROW EXECUTE PROCEDURE on_aaa_delete();

select add_aaa(58);
select add_aaa(5833);
select add_aaa(582);

delete from aaa;

table aaa;
table aaa_copy;

\c :node3

table aaa;
table aaa_copy;
