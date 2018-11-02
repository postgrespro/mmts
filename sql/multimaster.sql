
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

\c "dbname=regression port=64717 host=127.0.0.1"
table t;


-- test CTA replication inside explain

EXPLAIN ANALYZE create table explain_cta as select 42;



--- xx?

create user user1;
create schema user1;
alter schema user1 owner to user1;

\c "user=user1 dbname=regression"
create table user1.test(i int primary key);
create table user1.test2(i int primary key);

\c "user=user1 dbname=regression port=5433"
select * from test;

