create table atx_test1(a text);


-- check that commit of autonomous tx will not steal locks from parent tx
begin;
    insert into atx_test1 values (1);
    select count(*) from pg_locks where transactionid=txid_current();
    begin autonomous;
        insert into atx_test1 values (1);
        select count(*) from pg_locks where transactionid=txid_current();
    commit;
    -- here we still should see our lock
    select count(*) from pg_locks where transactionid=txid_current();
commit;

drop table atx_test1;
