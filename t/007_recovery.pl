use strict;
use warnings;

use Cluster;
use TestLib;
use Test::More tests => 6;

my $cluster = new Cluster(3);
$cluster->init();
$cluster->start();
$cluster->create_mm();

########################################################
# Check data integrity before and after recovery of single node.
# Easy variant: sequential pgbenches, recovery without concurrent load.
########################################################

my $hash0; my $hash1; my $hash2; my $oldhash;
my $hash_query = q{
select
    md5('(' || string_agg(aid::text || ', ' || abalance::text , '),(') || ')')
from
    (select * from pgbench_accounts order by aid) t;
};

$cluster->pgbench(1, ('-i', -s => '10') );
$cluster->pgbench(0, ('-n','-N', -T => '4') );
$cluster->pgbench(1, ('-n','-N', -T => '4') );
$cluster->pgbench(2, ('-n','-N', -T => '4') );

$cluster->{nodes}->[2]->stop('fast');
sleep($cluster->{recv_timeout});
$cluster->await_nodes( (0,1) );

$cluster->pgbench(0, ('-n','-N', -T => '4') );
$cluster->pgbench(1, ('-n','-N', -T => '4') );

$cluster->await_nodes( (0,1) ); # just in case we've faced random timeout before
$hash0 = $cluster->safe_psql(0, $hash_query);
$hash1 = $cluster->safe_psql(1, $hash_query);
is($hash0, $hash1, "Check that hash is the same before recovery");

$cluster->{nodes}->[2]->start;
$cluster->await_nodes( (2,0,1) );

$oldhash = $hash0;
$hash0 = $cluster->safe_psql(0, $hash_query);
$hash1 = $cluster->safe_psql(1, $hash_query);
$hash2 = $cluster->safe_psql(2, $hash_query);

note("$oldhash, $hash0, $hash1, $hash2");
is( (($hash0 eq $hash1) and ($hash1 eq $hash2) and ($oldhash eq $hash0)) , 1,
    "Check that hash is the same after recovery");

########################################################
# Check start after all nodes were disconnected
########################################################

$cluster->safe_psql(0, "create table if not exists t(k int primary key, v int);");

$cluster->safe_psql(0, "insert into t values(1, 10);");
$cluster->safe_psql(1, "insert into t values(2, 20);");
$cluster->safe_psql(2, "insert into t values(3, 30);");

my $sum0; my $sum1; my $sum2;

$cluster->{nodes}->[1]->stop('fast');
$cluster->{nodes}->[2]->stop('fast');

$cluster->{nodes}->[1]->start;
$cluster->{nodes}->[2]->start;

$cluster->await_nodes( (1,2,0) );

$sum0 = $cluster->safe_psql(0, "select sum(v) from t;");
$sum1 = $cluster->safe_psql(1, "select sum(v) from t;");
$sum2 = $cluster->safe_psql(2, "select sum(v) from t;");
is( (($sum0 == 60) and ($sum1 == $sum0) and ($sum2 == $sum0)) , 1,
    "Check that nodes are working and sync");

########################################################
# Check recovery during some load
########################################################

$cluster->pgbench(0, ('-i', -s => '10') );
$cluster->pgbench(0, ('-N', -T => '1') );
$cluster->pgbench(1, ('-N', -T => '1') );
$cluster->pgbench(2, ('-N', -T => '1') );

# kill node while neighbour is under load
my $pgb_handle = $cluster->pgbench_async(1, ('-N', -T => '20', -c => '5') );
sleep(5);
$cluster->{nodes}->[2]->stop('fast');
$cluster->pgbench_await($pgb_handle);

# start node while neighbour is under load
$pgb_handle = $cluster->pgbench_async(0, ('-N', -T => '20', -c => '5') );
sleep(5);
$cluster->{nodes}->[2]->start;
$cluster->pgbench_await($pgb_handle);

# await recovery
$cluster->await_nodes( (2,0,1) );

# check data identity
$hash0 = $cluster->safe_psql(0, $hash_query);
$hash1 = $cluster->safe_psql(1, $hash_query);
$hash2 = $cluster->safe_psql(2, $hash_query);
note("$hash0, $hash1, $hash2");
is( (($hash0 eq $hash1) and ($hash1 eq $hash2)) , 1, "Check that hash is the same");

$sum0 = $cluster->safe_psql(0, "select sum(abalance) from pgbench_accounts;");
$sum1 = $cluster->safe_psql(1, "select sum(abalance) from pgbench_accounts;");
$sum2 = $cluster->safe_psql(2, "select sum(abalance) from pgbench_accounts;");

note("Sums: $sum0, $sum1, $sum2");
is($sum2, $sum0, "Check that sum_2 == sum_0");
is($sum2, $sum1, "Check that sum_2 == sum_1");

$sum0 = $cluster->safe_psql(0, "select count(*) from pg_prepared_xacts;");
$sum1 = $cluster->safe_psql(1, "select count(*) from pg_prepared_xacts;");
$sum2 = $cluster->safe_psql(2, "select count(*) from pg_prepared_xacts;");

note("Number of prepared tx: $sum0, $sum1, $sum2");

$cluster->stop;
