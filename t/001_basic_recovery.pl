use strict;
use warnings;
use Cluster;
use TestLib;
use Test::More tests => 4;

my $cluster = new Cluster(3);
$cluster->init();
$cluster->start();
$cluster->create_mm();

my $ret;
my $psql_out;

###############################################################################
# Replication check
###############################################################################

$cluster->{nodes}->[0]->safe_psql('postgres', q{
	create table if not exists t(k int primary key, v int);
	insert into t values(1, 10);
});
$psql_out = $cluster->{nodes}->[2]->safe_psql('postgres', q{
	select v from t where k=1;
});
is($psql_out, '10', "Check replication while all nodes are up.");

###############################################################################
# Isolation regress checks
###############################################################################

# we can call pg_regress here

###############################################################################
# Work after node stop
###############################################################################

note("stopping node 2");
$cluster->{nodes}->[2]->stop;

sleep($cluster->{recv_timeout});
$cluster->await_nodes( (0,1) );

$cluster->safe_psql(0, "insert into t values(2, 20);");
$cluster->safe_psql(1, "insert into t values(3, 30);");
$cluster->safe_psql(0, "insert into t values(4, 40);"); 
$cluster->safe_psql(1, "insert into t values(5, 50);"); 

$psql_out = $cluster->safe_psql(0, "select v from t where k=4;");
is($psql_out, '40', "Check replication after node failure.");

###############################################################################
# Work after node start
###############################################################################

note("starting node 2");
$cluster->{nodes}->[2]->start;

# intentionaly start from 2
$cluster->await_nodes( (2,0,1) );

$cluster->safe_psql(0, "insert into t values(6, 60);"); 
$cluster->safe_psql(1, "insert into t values(7, 70);");
$cluster->safe_psql(0, "insert into t values(8, 80);");
$cluster->safe_psql(1, "insert into t values(9, 90);");

$psql_out = $cluster->safe_psql(2, "select v from t where k=8;");
is($psql_out, '80', "Check replication after failed node recovery.");

$psql_out = $cluster->safe_psql(2, "select v from t where k=5;");
is($psql_out, '50', "Check replication after failed node recovery.");

$cluster->stop();

1;
