use strict;
use warnings;
use Cluster;
use TestLib;
use Test::More tests => 5;

my $cluster = new Cluster(3);
$cluster->init();
$cluster->configure();
$cluster->start();
$cluster->await_nodes( (0,1,2) );

my $ret;
my $psql_out;

###############################################################################
# Replication check
###############################################################################

$cluster->psql(0, 'postgres', "
	create extension multimaster;
	create table if not exists t(k int primary key, v int);
	insert into t values(1, 10);");
$cluster->psql(1, 'postgres', "select v from t where k=1;", stdout => \$psql_out);
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
$cluster->await_nodes( (0,1) );

note("inserting 2 on node 0");
$ret = $cluster->psql(0, 'postgres', "insert into t values(2, 20);"); # this transaciton may fail
note("tx1 status = $ret");

note("inserting 3 on node 1");
$ret = $cluster->psql(1, 'postgres', "insert into t values(3, 30);"); # this transaciton may fail
note("tx2 status = $ret");

note("inserting 4 on node 0 (can fail)");
$ret = $cluster->psql(0, 'postgres', "insert into t values(4, 40);"); 
note("tx1 status = $ret");

note("inserting 5 on node 1 (can fail)");
$ret = $cluster->psql(1, 'postgres', "insert into t values(5, 50);"); 
note("tx2 status = $ret");

note("selecting");
$cluster->psql(1, 'postgres', "select v from t where k=4;", stdout => \$psql_out);
note("selected");
is($psql_out, '40', "Check replication after node failure.");

###############################################################################
# Work after node start
###############################################################################

note("starting node 2");
$cluster->{nodes}->[2]->start;

$cluster->await_nodes( (2) );

note("inserting 6 on node 0 (can fail)");
$cluster->psql(0, 'postgres', "insert into t values(6, 60);"); 
note("inserting 7 on node 1 (can fail)");
$cluster->psql(1, 'postgres', "insert into t values(7, 70);");

note("getting cluster state");
$cluster->psql(0, 'postgres', "select * from mtm.get_cluster_state();", stdout => \$psql_out);
note("Node 1 status: $psql_out");
$cluster->psql(1, 'postgres', "select * from mtm.get_cluster_state();", stdout => \$psql_out);
note("Node 2 status: $psql_out");
$cluster->psql(2, 'postgres', "select * from mtm.get_cluster_state();", stdout => \$psql_out);
note("Node 3 status: $psql_out");

note("inserting 8 on node 0");
$cluster->psql(0, 'postgres', "insert into t values(8, 80);");
note("inserting 9 on node 1");
$cluster->psql(1, 'postgres', "insert into t values(9, 90);");

note("selecting from node 2");
$cluster->psql(2, 'postgres', "select v from t where k=8;", stdout => \$psql_out);
note("selected");

is($psql_out, '80', "Check replication after failed node recovery.");

$cluster->psql(2, 'postgres', "select v from t where k=9;", stdout => \$psql_out);
note("selected");

is($psql_out, '90', "Check replication after failed node recovery.");

$cluster->stop();
1;
