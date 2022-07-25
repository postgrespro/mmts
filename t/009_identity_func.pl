use strict;
use warnings;
use Cluster;
use Test::More tests => 29;

my $cluster = new Cluster(3);
$cluster->init();
$cluster->start();
$cluster->create_mm(undef);

my $dbname = $cluster->{nodes}->[0]->{dbname};
my $nodes = $cluster->{nodes};
my $output;
my $err_out;

# ##############################################################################
#
# Incorrect query
#
# ##############################################################################
my $invalid_expr_pattern =
			".*failed to run query on node[0-9]+, snapshot .*: "
			. "ERROR:  relation \"t1\" does not exist\n";

# test node 1
$nodes->[0]->psql($dbname,
					"SELECT mtm.check_query('SELECT * FROM t1')",
					stdout => \$output, stderr => \$err_out);
is ( (($output eq '') and ($err_out ne '')), 1, "node1: check zero out on error");
like($err_out, qr{$invalid_expr_pattern}, "node1: check error output correctness");

# test node 2
$nodes->[1]->psql($dbname,
					"SELECT mtm.check_query('SELECT * FROM t1')",
					stdout => \$output, stderr => \$err_out);
is ( (($output eq '') and ($err_out ne '')), 1, "node2: check zero out on error");
like($err_out, qr{$invalid_expr_pattern}, "node2: check error output correctness");

# test node 3
$nodes->[2]->psql($dbname,
					"SELECT mtm.check_query('SELECT * FROM t1')",
					stdout => \$output, stderr => \$err_out);
is ( (($output eq '') and ($err_out ne '')), 1, "node3: check zero out on error");
like($err_out, qr{$invalid_expr_pattern}, "node3: check error output correctness");

# Substep: check no problems without one node
$nodes->[2]->stop();
$cluster->await_nodes_after_stop( [0,1] );
$nodes->[0]->psql($dbname,
					"SELECT mtm.check_query('SELECT * FROM t1')",
					stdout => \$output, stderr => \$err_out);
is ( (($output eq '') and ($err_out ne '')), 1, "node1: check zero out on error");
like($err_out, qr{$invalid_expr_pattern}, "node1: check error output correctness");

$nodes->[1]->psql($dbname,
					"SELECT mtm.check_query('SELECT * FROM t1')",
					stdout => \$output, stderr => \$err_out);
is ( (($output eq '') and ($err_out ne '')), 1, "node2: check zero out on error");
like($err_out, qr{$invalid_expr_pattern}, "node2: check error output correctness");

# Substep: node1 will be isolated
my $isolation_pattern = ".*node is not online\: current status .*";
$nodes->[1]->stop();
$nodes->[0]->psql($dbname,
					"SELECT mtm.check_query('SELECT * FROM t1')",
					stdout => \$output, stderr => \$err_out);
is ( (($output eq '') and ($err_out ne '')), 1, "node1: check zero out on error");
like($err_out, qr{$isolation_pattern}, "Check access to isolated node");

$nodes->[1]->start();
$nodes->[2]->start();
$cluster->await_nodes( [2,0,1] );

# ##############################################################################
#
# Interface functions protection.
#
# ##############################################################################
my $protection_pattern = "this function should only be called by mtm.check_query()";
$nodes->[0]->psql($dbname,
					"SELECT mtm.hold_backends();",
					stdout => \$output, stderr => \$err_out);
is ( (($output eq '') and ($err_out ne '')), 1, "hold_all() protection");
like($err_out, qr{$protection_pattern}, "Check error output");

$nodes->[0]->psql($dbname,
					"SELECT mtm.release_backends();",
					stdout => \$output, stderr => \$err_out);
is ( (($output eq '') and ($err_out ne '')), 1, "release_all() protection");
like($err_out, qr{$protection_pattern}, "Check error output");

$cluster->safe_psql(0, "CREATE TABLE t1 (a int PRIMARY KEY, b text);");
$nodes->[0]->psql($dbname,
					"SELECT mtm.check_query('SELECT * FROM t1')",
					stdout => \$output);
is( (($output eq 't')) , 1, "Check tables equivalence with no tuples");

# Check consistency in the case of two nodes
$nodes->[1]->stop();
$cluster->await_nodes_after_stop( [0,2] );
$nodes->[0]->psql($dbname,
					"SELECT mtm.check_query('SELECT * FROM t1')",
					stdout => \$output);
is( (($output eq 't')) , 1, "Check tables equivalence with one off node");

$cluster->safe_psql(0, "INSERT INTO t1 (a, b) VALUES (1, NULL);");
$nodes->[0]->psql($dbname,
					"SELECT mtm.check_query('SELECT * FROM t1')",
					stdout => \$output);

is( (($output eq 't')) , 1, "Check primitive table");
$nodes->[1]->start();
$cluster->await_nodes( [2,0,1] );

$cluster->safe_psql(0,
	"INSERT INTO t1 (a,b) (SELECT *, 'test' FROM generate_series(2,100) AS x1);
	");
$nodes->[0]->psql($dbname,
					"SELECT mtm.check_query('SELECT * FROM t1 ORDER BY a')",
					stdout => \$output);
is( (($output eq 't')) , 1, "Check big table");
$nodes->[0]->psql($dbname,
				"SELECT mtm.check_query('SELECT md5(string_agg(x1::text,''''))
				FROM (SELECT * FROM t1 ORDER BY a) AS x1');",
				stdout => \$output);
is( (($output eq 't')) , 1, "Another approach to check big table");

$nodes->[0]->psql($dbname,
				"SELECT mtm.check_query('SELECT mtm.status();');",
				stdout => \$output);
note("Check result: $output");
is( (($output eq 'f')) , 1, "Unsuccessful check");

$nodes->[2]->stop();
$cluster->await_nodes_after_stop( [0,1] );
$nodes->[0]->psql($dbname,
				"SELECT mtm.check_query('SELECT md5(string_agg(x1::text,''''))
				FROM (SELECT * FROM t1 ORDER BY a) AS x1');",
				stdout => \$output);
is( (($output eq 't')) , 1, "Check tables identity after one node was down");

$nodes->[2]->start();
$cluster->await_nodes( [2,0,1] );
$nodes->[0]->psql($dbname,
			"SELECT mtm.check_query('SELECT my_node_id FROM mtm.status();');",
			stdout => \$output);
is( (($output eq 'f')) , 1, "Check warning message on mismatch");

$nodes->[2]->psql($dbname,
			"SELECT mtm.check_query('SELECT a,b FROM t1, mtm.status() AS ms WHERE a > ms.my_node_id');",
			stdout => \$output, stderr => \$err_out);
note("Check result: $output");
is( (($output eq 'f')) , 1, "Check warning message on difference in rows number");
like($err_out,
	qr{.*query results mismatch\: 99 rows and 2 columns on node1\, 98 rows and 2 columns on node2},
	"Check format of the error message");

$nodes->[2]->psql($dbname,
			"SELECT mtm.check_query('SELECT b FROM t1 WHERE a = 1');",
			stdout => \$output);
note("Check result: $output");
is( (($output eq 't')) , 1, "Check equivalence of nulls");

$nodes->[0]->psql($dbname,
			"SELECT mtm.check_query('SELECT b FROM t1, mtm.status() AS ms WHERE a = ms.my_node_id');",
			stdout => \$output, stderr => \$err_out);
note("Check result: $output");
is( (($output eq 'f')) , 1, "Check warning message on difference in null and not null values");
like($err_out,
	qr{.*mismatch in column \'b\' of row 0\: null on node1\, test on node2},
	"Check format of the error message");

exit(0);

# Full pgbench test
$cluster->pgbench(0, ('-i', -s => '10') );
my $pgb0 = $cluster->pgbench_async(0, ('-N', -T => '30', -c => '5') );
my $pgb1 = $cluster->pgbench_async(1, ('-N', -T => '30', -c => '5') );
my $pgb2 = $cluster->pgbench_async(2, ('-N', -T => '30', -c => '5') );

$output='t';
for (my $i = 0; ($i < 3) and ($output eq 't'); $i++)
{
	$nodes->[0]->psql($dbname,
				"SELECT mtm.check_query('SELECT md5(string_agg(x1::text,''''))
				FROM (SELECT * FROM pgbench_accounts ORDER BY aid) AS x1');",
				stdout => \$output);
	note("check iteration $i, result: $output");
	is( (($output eq 't')) , 1, "Data on nodes are identic");
	sleep(6);
}

$cluster->pgbench_await($pgb0);
$cluster->pgbench_await($pgb1);
$cluster->pgbench_await($pgb2);

$cluster->stop();
