use strict;
use warnings;
use PostgresNode;
use Cluster;
use TestLib;
use Test::More tests => 4;

my $cluster = new Cluster(3);
$cluster->init();
$cluster->start();
$cluster->create_mm();

$cluster->safe_psql(0, q{
	CREATE EXTENSION pg_pathman;
	CREATE SCHEMA test_update_node;
	SET pg_pathman.enable_partitionrouter = ON;

	CREATE TABLE test_update_node.test_range(val NUMERIC NOT NULL, comment TEXT);
	CREATE INDEX val_idx ON test_update_node.test_range (val);
	INSERT INTO test_update_node.test_range SELECT i, i FROM generate_series(1, 100) i;
	SELECT create_range_partitions('test_update_node.test_range', 'val', 1, 10);

	ALTER TABLE test_update_node.test_range DROP COLUMN comment CASCADE;

	UPDATE test_update_node.test_range SET val = 115 WHERE val = 55;
	});

my $hash0; my $hash1; my $hash2;
my $hash_query = q{
select
    md5('(' || string_agg(val::text, '),(') || ')')
from
    (select * from test_update_node.test_range order by val) t;
};
$hash0 = $cluster->safe_psql(0, $hash_query);
$hash1 = $cluster->safe_psql(1, $hash_query);
$hash2 = $cluster->safe_psql(2, $hash_query);
note("$hash0, $hash1, $hash2");
is( (($hash0 eq $hash1) and ($hash1 eq $hash2)) , 1,
    "Check that hash is the same after query");

$cluster->safe_psql(0, q{
	CREATE TABLE unique_tbl (i int UNIQUE DEFERRABLE, t text);
	INSERT INTO unique_tbl VALUES (0, 'one');
	INSERT INTO unique_tbl VALUES (1, 'two');
	INSERT INTO unique_tbl VALUES (2, 'tree');
	INSERT INTO unique_tbl VALUES (3, 'four');
	INSERT INTO unique_tbl VALUES (4, 'five');
	});
$cluster->{nodes}->[1]->psql($cluster->{nodes}->[1]->{dbname}, q{
	-- default is immediate so this should fail right away
	UPDATE unique_tbl SET i = 1 WHERE i = 0;
	});
$cluster->safe_psql(0, q{
	UPDATE unique_tbl SET i = i+1;
	});

$hash_query = q{
select
    md5('(' || string_agg(i::text || ', ' || t::text , '),(') || ')')
from
    (select * from unique_tbl order by i) t;
};
$hash0 = $cluster->safe_psql(0, $hash_query);
$hash1 = $cluster->safe_psql(1, $hash_query);
$hash2 = $cluster->safe_psql(2, $hash_query);
note("$hash0, $hash1, $hash2");
is( (($hash0 eq $hash1) and ($hash1 eq $hash2)) , 1,
    "Check that hash is the same after query");

# ##############################################################################
#
# Check the PGPRO-3146 bug. Hard crash of backend causes restart of all postgres
# processes. Multimaster node must be survived after the crash and included into
# the multimaster after recovery.
#
# ##############################################################################

# Set GUC restart_after_crash in 'on' value
$cluster->stop();
foreach (0..$#{$cluster->{nodes}})
{
	$cluster->{nodes}->[$_]->append_conf('postgresql.conf', q{restart_after_crash = on});
}
$cluster->start();
$cluster->await_nodes( (0,1,2) );

# Simulate payload
$cluster->pgbench(0, ('-i', '-n', -s => '1') );
my $pgb1 = $cluster->pgbench_async(0, ('-n', -T => '15', -j=>'5', -c => '5') );
sleep(5);

my $pid0 = $cluster->safe_psql(0, "SELECT pid FROM pg_stat_activity
	WHERE	backend_type LIKE 'client backend'
			AND query LIKE 'UPDATE%' LIMIT 1;");

# Simulate hard crash
note("Simulate hard crash of a backend by SIGKILL to $pid0");
kill -9, $pid0;

$cluster->await_nodes( (0,1,2) );
is($cluster->is_data_identic( (0,1,2) ), 1, "check consistency after crash");


# ##############################################################################
#
# [PGPRO-3047] Test ALTER DOMAIN .. CONSTRAINT .. NOT VALID
#
# ##############################################################################

$hash0 = $cluster->safe_psql(0, "
	CREATE DOMAIN things AS INT;
	CREATE TABLE thethings (stuff things);
	INSERT INTO thethings (stuff) VALUES (55);
	ALTER DOMAIN things ADD CONSTRAINT meow CHECK (VALUE < 11) NOT VALID;
	UPDATE thethings SET stuff = 10;
	ALTER DOMAIN things VALIDATE CONSTRAINT meow;
");
my $result0 = $cluster->safe_psql(0, "SELECT * FROM thethings");
my $result1 = $cluster->safe_psql(1, "SELECT * FROM thethings");
my $result2 = $cluster->safe_psql(2, "SELECT * FROM thethings");
note("Value in the stuff column of thethings table is $result0 at the node1 and match to corresponding values from another nodes: 2 - $result1 and 3 - $result2 ");
is( (($result0 eq 10) and ($result0 eq $result1) and ($result1 eq $result2)), 1,
	"Check that update not aborted by violation of constraint on old tuple value");

# ##############################################################################
#
# [PGPRO-3047] Check for problems with different OIDs on multimaster nodes
# during logical replication of tuples contained attribute with domain over
# arrays of composite.
#
# ##############################################################################

# Check that OIDs are different.
$result0 = $cluster->safe_psql(0,
					"select oid from pg_class where relname like 'thethings';");
$result1 = $cluster->safe_psql(1,
					"select oid from pg_class where relname like 'thethings';");
$result2 = $cluster->safe_psql(2,
					"select oid from pg_class where relname like 'thethings';");
note("OIDS of the thethings relation: node1 - $result0, node2 - $result1, node3 - $result2");
is( ( ($result0 ne $result1) and ($result0 ne $result2) and ($result1 ne $result2) ), 1,
	"Check that oid of the thethings relation are different on each node");

# Do the test. Insertion of array type must be passed successfully.
# Source: regression test domain.sql
$cluster->safe_psql(0, "
	CREATE TYPE comptype AS (r float8, i float8);
	CREATE domain dcomptypea AS comptype[];
	CREATE table dcomptable (d1 dcomptypea UNIQUE);
	INSERT INTO dcomptable VALUES (array[row(1,2)]::dcomptypea);
");

$cluster->stop();

