use strict;
use warnings;
use PostgresNode;
use Cluster;
use TestLib;
use Test::More tests => 3;

my $cluster = new Cluster(3);
$cluster->init();
$cluster->start();

foreach (0..$#{$cluster->{nodes}})
{
    my $node = $cluster->{nodes}->[$_];
    $node->{dbname} = 'postgres';
}

foreach (0..$#{$cluster->{nodes}})
{
    my $node = $cluster->{nodes}->[$_];

    note($cluster->connstr($_));

    $cluster->safe_psql($_, qq{
        create extension multimaster;
        insert into mtm.cluster_nodes values
            (2, \$\$@{[ $cluster->connstr(0) ]}\$\$, '@{[ $_ == 0 ? 't' : 'f' ]}', 'f'),
            (4, \$\$@{[ $cluster->connstr(1) ]}\$\$, '@{[ $_ == 1 ? 't' : 'f' ]}', 'f'),
            (5, \$\$@{[ $cluster->connstr(2) ]}\$\$, '@{[ $_ == 2 ? 't' : 'f' ]}', 'f');
    });
}

$cluster->await_nodes( (0..$#{$cluster->{nodes}}) );

$cluster->safe_psql($_, q{
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

$cluster->stop();

