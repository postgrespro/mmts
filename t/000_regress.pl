# run sql/multimaster.sql tests
use Cluster;
use Test::More tests => 2;

# determenistic ports for expected files
$PostgresNode::last_port_assigned = 55431;

my $cluster = new Cluster(3);
$cluster->init(q{
	multimaster.volkswagen_mode = off
	# allow to spoof pg_prepared_xacts view
	allow_system_table_mods = on
});
$cluster->start();
$cluster->create_mm('regression');

my $port = $cluster->{nodes}->[0]->port;

###############################################################################
# multimaster regression tests
###############################################################################

my $ret = TestLib::system_log($ENV{'PG_REGRESS'},
    '--host=127.0.0.1', "--port=$port",
    '--use-existing', '--bindir=', 'multimaster');
if ($ret != 0)
{
    print "### Got regression! \n", TestLib::slurp_file('regression.diffs');
}
is($ret, 0, "multimaster regress");

###############################################################################
# postgres regression tests
###############################################################################

# configure db output format like pg_regress
$cluster->{nodes}->[0]->safe_psql('regression', q{
	ALTER DATABASE "regression" SET lc_messages TO 'C';
	ALTER DATABASE "regression" SET lc_monetary TO 'C';
	ALTER DATABASE "regression" SET lc_numeric TO 'C';
	ALTER DATABASE "regression" SET lc_time TO 'C';
	ALTER DATABASE "regression" SET timezone_abbreviations TO 'Default';
	DROP SCHEMA public CASCADE; -- clean database after previous test
	CREATE SCHEMA public;
	GRANT ALL ON SCHEMA public TO public;
});

# do not show transaction from concurrent backends in pg_prepared_xacts
$cluster->{nodes}->[0]->safe_psql('regression', q{
	CREATE ROLE regression LOGIN SUPERUSER;
	ALTER VIEW pg_prepared_xacts RENAME TO _pg_prepared_xacts;
	CREATE VIEW pg_prepared_xacts AS
		select * from _pg_prepared_xacts where gid not like 'MTM-%'
		ORDER BY transaction::text::bigint;
	ALTER TABLE pg_publication RENAME TO _pg_publication;
	CREATE VIEW pg_catalog.pg_publication AS SELECT oid,* FROM pg_catalog._pg_publication WHERE pubname<>'multimaster';
	ALTER TABLE pg_subscription RENAME TO _pg_subscription;
	CREATE VIEW pg_catalog.pg_subscription AS SELECT oid,* FROM pg_catalog._pg_subscription WHERE subname NOT LIKE 'mtm_sub_%';
});

$cluster->{nodes}->[0]->safe_psql('regression', q{
	ALTER SYSTEM SET allow_system_table_mods = 'off';
	ALTER SYSTEM SET multimaster.volkswagen_mode = 'on';
});
foreach my $node (@{$cluster->{nodes}}){
	$node->restart;
}
$cluster->await_nodes( (0,1,2) );

# load schedule without tablespace test which is not expected
# to work with several postgreses on a single node
my $schedule = TestLib::slurp_file('../../src/test/regress/parallel_schedule');
$schedule =~ s/test: tablespace/#test: tablespace/g;
unlink('parallel_schedule');
TestLib::append_to_file('parallel_schedule', $schedule);

TestLib::system_log($ENV{'PG_REGRESS'},
	'--host=127.0.0.1', "--port=$port",
	'--user=regression',
	'--use-existing', '--bindir=',
	'--schedule=parallel_schedule',
	'--dlpath=../../src/test/regress',
	'--outputdir=../../src/test/regress',
	'--inputdir=../../src/test/regress');
unlink('parallel_schedule');

# strip dates out of resulted regression.diffs
my $res_diff = TestLib::slurp_file('../../src/test/regress/regression.diffs');
$res_diff =~ s/(--- ).+(contrib\/mmts.+\.out)\t.+\n/$1$2\tCENSORED\n/g;
$res_diff =~ s/(\*\*\* ).+(contrib\/mmts.+\.out)\t.+\n/$1$2\tCENSORED\n/g;
$res_diff =~ s/(lo_import[ \(]')\/[^']+\//$1\/CENSORED\//g;
#SELECT lo_export(loid, '/home/alex/projects/ppro/postgrespro/contrib/mmts/../../src/test/regress/results/lotest.txt') FROM lotest_stash_values;
$res_diff =~ s/(lo_export.*\'\/).+\//$1CENSORED\//g;
unlink('results/regression.diffs');
TestLib::append_to_file('results/regression.diffs', $res_diff);

# finally compare regression.diffs with our version
$diff = TestLib::system_log("diff results/regression.diffs expected/regression.diffs");

is($diff, 0, "postgres regress");
