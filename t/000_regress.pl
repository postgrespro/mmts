# run sql/multimaster.sql tests
use Cluster;
use Test::More tests => 2;

# psql and pg_regress which calls psql calculates ascii table header
# width based on string len which can be different in bytes and utf
# codepoints
$ENV{LC_ALL} = 'en_US.UTF-8';

# determenistic ports for expected files
$PostgresNode::last_port_assigned = 55431;

my $cluster = new Cluster(3);
$cluster->init(q{
	multimaster.volkswagen_mode = on
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
});

# do not show transaction from concurrent backends in pg_prepared_xacts
$cluster->{nodes}->[0]->safe_psql('regression', q{
	ALTER VIEW pg_prepared_xacts RENAME TO _pg_prepared_xacts;
	CREATE VIEW pg_prepared_xacts AS select * from _pg_prepared_xacts where gid not like 'MTM-%';
});

$cluster->{nodes}->[0]->safe_psql('regression', "ALTER SYSTEM SET allow_system_table_mods = 'off'");
foreach my $node (@{$cluster->{nodes}}){
	$node->restart;
}

while ($cluster->{nodes}->[0]->psql('regression','SELECT version()')) {
	sleep 1;
}

# load schedule without tablespace test which is not expected
# to work with several postgreses on a single node
my $schedule = TestLib::slurp_file('../../src/test/regress/parallel_schedule');
$schedule =~ s/test: tablespace/#test: tablespace/g;
unlink('parallel_schedule');
TestLib::append_to_file('parallel_schedule', $schedule);

TestLib::system_log($ENV{'PG_REGRESS'},
	'--host=127.0.0.1', "--port=$port",
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
unlink('results/regression.diffs');
TestLib::append_to_file('results/regression.diffs', $res_diff);

# finally compare regression.diffs with our version
$diff = TestLib::system_log("diff results/regression.diffs expected/regression.diffs");

is($diff, 0, "postgres regress");
