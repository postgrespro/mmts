# run core regression tests on multimaster

use Cluster;
use File::Basename;
use IPC::Run 'run';
use Test::More tests => 1;

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
# postgres regression tests
###############################################################################

# configure db output format like pg_regress
$cluster->{nodes}->[0]->safe_psql('regression', q{
	ALTER DATABASE "regression" SET lc_messages TO 'C';
	ALTER DATABASE "regression" SET lc_monetary TO 'C';
	ALTER DATABASE "regression" SET lc_numeric TO 'C';
	ALTER DATABASE "regression" SET lc_time TO 'C';
	ALTER DATABASE "regression" SET timezone_abbreviations TO 'Default';
});

# do not show transaction from concurrent backends in pg_prepared_xacts
$cluster->{nodes}->[0]->safe_psql('regression', q{
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
});
foreach my $node (@{$cluster->{nodes}}){
	$node->restart;
}
$cluster->await_nodes( [0,1,2] );

# load schedule without tablespace test which is not expected
# to work with several postgreses on a single node
my $schedule = TestLib::slurp_file('../../src/test/regress/parallel_schedule');
$schedule =~ s/test: tablespace/#test: tablespace/g;
$schedule =~ s/largeobject//;
unlink('parallel_schedule');
TestLib::append_to_file('parallel_schedule', $schedule);

END {
	if(! $ENV{'KEEP_OUT'}) {
		unlink "../../src/test/regress/regression.diffs";
		my @outfiles = <../../src/test/regress/results/*.out>;
		unlink @outfiles;
	}
}

my $regress_shlib = TestLib::perl2host($ENV{REGRESS_SHLIB});
my $regress_libdir = dirname($regress_shlib);
TestLib::system_log($ENV{'PG_REGRESS'},
	'--host=' . $Cluster::mm_listen_address, "--port=$port",
	'--use-existing', '--bindir=',
	'--schedule=parallel_schedule',
	"--dlpath=${regress_libdir}",
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
mkdir('results');
unlink('results/regression.diff');

# finally compare regression.diffs with our version
# Do not use diffs extension as some upper level testing systems are searching for all
# *.diffs files.
TestLib::append_to_file('results/regression.diff', $res_diff);
$diff = TestLib::system_log("diff expected/regression.diff results/regression.diff");

# save diff of diff in file
run [ "diff", "-U3", "expected/regression.diff", "results/regression.diff" ], ">", "regression.diff.diff";
my $res = $?;
is($res, 0, "postgres regress");
