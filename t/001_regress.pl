# run core regression tests on multimaster

# tests known to fail currently and failure reasons:
# - create_index (CREATE INDEX CONCURRENTLY not supported due to deadlock
#     issues, see ddl.c)
# - same for index_including, index_including_gist
# - create_table (due to CTAS prepared statement)
# - sanity check (due to pg_publication/subscription masking and other mtm tables)
# - transactions (lack of COMMIT AND CHAIN support)
# - rowsecurity
# - atx, atx5
# - rules (_pg_prepared_xacts and similar)
# - publication, subscription (_pg_publication/subscription masking)
# - prepare (CTAS prepared statement)
# - indexing (again CIC).
#
# original test output/diffs are at $ENV{TESTDIR}/tmp_check/regress_outdir;
# (in normal build TESTDIR is just mmts/; in vpath it is 'external' mmts/)
# then diff is censored and copied to $ENV{TESTDIR}/results.

use Cluster;
use File::Basename;
use IPC::Run 'run';
use Test::More;

# With PGXS the sources are unavailable, so we can't obtain schedules and core
# test themselves.
if ($ENV{'PGXS'})
{
	# Test::More doesn't like no tests at all
	is(0, 0, "dummy");
	done_testing();
	exit(0);
}

# determenistic ports for expected files
if ($Cluster::pg_15_modules)
{
	$PostgreSQL::Test::Cluster::last_port_assigned = 55431;
}
else
{
	$PostgresNode::last_port_assigned = 55431;
}

my $cluster = new Cluster(3);
$cluster->init(q{
	multimaster.volkswagen_mode = on
	# allow to spoof pg_prepared_xacts view
	allow_system_table_mods = on
});
$cluster->start();
$cluster->create_mm('regression');

###############################################################################
# postgres regression tests
###############################################################################

# configure db output format like pg_regress
# In particular, pg_regress explicitly sets PGTZ=PST8PDT, and it turns out some
# tests (including DDL! (see volatile_partbound_test)) depend on current_time,
# so mtm receiver ought to use the same timezone to pass them.
$cluster->{nodes}->[0]->safe_psql('regression', q{
	ALTER DATABASE "regression" SET lc_messages TO 'C';
	ALTER DATABASE "regression" SET lc_monetary TO 'C';
	ALTER DATABASE "regression" SET lc_numeric TO 'C';
	ALTER DATABASE "regression" SET lc_time TO 'C';
	ALTER DATABASE "regression" SET timezone_abbreviations TO 'Default';
	ALTER DATABASE "regression" SET TimeZone TO 'PST8PDT';
});

# do not show transaction from concurrent backends in pg_prepared_xacts
$cluster->{nodes}->[0]->safe_psql('regression', q{
	ALTER VIEW pg_prepared_xacts RENAME TO _pg_prepared_xacts;
	CREATE VIEW pg_prepared_xacts AS
		select * from _pg_prepared_xacts where gid not like 'MTM-%'
		ORDER BY transaction::text::bigint;
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
my $schedule;
if ($Cluster::pg_15_modules)
{
	$schedule = PostgreSQL::Test::Utils::slurp_file('../../src/test/regress/parallel_schedule');
}
else
{
	$schedule = TestLib::slurp_file('../../src/test/regress/parallel_schedule');
}
$schedule =~ s/test: tablespace/#test: tablespace/g;
$schedule =~ s/test: cfs/#test: cfs/g;
$schedule =~ s/test: largeobject//; # serial schedule
$schedule =~ s/largeobject//; # parallel schedule
$schedule =~ s/atx0//; # parallel schedule
unlink('parallel_schedule');
if ($Cluster::pg_15_modules)
{
	PostgreSQL::Test::Utils::append_to_file('parallel_schedule', $schedule);
}
else
{
	TestLib::append_to_file('parallel_schedule', $schedule);
}

my $regress_shlib = $ENV{REGRESS_SHLIB};
my $regress_libdir = dirname($regress_shlib);
my $regress_outdir = "$ENV{TESTDIR}/tmp_check/regress_outdir";
mkdir($regress_outdir);
# REMOVEME: not needed in 14+, pg_regress fixed in upstream
mkdir("${regress_outdir}/sql");
mkdir("${regress_outdir}/expected");
if ($Cluster::pg_15_modules)
{
	PostgreSQL::Test::Utils::system_log($ENV{'PG_REGRESS'},
		'--host=' . $cluster->{nodes}->[0]->host, '--port=' . $cluster->{nodes}->[0]->port,
		'--use-existing', '--bindir=',
		'--schedule=parallel_schedule',
		"--dlpath=${regress_libdir}",
		'--inputdir=../../src/test/regress',
		"--outputdir=${regress_outdir}");
}
else
{
	TestLib::system_log($ENV{'PG_REGRESS'},
		'--host=' . $cluster->{nodes}->[0]->host, '--port=' . $cluster->{nodes}->[0]->port,
		'--use-existing', '--bindir=',
		'--schedule=parallel_schedule',
		"--dlpath=${regress_libdir}",
		'--inputdir=../../src/test/regress',
		"--outputdir=${regress_outdir}");
}
unlink('parallel_schedule');

# rename s/diffs/diff as some upper level testing systems are searching for all
# *.diffs files.
rename "${regress_outdir}/regression.diffs", "${regress_outdir}/regression.diff"
  or die "cannot rename file: $!";

# strip absolute paths and dates out of resulted regression.diffs
my $res_diff;
if ($Cluster::pg_15_modules)
{
	$res_diff = PostgreSQL::Test::Utils::slurp_file("${regress_outdir}/regression.diff");
}
else
{
	$res_diff = TestLib::slurp_file("${regress_outdir}/regression.diff");
}
# In <= 11 default diff format was context, since 12 unified; handing lines
# starting with ---|+++|*** covers both.
# To make someone's life easier, we prepend .. to make relative paths correct.
# (it allows goto file comparison in editors)
# This of course unfortunately doesn't work for VPATH.
$res_diff =~ s/(--- |\+\+\+ |\*\*\* ).+contrib\/mmts(.+\.out)\t.+\n/$1..$2\tCENSORED\n/g;
# Since 12 header like
#   diff -U3 /blabla/contrib/mmts/../../src/test/regress/expected/opr_sanity.out /blabla/mmts/../../src/test/regress/results/opr_sanity.out
# was added to each file diff
$res_diff =~ s/(diff ).+contrib\/mmts(.+\.out).+contrib\/mmts(.+\.out\n)/$1..$2 ..$3/g;
$res_diff =~ s/(lo_import[ \(]')\/[^']+\//$1\/CENSORED\//g;
#SELECT lo_export(loid, '/home/alex/projects/ppro/postgrespro/contrib/mmts/../../src/test/regress/results/lotest.txt') FROM lotest_stash_values;
$res_diff =~ s/(lo_export.*\'\/).+\//$1CENSORED\//g;
mkdir("$ENV{TESTDIR}/results");
unlink("$ENV{TESTDIR}/results/regression.diff");

# finally compare regression.diffs with our version
# Do not use diffs extension as some upper level testing systems are searching for all
# *.diffs files.
if ($Cluster::pg_15_modules)
{
	PostgreSQL::Test::Utils::append_to_file("$ENV{TESTDIR}/results/regression.diff", $res_diff);
}
else
{
    TestLib::append_to_file("$ENV{TESTDIR}/results/regression.diff", $res_diff);
}
# TODO: work with diffs on per-test basis
my $expected_file;
if (Cluster::is_ee())
{
	$expected_file = "expected/regression_ee.diff"
}
else
{
	$expected_file = "expected/regression_vanilla.diff"
}
# Remove lines which contains random data (like ports, users, etc) from output file
# Remove line which starts with '+ mtm_sub_' from output file because it contains random user
run [ "sed", "-i.bak", "/+ mtm_sub_/d", "$ENV{TESTDIR}/results/regression.diff" ];
# Remove line which starts from '+ multimaster' from output file because it contains random port number
run [ "sed", "-i.bak", "/+ multimaster/d", "$ENV{TESTDIR}/results/regression.diff" ];
if ($Cluster::pg_15_modules)
{
	$diff = PostgreSQL::Test::Utils::system_log("diff -U3 ${expected_file} $ENV{TESTDIR}/results/regression.diff");
}
else
{
	$diff = TestLib::system_log("diff -U3 ${expected_file} $ENV{TESTDIR}/results/regression.diff");
}
run [ "diff", "-U3", "${expected_file}", "$ENV{TESTDIR}/results/regression.diff" ], ">", "$ENV{TESTDIR}/regression.diff.diff";
my $res = $?;

is($res, 0, "postgres regress");

done_testing();
