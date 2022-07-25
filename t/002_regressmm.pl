# run sql/multimaster.sql tests
use Cluster;
use Test::More tests => 1;

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
	multimaster.volkswagen_mode = off
});
$cluster->start();
$cluster->create_mm('regression');

###############################################################################
# multimaster regression tests
###############################################################################

my @tests = ('multimaster');
# run atx test only on ee
if (Cluster::is_ee())
{
	push @tests, 'atx';
}

my $ret;
if ($Cluster::pg_15_modules)
{
	$ret = PostgreSQL::Test::Utils::system_log($ENV{'PG_REGRESS'},
		'--host=' . $cluster->{nodes}->[0]->host, '--port=' . $cluster->{nodes}->[0]->port,
		'--use-existing', '--bindir=', @tests);
	if ($ret != 0)
	{
		print "### Got regression! \n", PostgreSQL::Test::Utils::slurp_file('regression.diffs');
	}
}
else
{
	$ret = TestLib::system_log($ENV{'PG_REGRESS'},
		'--host=' . $cluster->{nodes}->[0]->host, '--port=' . $cluster->{nodes}->[0]->port,
		'--use-existing', '--bindir=', @tests);
	if ($ret != 0)
	{
		print "### Got regression! \n", TestLib::slurp_file('regression.diffs');
	}
}
is($ret, 0, "multimaster regress");
