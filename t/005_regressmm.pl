# run sql/multimaster.sql tests
use Cluster;
use Test::More tests => 1;

# determenistic ports for expected files
$PostgresNode::last_port_assigned = 55431;

my $cluster = new Cluster(3);
$cluster->init(q{
	multimaster.volkswagen_mode = off
});
$cluster->start();
$cluster->create_mm('regression');

my $port = $cluster->{nodes}->[0]->port;

###############################################################################
# multimaster regression tests
###############################################################################

my @tests = ('multimaster');
# run atx test only on ee
if (Cluster::is_ee())
{
	push @tests, 'atx';
}

my $ret = TestLib::system_log($ENV{'PG_REGRESS'},
    '--host=' . $Cluster::mm_listen_address, "--port=$port",
    '--use-existing', '--bindir=', @tests);
if ($ret != 0)
{
    print "### Got regression! \n", TestLib::slurp_file('regression.diffs');
}
is($ret, 0, "multimaster regress");
