# run sql/multimaster.sql tests

use Cluster;
# use TestLib;
use Test::More tests => 1;

my $cluster = new Cluster(3);
$cluster->init();
$cluster->start();
$cluster->create_mm('regression');

my $port = $cluster->{nodes}->[0]->port;

my $ret = TestLib::system_log($ENV{'PG_REGRESS'},
    '--host=127.0.0.1', "--port=$port",
    '--use-existing', '--bindir=', 'multimaster');

if ($ret != 0)
{
    print "### Got regression! \n", TestLib::slurp_file('regression.diffs');
}

is($ret, 0, "ok");
