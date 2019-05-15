# run sql/multimaster.sql tests

use Cluster;
# use TestLib;
use Test::More tests => 1;

my $cluster = new Cluster(3);
$cluster->init();
$cluster->start();
$cluster->create_mm('regression');

my $port = $cluster->{nodes}->[0]->port;

TestLib::system_or_bail($ENV{'PG_REGRESS'},
    '--host=127.0.0.1', "--port=$port",
    '--use-existing', '--bindir=', 'multimaster');

is(1,1, "ok");
