use strict;
use warnings;

use Cluster;
use TestLib;
use Test::More tests => 1;

my $cluster = new Cluster(2);
$cluster->init();
$cluster->start();
$cluster->create_mm();

my $seconds = 30;

$cluster->pgbench(0, ('-i', -s => '1') );
my $pgb_handle = $cluster->pgbench_async(0, ('-N', '-n', -T => $seconds, -c => 8) );

my $started = time();
while (time() - $started < $seconds)
{
	$cluster->safe_psql(0, "truncate pgbench_history;");
	note("truncated");
	sleep(1);
}
$cluster->pgbench_await($pgb_handle);
$cluster->stop('fast');

is(1, 1);
