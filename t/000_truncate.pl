use strict;
use warnings;

use Cluster;
use TestLib;
use Test::More tests => 1;
use IPC::Run qw(start finish);
use Cwd;

my $nnodes = 2;
my $cluster = new Cluster($nnodes);

$cluster->init();
$cluster->configure();
$cluster->start();
$cluster->await_nodes( (0,1) );

my ($out, $err, $rc);
my $seconds = 30;
my $total_err = '';
$out = '';

$cluster->pgbench(0, ('-i', -s => '1') );
my $pgb_handle = $cluster->pgbench_async(0, ('-N', '-n', -T => $seconds, -c => 8) );

my $started = time();
while (time() - $started < $seconds)
{
	($rc, $out, $err) = $cluster->psql(1, 'postgres', "truncate pgbench_history;");
	note("truncated");
	$total_err .= $err;
	sleep(1);
	# ($rc, $out, $err) = $cluster->psql(0, 'postgres', "truncate pgbench_history;");
	# $total_err .= $err;
	# sleep(0.5);
}
$cluster->pgbench_await($pgb_handle);

is($total_err, '', "truncate successful");
$cluster->stop();
1;
