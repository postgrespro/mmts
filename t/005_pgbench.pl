# Kinda bank test: on each node multiple clients transfer money from one acc to
# another, another bunch of clients make sure sum is constant always.

use strict;
use warnings;

use Cluster;
use Test::More tests => 2;

my $cluster = new Cluster(2);
$cluster->init();
$cluster->start();
$cluster->create_mm();

$cluster->safe_psql(0, q{
	create table t (k int primary key, v int);
	insert into t (select generate_series(0, 999), 0);
	create table reader_log (v int);
});

my $clients = 5;
my $seconds = 30;
my @benches = ();
foreach (0..$#{$cluster->{nodes}})
{
	push @benches, $cluster->pgbench_async($_,
		('-n', -T => $seconds, -c => $clients, -f => 'tests/reader.pgb'));
	push @benches, $cluster->pgbench_async($_,
		('-n', -T => $seconds, -c => $clients, -f => 'tests/writer.pgb', -R => 10));
}

$cluster->pgbench_await($_) foreach @benches;

my $out;

$out = $cluster->safe_psql(0,
	"select count(*) from reader_log where v != 0");
is($out, 0, "there is nothing except zeros in reader_log");

$out = $cluster->safe_psql(0,
	"select count(*) from reader_log where v = 0");
isnt($out, 0, "reader_log is not empty");

$cluster->stop;
