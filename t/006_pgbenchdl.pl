# Like pgbench.pl, but the probability of deadlocks is much higher; check that
# they get detected.

use strict;
use warnings;

use Cluster;
use Test::More tests => 1;
use Data::Dumper;

use POSIX ":sys_wait_h";

my $cluster = new Cluster(3);
$cluster->init();
$cluster->start();
$cluster->create_mm();

$cluster->safe_psql(0, q{
	create table transactions (id SERIAL primary key, dt timestamp default now(), uid int, amount int);
	create index on transactions using btree(uid);
	create table users (uid int primary key, sum bigint);
});

my $clients = 10;
my $seconds = 90;
my @benches = ();
foreach (0..$#{$cluster->{nodes}})
{
	push @benches, $cluster->pgbench_async($_,
		('-n', -T => $seconds, -c => $clients, -f => 'tests/deadl.pgb'));
}

sub isalive {
	my $benches = $_[0];
	my $any_alive = 0;
	waitpid(-1, WNOHANG);
	$any_alive = ($any_alive or (kill 0,$_->{'KIDS'}->[0]->{'PID'})) foreach @{$benches};
	return $any_alive;
}

# ensure num of successfull xacts steadily goes up, i.e. deadlocks are detected
# in time.
my $ptrans = 0;
my $dead_count = 0;
while (isalive(\@benches)) {
	my $trans = $cluster->safe_psql(0,
		"select count(*) from transactions");
	if ($ptrans == 0) {
		$ptrans = $trans;
	} elsif ($ptrans == $trans) {
		$dead_count++;
	} else {
		$dead_count = 0;
		$ptrans = $trans;
	}
	if ($dead_count >=3) {
		last;
	}
	sleep 2;
}

ok($dead_count < 3, 'at least one xact was committed during 6 seconds');
$cluster->stop;
