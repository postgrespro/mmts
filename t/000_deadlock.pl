# simple deadlock test

use strict;
use warnings;

use Cluster;

# Test whether we have both DBI and DBD::pg
my $dbdpg_rc = eval
{
  require DBI;
  require DBD::Pg;
  DBD::Pg->import(':async');
  1;
};

# And tell Test::More to skip the test entirely if not
require Test::More;
if (not $dbdpg_rc)
{
	Test::More->import(skip_all => 'DBI and DBD::Pg are not available');
}
else
{
	Test::More->import(tests => 1);
}

sub query_row
{
	my ($dbi, $sql, @keys) = @_;
	my $sth = $dbi->prepare($sql) || die;
	$sth->execute(@keys) || die;
	my $ret = $sth->fetchrow_array || undef;
	return $ret;
}

sub query_exec
{
	my ($dbi, $sql) = @_;
	my $rv = $dbi->do($sql) || die;
	return $rv;
}

sub query_exec_async
{
	my ($dbi, $sql) = @_;
	# Since we are not importing DBD::Pg at compilation time, we can't use
	# constants from it.
	my $DBD_PG_PG_ASYNC = 1;
	my $rv = $dbi->do($sql, {pg_async => $DBD_PG_PG_ASYNC}) || die;
	return $rv;
}

my $cluster = new Cluster(2);

$cluster->init();
$cluster->start();
$cluster->create_mm('regression');

my ($rc, $out, $err);
sleep(10);

$cluster->safe_psql(0, "create table t(k int primary key, v text)");
$cluster->safe_psql(0, "insert into t values (1, 'hello'), (2, 'world')");

my @conns = map { DBI->connect('DBI:Pg:' . $cluster->connstr($_)) } 0..1;

query_exec($conns[0], "begin");
query_exec($conns[1], "begin");

query_exec($conns[0], "update t set v = 'asd' where k = 1");
query_exec($conns[1], "update t set v = 'bsd'");

query_exec($conns[0], "update t set v = 'bar' where k = 2");
query_exec($conns[1], "update t set v = 'foo'");

query_exec_async($conns[0], "commit");
query_exec_async($conns[1], "commit");

my $timeout = 16;
while (--$timeout > 0)
{
	my $r0 = $conns[0]->pg_ready();
	my $r1 = $conns[1]->pg_ready();
	if ($r0 && $r1) {
		last;
	}
	sleep(1);
}

if ($timeout > 0)
{
	my $succeeded = 0;
	$succeeded++ if $conns[0]->pg_result();
	$succeeded++ if $conns[1]->pg_result();

	pass("queries finished");
}
else
{
	$conns[0]->pg_cancel() unless $conns[0]->pg_ready();
	$conns[1]->pg_cancel() unless $conns[1]->pg_ready();

	fail("queries timed out");
}

query_row($conns[0], "select * from t where k = 1");

$cluster->stop('fast');
