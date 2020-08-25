# Check local table, added after PGPRO-2883.
# ars: looks too specific in its current shape. At least local table
# modifications should be added

use strict;
use warnings;

use Cluster;
use TestLib;

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


sub query_exec
{
	my ($dbi, $sql) = @_;
	my $rv = $dbi->do($sql) || die;
	#diag("query_exec('$sql') = $rv\n");
	return $rv;
}

sub query_exec_async
{
	my ($dbi, $sql) = @_;
	# Since we are not importing DBD::Pg at compilation time, we can't use
	# constants from it.
	my $DBD_PG_PG_ASYNC = 1;
	my $rv = $dbi->do($sql, {pg_async => $DBD_PG_PG_ASYNC}) || die;
	#diag("query_exec_async('$sql')\n");
	return $rv;
}

my $cluster = new Cluster(2);

$cluster->init();
$cluster->start();
$cluster->create_mm('regression');

my ($rc, $out, $err);

#my @conns = map { DBI->connect('DBI:Pg:' . $cluster->connstr($_)) } 0..1;
my $conn = DBI->connect('DBI:Pg:' . $cluster->connstr(0));

query_exec($conn, "begin");

query_exec($conn, "create table t (k serial primary key, v int)");
query_exec($conn, "select mtm.make_table_local('t')");
query_exec_async($conn, "commit");

my $timeout = 5;
while (--$timeout > 0)
{
	my $r = $conn->pg_ready() and last;
	#diag("queries still running: [0]=$r\n");
	sleep(1);
}

if ($timeout > 0)
{
	#diag("queries finished\n");

	my $succeeded = 1;
	pass("queries finished");
}
else
{
	#diag("queries timed out\n");
	#$conn->pg_cancel() unless $conn->pg_ready();
	fail("queries timed out");
}

$cluster->stop('fast');
