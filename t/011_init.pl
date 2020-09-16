# test that after create_mm awaited nodes we won't get non-online state
# immediately later. Catches races in MtmGetCurrentStatus logic.

use Cluster;
use Test::More tests => 1;

my $cluster = new Cluster(3);
$cluster->init(q{
});
$cluster->start();
$cluster->create_mm('regression');

foreach(0..1000) # hopefully enough to catch all related races
{
	foreach (0..2)
	{
		$cluster->safe_psql($_, "select 42");
	}
}

is(0, 0, "dummy"); # Test::More doesn't like 0 tests, ha
