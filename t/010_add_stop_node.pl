use strict;
use warnings;

use Carp;
use PostgresNode;
use Cluster;
use TestLib;
use Test::More tests => 8;

# Generally add node with concurrent load (and failures) is not supported
# because of at least
# 1) it is not clear why non-donor nodes should properly keep WAL for new node;
# 2) if donor fails, it is not clear whether new node will obtain suitable
#    syncpoints to pull from non-donors;
# 3) A problem with slot creation and receiver start deadlocking each other,
#    see PGPRO-3618.
#
# drop_node with concurrent load is not safe at least because once it is done we
# can't determine origin node properly, so no its xacts would be replicated.
#
# An option is left for experiments/future work.
my $concurrent_load = 0;

my $cluster = new Cluster(3);
$cluster->init();
$cluster->start();

# XXXX: delete all '-n' ?

################################################################################
# manually setup nodes with sparse node_id's
################################################################################

foreach (0..$#{$cluster->{nodes}})
{
    my $node = $cluster->{nodes}->[$_];
    $node->{dbname} = 'postgres';
}

foreach (0..$#{$cluster->{nodes}})
{
    my $node = $cluster->{nodes}->[$_];

    note($cluster->connstr($_));

    $cluster->safe_psql($_, qq{
        create extension multimaster;
		select mtm.state_create('{2, 4, 5}');
        insert into mtm.cluster_nodes values
            (2, \$\$@{[ $cluster->connstr(0) ]}\$\$, '@{[ $_ == 0 ? 't' : 'f' ]}'),
            (4, \$\$@{[ $cluster->connstr(1) ]}\$\$, '@{[ $_ == 1 ? 't' : 'f' ]}'),
            (5, \$\$@{[ $cluster->connstr(2) ]}\$\$, '@{[ $_ == 2 ? 't' : 'f' ]}');
    });
}

$cluster->await_nodes( (0..$#{$cluster->{nodes}}) );

$cluster->pgbench(0, ('-i', '-n', -s => '10') );
$cluster->pgbench(0, ('-N', '-n', -t => '100') );
$cluster->pgbench(1, ('-N', '-n', -t => '100') ); # XXX: pgbench stucks here for quite a long time
$cluster->pgbench(2, ('-N', '-n', -t => '100') );

################################################################################
# auto recovery
################################################################################

$cluster->{nodes}->[2]->stop('fast');
# sometimes failure of one node forces another into short recovery
sleep(2);
$cluster->pgbench(0, ('-N', '-n', -T => '1') );
$cluster->{nodes}->[2]->start;

$cluster->await_nodes( (2,0,1) );
is($cluster->is_data_identic( (0,1,2) ), 1, "check auto recovery");

################################################################################
# add basebackuped node
################################################################################

# add table with sequence to check sequences after n_nodes change
$cluster->safe_psql(0, "create table test_seq(id serial primary key)");
$cluster->safe_psql(0, "insert into test_seq values(DEFAULT)");
$cluster->safe_psql(1, "insert into test_seq values(DEFAULT)");
$cluster->safe_psql(2, "insert into test_seq values(DEFAULT)");

my $pgb1;
my $pgb2;
if ($concurrent_load)
{
	$pgb1= $cluster->pgbench_async(0, ('-N', '-n', -T => '3600', -c => '2') );
	$pgb2= $cluster->pgbench_async(1, ('-N', '-n', -T => '3600', -c => '2') );
}

my $new_node_off = $cluster->add_node();
$cluster->{nodes}->[$new_node_off]->{dbname} = 'postgres';
my $sock = $cluster->hold_socket($new_node_off);
my $connstr = $cluster->connstr($new_node_off);
my $new_node_id = $cluster->safe_psql(0, "SELECT mtm.add_node(\$\$$connstr\$\$)");

is($new_node_id, 1, "sparse id assignment");
is($new_node_off, 3, "sparse id assignment");
if ($concurrent_load)
{
	$cluster->pgbench(0, ('-N', '-n', -t => '100') );
}
# ensure monitor creates slot for new node on donor which will be used by
# basebackup before proceeding
$cluster->poll_query_until(0, "select exists(select * from pg_replication_slots where slot_name = 'mtm_filter_slot_${new_node_id}');")
    or croak "timed out waiting for slot creation";
my $end_lsn = $cluster->backup_and_init(0, $new_node_off, $new_node_id);
$cluster->release_socket($sock);
$cluster->{nodes}->[$new_node_off]->append_conf('postgresql.conf', q{unix_socket_directories = ''
	});
$cluster->{nodes}->[$new_node_off]->start;
$cluster->await_nodes( (3,0,1,2) );
$cluster->safe_psql(0, "SELECT mtm.join_node('$new_node_id', '$end_lsn')");
note("join_node done");

if ($concurrent_load)
{
	sleep(5);
	IPC::Run::kill_kill($pgb1);
	IPC::Run::kill_kill($pgb2);
}

$cluster->await_nodes( (3,0,1,2) );
$cluster->pgbench(0, ('-N', '-n', -t => '100') );
$cluster->pgbench(3, ('-N', '-n', -t => '100') );

is($cluster->is_data_identic( (0,1,2,3) ), 1, "add basebackuped node");

my $bb_keycount = $cluster->safe_psql(3, q{
    select count(*) from mtm.config where key='basebackup'
});

is($bb_keycount, 0, "basebackup key was deleted");

# check that sequences in proper state
$cluster->safe_psql(0, "insert into test_seq values(DEFAULT)");
$cluster->safe_psql(1, "insert into test_seq values(DEFAULT)");
$cluster->safe_psql(2, "insert into test_seq values(DEFAULT)");
$cluster->safe_psql(3, "insert into test_seq values(DEFAULT)");

################################################################################
# basic check of recovery after add node succeeded
################################################################################

$cluster->{nodes}->[0]->stop('fast');
sleep(2);
$cluster->pgbench(3, ('-N', '-n', -T => '1') );
$cluster->{nodes}->[0]->start;

$cluster->await_nodes( (2,0,1) );
is($cluster->is_data_identic((0,1,2,3)), 1, "check recovery after add_node");

################################################################################
# drop one of the initial nodes
################################################################################

$cluster->safe_psql(1, "select mtm.drop_node(2)");
$cluster->{nodes}->[0]->stop('fast');

# check basic recovery after drop_node
$cluster->{nodes}->[1]->stop('fast');
sleep(2);
$cluster->pgbench(3, ('-N', '-n', -T => '1') );
$cluster->pgbench(2, ('-N', '-n', -T => '1') );
$cluster->{nodes}->[1]->start;
$cluster->await_nodes( (3,2,1) );
is($cluster->is_data_identic((1,2,3)), 1, "check recovery after drop_node");


# TODO: check that WALs are not kept for dropped node anymore

################################################################################
# XXX: check remove/add of same node
################################################################################

################################################################################
# XXX: check self remove
################################################################################
