use strict;
use warnings;
use PostgresNode;
use Cluster;
use TestLib;
use Test::More tests => 6;

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
        insert into mtm.cluster_nodes values
            (2, \$\$@{[ $cluster->connstr(0) ]}\$\$, '@{[ $_ == 0 ? 't' : 'f' ]}', 'f'),
            (4, \$\$@{[ $cluster->connstr(1) ]}\$\$, '@{[ $_ == 1 ? 't' : 'f' ]}', 'f'),
            (5, \$\$@{[ $cluster->connstr(2) ]}\$\$, '@{[ $_ == 2 ? 't' : 'f' ]}', 'f');
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
$cluster->pgbench(0, ('-N', '-n', -T => '1') );
$cluster->{nodes}->[2]->start;

$cluster->await_nodes( (2,0,1) );
is($cluster->is_data_identic( (0,1,2) ), 1, "check auto recovery");

################################################################################
# add basebackuped node
################################################################################

my $pgb1= $cluster->pgbench_async(0, ('-N', '-n', -T => '3600', -c => '2') );
my $pgb2= $cluster->pgbench_async(1, ('-N', '-n', -T => '3600', -c => '2') );

my $new_node_off = $cluster->add_node();
$cluster->{nodes}->[$new_node_off]->{dbname} = 'postgres';
my $sock = $cluster->hold_socket($new_node_off);
my $connstr = $cluster->connstr($new_node_off);
my $new_node_id = $cluster->safe_psql(0, "SELECT mtm.add_node(\$\$$connstr\$\$)");

is($new_node_id, 1, "sparse id assignment");
is($new_node_off, 3, "sparse id assignment");

$cluster->pgbench(0, ('-N', '-n', -t => '100') );

my $end_lsn = $cluster->backup_and_init(0, $new_node_off, $new_node_id);
$cluster->release_socket($sock);
$cluster->{nodes}->[$new_node_off]->start;
$cluster->await_nodes( (3,0,1,2) );
$cluster->safe_psql(0, "SELECT mtm.join_node('$new_node_id', '$end_lsn')");

sleep(5);
IPC::Run::kill_kill($pgb1);
IPC::Run::kill_kill($pgb2);

$cluster->await_nodes( (3,0,1,2) );
$cluster->pgbench(0, ('-N', '-n', -t => '100') );
$cluster->pgbench(3, ('-N', '-n', -t => '100') );

is($cluster->is_data_identic( (0,1,2,3) ), 1, "add basebackuped node");

my $bb_keycount = $cluster->safe_psql(3, q{
    select count(*) from mtm.config where key='basebackup'
});

is($bb_keycount, 0, "basebackup key was deleted");

################################################################################
# XXX: check remove/add of same node
################################################################################

################################################################################
# XXX: check self remove
################################################################################
