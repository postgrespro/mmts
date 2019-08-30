# PGPRO-2950
# Simple test for restore pg_dump backups with psql

use strict;
use warnings;

use PostgresNode;
use Cluster;
use TestLib;
use Test::More tests => 6;

my $tempdir = TestLib::tempdir;
my $connection_string;
my %connect_params;

# server to backup data from
my $node = get_new_node('master');
$node->init();
$node->start;

# create some data
$node->safe_psql( 'postgres', 'CREATE DATABASE test;' );
$node->safe_psql( 'test', 'CREATE TABLE mega_important_table AS SELECT generate_series::int as id, generate_series::text AS name FROM generate_series( 0, 999 );' );
$node->safe_psql( 'test', 'ALTER TABLE mega_important_table ADD CONSTRAINT mega_important_table_pk PRIMARY KEY ( id );' );

# control
my $res = $node->safe_psql( 'test', 'SELECT name FROM mega_important_table WHERE id = 555;' );
ok( $res == '555', 'init ok' );

$node->command_ok( ["pg_dump", "--inserts", "-f", "$tempdir/backup.sql", "test"] );

# multimaster cluster
my $cluster = new Cluster( 3 );
$cluster->init();
$cluster->start();
$cluster->create_mm( 'test' );

# load data to the cluster
$connection_string = $cluster->connstr( 0 );
%connect_params = split( /[\s=]/, $connection_string);
$node->command_ok( ["psql", "-h", $connect_params{'host'}, "-p", $connect_params{'port'}, "-f", "$tempdir/backup.sql", "test"] );

# control
$res = $cluster->safe_psql( 0, 'SELECT name FROM mega_important_table WHERE id = 555;' );

ok( $res == '555', 'node 0 ok' );

$res = $cluster->safe_psql( 1, 'SELECT name FROM mega_important_table WHERE id = 555;' );

ok( $res == '555', 'node 1 ok' );

$res = $cluster->safe_psql( 2, 'SELECT name FROM mega_important_table WHERE id = 555;' );

ok( $res == '555', 'node 2 ok' );

$node->stop;
$cluster->stop;
