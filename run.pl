#!/usr/bin/env perl

use File::Basename;
BEGIN { unshift @INC, '.'; unshift @INC, '../../src/test/perl' }
use Cluster;

my $n_nodes = 3;
my $action = $ARGV[0];

if ($action eq "--start")
{
	$PostgresNode::last_port_assigned = 65431;

	my $cluster = new Cluster($n_nodes);
	$cluster->init();
	$cluster->start();
	$cluster->create_mm('regression');

	# prevent PostgresNode.pm from shutting down nodes on exit in END {}
	@PostgresNode::all_nodes = ();
}
elsif ($action eq "--stop")
{
	for my $i (1..$n_nodes)
	{
		TestLib::system_log('pg_ctl',
			'-D', "$TestLib::tmp_check/t_run_node${i}_data/pgdata",
			'-m', 'fast',
			'stop');
	}
}
else
{
	die("Usage: run.pl --start/--stop\n");
}
