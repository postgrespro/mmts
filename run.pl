#!/usr/bin/env perl

use File::Basename;
BEGIN { unshift @INC, '.'; unshift @INC, '../../src/test/perl' }
use Cluster;

my $n_nodes = 3;
my $action = $ARGV[0];

if ($action eq "--start")
{
	my $cluster = new Cluster($n_nodes);
	$cluster->init();
	$cluster->configure();
	$cluster->start();
	$cluster->await_nodes( (0..$n_nodes-1) );
}
elsif ($action eq "--stop")
{
	for my $i (1..$n_nodes)
	{
		TestLib::system_or_bail('pg_ctl',
			'-D', "$TestLib::tmp_check/t_run_node${i}_data/pgdata",
			'-m', 'fast',
			'stop');
	}
}
else
{
	die("Usage: run.pl --start/--stop\n");
}



