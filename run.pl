#!/usr/bin/env perl

use File::Basename;
use Getopt::Long;
BEGIN { unshift @INC, '.'; unshift @INC, '../../src/test/perl' }
use Cluster;

my $n_nodes = 3;
my $referee = 0;
my $action = 'start';
GetOptions ("nnodes=i" => \$n_nodes,    # numeric
			"referee"   => \$referee,   # flag
			"action=s"  => \$action);	# strings
# referee works only with 2 nodes
if ($referee)
{
	$n_nodes = 2;
}

if ($action eq "start")
{
	$Cluster::last_port_assigned = 65431;

	my $cluster = new Cluster($n_nodes, $referee);
	$cluster->init();
	$cluster->start();
	$cluster->create_mm('regression');

	# prevent PostgresNode.pm from shutting down nodes on exit in END {}
	eval
	{
		if ($Cluster::pg_15_modules)
		{
			@PostgreSQL::Test::Cluster::all_nodes = ();
		}
		else
		{
			@PostgresNode::all_nodes = ();
		}
	};
}
elsif ($action eq "stop")
{
	eval
	{
		if ($Cluster::pg_15_modules)
		{
			my @datas = <$PostgreSQL::Test::Utils::tmp_check/*data>;
			foreach my $data (@datas) {
				PostgreSQL::Test::Utils::system_log('pg_ctl',
													'-D', "$data/pgdata",
													'-m', 'fast',
													'stop');
			}
			@PostgreSQL::Test::Cluster::all_nodes = ();
		}
		else
		{
			my @datas = <$TestLib::tmp_check/*data>;
			foreach my $data (@datas) {
				TestLib::system_log('pg_ctl',
									'-D', "$data/pgdata",
									'-m', 'fast',
									'stop');
			}
		}
	};
}
else
{
	die("Usage: run.pl action=<start|stop> [opts]\n");
}
