package Cluster;
use strict;
use warnings;
use PostgresNode;
use TestLib;
use Test::More;
use Cwd;
use IPC::Run;

sub new
{
	my ($class, $n_nodes) = @_;
	my @nodes = map { get_new_node("node$_") } (1..$n_nodes);

	my $self = {
		nodes => \@nodes,
		recv_timeout => 3
	};

	bless $self, $class;
	return $self;
}

sub init
{
	my ($self) = @_;
	my $nodes = $self->{nodes};

	foreach my $node (@$nodes)
	{
		$node->init(allows_streaming => 'logical');
		$node->append_conf('postgresql.conf', q{
			unix_socket_directories = ''
			listen_addresses = '127.0.0.1'
			max_connections = 50

			shared_preload_libraries = 'multimaster'

			max_prepared_transactions = 250
			max_worker_processes = 170
			max_wal_senders = 6
			max_replication_slots = 12
			wal_sender_timeout = 0
		});
	}
}

sub create_mm
{
	my ($self, $dbname) = @_;
	my $nodes = $self->{nodes};

	$self->await_nodes( (0..$#{$self->{nodes}}) );

	foreach my $node (@$nodes)
	{
		if (defined $dbname)
		{
			$node->{dbname} = $dbname;
			$node->safe_psql('postgres', "CREATE DATABASE $dbname");
		}
		else
		{
			$node->{dbname} = 'postgres';
		}
	}

	(my $my_connstr, my @peers) = map {
		$_->connstr($_->{dbname})
	} @{$self->{nodes}};

	my $ports = join ', ', map { $_->{_port}} @{$self->{nodes}};
	note( "Starting cluster with ports: $ports");

	my $node1 = $self->{nodes}->[0];
	my $peer_connstrs = join ',', @peers;
	$node1->safe_psql($node1->{dbname}, "create extension multimaster;");
	$node1->safe_psql($node1->{dbname}, qq(
		select mtm.init_cluster(\$\$$my_connstr\$\$, \$\${$peer_connstrs}\$\$);
	));

	$self->await_nodes( (0..$#{$self->{nodes}}) );
}

sub start
{
	my ($self) = @_;
	$_->start() foreach @{$self->{nodes}};
}

sub stop
{
	my ($self) = @_;
	$_->stop('fast') foreach @{$self->{nodes}};
}

sub safe_psql
{
	my ($self, $node_off, $query) = @_;
	my $node = $self->{nodes}->[$node_off];
	return $node->safe_psql($node->{dbname}, $query);
}

sub connstr
{
	my ($self, $node_off) = @_;
	my $node = $self->{nodes}->[$node_off];
	return $node->connstr($node->{dbname});
}

sub add_node()
{
	my ($self) = @_;

	push(@{$self->{nodes}}, get_new_node("node@{[$#{$self->{nodes}} + 2]}"));

	# $self->{nodes}->[0]->backup("backup_for_$new_node_id");
	# $new_node->init_from_backup($self->{nodes}->[0], "backup_for_$new_node_id");

	return $#{$self->{nodes}};
}

sub backup()
{
	my ($self, $from, $to) = @_;

	$self->{nodes}->[$from]->backup("backup_for_$to");
	$self->{nodes}->[$to]->init_from_backup($self->{nodes}->[$from], "backup_for_$to");
}

sub await_nodes()
{
	my ($self, @nodenums) = @_;

	foreach my $i (@nodenums)
	{
		my $dbname = $self->{nodes}->[$i]->{dbname};
		if (!$self->{nodes}->[$i]->poll_query_until($dbname, "select 't'"))
		{
			die "Timed out while waiting for mm node$i to became online";
		}
		else
		{
			print("Pleed node$i");
		}
	}
}

sub pgbench()
{
	my ($self, $node, @args) = @_;
	my $pgbench_handle = $self->pgbench_async($node, @args);
	$self->pgbench_await($pgbench_handle);
}

sub pgbench_async()
{
	my ($self, $node, @args) = @_;

	my ($in, $out, $err, $rc);
	$in = '';
	$out = '';

	my @pgbench_command = (
		'pgbench',
		-h => $self->{nodes}->[$node]->host(),
		-p => $self->{nodes}->[$node]->port(),
		$self->{nodes}->[$node]->{dbname} || 'postgres',
		@args
	);
	note("running pgbench: " . join(" ", @pgbench_command));
	my $handle = IPC::Run::start(\@pgbench_command, $in, $out);
	return $handle;
}

sub pgbench_await()
{
	my ($self, $pgbench_handle) = @_;
	IPC::Run::finish($pgbench_handle) or diag("WARNING: pgbench exited with $?");
	note("finished pgbench");
}

sub is_data_identic()
{
	my ($self, @nodenums) = @_;
	my $checksum = '';

	my $sql = "select md5('(' || string_agg(aid::text || ', ' || abalance::text , '),(') || ')')
			from (select * from pgbench_accounts order by aid) t;";

	foreach my $i (@nodenums)
	{
		my $current_hash = '';
		$self->{nodes}->[$i]->psql($self->{nodes}->[$i]->{dbname}, $sql, stdout => \$current_hash);
		note("hash$i: $current_hash");
		if ($current_hash eq '')
		{
			note("got empty hash from node $i");
			return 0;
		}
		if ($checksum eq '')
		{
			$checksum = $current_hash;
		}
		elsif ($checksum ne $current_hash)
		{
			note("got different hashes: $checksum ang $current_hash");
			return 0;
		}
	}

	note($checksum);
	return 1;
}

1;
