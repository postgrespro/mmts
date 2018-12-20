package Cluster;

use strict;
use warnings;

use PostgresNode;
use TestLib;
use Test::More;
use Cwd;

use Socket;

use IPC::Run;

sub check_port
{
	my ($host, $port) = @_;
	my $iaddr = inet_aton($host);
	my $paddr = sockaddr_in($port, $iaddr);
	my $proto = getprotobyname("tcp");
	my $available = 0;

	socket(SOCK, PF_INET, SOCK_STREAM, $proto)
		or die "socket failed: $!";

	if (bind(SOCK, $paddr) && listen(SOCK, SOMAXCONN))
	{
		$available = 1;
	}

	close(SOCK);
	return $available;
}

my %allocated_ports = ();
sub allocate_ports
{
	my @allocated_now = ();
	my ($host, $ports_to_alloc) = @_;

	while ($ports_to_alloc > 0)
	{
		my $port = int(rand() * 16384) + 49152;
		next if $allocated_ports{$port};
		if (check_port($host, $port))
		{
			$allocated_ports{$port} = 1;
			push(@allocated_now, $port);
			$ports_to_alloc--;
		}
	}

	return @allocated_now;
}

sub new
{
	my ($class, $nodenum) = @_;

	my $nodes = [];

	foreach my $i (1..$nodenum)
	{
		my $host = "127.0.0.1";
		my ($pgport, $arbiter_port) = allocate_ports($host, 2);

		if(defined $ENV{MMPORT}) {
			$pgport = $ENV{MMPORT};
			delete $ENV{MMPORT};
		}

		my $node = new PostgresNode("node$i", $host, $pgport);
		$node->{id} = $i;
		$node->{dbname} = 'postgres';
		$node->{arbiter_port} = $arbiter_port;
		$node->{mmconnstr} = "${ \$node->connstr($node->{dbname}) } arbiter_port=${ \$node->{arbiter_port} }";
		push(@$nodes, $node);
	}

	my $self = {
		nodenum => $nodenum,
		nodes => $nodes,
		recv_timeout => 9,
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
		$node->init(hba_permit_replication => 0);
	}
}

sub all_connstrs
{
	my ($self) = @_;
	my $nodes = $self->{nodes};
	return join(', ', map { "${ \$_->connstr($_->{dbname}) } arbiter_port=${ \$_->{arbiter_port} }" } @$nodes);
}


sub configure
{
	my ($self, $dbname) = @_;
	my $nodes = $self->{nodes};

	if (defined $dbname)
	{
		foreach my $node (@$nodes)
		{
			$node->{dbname} = $dbname;
			$node->append_conf("postgresql.conf", qq(
				listen_addresses = '127.0.0.1'
				unix_socket_directories = ''
			));
			$node->start();
			$node->safe_psql('postgres', "CREATE DATABASE $dbname");
			$node->stop();
		}
	}

	my $connstr = $self->all_connstrs();
	$connstr =~ s/'//gms;

	foreach my $node (@$nodes)
	{
		my $id = $node->{id};
		my $host = $node->host;
		my $pgport = $node->port;
		my $arbiter_port = $node->{arbiter_port};
		my $unix_sock_dir = $ENV{PGHOST};

		$node->append_conf("postgresql.conf", qq(
			# log_statement = all
			listen_addresses = '$host'
			unix_socket_directories = '$unix_sock_dir'
			port = $pgport
			max_prepared_transactions = 150
			max_connections = 10
			max_worker_processes = 100
			wal_level = logical
			max_wal_senders = 6
			wal_sender_timeout = 0
			max_replication_slots = 6
			shared_preload_libraries = 'multimaster'
			shared_buffers = 16MB

			multimaster.node_id = $id
			multimaster.conn_strings = '$connstr'
			multimaster.heartbeat_recv_timeout = 8050
			multimaster.heartbeat_send_timeout = 250
			multimaster.max_nodes = 6
			# XXX try without ignore_tables_without_pk
			multimaster.ignore_tables_without_pk = true
			# multimaster.volkswagen_mode = 1
			log_line_prefix = '%t [%p]: '
		));

		$node->append_conf("pg_hba.conf", qq(
			local replication all trust
			host replication all 127.0.0.1/32 trust
			host replication all ::1/128 trust
		));
	}
}

sub start
{
	my ($self) = @_;
	my $nodes = $self->{nodes};

	foreach my $node (@$nodes)
	{
		$node->start();
		$node->safe_psql($node->{dbname}, "create extension multimaster;");
		note( "Starting node with connstr 'port=@{[ $node->port() ]} host=@{[ $node->host() ]}'");
	}
}

sub stopnode
{
	my ($node, $mode) = @_;
	return 1 unless defined $node->{_pid};
	$mode = 'fast' unless defined $mode;
	my $name   = $node->name;
	note("stopping $name ${mode}ly");

	if ($mode eq 'kill') {
		killtree($node->{_pid});
		return 1;
	}

	my $pgdata = $node->data_dir;
	my $ret = TestLib::system_log('pg_ctl', '-D', $pgdata, '-m', 'fast', 'stop');
	my $pidfile = $node->data_dir . "/postmaster.pid";
	note("unlink $pidfile");
	unlink $pidfile;
	$node->{_pid} = undef;
	$node->_update_pid;

	if ($ret != 0) {
		note("$name failed to stop ${mode}ly");
		return 0;
	}

	return 1;
}

sub stopid
{
	my ($self, $idx, $mode) = @_;
	return stopnode($self->{nodes}->[$idx]);
}

sub dumplogs
{
	my ($self) = @_;
	my $nodes = $self->{nodes};

	note("Dumping logs:");
	foreach my $node (@$nodes) {
		note("##################################################################");
		note($node->{_logfile});
		note("##################################################################");
		my $filename = $node->{_logfile};
		open my $fh, '<', $filename or die "error opening $filename: $!";
		my $data = do { local $/; <$fh> };
		note($data);
		note("##################################################################\n\n");
	}
}

sub stop
{
	my ($self, $mode) = @_;
	my $nodes = $self->{nodes};
	$mode = 'fast' unless defined $mode;

	my $ok = 1;
	note("stopping cluster ${mode}ly");
	
	foreach my $node (@$nodes) {
		$node->stop($mode);
	}

	# $self->dumplogs();

	return $ok;
}

sub bail_out_with_logs
{
	my ($self, $msg) = @_;
	$self->dumplogs();
	BAIL_OUT($msg);
}

sub teardown
{
	my ($self) = @_;
	my $nodes = $self->{nodes};

	foreach my $node (@$nodes)
	{
		$node->teardown();
	}
}

sub psql
{
	my ($self, $index, @args) = @_;
	my $node = $self->{nodes}->[$index];
	return $node->psql(@args);
}

sub poll
{
	my ($self, $poller, $dbname, $pollee, $tries, $delay) = @_;
	my $node = $self->{nodes}->[$poller];
	for (my $i = 0; $i < $tries; $i++) {
		my $psql_out;
		my $pollee_plus_1 = $pollee + 1;
		$self->psql($poller, $dbname, "select mtm.poll_node($pollee_plus_1, true);", stdout => \$psql_out);
		if ($psql_out eq "t") {
			return 1;
		}
		my $tries_left = $tries - $i - 1;
		note("$poller poll for $pollee failed [$tries_left tries left]");
		sleep($delay);
	}
	return 0;
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
		@args,
		-h => $self->{nodes}->[$node]->host(),
		-p => $self->{nodes}->[$node]->port(),
		$self->{nodes}->[$node]->{dbname},
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

sub add_node()
{
	my ($self, %params) = @_;

	my $pgport;
	my $arbiter_port;
	my $connstrs;
	my $node_id;

	if (defined $params{node_id})
	{
		$node_id = $params{node_id};
		$pgport = $params{port};
		$arbiter_port = $params{arbiter_port};
		$connstrs = $self->all_connstrs();
	}
	else
	{
		$node_id = scalar(@{$self->{nodes}}) + 1;
		$pgport = (allocate_ports('127.0.0.1', 1))[0];
		$arbiter_port = (allocate_ports('127.0.0.1', 1))[0];
		$connstrs = $self->all_connstrs() . ", dbname=${ \$self->{nodes}->[0]->{dbname} } host=127.0.0.1 port=$pgport arbiter_port=$arbiter_port";
	}

	$connstrs =~ s/'//gms;

	my $node = PostgresNode->get_new_node("node${node_id}x");

	$self->{nodes}->[0]->backup("backup_for_$node_id");
	# do init from backup before setting host, since init_from_backup() checks
	# it default value
	$node->init_from_backup($self->{nodes}->[0], "backup_for_$node_id");

	$node->{_host} = '127.0.0.1';
	$node->{_port} = $pgport;
	$node->{port} = $pgport;
	$node->{host} = '127.0.0.1';
	$node->{arbiter_port} = $arbiter_port;
	$node->{mmconnstr} = "${ \$node->connstr($node->{dbname}) } arbiter_port=${ \$node->{arbiter_port} }";
	$node->append_conf("postgresql.conf", qq(
		multimaster.arbiter_port = $arbiter_port
		multimaster.conn_strings = '$connstrs'
		multimaster.node_id = $node_id
		port = $pgport
	));
	$node->append_conf("pg_hba.conf", qq(
		local replication all trust
		host replication all 127.0.0.1/32 trust
		host replication all ::1/128 trust
	));

	push(@{$self->{nodes}}, $node);
}

sub await_nodes()
{
	my ($self, @nodenums) = @_;

	foreach my $i (@nodenums)
	{
		if (!$self->{nodes}->[$i]->poll_query_until($self->{nodes}->[$i]->{dbname}, "select 't'"))
		{
			# sleep(3600);
			die "Timed out while waiting for mm node to became online";
		}
	}
}

1;
