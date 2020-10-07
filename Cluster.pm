package Cluster;

use strict;
use warnings;

use PostgresNode;
use TestLib;
use Test::More;
use Cwd;
use IPC::Run;
use Socket;

# 127.0.0.1 is not ok because we pick up random ephemeral port which might be
# occupied by client connection while node is down -- and then it would refuse
# to start later. At least on my linux the kernel binds clients to 127.0.0.1,
# so this works. Alternatively we could
#   1) bind to the socket manually immediately after shutdown and release before
#     the start, but this obviously only (greatly) reduces the danger;
#   2) forget we are a distributed thing and use unix sockets like vanilla tests
#
# UPD: well, bf member msvs-6-3 decided to take 127.0.0.2 for local connections,
# so 1) was also implemented for such cases.
# I don't like falling back to 2) because a) it is slightly easier to connect to
# ports b) Windows uses TCP anyway.
our $mm_listen_address = '127.0.0.2';

sub new
{
	my ($class, $n_nodes, $referee) = @_;

	# ask PostgresNode to use tcp and listen on mm_listen_address
	$PostgresNode::use_tcp = 1;
	$PostgresNode::test_pghost = $mm_listen_address;

	my @nodes = map { get_new_node("node$_") } (1..$n_nodes);
	$_->hold_port() foreach @nodes;

	my $self = {
		nodes => \@nodes,
		recv_timeout => 6
	};
	if (defined $referee && $referee)
	{
		$self->{referee} = get_new_node("referee");
		$self->{referee}->hold_port();
	}

	bless $self, $class;
	return $self;
}

sub init
{
	my ($self, $conf_opts) = @_;
	my $nodes = $self->{nodes};

	# use port range different to ordinary TAP tests
	$PostgresNode::last_port_assigned = int(rand() * 16384) + 32767;

	if (defined $self->{referee})
	{
		$self->{referee}->init();
	}

	my $hba = qq{
        host all all ${mm_listen_address}/32 trust
        host replication all ${mm_listen_address}/32 trust
	};

	# binary protocol doesn't tolerate strict alignment currently, so use it
	# everywhere but on arm (which is the only live arch which might be strict
	# here)
	my $binary_basetypes = 0;
	if (!$TestLib::windows_os)
	{
		my $uname_arch = `uname -m`;
		if ($? != 0) {
			BAIL_OUT("system uname -m failed");
		}
		$binary_basetypes = ($uname_arch =~ m/arm/) ? 0 : 1;
	}

	foreach my $node (@$nodes)
	{
		$node->init(allows_streaming => 'logical');
		$node->append_conf('postgresql.conf', qq{
			max_connections = 50
			log_line_prefix = '%m [%p] %i '
			log_statement = all

			shared_preload_libraries = 'multimaster, pg_pathman'

			max_prepared_transactions = 250
			max_worker_processes = 320
			max_wal_senders = 6
			max_replication_slots = 12

			# walsender-walreceiver keepalives
			wal_sender_timeout = 60s
			wal_receiver_status_interval = 10s

			# how often receivers restart
			wal_retrieve_retry_interval = 2s

			# uncomment to disable checkpoints
			# checkpoint_timeout = 1d
			# max_wal_size = 1TB

			multimaster.heartbeat_send_timeout = 100
			multimaster.heartbeat_recv_timeout = 5000

			multimaster.syncpoint_interval = 10MB

			# For add_stop node test we need at least 4 * 3 * (2 * trans_spill_threshold)
			# MB of shmem, and some bf members have only 2GB /dev/shm, so be
			# careful upping this.
			multimaster.trans_spill_threshold = 50MB

			multimaster.binary_basetypes = ${binary_basetypes}
		});
		$node->append_conf('pg_hba.conf', $hba);

		if (defined $self->{referee})
		{
			my $rport = $self->{referee}->port;
			my $rhost = $self->{referee}->host;
			# referee dbname is hardcoded as postgres for simplicity
			$node->append_conf('postgresql.conf', qq(
				multimaster.referee_connstring = 'dbname=postgres host=$rhost port=$rport'
));
			$node->append_conf('pg_hba.conf', $hba);
		}

		if (defined $conf_opts)
		{
			$node->append_conf('postgresql.conf', $conf_opts);
		}

		if (defined $ENV{'MTM_VW'})
		{
			$node->append_conf('postgresql.conf', q{
				multimaster.volkswagen_mode = on
			});
		}
	}
}

sub create_mm
{
	my ($self, $dbname) = @_;
	my $nodes = $self->{nodes};

	$self->await_nodes([0..$#{$self->{nodes}}], 0);

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

	if (defined $self->{referee})
	{
		$self->{referee}->safe_psql('postgres', 'create extension referee');
	}

	foreach (0..$#{$self->{nodes}})
	{
		my $nnodes = $_;

		# Simulate OID skew between multimaster nodes
		foreach (0..$nnodes)
		{
			# On pg_database oids
			$self->safe_psql($_, "CREATE DATABASE dummydatabase;");
			$self->safe_psql($_, "DROP DATABASE dummydatabase;");

			# On pg_class oids
			$self->safe_psql($_, "CREATE TABLE dummytable (r INT);");
			$self->safe_psql($_, "DROP TABLE dummytable;");

			# On pg_type oids
			$self->safe_psql($_, "CREATE TYPE dummytype AS (r INT);");
			$self->safe_psql($_, "DROP TYPE dummytype;");

			# On pg_authid oids
			$self->safe_psql($_, "CREATE ROLE dummyrole;");
			$self->safe_psql($_, "DROP ROLE dummyrole;");

			# On pg_am oids
			$self->safe_psql($_, "CREATE ACCESS METHOD dummygist TYPE INDEX HANDLER gisthandler;");
			$self->safe_psql($_, "DROP ACCESS METHOD dummygist;");

			# On pg_namespace oids
			$self->safe_psql($_, "CREATE SCHEMA dummynsp;");
			$self->safe_psql($_, "DROP SCHEMA dummynsp;");

			# XXX: pg_tablespace
		}
	}

	(my $my_connstr, my @peers) = map {
		$_->connstr($_->{dbname})
	} @{$self->{nodes}};

	my $node1 = $self->{nodes}->[0];
	my $peer_connstrs = join ',', @peers;
	$node1->safe_psql($node1->{dbname}, "create extension multimaster;");
	$node1->safe_psql($node1->{dbname}, qq(
		select mtm.init_cluster(\$\$$my_connstr\$\$, \$\${$peer_connstrs}\$\$);
	));

	$self->await_nodes([0..$#{$self->{nodes}}]);
}

sub start
{
	my ($self) = @_;
	my $ports = join ', ', map { $_->{_port}} @{$self->{nodes}};
	note( "starting cluster on ports: $ports");
	$_->start() foreach @{$self->{nodes}};
	if (defined $self->{referee})
	{
		my $rport = $self->{referee}->port;
		note("starting referee on port $rport");
		$self->{referee}->start();
	}

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

sub poll_query_until
{
	my ($self, $node_off, $query) = @_;
	my $node = $self->{nodes}->[$node_off];
	note("polling query $query");
	return $node->poll_query_until($node->{dbname}, $query);
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

	my $new_node = get_new_node("node@{[$#{$self->{nodes}} + 2]}");
	$new_node->hold_port();
	push(@{$self->{nodes}}, $new_node);

	return $#{$self->{nodes}};
}

sub command_output
{
	my ($cmd) = @_;
	my ($stdout, $stderr);
	print("# Running: " . join(" ", @{$cmd}) . "\n");
	my $result = IPC::Run::run $cmd, '>', \$stdout, '2>', \$stderr;
	ok($result, "@$cmd: exit code 0");
	# bb writes to stderr
	return $stderr;
}

sub backup_and_init()
{
	my ($self, $from, $to, $to_mmid) = @_;

	my $node = $self->{nodes}->[$from];
	my $backup_name = "backup_for_$to";
	my $backup_path = $node->backup_dir . '/' . $backup_name;
	my $port        = $node->port;
	my $name        = $node->name;

	print "# Taking pg_basebackup $backup_name from node \"$name\"\n";
	my $dumpres = command_output(['pg_basebackup', '-D', $backup_path, '-p', $port,
		'-h', $mm_listen_address, '--no-sync', '-v', '-S', "mtm_filter_slot_$to_mmid"]);

	print "# Backup finished\n";

	note($dumpres);

	(my $end_lsn) = $dumpres =~ /end point: (.+)/m;
	note($end_lsn);

	$self->{nodes}->[$to]->init_from_backup($node, $backup_name);

	return $end_lsn;
}

sub await_nodes()
{
	my ($self, $nodenums, $mm_ping) = @_;
	# use mtm.ping to ensure generational perturbations are over, unless
	# explicitly asked not to (e.g. in case mm is not created yet)
	$mm_ping //= 1;
	my $query = $mm_ping ? "select mtm.ping();" : "select 't';";

	print("await_nodes " . join(", ", @{$nodenums}) . "\n");
	foreach my $i (@$nodenums)
	{
		my $dbname = $self->{nodes}->[$i]->{dbname};
		if (!$self->{nodes}->[$i]->poll_query_until($dbname, $query))
		{
			die "Timed out waiting for mm node$i to become online";
		}
		else
		{
			print("Polled node$i\n");
		}
	}
}

# use after simulated failure of some node; waits for others to converge
sub await_nodes_after_stop()
{
	my ($self, $nodenums) = @_;
	# wait till others notice the node is down...
	# (more strictly speaking we should wait > heartbeat_recv_timeout, but we
	# don't emulate network issues, so the failure is noticed instantly)
	sleep(2);
	# and then ensure generation switch is over
	$self->await_nodes($nodenums);
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

# Override PostgresNode::start|stop to reserve the port while node is down
{
	# otherwise perl whines, thinking our override is redefine
	no warnings 'redefine';
	# that's how perl people call super from monkey-patch-overridden method
	# https://stackoverflow.com/a/575873/4014587
	my $postgres_node_start_super = \&PostgresNode::start;
	*PostgresNode::start = sub {
		my $self = shift;
		$self->release_port();
		$postgres_node_start_super->( $self, @_ );
	};

	my $postgres_node_stop_super = \&PostgresNode::stop;
	*PostgresNode::stop = sub {
		my $self = shift;
		$postgres_node_stop_super->( $self, @_ );
		$self->hold_port();
	};
}

# Bind to socket with our port so no one could use it while node is down.
sub PostgresNode::hold_port {
	my $self = shift;

	# do nothing if this is not mm node, it most probably uses unix socket
	return unless $self->host eq $mm_listen_address;

	# stop() might be called several times without start(), don't bind if
	# already did so
	return if defined $self->{held_socket};

	my $iaddr = inet_aton($self->host);
	my $paddr = sockaddr_in($self->port, $iaddr);
	my $proto = getprotobyname("tcp");

	socket(my $sock, PF_INET, SOCK_STREAM, $proto)
		or die "socket failed: $!";

	# As in postmaster, don't use SO_REUSEADDR on Windows
	setsockopt($sock, SOL_SOCKET, SO_REUSEADDR, pack("l", 1))
	  unless $TestLib::windows_os;
	bind($sock, $paddr) or die
	  "socket bind failed: $!";
	listen($sock, SOMAXCONN)
	  or die "socket listen failed: $!";

	$self->{held_socket} = $sock;
	note("bind to port @{[$self->port]}");
}

sub PostgresNode::release_port {
	my $self = shift;
	if (defined $self->{held_socket})
	{
		close($self->{held_socket});
		$self->{held_socket} = undef;
	}
	note("port released");
}

1;
