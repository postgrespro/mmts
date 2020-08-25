use strict;
use warnings;

use PostgresNode;
use TestLib;
use Test::More tests => 2;

my $psql_out = '';
my $psql_rc  = '';

my $node = get_new_node("london");
$node->init(allows_streaming => 1);
$node->append_conf(
	'postgresql.conf', qq(
	max_prepared_transactions = 10
	log_checkpoints = true
));
$node->start;

# Create table we'll use in the test transactions
$node->psql('postgres', "CREATE TABLE t_precommit_tbl (id int, msg text)");

###############################################################################
# Check ordinary restart
###############################################################################

$node->psql(
	'postgres', "
	BEGIN;
	INSERT INTO t_precommit_tbl VALUES (1, 'x');
	PREPARE TRANSACTION 'xact_prepared';

	BEGIN;
	INSERT INTO t_precommit_tbl VALUES (2, 'x');
	PREPARE TRANSACTION 'xact_precommited';

	SELECT pg_precommit_prepared('xact_precommited', 'precommited');");
$node->stop;
$node->start;


$node->psql(
	'postgres',
	"SELECT count(*) FROM pg_prepared_xacts WHERE state3pc = 'precommited'",
	stdout => \$psql_out);
is($psql_out, '1',
	"Check that state3pc preserved during reboot");

###############################################################################
# Check crash/recover
###############################################################################

$node->psql(
	'postgres', "
	BEGIN;
	INSERT INTO t_precommit_tbl VALUES (3, 'x');
	PREPARE TRANSACTION 'xact_prepared_2';

	BEGIN;
	INSERT INTO t_precommit_tbl VALUES (4, 'x');
	PREPARE TRANSACTION 'xact_precommited_2';

	SELECT pg_precommit_prepared('xact_precommited_2', 'precommited');");
$node->stop("immediate");
$node->start;


$node->psql(
	'postgres',
	"SELECT count(*) FROM pg_prepared_xacts WHERE state3pc = 'precommited'",
	stdout => \$psql_out);
is($psql_out, '2',
	"Check that state3pc preserved during reboot");