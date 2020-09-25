import unittest
import time
import datetime
import psycopg2
import random
import os
import socket
import subprocess
import logging
import warnings

from .failure_injector import *
from .bank_client import keep_trying, MtmClient
from . import log_helper  # configures loggers

log = logging.getLogger('root.test_helper')

TEST_WARMING_TIME = 3
TEST_DURATION = 10
TEST_MAX_RECOVERY_TIME = 1200
TEST_RECOVERY_TIME = 10
TEST_SETUP_TIME = 20
TEST_STOP_DELAY = 5

# Node host for dind (Docker-in-Docker execution)
# gitlab ensures dind container (in which pg containers ports are exposed) is
# available as 'docker', see
# https://docs.gitlab.com/ee/ci/docker/using_docker_images.html#accessing-the-services
NODE_HOST = 'docker' if 'DOCKER_HOST' in os.environ else '127.0.0.1'

class MMTestCase(unittest.TestCase):
    @classmethod
    def collectLogs(cls, referee=False):
        log.info('collecting logs')
        # non-standard &> doesn't work in e.g. default Debian dash, so
        # use old school > 2>&1
        subprocess.run('docker logs node1 >logs1 2>&1', shell=True)
        subprocess.run('docker logs node2 >logs2 2>&1', shell=True)
        if not referee:
            subprocess.run('docker logs node3 >logs3 2>&1', shell=True)
        else:
            subprocess.run('docker logs referee >logs_referee 2>&1', shell=True)

    # get 3 nodes up
    @classmethod
    def setUpClass(cls):
        # resolve hostname once during start as aoipg or docker have problems
        # with resolving hostname under a load
        host_ip = socket.gethostbyname(NODE_HOST)

        cls.dsns = [
            f"dbname=regression user=postgres host={host_ip} port=15432",
            f"dbname=regression user=postgres host={host_ip} port=15433",
            f"dbname=regression user=postgres host={host_ip} port=15434"
        ]
        cls.test_ok = True

        subprocess.check_call(['docker-compose', 'up', '--force-recreate',
                               '--build', '-d'])

        # # Some Docker container debug stuff
        # subprocess.check_call(['docker-compose', 'ps'])
        # subprocess.check_call(['sudo', 'iptables', '-S'])
        # subprocess.check_call(['sudo', 'iptables', '-L'])
        # subprocess.check_call(['docker-compose', 'logs'])

        # Wait for all nodes to become online
        try:
            [cls.awaitOnline(dsn) for dsn in cls.dsns]

            cls.client = MtmClient(cls.dsns, n_accounts=1000)
            cls.client.bgrun()
        except Exception as e:
            # collect logs even if fail in setupClass
            cls.collectLogs()
            raise e

    @classmethod
    def tearDownClass(cls):
        log.info('tearDownClass')

        # ohoh
        th = TestHelper()
        th.client = cls.client

        # collect logs for CI anyway
        try:
            # skip the check if test already failed
            if cls.test_ok:
                th.assertDataSync()
        finally:
            cls.client.stop()
            # Destroying containers is really unhelpful for local debugging, so
            # do this automatically only in CI.
            cls.collectLogs()
            if 'CI' in os.environ:
                subprocess.check_call(['docker-compose', 'down'])

    def setUp(self):
        # xxx why do we need this
        warnings.simplefilter("ignore", ResourceWarning)
        time.sleep(20)
        log.info('start new test')

    # For use in tearDown; says whether the test has failed. Unfortunately
    # unittest doesn't expose this. Works only for 3.4+. Taken from
    # https://stackoverflow.com/a/39606065/4014587
    def lastTestOk(self):
        result = self.defaultTestResult()
        self._feedErrorsToResult(result, self._outcome.errors)
        error = self.list2reason(result.errors)
        failure = self.list2reason(result.failures)
        return not error and not failure

    def list2reason(self, exc_list):
        if exc_list and exc_list[-1][0] is self:
            return exc_list[-1][1]

    def tearDown(self):
        # communicate to tearDownClass the result
        self.__class__.test_ok = self.lastTestOk()
        log.info('finish test')


class TestHelper(object):
    def assertIsolation(self, aggs):
        isolated = True
        for conn_id, agg in enumerate(aggs):
            isolated = isolated and agg['sumtotal']['isolation'] == 0
        if not isolated:
            raise AssertionError('Isolation failure')

    def assertCommits(self, aggs):
        commits = True
        for conn_id, agg in enumerate(aggs):
            commits = commits and 'commit' in agg['transfer']['finish']
        if not commits:
            log.error('No commits during aggregation interval')
            # time.sleep(100000)
            raise AssertionError('No commits during aggregation interval')

    def assertNoCommits(self, aggs):
        commits = True
        for conn_id, agg in enumerate(aggs):
            commits = commits and 'commit' in agg['transfer']['finish']
        if commits:
            raise AssertionError('There are commits during aggregation interval')

    def awaitCommit(self, node_id):
        total_sleep = 0

        while total_sleep <= TEST_MAX_RECOVERY_TIME:
            log.info(
                'waiting for commit on node {}, slept for {}, '
                'aggregates:'.format(node_id + 1, total_sleep))
            aggs = self.client.get_aggregates(clean=False, _print=True)
            log.info('=== transfer finishes: %s' %
                             aggs[node_id]['transfer']['finish'])
            if ('commit' in aggs[node_id]['transfer']['finish'] and
                    aggs[node_id]['transfer']['finish']['commit'] > 10):
                return
            # failing here while we are waiting for commit is of course ok,
            # e.g. the database might be starting up
            try:
                self.client.list_prepared(node_id)
            except psycopg2.Error as e:
                pass
            time.sleep(5)
            total_sleep += 5

        raise AssertionError('awaitCommit on node {} exceeded timeout {}'.format(node_id, TEST_MAX_RECOVERY_TIME))

    # if write is true, make writing xact
    @staticmethod
    def awaitOnline(dsn, write=False):
        total_sleep = 0
        one = 0
        con = None

        while total_sleep <= TEST_MAX_RECOVERY_TIME:
            try:
                con = psycopg2.connect(dsn + " connect_timeout=1")
                cur = con.cursor()
                if write:
                    cur.execute("create table if not exists bulka ();")
                else:
                    cur.execute("select 1")
                    one = int(cur.fetchone()[0])
                cur.close()
                log.info("{} is online!".format(dsn))
                return
            except Exception as e:
                log.info('waiting for {} to get online: {}'.format(dsn,
                                                                      str(e)))
                time.sleep(5)
                total_sleep += 5
            finally:
                if con is not None:
                    con.close()

        # Max recovery time was exceeded
        raise AssertionError(
            'awaitOnline on {} exceeded timeout {}s'.format(
                dsn, TEST_MAX_RECOVERY_TIME))

    def AssertNoPrepares(self):
        n_prepared = self.client.n_prepared_tx()
        if n_prepared != 0:
            print(self.client.oops)
            raise AssertionError('There are some unfinished tx')

    def assertDataSync(self):
        self.client.stop()

        try:
            # wait until prepares will be resolved

            # TODO: port graceful client termination from current stable
            # branches.  In that case I'd expect tests to pass without waiting
            # as there are at least several seconds between 'all nodes are
            # online' and this check (and with current hard client stop 1-2
            # hanged prepares are rarely but repeatedly seen). However generally
            # there is no strict guarantee 'nodes are online and no clients =>
            # there are no unresolved xacts' in current mtm, end of recovery
            # doesn't mean node doesn't have any prepares. Probably we could
            # add such guarantee via counter of orphaned xacts?
            keep_trying(40, 1, self.AssertNoPrepares, 'AssertNoPrepares')

            if not self.client.is_data_identic():
                raise AssertionError('Different data on nodes')

            # no new PREPARE should have appeared, the client is stopped
            # XXX actually they could: something like
            # - no prepare on 1, so going ahead to check 2
            # - prepare on 2
            # - waited until prepare on 2 is resolved
            # - ... but now it is sent to 1
            # probably we should just have here keep_trying is_data_identic
            #
            # UPD: well, that was issue of temp schema deletion which happened
            # on session exit and went though full fledged 3pc commit. This is
            # fixed now; but theoretically the problem still exists.
            self.AssertNoPrepares()
        except AssertionError:
            # further tests assume the client continues running
            self.client.bgrun()
            raise

        self.client.bgrun()

    def performRandomFailure(self, node, wait=0, nodes_wait_for_commit=[], node_wait_for_online=None, stop_load=False, nodes_assert_commit_during_failure=[]):
        FailureClass = random.choice(ONE_NODE_FAILURES)
        failure = FailureClass(node)

        log.info('simulating failure {} on node "{}"'.format(FailureClass.__name__, node))
        return self.performFailure(failure, wait, nodes_wait_for_commit, node_wait_for_online, stop_load, nodes_assert_commit_during_failure)

    def performFailure(self, failure, wait=0, nodes_wait_for_commit=[], node_wait_for_online=None, stop_load=False, nodes_assert_commit_during_failure=[]):

        time.sleep(TEST_WARMING_TIME)

        log.info('simulate failure')

        failure.start()

        self.client.clean_aggregates()
        log.info('started failure')

        time.sleep(TEST_DURATION)

        log.info('getting aggs during failure')
        aggs_failure = self.client.get_aggregates()
        # helps to bail out earlier, making the investigation easier
        for n in nodes_assert_commit_during_failure:
            self.assertCommits([aggs_failure[n]])

        time.sleep(wait)
        failure.stop()

        log.info('failure eliminated')

        self.client.clean_aggregates()

        if stop_load:
            time.sleep(3)
            log.info('aggs before client stop:')
            self.client.get_aggregates(clean=False)
            self.client.stop()

        if node_wait_for_online is not None:
            self.awaitOnline(node_wait_for_online)
        else:
            time.sleep(TEST_RECOVERY_TIME)

        if stop_load:
            self.client.bgrun()
            time.sleep(3)

        for node_wait_for_commit in nodes_wait_for_commit:
            self.awaitCommit(node_wait_for_commit)
        if len(nodes_wait_for_commit) == 0:
            time.sleep(TEST_RECOVERY_TIME)

        time.sleep(TEST_RECOVERY_TIME)
        log.info('aggs after failure:')
        aggs = self.client.get_aggregates()

        return aggs_failure, aggs

    @staticmethod
    def nodeExecute(dsn, statements):
        con = psycopg2.connect(dsn)
        try:
            con.autocommit = True
            cur = con.cursor()
            for statement in statements:
                cur.execute(statement)
            cur.close()
        finally:
            con.close()

    @staticmethod
    def nodeSelect(dsn, statement):
        con = psycopg2.connect(dsn + " connect_timeout=20")
        try:
            cur = con.cursor()
            cur.execute(statement)
            res = cur.fetchall()
            cur.close()
        finally:
            con.close()
        return res
