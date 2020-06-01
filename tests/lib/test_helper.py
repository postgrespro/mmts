import unittest
import time
import datetime
import psycopg2
import random
import os

from .failure_injector import *
from .bank_client import keep_trying

TEST_WARMING_TIME = 3
TEST_DURATION = 10
TEST_MAX_RECOVERY_TIME = 1200
TEST_RECOVERY_TIME = 10
TEST_SETUP_TIME = 20
TEST_STOP_DELAY = 5

# Node host for dind (Docker-in-Docker execution)
NODE_HOST = 'docker' if 'DOCKER_HOST' in os.environ else '127.0.0.1'

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
            print('No commits during aggregation interval')
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
            print('{} waiting for commit on node {}, slept for {}, aggregates:'.format(datetime.datetime.utcnow(), node_id + 1, total_sleep))
            aggs = self.client.get_aggregates(clean=False, _print=True)
            print('=== transfer finishes: ', aggs[node_id]['transfer']['finish'])
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

    @staticmethod
    def awaitOnline(dsn):
        total_sleep = 0
        one = 0
        con = None

        while total_sleep <= TEST_MAX_RECOVERY_TIME:
            try:
                con = psycopg2.connect(dsn + " connect_timeout=1")
                cur = con.cursor()
                cur.execute("select 1")
                one = int(cur.fetchone()[0])
                cur.close()
                print("{} {} is online!".format(datetime.datetime.utcnow(), dsn))
                return
            except Exception as e:
                print('{} waiting for {} to get online: {}'.format(datetime.datetime.utcnow(), dsn, str(e)))
                time.sleep(5)
                total_sleep += 5
            finally:
                if con is not None:
                    con.close()

        # Max recovery time was exceeded
        raise AssertionError('awaitOnline on {} exceeded timeout {}s'.format(dsn, TEST_MAX_RECOVERY_TIME))

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

    def performRandomFailure(self, node, wait=0, node_wait_for_commit=-1, node_wait_for_online=None, stop_load=False, nodes_assert_commit_during_failure=[]):
        FailureClass = random.choice(ONE_NODE_FAILURES)
        failure = FailureClass(node)

        print('Simulating failure {} on node "{}"'.format(FailureClass.__name__, node))
        return self.performFailure(failure, wait, node_wait_for_commit, node_wait_for_online, stop_load, nodes_assert_commit_during_failure)

    def performFailure(self, failure, wait=0, node_wait_for_commit=-1, node_wait_for_online=None, stop_load=False, nodes_assert_commit_during_failure=[]):

        time.sleep(TEST_WARMING_TIME)

        print('Simulate failure at ',datetime.datetime.utcnow())

        failure.start()

        self.client.clean_aggregates()
        print('Started failure at ',datetime.datetime.utcnow())

        time.sleep(TEST_DURATION)

        print('Getting aggs during failure at ',datetime.datetime.utcnow())
        aggs_failure = self.client.get_aggregates()
        # helps to bail out earlier, making the investigation easier
        for n in nodes_assert_commit_during_failure:
            self.assertCommits([aggs_failure[n]])

        time.sleep(wait)
        failure.stop()

        print('failure eliminated at', datetime.datetime.utcnow())

        self.client.clean_aggregates()

        if stop_load:
            time.sleep(3)
            print('{} aggs before client stop:'.format(datetime.datetime.utcnow()))
            self.client.get_aggregates(clean=False)
            self.client.stop()

        if node_wait_for_online != None:
            self.awaitOnline(node_wait_for_online)
        else:
            time.sleep(TEST_RECOVERY_TIME)

        if stop_load:
            self.client.bgrun()
            time.sleep(3)

        if node_wait_for_commit >= 0:
            self.awaitCommit(node_wait_for_commit)
        else:
            time.sleep(TEST_RECOVERY_TIME)

        time.sleep(TEST_RECOVERY_TIME)
        print('{} aggs after failure:'.format(datetime.datetime.utcnow()))
        aggs = self.client.get_aggregates()

        return (aggs_failure, aggs)

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
        con = psycopg2.connect(dsn + " connect_timeout=1")
        try:
            cur = con.cursor()
            cur.execute(statement)
            res = cur.fetchall()
            cur.close()
        finally:
            con.close()
        return res
