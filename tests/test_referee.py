#
# Based on Aphyr's test for CockroachDB.
#

import unittest
import time
import subprocess
import datetime
import docker
import warnings
import logging

from lib.bank_client import MtmClient
from lib.failure_injector import *
import lib.log_helper  # configures loggers
from lib.test_helper import *

log = logging.getLogger('root')

class RefereeTest(MMTestCase, TestHelper):
    @classmethod
    def setUpClass(cls):
        host_ip = socket.gethostbyname(NODE_HOST)

        cls.dsns = [
            f"dbname=regression user=postgres host={host_ip} port=15432",
            f"dbname=regression user=postgres host={host_ip} port=15433",
        ]
        cls.referee_dsn = f"dbname=regression user=postgres host={host_ip} port=15435"
        cls.test_ok = True

        subprocess.check_call(['docker-compose',
            '-f', 'support/two_nodes.yml',
            'up',
            '--force-recreate',
            '--build',
            '-d'])

        # Wait for all nodes to become online
        try:
            [cls.awaitOnline(dsn) for dsn in cls.dsns]

            cls.client = MtmClient(cls.dsns, n_accounts=1000)
            cls.client.bgrun()
            # create extension on referee
            cls.nodeExecute(cls.referee_dsn,
                            ['create extension referee'])
        except Exception as e:
            # collect logs even if fail in setupClass
            cls.collectLogs(referee=True)
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
            cls.collectLogs(referee=True)
            if 'CI' in os.environ:
                subprocess.check_call(['docker-compose', 'down'])


    def test_neighbor_restart(self):
        log.info('### test_neighbor_restart ###')

        aggs_failure, aggs = self.performFailure(
            RestartNode('node2'), nodes_wait_for_online=[self.dsns[1]],
            stop_load=True)

        self.assertCommits(aggs_failure[:1])
        self.assertNoCommits(aggs_failure[1:])
        self.assertIsolation(aggs_failure)

        self.assertCommits(aggs)
        self.assertIsolation(aggs)

    def test_node_crash(self):
        log.info('### test_node_crash ###')

        aggs_failure, aggs = self.performFailure(
            CrashRecoverNode('node2'), nodes_wait_for_online=
            [self.dsns[1]],
            stop_load=True)

        self.assertCommits(aggs_failure[:1])
        self.assertNoCommits(aggs_failure[1:])
        self.assertIsolation(aggs_failure)

        self.assertCommits(aggs)
        self.assertIsolation(aggs)

    def test_partition_referee(self):
        log.info('### test_partition_referee ###')

        aggs_failure, aggs = self.performFailure(
            SingleNodePartition('node2'), nodes_wait_for_online=
            [self.dsns[1]],
            stop_load=True)

        self.assertCommits(aggs_failure[:1])
        self.assertNoCommits(aggs_failure[1:])
        self.assertIsolation(aggs_failure)

        self.assertCommits(aggs)
        self.assertIsolation(aggs)

    # cut one node from neighbour and referee, ensure neighbour works as
    # winner, repair network, wait until isolated node gets online, repeat with
    # another node.
    def test_double_failure_referee(self):
        log.info('### test_double_failure_referee ###')

        aggs_failure, aggs = self.performFailure(
            SingleNodePartition('node2'), nodes_wait_for_online=
            [self.dsns[1]], stop_load=True)

        self.assertCommits(aggs_failure[:1])
        self.assertNoCommits(aggs_failure[1:])
        self.assertIsolation(aggs_failure)

        self.assertCommits(aggs)
        self.assertIsolation(aggs)

        aggs_failure, aggs = self.performFailure(
            SingleNodePartition('node1'), nodes_wait_for_online=
            [self.dsns[0]], stop_load=True)

        self.assertNoCommits(aggs_failure[:1])
        self.assertCommits(aggs_failure[1:])
        self.assertIsolation(aggs_failure)

        self.assertCommits(aggs)
        self.assertIsolation(aggs)

    # - get node 1 down, ensure 2 works as winner
    # - restart 2, ensure it continue working as winner
    # - stop 2, start 1, ensure nothing working as winner is down
    # - start 2, ensure referee grant is cleared
    # it intersects with both test_winner_restart and
    # test_consequent_shutdown...
    def test_saved_referee_decision(self):
        log.info('### test_saved_referee_decision ###')
        docker_api = docker.from_env()

        log.info('#### down on(winner) || on')
        log.info('###########################')
        aggs_failure, aggs = self.performFailure(StopNode('node1'))

        self.assertNoCommits(aggs_failure[:1])
        self.assertCommits(aggs_failure[1:])
        self.assertIsolation(aggs_failure)

        self.assertNoCommits(aggs[:1])
        self.assertCommits(aggs[1:])
        self.assertIsolation(aggs)

        log.info('#### down restart(winner) || down')
        log.info('###########################')
        docker_api.containers.get('referee').stop()
        aggs_failure, aggs = self.performFailure(RestartNode('node2'),
                                                 nodes_wait_for_commit=[1])

        # without saved decision node2 will be endlessly disabled here

        self.assertNoCommits(aggs_failure)
        self.assertIsolation(aggs_failure)

        self.assertNoCommits(aggs[:1])
        self.assertCommits(aggs[1:])
        self.assertIsolation(aggs)

        log.info('#### up down(winner) || down')
        log.info('###########################')
        docker_api.containers.get('node2').stop()
        docker_api.containers.get('node1').start()
        aggs_failure, aggs = self.performFailure(NoFailure())

        self.assertNoCommits(aggs_failure)
        self.assertIsolation(aggs_failure)
        self.assertNoCommits(aggs)
        self.assertIsolation(aggs)

        log.info('#### up down(winner) || up')
        log.info('###########################')
        docker_api.containers.get('referee').start()
        aggs_failure, aggs = self.performFailure(NoFailure())

        self.assertNoCommits(aggs_failure)
        self.assertIsolation(aggs_failure)
        self.assertNoCommits(aggs)
        self.assertIsolation(aggs)

        self.client.get_aggregates(clean=False)
        self.client.stop()

        log.info('#### up up(winner) || up')
        log.info('###########################')
        docker_api.containers.get('node2').start()
        self.awaitOnline(self.dsns[1])
        self.awaitOnline(self.dsns[0])

        self.client.bgrun()
        time.sleep(3)

        # give it time to clean old decision
        time.sleep(5)

        log.info('#### check that decision is cleaned')
        log.info('###########################')
        con = psycopg2.connect(self.referee_dsn)
        con.autocommit = True
        cur = con.cursor()
        cur.execute(
            "select node_id from referee.decision where key = 'winner'")
        decisions_count = cur.rowcount
        cur.close()
        con.close()

        self.assertEqual(decisions_count, 0)

    # test that winner continues working after restart
    def test_winner_restart(self):
        log.info('### test_winner_restart ###')

        aggs_failure, aggs = self.performFailure(StopNode('node1'))

        self.assertNoCommits(aggs_failure[:1])
        self.assertCommits(aggs_failure[1:])
        self.assertIsolation(aggs_failure)

        self.assertNoCommits(aggs[:1])
        self.assertCommits(aggs[1:])
        self.assertIsolation(aggs)

        aggs_failure, aggs = self.performFailure(
            RestartNode('node2'), nodes_wait_for_commit=[1])

        self.assertNoCommits(aggs_failure)
        self.assertIsolation(aggs_failure)

        self.assertNoCommits(aggs[:1])
        self.assertCommits(aggs[1:])
        self.assertIsolation(aggs)

        self.client.get_aggregates(clean=False)
        self.client.stop()

        # need to start node1 to perform consequent tests
        docker_api = docker.from_env()
        docker_api.containers.get('node1').start()
        self.awaitOnline(self.dsns[0])

        self.client.bgrun()
        time.sleep(3)

    # test that winner continues working after crash-recover
    def test_winner_crash(self):
        log.info('### test_winner_crash ###')

        aggs_failure, aggs = self.performFailure(StopNode('node1'))

        self.assertNoCommits(aggs_failure[:1])
        self.assertCommits(aggs_failure[1:])
        self.assertIsolation(aggs_failure)

        self.assertNoCommits(aggs[:1])
        self.assertCommits(aggs[1:])
        self.assertIsolation(aggs)

        aggs_failure, aggs = self.performFailure(CrashRecoverNode('node2'),
                                                 nodes_wait_for_commit=[1])

        self.assertNoCommits(aggs_failure)
        self.assertIsolation(aggs_failure)

        self.assertNoCommits(aggs[:1])
        self.assertCommits(aggs[1:])
        self.assertIsolation(aggs)

        self.client.get_aggregates(clean=False)
        self.client.stop()

        # need to start node1 to perform consequent tests
        docker_api = docker.from_env()
        docker_api.containers.get('node1').start()
        self.awaitOnline(self.dsns[0])

        self.client.bgrun()
        time.sleep(3)

    # - get down node 1, ensure 2 works as winner
    # - get down node 2, ensure nothing is working
    # - get 1 up, ensure it can't work as winner is offline
    # - get 2 up, ensure both nodes work now
    # - get referee up, ensure grant is cleared
    def test_consequent_shutdown(self):
        log.info('### test_consequent_shutdown ###')
        docker_api = docker.from_env()

        log.info('#### down on(winner) || on')
        log.info('##########################')
        aggs_failure, aggs = self.performFailure(StopNode('node1'))

        self.assertNoCommits(aggs_failure[:1])
        self.assertCommits(aggs_failure[1:])
        self.assertIsolation(aggs_failure)

        self.assertNoCommits(aggs[:1])
        self.assertCommits(aggs[1:])
        self.assertIsolation(aggs)

        log.info('#### down down(winner) || on')
        log.info('############################')
        aggs_failure, aggs = self.performFailure(StopNode('node2'))

        self.assertNoCommits(aggs_failure)
        self.assertIsolation(aggs_failure)

        self.assertNoCommits(aggs)
        self.assertIsolation(aggs)

        log.info('#### down down(winner) || down')
        log.info('##############################')
        docker_api.containers.get('referee').stop()
        time.sleep(3)

        log.info('#### up down(winner) || down')
        log.info('############################')
        docker_api.containers.get('node1').start()
        aggs_failure, aggs = self.performFailure(NoFailure())

        self.assertNoCommits(aggs_failure)
        self.assertIsolation(aggs_failure)
        self.assertNoCommits(aggs)
        self.assertIsolation(aggs)

        log.info('#### up up(winner) || down')
        log.info('##########################')
        self.client.get_aggregates(clean=False)
        self.client.stop()
        docker_api.containers.get('node2').start()
        self.awaitOnline(self.dsns[1])
        self.awaitOnline(self.dsns[0])
        self.client.bgrun()
        time.sleep(3)

        aggs_failure, aggs = self.performFailure(NoFailure())

        self.assertCommits(aggs_failure)
        self.assertIsolation(aggs_failure)
        self.assertCommits(aggs)
        self.assertIsolation(aggs)

        log.info('#### up up || up(2 -> 0)')
        log.info('########################')
        docker_api.containers.get('referee').start()
        time.sleep(5)

        log.info('#### check that decision is cleaned')
        log.info('###################################')
        self.awaitOnline(self.referee_dsn)
        con = psycopg2.connect(self.referee_dsn)
        con.autocommit = True
        cur = con.cursor()
        cur.execute(
            "select node_id from referee.decision where key = 'winner'")
        decisions_count = cur.rowcount
        cur.close()
        con.close()

        self.assertEqual(decisions_count, 0)


if __name__ == '__main__':
    unittest.main()
