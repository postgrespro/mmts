#
# Based on Aphyr's test for CockroachDB.
#

import unittest
import time
import subprocess
import datetime
import docker
import warnings

from lib.bank_client import MtmClient
from lib.failure_injector import *
from lib.test_helper import *


class RefereeTest(unittest.TestCase, TestHelper):

    @classmethod
    def setUpClass(cls):
        subprocess.check_call(['docker-compose',
            '-f', 'support/two_nodes.yml',
            'up',
            '--force-recreate',
            '--build',
            '-d'])

        cls.client = MtmClient([
            "dbname=regression user=postgres host=127.0.0.1 port=15432",
            "dbname=regression user=postgres host=127.0.0.1 port=15433"
        ], n_accounts=1000)
        cls.client.bgrun()

        # create extension on referee
        cls.nodeExecute("dbname=regression user=postgres host=127.0.0.1 port=15435", ['create extension referee'])

    @classmethod
    def tearDownClass(cls):
        print('tearDown')
        cls.client.stop()

        # ohoh
        th = TestHelper()
        th.client = cls.client
        th.assertDataSync()
        cls.client.stop()

        # subprocess.check_call(['docker-compose','down'])

    def setUp(self):
        warnings.simplefilter("ignore", ResourceWarning)

        print('Start new test at ',datetime.datetime.utcnow())

    def tearDown(self):
        print('Finish test at ',datetime.datetime.utcnow())

    def test_neighbor_restart(self):
        print('### test_neighbor_restart ###')

        aggs_failure, aggs = self.performFailure(RestartNode('node2'), node_wait_for_online="dbname=regression user=postgres host=127.0.0.1 port=15433", stop_load=True)

        self.assertCommits(aggs_failure[:1])
        self.assertNoCommits(aggs_failure[1:])
        self.assertIsolation(aggs_failure)

        self.assertCommits(aggs)
        self.assertIsolation(aggs)


    def test_node_crash(self):
        print('### test_node_crash ###')

        aggs_failure, aggs = self.performFailure(CrashRecoverNode('node2'), node_wait_for_online="dbname=regression user=postgres host=127.0.0.1 port=15433", stop_load=True)

        self.assertCommits(aggs_failure[:1])
        self.assertNoCommits(aggs_failure[1:])
        self.assertIsolation(aggs_failure)

        self.assertCommits(aggs)
        self.assertIsolation(aggs)


    def test_partition_referee(self):
        print('### test_partition_referee ###')

        aggs_failure, aggs = self.performFailure(SingleNodePartition('node2'), node_wait_for_online="dbname=regression user=postgres host=127.0.0.1 port=15433", stop_load=True)

        self.assertCommits(aggs_failure[:1])
        self.assertNoCommits(aggs_failure[1:])
        self.assertIsolation(aggs_failure)

        self.assertCommits(aggs)
        self.assertIsolation(aggs)


    # cut one node from neighbour and referee, ensure neighbour works as winner,
    # repair network, wait until isolated node gets online, repeat with another
    # node.
    def test_double_failure_referee(self):
        print('### test_double_failure_referee ###')

        aggs_failure, aggs = self.performFailure(SingleNodePartition('node2'), node_wait_for_online="dbname=regression user=postgres host=127.0.0.1 port=15433", stop_load=True)

        self.assertCommits(aggs_failure[:1])
        self.assertNoCommits(aggs_failure[1:])
        self.assertIsolation(aggs_failure)

        self.assertCommits(aggs)
        self.assertIsolation(aggs)

        aggs_failure, aggs = self.performFailure(SingleNodePartition('node1'), node_wait_for_online="dbname=regression user=postgres host=127.0.0.1 port=15432", stop_load=True)

        self.assertNoCommits(aggs_failure[:1])
        self.assertCommits(aggs_failure[1:])
        self.assertIsolation(aggs_failure)

        self.assertCommits(aggs)
        self.assertIsolation(aggs)


    # - get node 1 down, ensure 2 works as winner
    # - restart 2, ensure it continue working as winner
    # - stop 2, start 1, ensure nothing working as winner is down
    # - start 2, ensure referee grant is cleared
    # it intersects with both test_winner_restart and test_consequent_shutdown...
    def test_saved_referee_decision(self):
        print('### test_saved_referee_decision ###')
        docker_api = docker.from_env()

        print('#### down on(winner) || on')
        print('###########################')
        aggs_failure, aggs = self.performFailure(StopNode('node1'))

        self.assertNoCommits(aggs_failure[:1])
        self.assertCommits(aggs_failure[1:])
        self.assertIsolation(aggs_failure)

        self.assertNoCommits(aggs[:1])
        self.assertCommits(aggs[1:])
        self.assertIsolation(aggs)

        print('#### down restart(winner) || down')
        print('###########################')
        docker_api.containers.get('referee').stop()
        aggs_failure, aggs = self.performFailure(RestartNode('node2'), nodes_wait_for_commit=[1])

        # without saved decision node2 will be endlessy disabled here

        self.assertNoCommits(aggs_failure)
        self.assertIsolation(aggs_failure)

        self.assertNoCommits(aggs[:1])
        self.assertCommits(aggs[1:])
        self.assertIsolation(aggs)

        print('#### up down(winner) || down')
        print('###########################')
        docker_api.containers.get('node2').stop()
        docker_api.containers.get('node1').start()
        aggs_failure, aggs = self.performFailure(NoFailure())

        self.assertNoCommits(aggs_failure)
        self.assertIsolation(aggs_failure)
        self.assertNoCommits(aggs)
        self.assertIsolation(aggs)

        print('#### up down(winner) || up')
        print('###########################')
        docker_api.containers.get('referee').start()
        aggs_failure, aggs = self.performFailure(NoFailure())

        self.assertNoCommits(aggs_failure)
        self.assertIsolation(aggs_failure)
        self.assertNoCommits(aggs)
        self.assertIsolation(aggs)

        self.client.get_aggregates(clean=False)
        self.client.stop()

        print('#### up up(winner) || up')
        print('###########################')
        docker_api.containers.get('node2').start()
        self.awaitOnline("dbname=regression user=postgres host=127.0.0.1 port=15433")
        self.awaitOnline("dbname=regression user=postgres host=127.0.0.1 port=15432")

        self.client.bgrun()
        time.sleep(3)

        # give it time to clean old decision
        time.sleep(5)

        print('#### check that decision is cleaned')
        print('###########################')
        con = psycopg2.connect("dbname=regression user=postgres host=127.0.0.1 port=15435")
        con.autocommit = True
        cur = con.cursor()
        cur.execute("select node_id from referee.decision where key = 'winner'")
        decisions_count = cur.rowcount
        cur.close()
        con.close()

        self.assertEqual(decisions_count, 0)


    # test that winner continues working after restart
    def test_winner_restart(self):
        print('### test_winner_restart ###')

        aggs_failure, aggs = self.performFailure(StopNode('node1'))

        self.assertNoCommits(aggs_failure[:1])
        self.assertCommits(aggs_failure[1:])
        self.assertIsolation(aggs_failure)

        self.assertNoCommits(aggs[:1])
        self.assertCommits(aggs[1:])
        self.assertIsolation(aggs)

        aggs_failure, aggs = self.performFailure(RestartNode('node2'), nodes_wait_for_commit=[1])

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
        self.awaitOnline("dbname=regression user=postgres host=127.0.0.1 port=15432")

        self.client.bgrun()
        time.sleep(3)

    # test that winner continues working after crash-recover
    def test_winner_crash(self):
        print('### test_winner_crash ###')

        aggs_failure, aggs = self.performFailure(StopNode('node1'))

        self.assertNoCommits(aggs_failure[:1])
        self.assertCommits(aggs_failure[1:])
        self.assertIsolation(aggs_failure)

        self.assertNoCommits(aggs[:1])
        self.assertCommits(aggs[1:])
        self.assertIsolation(aggs)

        aggs_failure, aggs = self.performFailure(CrashRecoverNode('node2'), nodes_wait_for_commit=[1])

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
        self.awaitOnline("dbname=regression user=postgres host=127.0.0.1 port=15432")

        self.client.bgrun()
        time.sleep(3)

    # - get down node 1, ensure 2 works as winner
    # - get down node 2, ensure nothing is working
    # - get 1 up, ensure it can't work as winner is offline
    # - get 2 up, ensure both nodes work now
    # - get referee up, ensure grant is cleared
    def test_consequent_shutdown(self):
        print('### test_consequent_shutdown ###')
        docker_api = docker.from_env()


        print('#### down on(winner) || on')
        print('##########################')
        aggs_failure, aggs = self.performFailure(StopNode('node1'))

        self.assertNoCommits(aggs_failure[:1])
        self.assertCommits(aggs_failure[1:])
        self.assertIsolation(aggs_failure)

        self.assertNoCommits(aggs[:1])
        self.assertCommits(aggs[1:])
        self.assertIsolation(aggs)


        print('#### down down(winner) || on')
        print('############################')
        aggs_failure, aggs = self.performFailure(StopNode('node2'))

        self.assertNoCommits(aggs_failure)
        self.assertIsolation(aggs_failure)

        self.assertNoCommits(aggs)
        self.assertIsolation(aggs)


        print('#### down down(winner) || down')
        print('##############################')
        docker_api.containers.get('referee').stop()
        time.sleep(3)


        print('#### up down(winner) || down')
        print('############################')
        docker_api.containers.get('node1').start()
        aggs_failure, aggs = self.performFailure(NoFailure())

        self.assertNoCommits(aggs_failure)
        self.assertIsolation(aggs_failure)
        self.assertNoCommits(aggs)
        self.assertIsolation(aggs)

        print('#### up up(winner) || down')
        print('##########################')
        self.client.get_aggregates(clean=False)
        self.client.stop()
        docker_api.containers.get('node2').start()
        self.awaitOnline("dbname=regression user=postgres host=127.0.0.1 port=15433")
        self.awaitOnline("dbname=regression user=postgres host=127.0.0.1 port=15432")
        self.client.bgrun()
        time.sleep(3)

        aggs_failure, aggs = self.performFailure(NoFailure())

        self.assertCommits(aggs_failure)
        self.assertIsolation(aggs_failure)
        self.assertCommits(aggs)
        self.assertIsolation(aggs)

        print('#### up up || up(2 -> 0)')
        print('########################')
        docker_api.containers.get('referee').start()
        time.sleep(5)

        print('#### check that decision is cleaned')
        print('###################################')
        self.awaitOnline("dbname=regression user=postgres host=127.0.0.1 port=15435")
        con = psycopg2.connect("dbname=regression user=postgres host=127.0.0.1 port=15435")
        con.autocommit = True
        cur = con.cursor()
        cur.execute("select node_id from referee.decision where key = 'winner'")
        decisions_count = cur.rowcount
        cur.close()
        con.close()

        self.assertEqual(decisions_count, 0)


if __name__ == '__main__':
    unittest.main()
