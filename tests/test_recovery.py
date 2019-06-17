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

class RecoveryTest(unittest.TestCase, TestHelper):

    @classmethod
    def setUpClass(cls):
        cls.dsns = [
            "dbname=regression user=postgres host=127.0.0.1 port=15432",
            "dbname=regression user=postgres host=127.0.0.1 port=15433",
            "dbname=regression user=postgres host=127.0.0.1 port=15434"
        ]

        subprocess.check_call(['docker-compose','up',
            '--force-recreate',
            '--build',
            '-d'])

        # Wait for all nodes to become online
        [ cls.awaitOnline(dsn) for dsn in cls.dsns ]

        cls.client = MtmClient(cls.dsns, n_accounts=1000)
        cls.client.bgrun()

    @classmethod
    def tearDownClass(cls):
        print('tearDown')
        cls.client.stop()

        time.sleep(TEST_STOP_DELAY)

        if not cls.client.is_data_identic():
            raise AssertionError('Different data on nodes')

        if cls.client.no_prepared_tx() != 0:
            raise AssertionError('There are some uncommitted tx')

        # XXX: check nodes data identity here
        # subprocess.check_call(['docker-compose','down'])

    def setUp(self):
        warnings.simplefilter("ignore", ResourceWarning)
        time.sleep(20)
        print('Start new test at ',datetime.datetime.utcnow())

    def tearDown(self):
        print('Finish test at ',datetime.datetime.utcnow())

    def test_normal_operations(self):
        print('### test_normal_operations ###')

        aggs_failure, aggs = self.performFailure(NoFailure())

        self.assertCommits(aggs_failure)
        self.assertIsolation(aggs_failure)

        self.assertCommits(aggs)
        self.assertIsolation(aggs)


    def test_node_partition(self):
        print('### test_node_partition ###')

        aggs_failure, aggs = self.performFailure(SingleNodePartition('node3'),
            node_wait_for_online="dbname=regression user=postgres host=127.0.0.1 port=15434", stop_load=True)

        self.assertCommits(aggs_failure[:2])
        self.assertNoCommits(aggs_failure[2:])
        self.assertIsolation(aggs_failure)

        self.assertCommits(aggs)
        self.assertIsolation(aggs)

    # def test_edge_partition(self):
    #     print('### test_edge_partition ###')

    #     aggs_failure, aggs = self.performFailure(EdgePartition('node1', 'node3'),
    #         node_wait_for_online="dbname=regression user=postgres host=127.0.0.1 port=15434", stop_load=True)

    #     self.assertTrue( ('commit' in aggs_failure[0]['transfer']['finish']) or ('commit' in aggs_failure[2]['transfer']['finish']) )
    #     self.assertCommits(aggs_failure[1:2]) # second node
    #     self.assertIsolation(aggs_failure)

    #     self.assertCommits(aggs)
    #     self.assertIsolation(aggs)

    def test_node_restart(self):
        print('### test_node_restart ###')

        aggs_failure, aggs = self.performFailure(RestartNode('node3'),
            node_wait_for_online="dbname=regression user=postgres host=127.0.0.1 port=15434", stop_load=True)

        self.assertCommits(aggs_failure[:2])
        self.assertNoCommits(aggs_failure[2:])
        self.assertIsolation(aggs_failure)

        self.assertCommits(aggs)
        self.assertIsolation(aggs)

    def test_node_crash(self):
        print('### test_node_crash ###')

        aggs_failure, aggs = self.performFailure(CrashRecoverNode('node3'),
            node_wait_for_online="dbname=regression user=postgres host=127.0.0.1 port=15434", stop_load=True)

        self.assertCommits(aggs_failure[:2])
        self.assertNoCommits(aggs_failure[2:])
        self.assertIsolation(aggs_failure)

        self.assertCommits(aggs)
        self.assertIsolation(aggs)

    # def test_node_pause(self):
    #     print('### test_node_pause ###')

    #     aggs_failure, aggs = self.performFailure(FreezeNode('node3'),
    #         node_wait_for_commit=2, stop_load=True)

    #     self.assertCommits(aggs_failure[:2])
    #     self.assertNoCommits(aggs_failure[2:])
    #     self.assertIsolation(aggs_failure)

    #     self.assertCommits(aggs)
    #     self.assertIsolation(aggs)

    def test_node_bicrash(self):
        print('### test_node_bicrash ###')

        for _ in range(10):
            aggs_failure, aggs = self.performFailure(CrashRecoverNode('node3'),
                node_wait_for_online="dbname=regression user=postgres host=127.0.0.1 port=15434", stop_load=True)

            self.assertCommits(aggs_failure[:2])
            self.assertNoCommits(aggs_failure[2:])
            self.assertIsolation(aggs_failure)

            self.assertCommits(aggs)
            self.assertIsolation(aggs)

            aggs_failure, aggs = self.performFailure(CrashRecoverNode('node1'),
                node_wait_for_online="dbname=regression user=postgres host=127.0.0.1 port=15432", stop_load=True)

            self.assertNoCommits(aggs_failure[0:1])  # [1]
            self.assertCommits(aggs_failure[1:]) # [2, 3]
            self.assertIsolation(aggs_failure)

            self.assertCommits(aggs)
            self.assertIsolation(aggs)

            self.assertDataSync()
            print('### iteration okay ###')


if __name__ == '__main__':
    unittest.main()

