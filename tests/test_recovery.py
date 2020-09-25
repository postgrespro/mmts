#!/usr/bin/env python3

#
# Based on Aphyr's test for CockroachDB.
#

import unittest
import time
import subprocess
import datetime
import docker
import logging
import warnings

from lib.bank_client import MtmClient
from lib.failure_injector import *
import lib.log_helper  # configures loggers
from lib.test_helper import *

log = logging.getLogger('root')

class RecoveryTest(unittest.TestCase, TestHelper):

    @classmethod
    def setUpClass(cls):
        cls.dsns = [
            "dbname=regression user=postgres host=127.0.0.1 port=15432",
            "dbname=regression user=postgres host=127.0.0.1 port=15433",
            "dbname=regression user=postgres host=127.0.0.1 port=15434"
        ]

        subprocess.check_call(['docker-compose', 'up',
                               '--force-recreate',
                               '--build',
                               '-d'])

        # Wait for all nodes to become online
        [cls.awaitOnline(dsn) for dsn in cls.dsns]

        cls.client = MtmClient(cls.dsns, n_accounts=1000)
        cls.client.bgrun()

    @classmethod
    def tearDownClass(cls):
        log.info('tearDown')

        # ohoh
        th = TestHelper()
        th.client = cls.client

        th.assertDataSync()
        cls.client.stop()

        # subprocess.check_call(['docker-compose','down'])

    def setUp(self):
        warnings.simplefilter("ignore", ResourceWarning)
        time.sleep(20)
        log.info('start new test')

    def tearDown(self):
        log.info('finish test')

    def test_normal_operations(self):
        log.info('### test_normal_operations ###')

        aggs_failure, aggs = self.performFailure(NoFailure())

        self.assertCommits(aggs_failure)
        self.assertIsolation(aggs_failure)

        self.assertCommits(aggs)
        self.assertIsolation(aggs)

    def test_node_partition(self):
        log.info('### test_node_partition ###')

        aggs_failure, aggs = self.performFailure(
            SingleNodePartition('node3'), node_wait_for_online=
            "dbname=regression user=postgres host=127.0.0.1 port=15434",
            stop_load=True)

        self.assertCommits(aggs_failure[:2])
        self.assertNoCommits(aggs_failure[2:])
        self.assertIsolation(aggs_failure)

        self.assertCommits(aggs)
        self.assertIsolation(aggs)

    def test_edge_partition(self):
        log.info('### test_edge_partition ###')

        aggs_failure, aggs = self.performFailure(
            EdgePartition('node1', 'node3'), node_wait_for_online=
            "dbname=regression user=postgres host=127.0.0.1 port=15434",
            stop_load=False)

        self.assertTrue(('commit' in aggs_failure[0]['transfer']['finish']) or
                        ('commit' in aggs_failure[2]['transfer']['finish']))
        self.assertCommits(aggs_failure[1:2])  # second node
        self.assertIsolation(aggs_failure)

        self.assertCommits(aggs)
        self.assertIsolation(aggs)

    def test_node_restart(self):
        log.info('### test_node_restart ###')

        aggs_failure, aggs = self.performFailure(
            RestartNode('node3'), node_wait_for_online=
            "dbname=regression user=postgres host=127.0.0.1 port=15434",
            stop_load=True)

        self.assertCommits(aggs_failure[:2])
        self.assertNoCommits(aggs_failure[2:])
        self.assertIsolation(aggs_failure)

        self.assertCommits(aggs)
        self.assertIsolation(aggs)

    def test_node_crash(self):
        log.info('### test_node_crash ###')

        aggs_failure, aggs = self.performFailure(
            CrashRecoverNode('node3'), node_wait_for_online=
            "dbname=regression user=postgres host=127.0.0.1 port=15434",
            stop_load=True)

        self.assertCommits(aggs_failure[:2])
        self.assertNoCommits(aggs_failure[2:])
        self.assertIsolation(aggs_failure)

        self.assertCommits(aggs)
        self.assertIsolation(aggs)

    def test_node_pause(self):
        log.info('### test_node_pause ###')

        aggs_failure, aggs = self.performFailure(FreezeNode('node3'),
                                                 nodes_wait_for_commit=[2],
                                                 stop_load=True)

        self.assertCommits(aggs_failure[:2])
        self.assertNoCommits(aggs_failure[2:])
        self.assertIsolation(aggs_failure)

        self.assertCommits(aggs)
        self.assertIsolation(aggs)

    def test_node_bicrash(self):
        log.info('### test_node_bicrash ###')

        for i in range(10):
            aggs_failure, aggs = self.performFailure(
                CrashRecoverNode('node3'), node_wait_for_online=
                "dbname=regression user=postgres host=127.0.0.1 port=15434",
                stop_load=True)

            self.assertCommits(aggs_failure[:2])
            self.assertNoCommits(aggs_failure[2:])
            self.assertIsolation(aggs_failure)

            self.assertCommits(aggs)
            self.assertIsolation(aggs)

            aggs_failure, aggs = self.performFailure(
                CrashRecoverNode('node1'), node_wait_for_online=
                "dbname=regression user=postgres host=127.0.0.1 port=15432",
                stop_load=True)

            self.assertNoCommits(aggs_failure[0:1])  # [1]
            self.assertCommits(aggs_failure[1:])  # [2, 3]
            self.assertIsolation(aggs_failure)

            self.assertCommits(aggs)
            self.assertIsolation(aggs)

            self.assertDataSync()
            log.info('### iteration {} okay ###'.format(i + 1))


# useful to temporary run inidividual tests for debugging
def suite():
    suite = unittest.TestSuite()
    suite.addTest(RecoveryTest('test_normal_operations'))
    return suite


if __name__ == "__main__":
    # all tests
    unittest.main()

    # runner = unittest.TextTestRunner(verbosity=2, failfast=True)
    # runner.run(suite())
