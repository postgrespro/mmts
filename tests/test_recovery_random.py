#!/usr/bin/env python3

#
# Based on Aphyr's test for CockroachDB.
#
# Randomized recovery test for multimaster. Currently it picks a random node,
# crash-recovers it or drops/rejects packets to and from it under load and
# checks that things are ok, i.e. the rest two continue working and after
# eliminating the failure the victim successfully recovers, with no hanged
# prepares and data being identic everywhere. Lather, rinse, repeat.

import datetime
import docker
import os
import random
import socket
import subprocess
import time
import unittest
import warnings
import logging

from lib.bank_client import MtmClient
from lib.failure_injector import *
import lib.log_helper  # configures loggers
from lib.test_helper import *

log = logging.getLogger('root')

class RecoveryTest(unittest.TestCase, TestHelper):

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
        print('tearDownClass')

        # ohoh
        th = TestHelper()
        th.client = cls.client

        # collect logs for CI anyway
        try:
            th.assertDataSync()
        finally:
            cls.client.stop()
            # Destroying containers is really unhelpful for local debugging, so
            # do this automatically only in CI.
            if 'CI' in os.environ:
                cls.collectLogs()
                subprocess.check_call(['docker-compose', 'down'])

    @classmethod
    def collectLogs(cls):
        log.info('collecting logs')
        # subprocess.run(
        # 'docker-compose logs --no-color > mmts.log', shell=True)
        # non-standard &> doesn't work in e.g. default Debian dash, so
        # use old school > 2>&1
        subprocess.run('docker logs node1 >mmts_node1.log 2>&1', shell=True)
        subprocess.run('docker logs node2 >mmts_node2.log 2>&1', shell=True)
        subprocess.run('docker logs node3 >mmts_node3.log 2>&1', shell=True)

    def setUp(self):
        warnings.simplefilter("ignore", ResourceWarning)
        time.sleep(20)
        log.info('start new test')

    def tearDown(self):
        log.info('finish test')

    def test_random_disasters(self):
        log.info('### test_random_disasters ###')

        for i in range(1, 16):
            log.info(f'Running round #{i} of test_random_disasters')
            node_number = random.choice(range(1, 4))
            port = 15431 + node_number

            nodes_assert_commit_during_failure = [n for n in range(3) if n !=
                                                  node_number - 1]
            aggs_failure, aggs = self.performRandomFailure(
                f'node{node_number}',
                nodes_wait_for_commit=[n for n in range(3)],
                node_wait_for_online=f"dbname=regression user=postgres host={NODE_HOST} port={port}",
                stop_load=True,
                nodes_assert_commit_during_failure=
                nodes_assert_commit_during_failure)

            for n in range(3):
                if n == node_number - 1:
                    self.assertNoCommits([aggs_failure[n]])
                else:
                    self.assertCommits([aggs_failure[n]])

            self.assertIsolation(aggs_failure)
            self.assertCommits(aggs)
            self.assertIsolation(aggs)
            self.assertDataSync()

            log.info(f'iteration #{i} is OK')

# useful to temporary run inidividual tests for debugging


def suite():
    suite = unittest.TestSuite()
    suite.addTest(RecoveryTest('test_tmp'))
    return suite


if __name__ == '__main__':
    unittest.main()

    # runner = unittest.TextTestRunner(verbosity=2, failfast=True)
    # runner.run(suite())
