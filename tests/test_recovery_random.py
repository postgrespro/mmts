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

class RecoveryTest(MMTestCase, TestHelper):
    def test_normal_operations(self):
        log.info('### test_normal_operations ###')

        aggs_failure, aggs = self.performFailure(NoFailure())

        self.assertCommits(aggs_failure)
        self.assertIsolation(aggs_failure)

        self.assertCommits(aggs)
        self.assertIsolation(aggs)

    # main random tests
    def test_random_disasters(self):
        log.info('### test_random_disasters ###')

        for i in range(1, 16):
            log.info(f'running round #{i} of test_random_disasters')
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

    # sausage topology test
    def test_edge_partition(self):
        log.info('### test_edge_partition ###')

        aggs_failure, aggs = self.performFailure(
            EdgePartition('node1', 'node3'), node_wait_for_online=
            "dbname=regression user=postgres host=127.0.0.1 port=15434",
            stop_load=True)

        self.assertTrue(('commit' in aggs_failure[0]['transfer']['finish']) or
                        ('commit' in aggs_failure[2]['transfer']['finish']))
        self.assertCommits(aggs_failure[1:2])  # second node
        self.assertIsolation(aggs_failure)

        self.assertCommits(aggs)
        self.assertIsolation(aggs)

    # can be used for manual running of some particular failure
    def _test_single_failure(self):
        log.info('### test_single_failure ###')

        failure = CrashRecoverNode('node3')
        aggs_failure, aggs = self.performFailure(
            failure, node_wait_for_online=
            "dbname=regression user=postgres host=127.0.0.1 port=15434",
            stop_load=True)

        self.assertCommits(aggs_failure[:2])
        self.assertNoCommits(aggs_failure[2:])
        self.assertIsolation(aggs_failure)

        self.assertCommits(aggs)
        self.assertIsolation(aggs)


# you can run single test with something like
# python -u -m unittest test_recovery.RecoveryTest.test_single_failure
if __name__ == '__main__':
    # run all tests
    unittest.main()
