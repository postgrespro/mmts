#
# Based on Aphyr's test for CockroachDB.
#

import unittest
import time
import subprocess
import datetime
import docker
import warnings
import random
import socket

from lib.bank_client import MtmClient
from lib.failure_injector import *
from lib.test_helper import *

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

        subprocess.check_call(['docker-compose', 'up',
            '--force-recreate',
            '--build',
            '-d'])

        # # Some Docker container debug stuff
        # subprocess.check_call(['docker-compose', 'ps'])
        # subprocess.check_call(['sudo', 'iptables', '-S'])
        # subprocess.check_call(['sudo', 'iptables', '-L'])
        # subprocess.check_call(['docker-compose', 'logs'])

        # Wait for all nodes to become online
        [ cls.awaitOnline(dsn) for dsn in cls.dsns ]

        cls.client = MtmClient(cls.dsns, n_accounts=1000)
        cls.client.bgrun()

    @classmethod
    def tearDownClass(cls):
        print('tearDown')
        cls.client.stop()

        time.sleep(TEST_STOP_DELAY)
        # subprocess.run('docker-compose logs --no-color > mmts.log', shell=True)
        subprocess.run('docker logs node1 &> mmts_node1.log', shell=True)
        subprocess.run('docker logs node2 &> mmts_node2.log', shell=True)
        subprocess.run('docker logs node3 &> mmts_node3.log', shell=True)

        if not cls.client.is_data_identic():
            raise AssertionError('Different data on nodes')

        if cls.client.no_prepared_tx() != 0:
            raise AssertionError('There are some uncommitted tx')

        # XXX: check nodes data identity here
        subprocess.check_call(['docker-compose', 'down'])

    def setUp(self):
        warnings.simplefilter("ignore", ResourceWarning)
        time.sleep(20)
        print('Start new test at ', datetime.datetime.utcnow())

    def tearDown(self):
        print('Finish test at ', datetime.datetime.utcnow())

    def test_random_disasters(self):
        print('### test_random_disasters ###')

        for i in range(1, 16):
            print(f'Running round #{i} of test_random_disasters')
            node_number = random.choice(range(1, 4))
            port = 15431 + node_number

            aggs_failure, aggs = self.performRandomFailure(f'node{node_number}', node_wait_for_commit=node_number - 1,
                node_wait_for_online=f"dbname=regression user=postgres host={NODE_HOST} port={port}", stop_load=True)

            for n in range(3):
                if n == node_number - 1:
                    self.assertNoCommits([aggs_failure[n]])
                else:
                    self.assertCommits([aggs_failure[n]])

            self.assertIsolation(aggs_failure)
            self.assertCommits(aggs)
            self.assertIsolation(aggs)

            print(f'Iteration #{i} is OK')


if __name__ == '__main__':
    unittest.main()
