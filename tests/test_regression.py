import unittest
import subprocess
import time

from lib.bank_client import MtmClient
from lib.test_helper import *

class RecoveryTest(unittest.TestCase, TestHelper):

    @classmethod
    def setUpClass(cls):
        cls.dsns = [
            "dbname=regression user=postgres host=127.0.0.1 port=15432",
            "dbname=regression user=postgres host=127.0.0.1 port=15433",
            "dbname=regression user=postgres host=127.0.0.1 port=15434"
        ]

        print('setUp')
        subprocess.check_call(['docker-compose','up',
           '--force-recreate',
           '--build',
           '-d'])

        # Wait for all nodes to become online
        [ cls.awaitOnline(dsn) for dsn in cls.dsns ]

        cls.client = MtmClient(cls.dsns, n_accounts=1000)

    @classmethod
    def tearDownClass(cls):
        print('tearDown')
#        subprocess.check_call(['docker-compose','down'])

    def test_regression(self):
        # XXX: make smth clever here
        time.sleep(10)
        subprocess.check_call(['docker', 'exec',
            'node1',
            '/pg/mmts/tests/support/docker-regress.sh',
        ])

if __name__ == '__main__':
    unittest.main()
