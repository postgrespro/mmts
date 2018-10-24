import unittest
import subprocess
import time

from lib.bank_client import MtmClient

class RecoveryTest(unittest.TestCase):

    @classmethod
    def setUpClass(cls):
        print('setUp')
        subprocess.check_call(['docker-compose','up',
           '--force-recreate',
           '--build',
           '-d'])
        time.sleep(10)
        cls.client = MtmClient([
            "dbname=regression user=postgres host=127.0.0.1 port=15432",
            "dbname=regression user=postgres host=127.0.0.1 port=15433",
            "dbname=regression user=postgres host=127.0.0.1 port=15434"
        ], n_accounts=1000)

    @classmethod
    def tearDownClass(cls):
        print('tearDown')
#        subprocess.check_call(['docker-compose','down'])

    def test_regression(self):
        # XXX: make smth clever here
        time.sleep(10)
        subprocess.check_call(['docker', 'exec',
            'node1',
            '/pg/mmts/tests2/support/docker-regress.sh',
        ])

if __name__ == '__main__':
    unittest.main()
