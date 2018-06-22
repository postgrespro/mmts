import unittest
import subprocess
import time

from mm_cluster import Cluster

NUM_NODES = 3

class TestDDL(unittest.TestCase):

    @classmethod
    def setUpClass(cls):
        cls.cluster = Cluster(NUM_NODES)
        cls.cluster.print_conninfo()
        cls.cluster.start().await_online((0,1,2))
        # cls.cluster.print_conninfo()

    @classmethod
    def tearDownClass(cls):
        cls.cluster.stop()

    # Check that recovery properly processes
    def test_dll_recovery(self):
        # create table while one node is stopped
        self.cluster.nodes[2].stop()
        self.cluster.await_online((0,1))
        self.cluster.nodes[0].safe_psql(query='create table t(id int primary key)')

        # now if second node didn't store logical message with DDL and third
        # node will recover from second then it will not receive this
        # 'create table' (PGPRO-1699)
        self.cluster.nodes[2].start()
        self.cluster.await_online((0,1,2))
        self.cluster.nodes[2].safe_psql(query='insert into t values(42)')


if __name__ == '__main__':
    unittest.main()