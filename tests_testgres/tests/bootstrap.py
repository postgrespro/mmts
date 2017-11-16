import unittest
from cluster import Cluster


class Bootstrap(unittest.TestCase):
    def test_bootstrap(self):
        with Cluster(3).start().install() as cluster:
            for node in cluster.nodes:
                status = 'select status from mtm.get_cluster_state()'

                self.assertTrue(node.status())
                self.assertTrue(node.execute('postgres', 'select true'))
                self.assertTrue(node.execute('postgres', status))
