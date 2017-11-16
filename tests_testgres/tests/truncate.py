import unittest
import subprocess
import time

from cluster import Cluster


NUM_NODES = 2
BENCH_SEC = 30


class TestTruncate(unittest.TestCase):
    def test_truncate(self):
        with Cluster(NUM_NODES).start().install() as cluster:
            assert(NUM_NODES >= 2)

            for node in cluster.nodes:
                self.assertTrue(node.status())

            node_1 = cluster.nodes[0]
            node_1.pgbench_init(dbname=cluster.dbname)

            pgbench = node_1.pgbench(dbname=cluster.dbname,
                                     stdout=subprocess.PIPE,
                                     stderr=subprocess.STDOUT,
                                     options=['-T%i' % BENCH_SEC])

            count = 0
            started = time.time()
            while time.time() - started < BENCH_SEC:
                for node in cluster.nodes:
                    node.safe_psql(dbname=cluster.dbname,
                                   username=cluster.username,
                                   query='truncate pgbench_history;')

                    node.safe_psql(dbname=cluster.dbname,
                                   username=cluster.username,
                                   query='vacuum full;')

                count += 1

                # check that pgbench has been running for at least 1 loop
                assert (count > 0 or pgbench.poll is not None)

                time.sleep(0.5)

            assert(count > 0)
            print("{}: executed truncate {} times"
                  .format(self.test_truncate.__name__, count))

            pgbench.wait()
