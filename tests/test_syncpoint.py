#
# Basic syncpoint sanity check: ensure in normal mode (all nodes are up and
# running) old wal files are erased once they are not needed anymore.
# On the other hand we must ensure that if a node of the cluster is out of
# order the older wal files needed for the node recovery are NOT erased.
#

import unittest
import time
import subprocess
import datetime
import docker
import warnings
import pprint

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
        print('tearDown')
        cls.client.stop()

        time.sleep(TEST_STOP_DELAY)

        if not cls.client.is_data_identic():
            raise AssertionError('Different data on nodes')

        if cls.client.no_prepared_tx() != 0:
            raise AssertionError('There are some uncommitted tx')

    def setUp(self):
        warnings.simplefilter("ignore", ResourceWarning)
        time.sleep(20)
        print('Start new test at ', datetime.datetime.utcnow())

    def tearDown(self):
        print('Finish test at ', datetime.datetime.utcnow())

    # Returns the newest wal
    def _get_last_wal(self, dsn):
        return self.nodeSelect(dsn, "SELECT name FROM pg_ls_waldir() WHERE "
                                    "name ~ '^[0-9A-F]+$' ORDER BY "
                                    "name DESC LIMIT 1")[0][0]

    # Returns the oldest existing wal
    def _get_first_wal(self, dsn):
        return self.nodeSelect(dsn, "SELECT name FROM pg_ls_waldir() WHERE "
                                    "name ~ '^[0-9A-F]+$' ORDER BY "
                                    "name LIMIT 1")[0][0]

    # If running in not degraded mode (i.e. degraded_node == 0), the function
    # returns the file name of wal where the most recent restart_lsn for
    # neighbour nodes.
    #
    # In degraded mode (degraded_node != 0), the function returns the file name
    # of restart_lsn of node that does not currently exist in the cluster.
    # This file must not be removed until the node returns to cluster.
    def _get_restart_lsn_wal(self, dsn, degraded_node=0):
        if degraded_node == 0:
            wals = self.nodeSelect(dsn, "SELECT pg_walfile_name(restart_lsn) "
                                        "FROM pg_replication_slots WHERE "
                                        "slot_name LIKE 'mtm_recovery_slot_%'")
            latest_wal = '000000000000000000000000'
            for wal in wals:
                if wal[0] > latest_wal:
                    latest_wal = wal[0]
            return latest_wal
        else:
            return self.nodeSelect(dsn, "SELECT pg_walfile_name(restart_lsn) "
                                        "FROM pg_replication_slots WHERE "
                                        "slot_name='mtm_recovery_slot_%i'" %
                                   degraded_node)[0][0]

    # Returns two arrays.
    # For each node in dsns, the first one contains the most recent wal
    # segment name, the second result of  _get_restart_lsn_wal
    def _get_wals(self, dsns, degraded_node=0):
        lastwal_begin = []
        firsttwal_begin = []
        for i in range(len(dsns)):
            lastwal_begin.append(self._get_last_wal(dsns[i]))
            firsttwal_begin.append(
                self._get_restart_lsn_wal(dsns[i], degraded_node))
        return [lastwal_begin, firsttwal_begin]

    # Waits (up to iterations * iteration_sleep seconds)
    # until at least wals_to_pass segments appear on each node since
    # lastwal_begin positions.
    # Then returns True if firstwal_begin wal segment at each node was
    # removed. Returns False if all these segments are kept.
    # Raises exception in other cases.
    def _is_wal_trimmed(self, dsns, lastwal_begin, firsttwal_begin,
                        wals_to_pass=5,
                        iteration_sleep=20,
                        iterations=1000):

        print(lastwal_begin)
        print(firsttwal_begin)
        lastwalp = lastwal_begin.copy()
        timeout = True
        for j in range(iterations):
            time.sleep(iteration_sleep)
            wal_passed = True
            for i in range(len(dsns)):
                lw = self._get_last_wal(dsns[i])
                wal_passed = wal_passed and (
                        int(lw, 16) - int(lastwal_begin[i],
                                          16) >= wals_to_pass)
                if lw > lastwalp[i]:
                    self.nodeExecute(dsns[i], ['CHECKPOINT'])
                    print('node%i: %s\t%s\t%i\t%s' % (
                        i + 1, lw, self._get_first_wal(dsns[i]),
                        int(lw, 16) - int(lastwal_begin[i], 16),
                        lastwal_begin[i]))
                    lastwalp[i] = lw
            if wal_passed:
                timeout = False
                print('%i wals passed!' % wals_to_pass)
                break
        if timeout:
            raise AssertionError('Time is out')
        all_wals_trimmed = True
        all_wals_untrimmed = True
        trimmed_node = []
        for i in range(len(dsns)):
            trimmed_node.append(
                firsttwal_begin[i] < self._get_first_wal(dsns[i]))
            all_wals_trimmed = all_wals_trimmed and trimmed_node[i]
            all_wals_untrimmed = all_wals_untrimmed and not trimmed_node[i]
        if all_wals_trimmed == all_wals_untrimmed == False:
            for i in range(len(trimmed_node)):
                print('node%i: %r' % (i + 1, trimmed_node[i]))
            raise AssertionError('Some wals was trimmed, but some was not')
        elif all_wals_trimmed == all_wals_untrimmed == True:
            raise AssertionError('Unexpected error')
        else:
            return all_wals_trimmed

    def test_syncpoints(self):
        print('### test_syncpoints ###')
        print('Stopping client')
        self.client.stop()
        print('Client stopped')

        # disable fsync for faster test execution
        # checkpoint ensures wal we expect to be removed in the first test is
        # indeed of the workload we've created
        for dsn in self.dsns:
            self.nodeExecute(dsn, ["ALTER SYSTEM SET fsync = 'off'",
                                   "SELECT pg_reload_conf()",
                                   "CHECKPOINT"])
        print('Fsync is turned off')
        time.sleep(5)

        # check that wals are trimmed when everyone is online
        (lastwal_begin, firsttwal_begin) = self._get_wals(self.dsns)
        self.client.bgrun()
        if not self._is_wal_trimmed(self.dsns, lastwal_begin, firsttwal_begin):
            raise AssertionError('Wals are not trimmed in normal mode')

        self.client.stop()
        failure = CrashRecoverNode('node3')
        failure.start()
        (lastwal_begin, firsttwal_begin) = self._get_wals(self.dsns[:2], 3)
        self.client.bgrun()
        if self._is_wal_trimmed(self.dsns[:2], lastwal_begin, firsttwal_begin):
            raise AssertionError('Wals are trimmed in degraded mode')
        failure.stop()
        time.sleep(20)
        self.client.stop()
        self.awaitOnline(self.dsns[2])


if __name__ == '__main__':
    unittest.main()
