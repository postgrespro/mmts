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
import logging.config
import logging

from lib.bank_client import MtmClient
from lib.failure_injector import *
from lib.test_helper import *
logging.config.fileConfig('lib/logging.conf')
log = logging.getLogger('syncpoint')


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

        # ohoh
        th = TestHelper()
        th.client = cls.client

        th.assertDataSync()
        cls.client.stop()

    def setUp(self):
        warnings.simplefilter("ignore", ResourceWarning)
        time.sleep(20)
        log.info('Start new test')

    def tearDown(self):
        log.info('Finish test')

    # Returns the newest wal
    def _get_last_wal(self, dsn):
        return self.nodeSelect(dsn, "SELECT name FROM pg_ls_waldir() WHERE "
                                    "name ~ '^[0-9A-F]+$' ORDER BY "
                                    "name DESC LIMIT 1")[0][0]

    def _get_last_wals(self, dsns):
        return [self._get_last_wal(dsn) for dsn in dsns]

    # Returns the oldest existing wal
    def _get_first_wal(self, dsn):
        # recycle old segments
        self.nodeExecute(dsn, ["CHECKPOINT"])
        return self.nodeSelect(dsn, "SELECT name FROM pg_ls_waldir() WHERE "
                                    "name ~ '^[0-9A-F]+$' ORDER BY "
                                    "name LIMIT 1")[0][0]

    def _get_first_wals(self, dsns):
        return [self._get_first_wal(dsn) for dsn in dsns]

    # get restart_lsn segment of slot to the given node
    def _get_slot_wal(self, dsn, recepient):
        return self.nodeSelect(dsn, """
        SELECT pg_walfile_name(restart_lsn)
        FROM pg_replication_slots WHERE slot_name = 'mtm_slot_{}'
        """.format(recepient))[0][0]

    def _get_slot_wals(self, dsns, recepient):
        return [self._get_slot_wal(dsn, recepient) for dsn in dsns]

    # Waits (up to iterations * iteration_sleep seconds)
    # until at least wals_to_pass segments appear on each node
    def _wait_wal(self, dsns, wals_to_pass=5,
                  iteration_sleep=20,
                  iterations=1000):
        last_wals_initial = self._get_last_wals(dsns)
        log.debug("waiting for wal, last_wals_initial={}, first_wals={}"
                  .format(last_wals_initial, self._get_first_wals(dsns)))
        for j in range(iterations):
            time.sleep(iteration_sleep)
            last_wals = self._get_last_wals(dsns)
            log.debug("waiting for wal, last_wals={}, first_wals={}"
                      .format(last_wals, self._get_first_wals(dsns)))
            # xxx: this is only correct for first 4GB of WAL due to the hole in
            # WAL file naming
            if all(int(lw, 16) - int(lw_i, 16) >= wals_to_pass
                   for (lw_i, lw) in zip(last_wals_initial, last_wals)):
                return

        raise AssertionError('timed out while waiting for wal')

    def _chk_rec_trim(self, dsn, other_dsns, iteration_sleep=2,
                      iterations=1000):
        log.info('checking if wals were trimmed during recovery')
        dsns = other_dsns + [dsn]
        first_wals_before = self._get_first_wals(dsns)
        first_wals = []
        wals_trimmed = False
        status = ''
        for j in range(iterations):
            time.sleep(iteration_sleep)
            last_wals = self._get_last_wals(dsns)
            first_wals = self._get_first_wals(dsns)
            status = self.nodeSelect(dsn,
                                     'SELECT status from mtm.status()')[0][0]
            log.debug("status: %s" % status)
            log.debug('first wals - %s, ' % first_wals)
            log.debug('last wals - %s' % last_wals)
            if status == 'online':
                break
            wals_trimmed = wals_trimmed or all(b<a for (b,a) in zip(
                first_wals_before, first_wals))
        if status != 'online':
            raise Exception('timed out waiting for online status')
        if not wals_trimmed:
            raise Exception(
                'wals were not trimmed during recovery, fw_before: %s, fw: %s'
                % (first_wals_before, first_wals))
        return

    def test_syncpoints(self):
        log.info('### test_syncpoints ###')
        self.client.stop()

        # disable fsync for faster test execution
        # checkpoint ensures wal we expect to be removed in the first test is
        # indeed of the workload we've created
        for dsn in self.dsns:
            self.nodeExecute(dsn, ["ALTER SYSTEM SET fsync = 'off'",
                                   "SELECT pg_reload_conf()",
                                   "CHECKPOINT"])
        log.debug('fsync is turned off')
        time.sleep(5)

        # check that wals are trimmed when everyone is online
        first_wals_before = self._get_first_wals(self.dsns)
        self.client.bgrun()
        # Note that _get_first_wals called inside for logging purposes has
        # useful side effect: checkpoint recycles WAL and at the same time logs
        # xl_running_xacts for future advancement. With default settings
        # checkpoints may occur too rarely to pass assert.
        self._wait_wal(self.dsns)
        first_wals_after = self._get_first_wals(self.dsns)
        if not all(b < a for (b, a) in zip(first_wals_before, first_wals_after)):
            raise AssertionError('segments on some nodes were not trimmed in normal mode: before={}, after={}'.format(first_wals_before, first_wals_after))


        # now check that wal is preserved if some node is offline
        self.client.stop()
        failure = CrashRecoverNode('node3')
        log.info('putting node 3 down')
        failure.start()
        # getting first_wals here would be too strict -- unlikely, but probably
        # there is some WAL which is not needed by offline node
        slot_wals_before = self._get_slot_wals(self.dsns[:2], 3)
        self.client.bgrun()
        self._wait_wal(self.dsns[:2], 10)
        first_wals_after = self._get_first_wals(self.dsns[:2])
        if not all(b >= a for (b, a) in zip(slot_wals_before, first_wals_after)):
            raise AssertionError('segments on some nodes were trimmed in degraded mode: before={}, after={}'.format(slot_wals_before, first_wals_after))

        log.info('getting node 3 up')
        failure.stop()
        # This allows to connect to MM node during recovery
        recovery_dsn = self.dsns[2]+' application_name=mtm_admin'
        # Wait for node becomes accessible (in recovery mode)
        self.awaitOnline(recovery_dsn)
        self._chk_rec_trim(recovery_dsn, self.dsns[:2])
        self.awaitOnline(self.dsns[2])
        # Now stop client
        self.client.stop()


if __name__ == '__main__':
    unittest.main()
