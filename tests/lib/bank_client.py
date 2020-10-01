#!/usr/bin/env python3
import asyncio
# import uvloop
import aiopg
import random
import psycopg2
from psycopg2.extensions import *
import time
import datetime
import copy
import aioprocessing
import multiprocessing
import logging
import re
import signal
import pprint
import uuid
import traceback

from . import log_helper  # configures loggers

log = logging.getLogger('root.bank_client')

class MtmTxAggregate(object):

    def __init__(self):
        # self.name = name
        self.isolation = 0  # number of isolation errors, for total_tx
        self.clear_values()

    def clear_values(self):
        self.max_latency = 0.0
        # xact finish status ('commit' or error msg) => counter
        self.finish = {}

    def start_tx(self):
        self.start_time = datetime.datetime.now()

    def finish_tx(self, status):
        latency = (datetime.datetime.now() - self.start_time).total_seconds()

        if "is aborted on node" in status:
            status = re.sub(r'MTM-.+\)', '<censored>', status)

        if latency > self.max_latency:
            self.max_latency = latency

        if status not in self.finish:
            self.finish[status] = 1
        else:
            self.finish[status] += 1

    def __add__(self, other):
        res = MtmTxAggregate()
        res.max_latency = max(self.max_latency, other.max_latency)
        res.isolation = self.isolation + other.isolation
        res.finish = { k: self.finish.get(k, 0) + other.finish.get(k, 0) for k in set(self.finish) | set(other.finish) }
        return res

    def __repr__(self):
        return str(self.as_dict())

    def as_dict(self):
        return {
            # 'running_latency': 'xxx', #(datetime.datetime.now() - self.start_time).total_seconds(),
            'max_latency': self.max_latency,
            'isolation': self.isolation,
            'finish': copy.deepcopy(self.finish)
        }


def keep_trying(tries, delay, method, name, *args, **kwargs):
    for t in range(tries):
        try:
            return method(*args, **kwargs)
        except Exception as e:
            if t == tries - 1:
                raise Exception("%s failed all %d tries" % (name, tries)) from e
            log.info("%s failed [%d of %d]: %s" % (name, t + 1, tries, str(e)))
            time.sleep(delay)
    raise Exception("this should not happen")


class MtmClient(object):

    def __init__(self, dsns, n_accounts=100000):
        # logging.basicConfig(level=logging.DEBUG)
        self.n_accounts = n_accounts
        self.dsns = dsns
        self.running = False

        # self.create_extension()

        self.total = 0
        # each dict is aggname_prefix => list of MtmTxAggregate, one for each
        # coroutine
        self.aggregates = [{} for e in dsns]
        keep_trying(40, 1, self.create_extension, 'self.create_extension')
        keep_trying(40, 1, self.await_nodes, 'self.await_nodes')

        self.initdb()

        log.info('initialized')

        self.nodes_state_fields = ["id", "disabled", "disconnected", "catchUp", "slotLag",
            "avgTransDelay", "lastStatusChange", "oldestSnapshot", "SenderPid",
            "SenderStartTime ", "ReceiverPid", "ReceiverStartTime", "connStr"]
        self.oops = '''
                        . . .
                         \|/
                       `--+--'
                         /|\
                        ' | '
                          |
                          |
                      ,--'#`--.
                      |#######|
                   _.-'#######`-._
                ,-'###############`-.
              ,'#####################`,
             /#########################\
            |###########################|
           |#############################|
           |#############################|
           |#############################|
           |#############################|
            |###########################|
             \#########################/
              `.#####################,'
                `._###############_,'
                   `--..#####..--'
'''

    def initdb(self):
        conn = psycopg2.connect(self.dsns[0])
        try:
            cur = conn.cursor()
            cur.execute('drop table if exists bank_test')
            cur.execute('create table bank_test(uid int primary key, amount int)')
            cur.execute('create table insert_test(id text primary key)')
            cur.execute('''
            insert into bank_test
            select *, 0 from generate_series(0, %s)''',
                        (self.n_accounts,))
            conn.commit()
            cur.close()
        finally:
            conn.close()

    def execute(self, node_id, statements):
        con = psycopg2.connect(self.dsns[node_id])
        try:
            con.autocommit = True
            cur = con.cursor()
            for statement in statements:
                cur.execute(statement)
            cur.close()
        finally:
            con.close()

    def await_nodes(self):
        log.info("await_nodes")

        for dsn in self.dsns:
            con = psycopg2.connect(dsn)
            try:
                con.autocommit = True
                cur = con.cursor()
                # ensure generational perturbations after init have calmed down
                cur.execute('select mtm.ping();')
                cur.close()
            finally:
                con.close()

    def create_extension(self):

        log.info("create extension")

        for dsn in self.dsns:
            con = psycopg2.connect(dsn)
            try:
                con.autocommit = True
                cur = con.cursor()
                cur.execute('create extension if not exists multimaster')
                cur.close()
            finally:
                con.close()

        connstr = " "
        for i in range(len(self.dsns)-1):
            connstr = connstr + "\"dbname=regression user=pg host=192.168.253."+str(i+2) + "\""
            if i < len(self.dsns) - 2:
                connstr = connstr + ", "

        conn = psycopg2.connect(self.dsns[0])
        try:
            cur = conn.cursor()
            cur.execute("select mtm.init_cluster($$%s$$, $${%s}$$);" %
                        ("dbname=regression user=pg host=192.168.253.1", connstr))
            conn.commit()
            cur.close()
        finally:
            conn.close()

    def is_data_identic(self):
        hashes = set()
        hashes2 = set()

        for dsn in self.dsns:
            try:
                con = psycopg2.connect(dsn)
                try:
                    cur = con.cursor()
                    cur.execute("""
                    select
                    md5('(' || string_agg(uid::text || ', ' || amount::text , '),(') || ')')
                    from
                    (select * from bank_test order by uid) t;""")
                    hashes.add(cur.fetchone()[0])

                    cur.execute("""
                    select md5(string_agg(id, ','))
                    from (select id from insert_test order by id) t;""")
                    hashes2.add(cur.fetchone()[0])
                    cur.close()
                finally:
                    con.close()
            except Exception as e:
                log.error("is_data_identic failed to query {}".format(dsn))
                raise e

        log.info("bank_test hashes: {}".format(hashes))
        log.info("insert_test hashes: {}".format(hashes2))
        return (len(hashes) == 1 and len(hashes2) == 1)

    def n_prepared_tx(self):
        total_n_prepared = 0

        for dsn_ind in range(0, len(self.dsns)):
            n_prepared = self.list_prepared(dsn_ind)
            total_n_prepared += n_prepared

        log.info("total num of prepared xacts on all nodes: %d" % total_n_prepared)
        return total_n_prepared

    def list_prepared(self, node_id):
        n_prepared = 0
        con = psycopg2.connect(self.dsns[node_id] + " application_name=mtm_admin")
        log.info("listing prepared xacts on node {} ({}):".format(node_id + 1, self.dsns[node_id]))
        try:
            cur = con.cursor()
            cur.execute('select * from pg_prepared_xacts order by prepared;')
            for pxact in cur.fetchall():
                for i, col in enumerate(["transaction", "gid", "prepared", "owner", "database", "state3pc"]):
                    print(pxact[i], end="\t")
                print("\n")
                n_prepared += 1
        finally:
            con.close()
        log.info("total {} prepared xacts\n----\n".format(n_prepared))
        return n_prepared

    def insert_counts(self):
        counts = []

        for dsn in self.dsns:
            con = psycopg2.connect(dsn)
            try:
                cur = con.cursor()
                cur.execute("select count(*) from insert_test;")
                counts.append(int(cur.fetchone()[0]))
                cur.close()
            finally:
                con.close()

        return counts

    async def status(self):
        while self.running:
            try:
                msg = await self.child_pipe.coro_recv()
                if msg == 'status' or msg == 'status_noclean':
                    serialized_aggs = []

                    for node_id, node_aggs in enumerate(self.aggregates):
                        serialized_aggs.append({})
                        for aggname, aggarray in node_aggs.items():
                            total_agg = MtmTxAggregate()
                            for agg in aggarray:
                                total_agg += agg
                                if msg == 'status':
                                    agg.clear_values()
                            serialized_aggs[node_id][aggname] = total_agg.as_dict()

                    await self.child_pipe.coro_send(serialized_aggs)
                elif msg == 'exit':
                    break
                else:
                    log.warning('evloop: unknown message')
            except EOFError as eoferror:
                log.error('status worker detected eof (driver exited), shutting down the client')
                break

        # End of work. Wait for tasks and stop event loop.
        self.running = False # mark for other coroutines to exit
        tasks = [t for t in asyncio.all_tasks() if t is not asyncio.current_task()]
        await asyncio.gather(*tasks)

        self.loop.stop()


    async def exec_tx(self, tx_block, node_id, aggname_prefix, conn_i):
        conn_name = "node_{}_{}_{}".format(node_id + 1, aggname_prefix, conn_i)

        if aggname_prefix not in self.aggregates[node_id]:
            self.aggregates[node_id][aggname_prefix] = []

        agg = MtmTxAggregate()
        self.aggregates[node_id][aggname_prefix].append(agg)
        dsn = self.dsns[node_id] + " application_name={}".format(conn_name)

        conn = cur = False

        while self.running:
            agg.start_tx()

            try:
                # I've failed to find this in the docs, but testing shows
                # whenever connection fails it gets closed automatically (as
                # well as associated cursors), so there is no need to close
                # conn/cursor in exception handler: just reconnect if it is
                # dead.
                if (not conn) or conn.closed:
                        log.debug("{} connecting".format(conn_name))
                        # enable_hstore tries to perform select from database
                        # which in case of select's failure will lead to exception
                        # and stale connection to the database
                        conn = await aiopg.connect(dsn, enable_hstore=False, timeout=1)
                        log.debug('{} connected'.format(conn_name))

                if (not cur) or cur.closed:
                        # big timeout here is important because on timeout
                        # expiration psycopg tries to call PQcancel() which
                        # tries to create blocking connection to postgres and
                        # blocks evloop
                        cur = await conn.cursor(timeout=3600)

                        # await cur.execute("select pg_backend_pid()")
                        # bpid = await cur.fetchone()
                        # log.debug('{} pid is {}'.format(conn_name, bpid[0]))

                # ROLLBACK tx after previous exception.
                # Doing this here instead of except handler to stay inside try
                # block.
                status = await conn.get_transaction_status()
                if status != TRANSACTION_STATUS_IDLE:
                    await cur.execute('rollback')

                await tx_block(conn, cur, agg, conn_i)
                agg.finish_tx('commit')

            except psycopg2.Error as e:
                msg = str(e).strip()
                log.debug('{} caught psycopg exception {}'.format(conn_name, msg))
                # log.debug('{} caught psycopg exception {}, traceback: {}: {}'.format(conn_name, type(e), msg, traceback.format_exc()))
                agg.finish_tx(msg)
                # Give evloop some free time.
                # In case of continuous excetions we can loop here without returning
                # back to event loop and block it
                await asyncio.sleep(0.5)

            except BaseException as e:
                msg = str(e).strip()
                agg.finish_tx(msg)
                log.error(
                    'Caught exception %s, %s, %d, %s' %
                    (type(e), aggname_prefix, conn_i + 1, msg))

                # Give evloop some free time.
                # In case of continuous excetions we can loop here without returning
                # back to event loop and block it
                await asyncio.sleep(0.5)

        # Close connection during client termination
        # XXX: I tried to be polite and close the cursor beforehand, but it
        # sometimes gives 'close cannot be used while an asynchronous query
        # is underway'.
        # log.debug('{} closing conn and exiting'.format(conn_name))
        if conn:
            conn.close()
        # log.debug('{} conn closed, exiting'.format(conn_name))

    async def transfer_tx(self, conn, cur, agg, conn_i):
        amount = 1
        # to avoid deadlocks:
        from_uid = random.randint(1, self.n_accounts - 2)
        to_uid = from_uid + 1
        await cur.execute('begin')
        await cur.execute('''update bank_test
            set amount = amount - %s
            where uid = %s''',
            (amount, from_uid))
        assert(cur.rowcount == 1)
        await cur.execute('''update bank_test
            set amount = amount + %s
            where uid = %s''',
            (amount, to_uid))
        assert(cur.rowcount == 1)
        await cur.execute('commit')

    async def insert_tx(self, conn, cur, agg, conn_i):
        query = "insert into insert_test values ('%s')" % (uuid.uuid4())
        await cur.execute(query)

    async def total_tx(self, conn, cur, agg, conn_i):
        await cur.execute("select sum(amount), count(*), count(uid) from bank_test")
        total = await cur.fetchone()
        if total[0] != self.total:
            agg.isolation += 1
            log.error('Isolation error, total ', self.total, ' -> ', total[0], ', node ', conn_i+1)
            self.total = total[0]
            # print(self.oops)
            # await cur.execute('select * from pg_prepared_xacts order by prepared;')
            # pxacts = await cur.fetchall()
            # for pxact in pxacts:
            #     for i, col in enumerate(["transaction", "gid", "prepared", "owner", "database", "state3pc"]):
            #         print(pxact[i], end="\t")
            #     print("\n")

    def run(self, numworkers):
        # close the unused end to get eof if parent exits
        self.parent_pipe.close()
        # asyncio.set_event_loop_policy(uvloop.EventLoopPolicy())
        self.loop = asyncio.get_event_loop()
        # set True to show traceback of unhandled asyncio exceptions
        # https://docs.python.org/3/library/asyncio-dev.html
        self.loop.set_debug(enabled=False)

        for i, _ in enumerate(self.dsns):
            for j in range(numworkers['transfer']):
                self.loop.create_task(self.exec_tx(self.transfer_tx, i, 'transfer', j))
            for j in range(numworkers['sumtotal']):
                self.loop.create_task(self.exec_tx(self.total_tx, i, 'sumtotal', 0))
            for j in range(numworkers['inserter']):
                self.loop.create_task(self.exec_tx(self.insert_tx, i, 'inserter', j))

        self.loop.create_task(self.status())

        try:
            self.running = True
            self.loop.run_forever()
        finally:
             self.loop.close()

    def bgrun(self, numworkers=None):
        log.info('starting evloop in different process')

        # default number of workers
        if numworkers is None:
            numworkers = {
                'transfer': 1,
                'sumtotal': 1,
                'inserter': 10
            }
        self.running = True

        self.parent_pipe, self.child_pipe = aioprocessing.AioPipe()
        self.evloop_process = multiprocessing.Process(target=self.run, args=(numworkers,))
        # this will forcibly kill the process instead of (infinitely) waiting
        # for it to die when the main one exits
        self.evloop_process.daemon = True
        self.evloop_process.start()
        # close the unused end to get eof if child exits
        self.child_pipe.close()

    # returns list indexed by node id - 1; each member is dict
    # {agg name (transaction type, e.g. 'transfer') :
    #    dict as returned by as_dict}
    def get_aggregates(self, _print=True, clean=True):
        if clean:
            self.parent_pipe.send('status')
        else:
            self.parent_pipe.send('status_noclean')

        resp = self.parent_pipe.recv()

        if _print:
            MtmClient.print_aggregates(resp)
        return resp

    def clean_aggregates(self):
        self.parent_pipe.send('status')
        self.parent_pipe.recv()
        log.info('aggregates cleaned')

    def stop(self):
        if self.evloop_process is None:
            return
        log.info('stopping client')
        self.parent_pipe.send('exit')
        self.evloop_process.join()
        self.evloop_process = None
        log.info('client stopped')

    @classmethod
    def print_aggregates(cls, aggs):
            columns = ['max_latency', 'isolation', 'finish']

            # print table header
            print("\t\t", end="")
            for col in columns:
                print(col, end="\t")
            print("\n", end="")

            for conn_id, agg_conn in enumerate(aggs):
                for aggname, agg in agg_conn.items():
                    print("Node %d: %s\t" % (conn_id + 1, aggname), end="")
                    for col in columns:
                        if isinstance(agg[col], float):
                            print("%.2f\t" % (agg[col],), end="\t")
                        else:
                            print(agg[col], end="\t")
                    print("")
            print("")


if __name__ == "__main__":
    c = MtmClient(['dbname=regression user=postgres host=127.0.0.1 port=15432',
        'dbname=regression user=postgres host=127.0.0.1 port=15433'], n_accounts=10000)
    c.bgrun()
    while True:
        time.sleep(5)
        print('='*80)
        aggs = c.get_aggregates()
        # MtmClient.print_aggregates(aggs)
