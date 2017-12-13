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
import pprint
import uuid

class MtmTxAggregate(object):

    def __init__(self):
        # self.name = name
        self.isolation = 0
        self.clear_values()

    def clear_values(self):
        self.max_latency = 0.0
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
            print("%s failed [%d of %d]: %s" % (name, t + 1, tries, str(e)))
            time.sleep(delay)
    raise Exception("this should not happen")

class MtmClient(object):

    def __init__(self, dsns, n_accounts=100000):
        # logging.basicConfig(level=logging.DEBUG)
        self.n_accounts = n_accounts
        self.dsns = dsns
        self.total = 0
        self.aggregates = [{} for e in dsns]
        keep_trying(40, 1, self.initdb, 'self.initdb')
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
        cur = conn.cursor()
        cur.execute('create extension if not exists multimaster')
        conn.commit()
        cur.execute('drop table if exists bank_test')
        cur.execute('create table bank_test(uid int primary key, amount int)')
        cur.execute('create table insert_test(id text primary key)')
        cur.execute('''
                insert into bank_test
                select *, 0 from generate_series(0, %s)''',
                (self.n_accounts,))
        conn.commit()
        cur.close()
        conn.close()

    def execute(self, node_id, statements):
        con = psycopg2.connect(self.dsns[node_id])
        con.autocommit = True
        cur = con.cursor()
        for statement in statements:
            cur.execute(statement)
        cur.close()
        con.close()

    def is_data_identic(self):
        hashes = set()

        for dsn in self.dsns:
            con = psycopg2.connect(dsn)
            cur = con.cursor()
            cur.execute("""
                select
                    md5('(' || string_agg(uid::text || ', ' || amount::text , '),(') || ')')
                from
                    (select * from bank_test order by uid) t;""")
            hashes.add(cur.fetchone()[0])
            cur.close()
            con.close()

        print(hashes)
        return (len(hashes) == 1)

    def no_prepared_tx(self):
        n_prepared = 0

        for dsn in self.dsns:
            con = psycopg2.connect(dsn)
            cur = con.cursor()
            cur.execute("select count(*) from pg_prepared_xacts;")
            n_prepared += int(cur.fetchone()[0])
            cur.close()
            con.close()

        print("n_prepared = %d" % (n_prepared))
        return (n_prepared)

    def insert_counts(self):
        counts = []

        for dsn in self.dsns:
            con = psycopg2.connect(dsn)
            cur = con.cursor()
            cur.execute("select count(*) from insert_test;")
            counts.append(int(cur.fetchone()[0]))
            cur.close()
            con.close()

        return counts

    @asyncio.coroutine
    def status(self):
        while self.running:
            msg = yield from self.child_pipe.coro_recv()
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

                yield from self.child_pipe.coro_send(serialized_aggs)
            else:
                print('evloop: unknown message')

    @asyncio.coroutine
    def exec_tx(self, tx_block, node_id, aggname_prefix, conn_i):
        # aggname = "%i_%s_%i" % (aggname_prefix, conn_i)

        if aggname_prefix not in self.aggregates[node_id]:
            self.aggregates[node_id][aggname_prefix] = []

        agg = MtmTxAggregate()
        self.aggregates[node_id][aggname_prefix].append(agg)
        dsn = self.dsns[node_id]

        conn = cur = False

        while self.running:
            agg.start_tx()

            try:
                if (not conn) or conn.closed:
                        # enable_hstore tries to perform select from database
                        # which in case of select's failure will lead to exception
                        # and stale connection to the database
                        conn = yield from aiopg.connect(dsn, enable_hstore=False, timeout=1)
                        print('Connected %s, %d' % (aggname_prefix, conn_i + 1) )

                if (not cur) or cur.closed:
                        # big timeout here is important because on timeout
                        # expiration psycopg tries to call PQcancel() which
                        # tries to create blocking connection to postgres and
                        # blocks evloop
                        cur = yield from conn.cursor(timeout=3600)

                # ROLLBACK tx after previous exception.
                # Doing this here instead of except handler to stay inside try
                # block.
                status = yield from conn.get_transaction_status()
                if status != TRANSACTION_STATUS_IDLE:
                    yield from cur.execute('rollback')

                yield from tx_block(conn, cur, agg, conn_i)
                agg.finish_tx('commit')

            except psycopg2.Error as e:
                msg = str(e).strip()
                agg.finish_tx(msg)
                # Give evloop some free time.
                # In case of continuous excetions we can loop here without returning
                # back to event loop and block it
                yield from asyncio.sleep(0.5)

            except BaseException as e:
                msg = str(e).strip()
                agg.finish_tx(msg)
                print('Caught exception %s, %s, %d, %s' % (type(e), aggname_prefix, conn_i + 1, msg) )

                # Give evloop some free time.
                # In case of continuous excetions we can loop here without returning
                # back to event loop and block it
                yield from asyncio.sleep(0.5)

        print("We've count to infinity!")

    @asyncio.coroutine
    def transfer_tx(self, conn, cur, agg, conn_i):
        amount = 1
        # to avoid deadlocks:
        from_uid = random.randint(1, self.n_accounts - 2)
        to_uid = from_uid + 1
        yield from cur.execute('begin')
        yield from cur.execute('''update bank_test
            set amount = amount - %s
            where uid = %s''',
            (amount, from_uid))
        assert(cur.rowcount == 1)
        yield from cur.execute('''update bank_test
            set amount = amount + %s
            where uid = %s''',
            (amount, to_uid))
        assert(cur.rowcount == 1)
        yield from cur.execute('commit')

    @asyncio.coroutine
    def insert_tx(self, conn, cur, agg, conn_i):
        query = "insert into insert_test values ('%s')" % (uuid.uuid4())
        yield from cur.execute(query)

    @asyncio.coroutine
    def total_tx(self, conn, cur, agg, conn_i):
        yield from cur.execute("select sum(amount), count(*), count(uid), current_setting('multimaster.node_id') from bank_test")
        total = yield from cur.fetchone()
        if total[0] != self.total:
            agg.isolation += 1
            print(datetime.datetime.utcnow(), 'Isolation error, total ', self.total, ' -> ', total[0], ', node ', conn_i+1)
            self.total = total[0]
            print(self.oops)
            # yield from cur.execute('select * from mtm.get_nodes_state()')
            # nodes_state = yield from cur.fetchall()
            # for i, col in enumerate(self.nodes_state_fields):
            #     print("%17s" % col, end="\t")
            #     for j in range(3):
            #          print("%19s" % nodes_state[j][i], end="\t")
            #     print("\n")

    def run(self):
        # asyncio.set_event_loop_policy(uvloop.EventLoopPolicy())
        self.loop = asyncio.get_event_loop()

        for i, _ in enumerate(self.dsns):
            for j in range(1):
                asyncio.ensure_future(self.exec_tx(self.transfer_tx, i, 'transfer', j))
            asyncio.ensure_future(self.exec_tx(self.total_tx, i, 'sumtotal', 0))
            for j in range(2):
                asyncio.ensure_future(self.exec_tx(self.insert_tx, i, 'inserter', j))

        asyncio.ensure_future(self.status())

        self.loop.run_forever()

    def bgrun(self):
        print('Starting evloop in different process')

        self.running = True

        self.parent_pipe, self.child_pipe = aioprocessing.AioPipe()
        self.evloop_process = multiprocessing.Process(target=self.run, args=())
        self.evloop_process.start()

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

    def stop(self):
        self.running = False
        self.evloop_process.terminate()
        time.sleep(3)

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
