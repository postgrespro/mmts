#!/usr/bin/env python
#
# Multimaster testing framework based on testgres.
# Copyright (c) 2017, Postgres Professional
#
# Execute this file in order to run 3-node mm cluster

import time
import os

from testgres import PostgresNode, NodeStatus, NodeBackup
from testgres import reserve_port, release_port
from testgres.defaults import default_username
from testgres.enums import XLogMethod

DEFAULT_XLOG_METHOD = XLogMethod.fetch

# track important changes
__version__ = 0.1


class Cluster(object):
    def __init__(self,
                 nodes,
                 max_nodes=None,
                 dbname='postgres',
                 username=default_username(),
                 max_connections=100):

        max_nodes = max_nodes or nodes
        assert(max_nodes >= nodes)
        assert(nodes >= 1)

        # maximum amount of nodes
        self.max_nodes = max_nodes

        # list of ClusterNodes
        self.nodes = []

        # connection settings
        self.dbname = dbname
        self.username = username

        # generate pairs of ports for multimaster
        self.ports = [(reserve_port(), reserve_port()) for _ in range(nodes)]

        # generate connection string
        conn_strings = self._build_mm_conn_strings(self.ports,
                                                   dbname,
                                                   username)

        for i in range(nodes):
            pg_port, mm_port = self.ports[i]

            node_id = i + 1
            node = ClusterNode(name=''.join(['node_', str(node_id)]),
                               pg_port=pg_port,
                               mm_port=mm_port)

            node.init().mm_init(node_id,
                                max_nodes,
                                conn_strings,
                                max_connections)

            self.nodes.append(node)

    @staticmethod
    def _get_mm_conn_string():
        return (
            "host=127.0.0.1 "
            "dbname={} "
            "user={} "
            "port={} "
            "arbiter_port={}"
        )

    @staticmethod
    def _build_mm_conn_strings(ports, dbname, username):
        return ','.join([
            Cluster
            ._get_mm_conn_string()
            .format(dbname,
                    username,
                    pg_port,
                    mm_port) for pg_port, mm_port in ports
        ])

    def __enter__(self):
        return self

    def __exit__(self, type, value, traceback):
        self.cleanup()

    def free_ports(self):
        for p1, p2 in self.ports:
            release_port(p1)
            release_port(p2)

    def free_nodes(self):
        for node in self.nodes:
            node.cleanup()

    def cleanup(self):
        self.free_nodes()
        self.free_ports()

        return self

    def start(self):
        for node in self.nodes:
            node.start()

        return self

    def stop(self):
        for node in self.nodes:
            node.stop()

        return self

    def restart(self):
        for node in self.nodes:
            node.restart()

        return self

    def reload(self):
        for node in self.nodes:
            node.reload()

        return self

    def print_conninfo(self):
        print(self._build_mm_conn_strings(self.ports,
                                            self.dbname,
                                            self.username))

    def install(self):
        for node in self.nodes:
            node.safe_psql("create extension multimaster;",
                         dbname=self.dbname,
                         username=self.username)

        return self

    def add_node(self):
        if len(self.nodes) == self.max_nodes:
            raise Exception("max amount of nodes reached ({})"
                            .format(self.max_nodes))

        pg_port, mm_port = reserve_port(), reserve_port()
        node_id = len(self.nodes) + 1

        # request multimaster config changes
        conn_string = self._get_mm_conn_string().format(self.dbname,
                                                        self.username,
                                                        pg_port,
                                                        mm_port)
        add_node_cmd = "select mtm.add_node('{}')".format(conn_string)
        self.execute_any(dbname=self.dbname,
                         username=self.username,
                         query=add_node_cmd,
                         commit=True)

        # copy node with new ports
        backup = self.node_any().backup()
        node = backup.spawn_primary(name=''.join(['node_', str(node_id)]),
                                    node_id=node_id,
                                    pg_port=pg_port,
                                    mm_port=mm_port)

        # register node and its ports
        self.nodes.append(node)
        self.ports.append((pg_port, mm_port))

        # build new connection strings
        conn_strings = self._build_mm_conn_strings(self.ports,
                                                   self.dbname,
                                                   self.username)

        # patch connection strings
        for node in self.nodes:
            node.append_conf("postgresql.conf", "\n")
            node.append_conf("postgresql.conf",
                             "multimaster.conn_strings = '{}'"
                             .format(conn_strings))

        # finally start it
        node.start()

        return self

    def execute_any(self, dbname, query, username=None, commit=False):
        return self.node_any().execute(dbname=dbname,
                                       username=username,
                                       query=query,
                                       commit=commit)

    def await_online(self, node_ids):
        # await for heartbeat timeout
        time.sleep(5)

        for node_id in node_ids:
            self.nodes[node_id].poll_query_until(dbname=self.dbname,
                                    username=self.username,
                                    query="select true",
                                    raise_programming_error=False,
                                    raise_internal_error=False,
                                    expected=True)
        return self

    def node_any(self, status=NodeStatus.Running):
        for node in self.nodes:
            if node.status():
                return node

        raise Exception("at least one node must be running")


class ClusterNodeBackup(NodeBackup):
    def __init__(self,
                 node,
                 base_dir=None,
                 username=None,
                 xlog_method=DEFAULT_XLOG_METHOD):

        super(ClusterNodeBackup, self).__init__(node,
                                                base_dir=base_dir,
                                                username=username,
                                                xlog_method=xlog_method)

    def spawn_primary(self, name, node_id, pg_port, mm_port, destroy=True):
        base_dir = self._prepare_dir(destroy)

        # build a new PostgresNode
        node = ClusterNode(name=name,
                           base_dir=base_dir,
                           master=self.original_node,
                           pg_port=pg_port,
                           mm_port=mm_port)

        node.append_conf("postgresql.conf", "\n")
        node.append_conf("postgresql.conf",
                         "port = {}".format(pg_port))
        node.append_conf("postgresql.conf",
                         "multimaster.arbiter_port = {}".format(mm_port))
        node.append_conf("postgresql.conf",
                         "multimaster.node_id = {}".format(node_id))

        return node

    def spawn_replica(self, name):
        raise Exception("not implemented yet")


class ClusterNode(PostgresNode):
    def __init__(self,
                 name,
                 pg_port,
                 mm_port,
                 base_dir=None,
                 use_logging=False,
                 master=None):

        if base_dir is None:
            base_dir = os.path.dirname(os.path.abspath(__file__))

        super(ClusterNode, self).__init__(name=name,
                                          port=pg_port,
                                          base_dir=base_dir + '/../tmp_check/' + name)

        self.mm_port = mm_port

    def teardown(self):
        self.stop(['-c', 'immediate'])

    def mm_init(self, node_id, max_nodes, conn_strings, max_connections):
        mm_port = self.mm_port

        conf_lines = (
            "shared_preload_libraries='multimaster'\n"

            "max_connections = {0}\n"
            "max_prepared_transactions = {1}\n"
            "max_worker_processes = {2}\n"

            "wal_level = logical\n"
            "max_wal_senders = {5}\n"
            "max_replication_slots = {5}\n"

            "multimaster.conn_strings = '{3}'\n"
            "multimaster.arbiter_port = {4}\n"
            "multimaster.max_nodes = {5}\n"
            "multimaster.node_id = {6}\n"

        ).format(max_connections,
                 max_connections * max_nodes,
                 (max_connections + 3) * max_nodes,
                 conn_strings,
                 mm_port,
                 max_nodes,
                 node_id)

        self.append_conf("postgresql.conf", conf_lines)

        return self

    def backup(self, username=None, xlog_method=DEFAULT_XLOG_METHOD):
        return ClusterNodeBackup(node=self,
                                 username=username,
                                 xlog_method=xlog_method)


if __name__ == "__main__":
    import os

    if os.environ.get('PG_CONFIG') is None and \
       os.environ.get('PG_BIN') is None:

        # Do you rely on system PostgreSQL?
        print("WARNING: both PG_CONFIG and PG_BIN are not set")

    # Start mm cluster
    with Cluster(3).start().install() as cluster:
        print("Cluster is working")

        node_id = 0
        for node in cluster.nodes:
            node_id += 1

            print("Node #{}".format(node_id))
            print("\t-> port: {}".format(node.port))
            print("\t-> arbiter port: {}".format(node.mm_port))
            print("\t-> dir: {}".format(node.base_dir))
            print()

        print("Press ctrl+C to exit")

        # Sleep forever
        while True:
            import time
            time.sleep(1)
