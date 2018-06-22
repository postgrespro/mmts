#!/usr/bin/env python

from mm_cluster import Cluster


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

    jshell = """
/env --class-path /usr/share/java/postgresql-jdbc/postgresql-jdbc4.jar

import java.sql.*;
Class.forName("org.postgresql.Driver");

int port1 = {};
int port2 = {};
int port3 = {};
String connstring = String.format("jdbc:postgresql://localhost:%d,localhost:%d,localhost:%d/postgres", port1, port2, port3);

/* connect to DB */
Connection con = DriverManager.getConnection(connstring);

/* show help */
System.out.println("Use 'con' object!");

/* execute some commands */
System.out.println("Execute 'SELECT 1'");
Statement stmt = con.createStatement();
ResultSet rs = stmt.executeQuery("select 1");
rs.next();
String s = rs.getString(1);
System.out.println("result = " + s);
    """.format(cluster.nodes[0].port,
               cluster.nodes[1].port,
               cluster.nodes[2].port)

    with open('connect.jsh', 'w') as f:
        f.write(jshell)
        print("Now run jshell with connect.jsh")
        print()

    print("Press ctrl+C to exit")

    while True:
        import time
        time.sleep(1)
