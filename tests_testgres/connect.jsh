
/env --class-path /usr/share/java/postgresql-jdbc/postgresql-jdbc4.jar

import java.sql.*;
Class.forName("org.postgresql.Driver");

int port1 = 12928;
int port2 = 16682;
int port3 = 18521;
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
    