package org.sunddenly.hive;

import java.sql.Connection;
import java.sql.DriverManager;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.sql.Statement;

public class HiveTest {
	public static void main(String[] args) throws ClassNotFoundException, SQLException {
		Class.forName("org.apache.hadoop.hive.jdbc.HiveDriver");
		Connection con = DriverManager.getConnection("jdbc:hive://192.168.1.109:10000/default", "hive", "hadoop");
		Statement stmt = con.createStatement();
		String querySQL="SELECT * FROM default.loginfo";
		ResultSet res = stmt.executeQuery(querySQL);  
		while (res.next()) {
		System.out.println(res.getString(1));
		}
	}
}

