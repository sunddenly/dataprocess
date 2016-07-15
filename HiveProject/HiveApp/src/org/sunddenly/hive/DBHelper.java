package org.sunddenly.hive;

import java.sql.Connection;
import java.sql.DriverManager;
import java.sql.SQLException;

public class DBHelper {
	/**
	 * 该类主要功能是负责建立Hive和MySQL的连接
	 * 由于每个连接的开销比较大，所以此类的设计采用设计模式的单件模式
	 */
	private static Connection conToHive=null;
	private static Connection conToMySQL=null;
	
	private DBHelper(){}
	//获得与Hive连接，如果连接已经初始化，则直接返回
	public static Connection getHiveConn() throws SQLException{
		try{
			Class.forName("org.apache.hadoop.hive.jdbc.HiveDriver");
		}catch(ClassNotFoundException err){
			err.printStackTrace();
			System.exit(1);
		}
		conToHive=DriverManager.getConnection("jdbc:hive://192.168.1.109:10000/defalut","hive","hadoop");
		return conToHive;
	}
	//获得与MySQL连接
	public static Connection getMySQLConn() throws SQLException{
		if(conToMySQL==null){
			try{
				Class.forName("com.mysql.jdbc.Driver");
			}catch(ClassNotFoundException err){
				err.printStackTrace();
				System.exit(1);
			}
			conToMySQL=DriverManager.getConnection("jdbc:mysql://192.168.1.109:3306/hive?useUnicode=true&characterEncoding=UTF-8","root","hadoop");
		}
		return conToMySQL;
	}
	public static void closeHiveConn() throws SQLException{
		if(conToHive!=null){
			conToHive.close();
		}
	}
	public static void closeMySQLConn() throws SQLException{
		if(conToMySQL==null){
			conToMySQL.close();
		}
	}
}
