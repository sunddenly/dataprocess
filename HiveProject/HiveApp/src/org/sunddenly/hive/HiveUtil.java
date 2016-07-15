package org.sunddenly.hive;

import java.sql.Connection;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.sql.Statement;

/**
 * 针对Hive的工具类
 */
public class HiveUtil {
	//创建表
	public static void createTable(String sql) throws SQLException{
		Connection hiveConn = DBHelper.getHiveConn();
		Statement stat=hiveConn.createStatement();
		stat.executeQuery(sql);
	}
	//依据条件查询数据
	public static ResultSet queryData(String sql) throws SQLException{
		Connection hiveConn = DBHelper.getHiveConn();
		Statement stat = hiveConn.createStatement();
		ResultSet res = stat.executeQuery(sql);
		return res;
	}
	//加载数据
	public static void loadData(String sql) throws SQLException{
		Connection hiveConn = DBHelper.getHiveConn();
		Statement stat = hiveConn.createStatement();
		stat.executeQuery(sql);
	}
	//把数据存储到mySQL中
	public static void hiveToMySQL(ResultSet res) throws SQLException{
		Connection mySQLConn = DBHelper.getMySQLConn();
		Statement stat = mySQLConn.createStatement();
		while(res.next()){
			String date = res.getString(1);
			String time = res.getString(2);
			String type = res.getString(3);
			String relateClass = res.getString(4);
			String information=res.getString(5)+res.getString(6)+res.getString(7);
			
			if(date!=null){
				StringBuffer sql = new StringBuffer();
				sql.append("insert into hadooplog values(0,'");
				sql.append(date + "','");
				sql.append(time + "','");
				sql.append(type + "','");
				sql.append(relateClass + "','");
				sql.append(information + "')");
				int i = stat.executeUpdate(sql.toString());
			}
		}
	}
}
