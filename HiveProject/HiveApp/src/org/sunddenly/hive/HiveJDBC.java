package org.sunddenly.hive;

import java.sql.Connection;
import java.sql.DriverManager;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.sql.Statement;

import org.apache.hadoop.hive.ql.parse.HiveParser.sysFuncNames_return;

public class HiveJDBC {
	private static final String DRIVERNAME="org.apache.hadoop.hive.jdbc.HiveDriver";
	private static String sql="";
	private static ResultSet res;
	private static String TableName="userinfo";
	public static void main(String[] args) throws ClassNotFoundException, SQLException {
		
		//建立连接
		Class.forName(DRIVERNAME);
		Connection con = DriverManager.getConnection("jdbc:hive://192.168.1.109:10000/default","hive","hadoop");
		Statement stat = con.createStatement();
		//创建Hive表
		/** 表存在则删除*/
		sql="drop table " +TableName;
		stat.executeQuery(sql);
		/** 不存在则创建*/
		sql="create table "+TableName+" (Key int,Value string) ";
		sql+=" row format delimited fields terminated by '\t'";
		stat.executeQuery(sql);
		
		//执行show tables操作
		sql="show tables '" +TableName+ "'";
		System.out.println("Running :"+sql);
		res=stat.executeQuery(sql);
		System.out.println("执行Show Tables的运行结果为:");
		while(res.next()){
			System.out.println(res.getString(1));
		}
		
		//执行describe Table操作
		sql="describe "+TableName;
		System.out.println("Running :"+sql);
		res=stat.executeQuery(sql);
		System.out.println("执行Describe Table的运行结果:");
		while(res.next()){
			System.out.println(res.getString(1)+"\t"+res.getString(2));
		}
		//执行load data into table 操作
		String inpathfile="/home/hadoop/userinfo.txt";
		sql="load data local inpath '" +inpathfile+ "' into table "+TableName;
		System.out.println("Running :"+sql);
		stat.executeQuery(sql);
				
		//执行Select Query操作
		sql="select * from "+TableName;
		System.out.println("Running :"+sql);
		res=stat.executeQuery(sql);
		System.out.println("执行 select * query运行结果:");
		while(res.next()){
			System.out.println(res.getString(1)+"\t"+res.getString(2));
		}
		
		//执行Regular Hive Select操作
		sql="select count(1) from "+TableName;
		System.out.println("Running :"+sql);
		res=stat.executeQuery(sql);
		System.out.println("执行 Regular Hive Query运行结果:");
		while(res.next()){
			System.out.println(res.getString(1));
		}

	}
}
