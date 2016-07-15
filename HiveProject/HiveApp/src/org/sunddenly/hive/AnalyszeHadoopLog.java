package org.sunddenly.hive;

import java.sql.ResultSet;
import java.sql.SQLException;

public class AnalyszeHadoopLog {
	public static void main(String[] args) throws SQLException {
		StringBuffer sql=new StringBuffer();
		
		// 第一步：在Hive中创建表
		sql.append("create table if not exists loginfo(rdate string,");
		sql.append("time array<string>,type string,relateclass string,");
		sql.append("information1 string,information2 string,information3 ");
		sql.append("string) row format delimited fields terminated by ' '");
		sql.append(" collection items terminated by ','");
		sql.append(" map keys terminated by ':'");
		
		System.out.println(sql);
		HiveUtil.createTable(sql.toString());
		// 第二步：加载Hadoop日志文件
		sql.delete(0, sql.length());
		sql.append("load data local inpath ");
		sql.append("'/home/hadoop/hadoop.log'");
		sql.append(" overwrite into table loginfo");
		System.out.println(sql);
		HiveUtil.loadData(sql.toString());
		
		// 第三步：查询有用信息
		sql.delete(0, sql.length());
		sql.append("select rdate,time[0],type,relateclass,");
		sql.append("information1,information2,information3 ");
		sql.append("from loginfo where type='WARN'");
		
		ResultSet res = HiveUtil.queryData(sql.toString());
		
		// 第四步：查出的信息经过变换后保存到MySQL中
		HiveUtil.hiveToMySQL(res);
		// 第五步：关闭Hive连接
		DBHelper.closeHiveConn();
		// 第六步：关闭MySQL连接
		DBHelper.closeMySQLConn();
	}
}
