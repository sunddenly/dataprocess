package org.hebut.driver;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;
import org.hebut.chk.CHKMapper;
import org.hebut.chk.CHKReducer;
import org.hebut.ssp.*;
import org.hebut.util.CHKUtil;

public class SSPAlgorithm{
	
	public static void main(String[] args) throws Exception{
		int iterator = 1;
		String chkpath;
		do{
			Job sspJob = getSSPJob(getSSPInpath(iterator),getSSPOutpath(iterator));
			sspJob.waitForCompletion(true);
			Job chkjob = getCHKJob(getCHKInpath(iterator),getCHKOutpath(iterator));
			chkjob.waitForCompletion(true);
			chkpath =getCHKOutpath(iterator)+"/part-r-00000";
			iterator ++;
		}while(!CHKUtil.canStop(chkpath));
	}
	public static Job getSSPJob(String inpath,String outpath)throws Exception{
		Configuration conf = new Configuration();
		Job job = new Job(conf, "sssp");
		job.setJarByClass(SSPAlgorithm.class);
		job.setMapperClass(SSPMapper.class);
		job.setReducerClass(SSPReducer.class);
		job.setOutputKeyClass(IntWritable.class);
		job.setOutputValueClass(Text.class);
		FileInputFormat.addInputPath(job, new Path(inpath));
		FileOutputFormat.setOutputPath(job, new Path(outpath));
		return job;
	}
	public static Job getCHKJob(String inpath,String outpath)throws Exception{
		Configuration conf = new Configuration();
		Job job = new Job(conf, "chk");
		job.setJarByClass(SSPAlgorithm.class);
		job.setMapperClass(CHKMapper.class);
		job.setReducerClass(CHKReducer.class);
		job.setOutputKeyClass(IntWritable.class);
		job.setOutputValueClass(IntWritable.class);
		FileInputFormat.addInputPath(job, new Path(inpath));
		FileOutputFormat.setOutputPath(job, new Path(outpath));
		return job;
	}
	public static String getSSPInpath(int iterator){
		if(iterator == 1){
			return "hdfs://hadoop0:9000/user/root/input";
		}
		else{
			return "hdfs://hadoop0:9000/user/root/output" + (iterator - 1);
		}
	}
	public static String getSSPOutpath(int iterator){
		return "hdfs://hadoop0:9000/user/root/output" + iterator;
	}
	public static String getCHKInpath(int iterator){
		return "hdfs://hadoop0:9000/user/root/output" + iterator;
	}
	public static String getCHKOutpath(int iterator){
		return "hdfs://hadoop0:9000/user/root/chkout" + iterator;
	}
}