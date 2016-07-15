package org.mapreduce.partition;

import java.io.DataInput;
import java.io.DataOutput;
import java.io.IOException;
import java.net.URI;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.io.Writable;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.Reducer;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.input.TextInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;
import org.apache.hadoop.mapreduce.lib.output.TextOutputFormat;
import org.apache.hadoop.mapreduce.lib.partition.HashPartitioner;

public class phonePartition {
	static final String INPUT_PATH = "hdfs://hadoop:9000/phoneInput";
	static final String OUT_PATH = "hdfs://hadoop:9000/phoneOutput";
	static final Path inpath=new Path(INPUT_PATH);
	static final Path outpath=new Path(OUT_PATH);
	public static void main(String[] args) throws Exception{
		Configuration conf = new Configuration();
		FileSystem fileSystem=FileSystem.get(new URI(INPUT_PATH),conf);
		if(fileSystem.exists(outpath)){
			fileSystem.delete(outpath,true);
			
		}
		//job初始化
		Job job = new Job(conf, phonePartition.class.getSimpleName());
		job.setJarByClass(phonePartition.class);		
		
		//设置map输入路径与格式	
		FileInputFormat.setInputPaths(job, INPUT_PATH);
		job.setInputFormatClass(TextInputFormat.class);
		
		//设置map处理类与map输出类型
		job.setMapperClass(Map.class);
		job.setMapOutputKeyClass(Text.class);
		job.setMapOutputValueClass(KpiWritable.class);
		
		//设置分区
		job.setPartitionerClass(KpiPartitioner.class);
		job.setNumReduceTasks(2);
		
		//设置reduce处理类与reduce输出类型
		job.setReducerClass(Reduce.class);		
		job.setOutputKeyClass(Text.class);
		job.setOutputValueClass(KpiWritable.class);
		
		//设置reduce输出路径与格式
		FileOutputFormat.setOutputPath(job, new Path(OUT_PATH));
		job.setOutputFormatClass(TextOutputFormat.class);
		
		job.waitForCompletion(true);
	}

	static class Map extends Mapper<LongWritable, Text, Text, KpiWritable>{
		protected void map(LongWritable key, Text value, org.apache.hadoop.mapreduce.Mapper<LongWritable,Text,Text,KpiWritable>.Context context) throws IOException ,InterruptedException {
			String line = value.toString();
			String[] lineSplited = line.split("\t");
			String msisdn = lineSplited[1];
			Text keyinfo = new Text(msisdn);
			KpiWritable valueinfo = new KpiWritable(lineSplited[6],lineSplited[7],lineSplited[8],lineSplited[9]);
			context.write(keyinfo, valueinfo);
		};
	}
	 static class KpiPartitioner extends HashPartitioner<Text, KpiWritable>{
	        @Override
	        public int getPartition(Text key, KpiWritable value, int numReduceTasks) {
	            return (key.toString().length()==11)?0:1;
	        }
	    }
	static class Reduce extends Reducer<Text, KpiWritable, Text, KpiWritable>{
		/**
		 * @param	k2	表示整个文件中不同的手机号码	
		 * @param	v2s	表示该手机号在不同时段的流量的集合
		 */
		protected void reduce(Text k2, java.lang.Iterable<KpiWritable> v2s, org.apache.hadoop.mapreduce.Reducer<Text,KpiWritable,Text,KpiWritable>.Context context) throws IOException ,InterruptedException {
			long upPackNum = 0L;
			long downPackNum = 0L;
			long upPayLoad = 0L;
			long downPayLoad = 0L;
			
			for (KpiWritable kpiWritable : v2s) {
				upPackNum += kpiWritable.upPackNum;
				downPackNum += kpiWritable.downPackNum;
				upPayLoad += kpiWritable.upPayLoad;
				downPayLoad += kpiWritable.downPayLoad;
			}
			
			final KpiWritable v3 = new KpiWritable(upPackNum+"", downPackNum+"", upPayLoad+"", downPayLoad+"");
			context.write(k2, v3);
		};
	}
}

class KpiWritable implements Writable{
	long upPackNum;
	long downPackNum;
	long upPayLoad;
	long downPayLoad;
	
	public KpiWritable(){}
	
	public KpiWritable(String upPackNum, String downPackNum, String upPayLoad, String downPayLoad){
		this.upPackNum = Long.parseLong(upPackNum);
		this.downPackNum = Long.parseLong(downPackNum);
		this.upPayLoad = Long.parseLong(upPayLoad);
		this.downPayLoad = Long.parseLong(downPayLoad);
	}
	
	
	@Override
	public void readFields(DataInput in) throws IOException {
		this.upPackNum = in.readLong();
		this.downPackNum = in.readLong();
		this.upPayLoad = in.readLong();
		this.downPayLoad = in.readLong();
	}

	@Override
	public void write(DataOutput out) throws IOException {
		out.writeLong(upPackNum);
		out.writeLong(downPackNum);
		out.writeLong(upPayLoad);
		out.writeLong(downPayLoad);
	}
	
	@Override
	public String toString() {
		return upPackNum + "\t" + downPackNum + "\t" + upPayLoad + "\t" + downPayLoad;
	}
}
