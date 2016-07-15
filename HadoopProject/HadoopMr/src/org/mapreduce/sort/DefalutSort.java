package org.mapreduce.sort;

import java.io.IOException;
import java.net.URI;
import java.net.URISyntaxException;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.Reducer;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.input.TextInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;
import org.apache.hadoop.mapreduce.lib.output.TextOutputFormat;
import org.apache.hadoop.mapreduce.lib.partition.HashPartitioner;

public class DefalutSort {
	static final String INPUT_PATH = "hdfs://hadoop:9000/SortIn";
	static final String OUT_PATH = "hdfs://hadoop:9000/SortOut";
	static final Path inpath=new Path(INPUT_PATH);
	static final Path outpath=new Path(OUT_PATH);	
	public static void main(String[] args) throws IOException, URISyntaxException, InterruptedException, ClassNotFoundException {
		Configuration conf = new Configuration();
		FileSystem fileSystem = FileSystem.get(new URI(INPUT_PATH), conf);
		if(fileSystem.exists(outpath)){
			fileSystem.delete(outpath,true);
		}
		//job初始化
		Job job = new Job(conf,DefalutSort.class.getSimpleName());
		job.setJarByClass(DefalutSort.class);		
		//设置Map输入路径与格式
		FileInputFormat.setInputPaths(job, inpath);
		job.setInputFormatClass(TextInputFormat.class);
		//设置Map处理类与输出类型
		job.setMapperClass(Map.class);
		job.setMapOutputKeyClass(IntWritable.class);
		job.setMapOutputValueClass(IntWritable.class);
		
		job.setPartitionerClass(HashPartitioner.class);
		job.setNumReduceTasks(1);
		//设置Reduce处理类与输出类型
		job.setReducerClass(Reduce.class);
		job.setOutputKeyClass(IntWritable.class);
		job.setOutputValueClass(IntWritable.class);
		//设置Reduce输出路径与格式
		FileOutputFormat.setOutputPath(job, outpath);
		job.setOutputFormatClass(TextOutputFormat.class);
		
		job.waitForCompletion(true);
	}
	public static class Map extends Mapper<Object,Text,IntWritable,IntWritable>{
		@Override
		protected void map(Object key,Text value,Mapper<Object,Text, IntWritable, IntWritable>.Context context)throws IOException, InterruptedException {
			IntWritable keyinfo = new IntWritable();
			IntWritable valueinfo = new IntWritable();
			String line = value.toString();
			String[] lineSplited = line.split("\t");
			keyinfo.set(Integer.parseInt(lineSplited[0]));
			valueinfo.set(Integer.parseInt(lineSplited[1]));
			context.write(keyinfo, valueinfo);
		}	
	}
	public static class Reduce extends Reducer<IntWritable, IntWritable,IntWritable, IntWritable>{
		@Override
		protected void reduce(IntWritable key,Iterable<IntWritable> values,Reducer<IntWritable, IntWritable, IntWritable, IntWritable>.Context context)throws IOException, InterruptedException {
			for (IntWritable val : values) {
				context.write(key, val);
			}
		}	
	}
}
