package org.mapreduce.sort;

import java.io.DataInput;
import java.io.DataOutput;
import java.io.IOException;
import java.net.URI;
import java.net.URISyntaxException;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.io.WritableComparable;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.Reducer;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.input.TextInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;
import org.apache.hadoop.mapreduce.lib.output.TextOutputFormat;
import org.apache.hadoop.mapreduce.lib.partition.HashPartitioner;

public class UserSort {
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
		Job job = new Job(conf,UserSort.class.getSimpleName());
		job.setJarByClass(UserSort.class);		
		//设置Map输入路径与格式
		FileInputFormat.setInputPaths(job, inpath);
		job.setInputFormatClass(TextInputFormat.class);
		//设置Map处理类与输出类型
		job.setMapperClass(Map.class);
		job.setMapOutputKeyClass(NewKey.class);
		job.setMapOutputValueClass(LongWritable.class);
		//设置分区
		job.setPartitionerClass(HashPartitioner.class);
		job.setNumReduceTasks(1);
		//设置Reduce处理类与输出类型
		job.setReducerClass(Reduce.class);
		job.setOutputKeyClass(LongWritable.class);
		job.setOutputValueClass(LongWritable.class);
		//设置Reduce输出路径与格式
		FileOutputFormat.setOutputPath(job, outpath);
		job.setOutputFormatClass(TextOutputFormat.class);
		
		job.waitForCompletion(true);
	}
	
	public static class Map extends Mapper<Object,Text,NewKey,LongWritable>{
		@Override
		protected void map(Object key, Text value,
				Mapper<Object, Text, NewKey, LongWritable>.Context context)
				throws IOException, InterruptedException {
			String line = value.toString();
			String[] lineSplited = line.split("\t");
			NewKey keyinfo = new NewKey(Long.parseLong(lineSplited[0]),Long.parseLong(lineSplited[1]));
			LongWritable valueinfo = new LongWritable(Long.parseLong(lineSplited[1]));
			context.write(keyinfo, valueinfo);
		}
	}
	public static class Reduce extends Reducer<NewKey,LongWritable,LongWritable,LongWritable>{

		@Override
		protected void reduce(
				NewKey key,
				Iterable<LongWritable> values,
				Reducer<NewKey, LongWritable, LongWritable, LongWritable>.Context context)
				throws IOException, InterruptedException {
			context.write(new LongWritable(key.first), new LongWritable(key.second));
		}
		
	}
	//问：为什么实现该类？
	//答：因为原来的key不能让value参与排序，把原来的key和value封装到一个类中，作为新的key
	public static class NewKey implements WritableComparable<NewKey>{
		Long first;
		Long second;
		
		public NewKey() {}				
		public NewKey(long first,long second) {
			this.first = first;
			this.second = second;
		}
		//序列化
		@Override
		public void write(DataOutput out) throws IOException {
			out.writeLong(first);
			out.writeLong(second);
		}
		//反序列化
		@Override
		public void readFields(DataInput in) throws IOException {
			this.first=in.readLong();
			this.second=in.readLong();
		}
		//当基于NewKey进行排序时,会调用该方法
		//当第一列不同时，升序
		//当第一列相同时，第二列升序
		public int compareTo(NewKey o) {
			long flag = this.first-o.first;
			if(flag!=0){
				return (int)flag;
			}
			return (int)(this.second-o.second);
		}
		@Override
		public int hashCode() {
			return this.first.hashCode()+this.second.hashCode();
		}
		@Override
		public boolean equals(Object obj) {
			if(!(obj instanceof NewKey)){
				return false;
			}
			NewKey newkey=(NewKey)obj;
			return ((this.first==newkey.first)&&(this.second==newkey.second));
		}
		
	}
}