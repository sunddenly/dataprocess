package org.mapreduce.cases;

import java.io.IOException;
import java.net.URI;
import java.net.URISyntaxException;

import javax.swing.SortingFocusTraversalPolicy;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.Reducer;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;

public class Sort {
	static final String INPUT_PATH = "hdfs://hadoop:9000/SortInput";
    static final String OUT_PATH = "hdfs://hadoop:9000/SortOutput";
    static final Path inputpath=new Path(INPUT_PATH);
    static final Path outpath=new Path(OUT_PATH);
    
	//map将输入中的value转化成Intwritable类型,作为输出key
	public static class Map extends Mapper<Object,Text,IntWritable,IntWritable>{
		@Override
		protected void map(Object key, Text value,Mapper<Object, Text, IntWritable, IntWritable>.Context context)throws IOException, InterruptedException {

			String line = value.toString();
			IntWritable data = new IntWritable(Integer.parseInt(line));
			context.write(data, new IntWritable(1));
		}
	}
	
	//reduce将输入key复制到reduce输出的value上,然后根据输入的
	//value-list中元素个数来决定输出次数
	public static class Reduce extends Reducer<IntWritable,IntWritable,IntWritable,IntWritable> {		
		IntWritable linenum=new IntWritable(1);
		
		@Override
		protected void reduce(IntWritable key,Iterable<IntWritable> values,Reducer<IntWritable, IntWritable, IntWritable, IntWritable>.Context context)throws IOException, InterruptedException {
			
			for (IntWritable val : values) {
				context.write(linenum, key);
				linenum=new IntWritable(linenum.get()+1);
			}			
		}	
	}
	public static void main(String[] args) throws IOException, URISyntaxException, InterruptedException, ClassNotFoundException {
		Configuration conf = new Configuration();
		
		FileSystem fileSystem = FileSystem.get(new URI(INPUT_PATH), conf);
		if(fileSystem.exists(outpath)){
			fileSystem.delete(outpath,true);
		}
		Job job = new Job(conf,Sort.class.getSimpleName());
		
		//设置Map和Reduce处理类
		job.setJarByClass(Sort.class);
		job.setMapperClass(Map.class);
		job.setReducerClass(Reduce.class);
		
		//设置输出类型
		job.setOutputKeyClass(IntWritable.class);
		job.setOutputValueClass(IntWritable.class);
		
		//设置输入和输出目录
		FileInputFormat.setInputPaths(job,inputpath);
		FileOutputFormat.setOutputPath(job,outpath);;
		
		job.waitForCompletion(true);
	}	
}
