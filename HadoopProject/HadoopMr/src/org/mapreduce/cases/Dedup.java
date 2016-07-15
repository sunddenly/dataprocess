package org.mapreduce.cases;

import java.io.IOException;
import java.net.URI;
import java.net.URISyntaxException;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.Reducer;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;


public class Dedup {
	static final String INPUT_PATH = "hdfs://hadoop0:9000/DedupInput";
    static final String OUT_PATH = "hdfs://hadoop0:9000/DedupOutput";
    static final Path outpath = new Path(OUT_PATH);
    static final Path inutput=new Path(INPUT_PATH);
    
    public static void main(String[] args) throws IOException, URISyntaxException, InterruptedException, ClassNotFoundException {
		Configuration conf = new Configuration();
		FileSystem fileSystem=FileSystem.get(new URI(INPUT_PATH),conf);
		
		if(fileSystem.exists(outpath)){
			fileSystem.delete(outpath,true);
		}
		
		Job job = new Job(conf,Dedup.class.getSimpleName());
		job.setJarByClass(Dedup.class);
		
		//设置Map、Combine和Reduce处理类
		job.setMapperClass(Map.class);
		job.setCombinerClass(Reduce.class);
		job.setReducerClass(Reduce.class);
		
		//设置输出类型
		job.setOutputKeyClass(Text.class);
		job.setOutputValueClass(Text.class);
		
		//设置输入和输出目录
		FileInputFormat.setInputPaths(job, inutput);;	
		FileOutputFormat.setOutputPath(job, outpath);
		
		job.waitForCompletion(true);
	}

	public static class Map extends Mapper<Object, Text, Text, Text>{
		
		private static Text line=new Text();
		@Override
		protected void map(Object key, Text value,Mapper<Object, Text, Text, Text>.Context context)throws IOException, InterruptedException {
			line=value;
			context.write(line, new Text(""));
		}
		
	}
	
	public static class Reduce extends Reducer<Text, Text, Text, Text>{

		@Override
		protected void reduce(Text arg0, Iterable<Text> arg1,Reducer<Text, Text, Text, Text>.Context arg2)throws IOException, InterruptedException {
			arg2.write(arg0, new Text(""));
		}
	}
	
} 

