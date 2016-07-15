package org.mapreduce.cases;

import java.io.IOException;
import java.net.URI;
import java.net.URISyntaxException;
import java.util.StringTokenizer;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.InputSplit;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.Reducer;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.input.FileSplit;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;
import org.apache.hadoop.util.GenericOptionsParser;

public class InvertedIndex {
	static final String INPUT_PATH="hdfs://hadoop:9000/InvertInput";
	static final String OUT_PATH="hdfs://hadoop:9000/InvertOutput";
	static final Path inpath=new Path(INPUT_PATH);
	static final Path outpath=new Path(OUT_PATH);
	public static class Map extends Mapper<Object,Text,Text,Text>{
		private Text keyInfo = new Text(); // 存储单词和URL组合
	    private Text valueInfo = new Text(); // 存储词频
	    private FileSplit split; // 存储Split对象
		//实现map
		@Override
		protected void map(Object key, Text value,Mapper<Object, Text, Text, Text>.Context context)throws IOException, InterruptedException {
			// 获得<key,value>对所属的FileSplit对象
			String line = value.toString();
			String[] lineSplited = line.split("\t");
			int i=0;
			int len=lineSplited.length;
			if(len==0)return;
			
			split = (FileSplit) context.getInputSplit();
			// key值由单词和URL组成，如"MapReduce：file1.txt"
            // 获取文件的完整路径
            // keyInfo.set(itr.nextToken()+":"+split.getPath().toString());
            // 这里为了好看，只获取文件的名称。
			int splitIndex = split.getPath().toString().indexOf("file");
			String splitname = split.getPath().toString().substring(splitIndex);
			while (i<len) {
				keyInfo.set(lineSplited[i]+":"+splitname);
				valueInfo.set("1");
				context.write(keyInfo, valueInfo);
				i++;
			}
		}
	}
	public static class Combine extends Reducer<Text,Text,Text,Text>{
		private Text valueinfo=new Text();
		private Text keyinfo=new Text();
		//实现reduce
		@Override
		protected void reduce(Text key, Iterable<Text> values,Reducer<Text, Text, Text, Text>.Context context)throws IOException, InterruptedException {
			//统计词频
			int sum=0;
			for (Text val: values) {
				sum+=Integer.parseInt(val.toString());
			}
			int splitindex = key.toString().indexOf(":");
			String splitname = key.toString().substring(splitindex+1);
			// 重新设置value值由URL和词频组成
			valueinfo.set(splitname+":"+sum);
			// 重新设置key值为单词
			keyinfo.set(key.toString().substring(0,splitindex));
			context.write(keyinfo, valueinfo);
		}		
	}
	public static class Reduce extends Reducer<Text,Text,Text,Text>{
		private Text result=new Text();
		//实现reduce
		@Override
		protected void reduce(Text key, Iterable<Text> values,Reducer<Text, Text, Text, Text>.Context context)throws IOException, InterruptedException {
			//生成文档列表
			String filelist = new String();
			for (Text val : values) {
				filelist+=val.toString()+";";
			}
			result.set(filelist);
			context.write(key, result);
		}	
	}
	public static void main(String[] args) throws IOException, URISyntaxException, InterruptedException, ClassNotFoundException {
		Configuration conf = new Configuration();
		FileSystem fileSystem = FileSystem.get(new URI(INPUT_PATH), conf);
		if(fileSystem.exists(outpath)){
			fileSystem.delete(outpath,true);
		}
		
		Job job = new Job(conf,InvertedIndex.class.getSimpleName());
		job.setJarByClass(InvertedIndex.class);
		 
		//设置Map、Combine、Reduce处理类
		job.setMapperClass(Map.class);
		job.setCombinerClass(Combine.class);
		job.setReducerClass(Reduce.class);
		
		//设置Map输出类型
		job.setMapOutputKeyClass(Text.class);
		job.setMapOutputValueClass(Text.class);
		
		//设置Reduce输出类型
		job.setOutputKeyClass(Text.class);
		job.setOutputValueClass(Text.class);
		
		//设置输入输出路径
		FileInputFormat.setInputPaths(job, inpath);
		FileOutputFormat.setOutputPath(job, outpath);
		
		job.waitForCompletion(true);
	}
} 
