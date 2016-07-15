package org.mapreduce.cases;

import java.io.IOException;
import java.net.URI;
import java.net.URISyntaxException;
import java.util.Iterator;
import java.util.StringTokenizer;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.Reducer;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.input.TextInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;
import org.apache.hadoop.mapreduce.lib.output.TextOutputFormat;

public class Score {
	static final String INPUT_PATH="hdfs://hadoop:9000/ScoreInput";
	static final String OUTPUT_PATH="hdfs://hadoop:9000/ScoreOutput";
	static final Path inpath=new Path(INPUT_PATH);
	static final Path outpath=new Path(OUTPUT_PATH);
	
	public static class Map extends Mapper<LongWritable,Text,Text,IntWritable>{
		
		Text name=new Text();
		IntWritable score=new IntWritable();		
		//实现map函数
		@Override
		protected void map(LongWritable key, Text value,Mapper<LongWritable, Text, Text, IntWritable>.Context context)throws IOException, InterruptedException {
			//将输入每行文本转换为String
			String line=value.toString();
			//每行按tap切分
			if(line.length()>0){
				String[] lineSplited=line.split("\t");
				name.set(lineSplited[0]);//名字部分
				score.set(Integer.parseInt(lineSplited[1]));//成绩部分
				context.write(name,score);//输出成绩和姓名
			}
		}
	
	}
	
	public static class Reduce extends Reducer<Text,IntWritable, Text,IntWritable>{
		int avgScore;
		int totalScore=0;
		int count=0;
		//实现reduce函数
		@Override
		protected void reduce(Text key, Iterable<IntWritable> values,Reducer<Text, IntWritable, Text, IntWritable>.Context context)throws IOException, InterruptedException {
			Iterator<IntWritable> iterator = values.iterator();
			while(iterator.hasNext()){
				totalScore+=iterator.next().get();//计算总分
				count++;//计算总科目
			}
			avgScore=totalScore/count;//计算平均分
			context.write(key, new IntWritable(avgScore));//输出平均分
		}	
	}
	public static void main(String[] args) throws IOException, URISyntaxException, InterruptedException, ClassNotFoundException {
		Configuration conf = new Configuration();
		FileSystem fileSystem = FileSystem.get(new URI(INPUT_PATH), conf);
		if(fileSystem.exists(outpath)){
			fileSystem.delete(outpath,true);
		}
		
		Job job = new Job(conf,Score.class.getSimpleName());			
		job.setJarByClass(Score.class);
		//设置Map、Combiner、Reducer处理类
		job.setMapperClass(Map.class);
		job.setCombinerClass(Reduce.class);
		job.setReducerClass(Reduce.class);
		//设置Reduce输出类型
		job.setOutputKeyClass(Text.class);
		job.setOutputValueClass(IntWritable.class);
		//设置输入、输出格式
		job.setInputFormatClass(TextInputFormat.class);
		job.setOutputFormatClass(TextOutputFormat.class);
		//设置输入、输出路径
		FileInputFormat.setInputPaths(job, inpath);
		FileOutputFormat.setOutputPath(job, outpath);
		
		job.waitForCompletion(true);
	}
}
