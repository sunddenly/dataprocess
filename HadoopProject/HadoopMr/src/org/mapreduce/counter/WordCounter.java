package org.mapreduce.counter;

import java.net.URI;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Counter;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.Reducer;
import org.apache.hadoop.mapreduce.Reducer.Context;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.input.TextInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;
import org.apache.hadoop.mapreduce.lib.output.TextOutputFormat;
import org.apache.hadoop.mapreduce.lib.partition.HashPartitioner;
import org.mapreduce.counter.WordCount.Map;
import org.mapreduce.counter.WordCount.Reduce;

public class WordCounter {
	static final String INPUT_PATH = "hdfs://hadoop:9000/WordInput";
    static final String OUT_PATH = "hdfs://hadoop:9000/WordOutput";
    static final Path inpath=new Path(INPUT_PATH);
    static final Path outpath=new Path(OUT_PATH);   
    
    public static void main(String[] args) throws Exception {
        
        Configuration conf = new Configuration();   
        FileSystem fileSystem = FileSystem.get(new URI(INPUT_PATH), conf);  
        if(fileSystem.exists(outpath)){
            fileSystem.delete(outpath, true);
        }
        //初始化Job
        Job job = new Job(conf , WordCount.class.getSimpleName());
        job.setJarByClass(WordCount.class);
        
        //设置Map、Combine、Reduce处理类
        job.setMapperClass(Map.class);
        job.setPartitionerClass(HashPartitioner.class); 
        //job.setCombinerClass(Reduce.class);
        job.setReducerClass(Reduce.class); 
        
        //设置Map输出类型
        job.setMapOutputKeyClass(Text.class);
        job.setMapOutputValueClass(LongWritable.class); 
        
        //设置Reduce输出类型
        job.setOutputKeyClass(Text.class);//指定reduce的输出类型
        job.setOutputValueClass(LongWritable.class); 
        
        //设置输入、输出格式
        job.setInputFormatClass(TextInputFormat.class);
        job.setOutputFormatClass(TextOutputFormat.class);
        
        //设置输入、输出路径       
        FileInputFormat.setInputPaths(job,inpath);
        FileOutputFormat.setOutputPath(job,outpath);  
        
        job.waitForCompletion(true);//把job提交给JobTracker运行
    }
    
    static class Map extends Mapper<LongWritable, Text, Text, LongWritable>{
        protected void map(LongWritable key, Text value, Context context) throws java.io.IOException ,InterruptedException {            
        	String line = value.toString(); 
        	
        	Counter HelloCounter = context.getCounter("Sensitive Words", "Hello");
            if(line.contains("Hello")){
            	HelloCounter.increment(1L);
            }  
            
            String[] lineSplited = line.split("\t");
            for (String word : lineSplited) {
                context.write(new Text(word), new LongWritable(1));
            }
        };
    }
  
    static class Reduce extends Reducer<Text, LongWritable, Text, LongWritable>{
        protected void reduce(Text key, java.lang.Iterable<LongWritable> values, Context context) throws java.io.IOException ,InterruptedException {
            long count = 0L;
            for (LongWritable val:values) {
                count+= val.get();
            }
            context.write(key, new LongWritable(count));
        };
    }      
}
