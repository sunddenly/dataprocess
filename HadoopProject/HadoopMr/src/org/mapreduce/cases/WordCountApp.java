package org.mapreduce.cases;

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
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.input.TextInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;
import org.apache.hadoop.mapreduce.lib.output.TextOutputFormat;
import org.apache.hadoop.mapreduce.lib.partition.HashPartitioner;

public class WordCountApp {
    static final String INPUT_PATH = "hdfs://hadoop:9000/input";
    static final String OUT_PATH = "hdfs://hadoop:9000/output";
    
    public static void main(String[] args) throws Exception {
        
        Configuration conf = new Configuration();
        
        final FileSystem fileSystem = FileSystem.get(new URI(INPUT_PATH), conf);
        final Path outPath = new Path(OUT_PATH);
        
        if(fileSystem.exists(outPath)){
            fileSystem.delete(outPath, true);
        }        
        final Job job = new Job(conf , WordCountApp.class.getSimpleName());
        
        //1.1指定读取的文件位于哪里
        FileInputFormat.setInputPaths(job, INPUT_PATH);        
        job.setInputFormatClass(TextInputFormat.class);//指定如何对输入文件进行格式化，把输入文件每一行解析成键值对
        
        //1.2 指定自定义的map类
        job.setMapperClass(MyMapper.class);
        job.setMapOutputKeyClass(Text.class);//map输出的<k,v>类型。
        job.setMapOutputValueClass(LongWritable.class);//如果<k3,v3>的类型与<k2,v2>类型一致，则可以省略
        
        //1.3 分区
        job.setPartitionerClass(HashPartitioner.class);        
        job.setNumReduceTasks(1);//有一个reduce任务运行                
        
        job.setCombinerClass(MyReducer.class);
        //2.2 指定自定义reduce类
        job.setReducerClass(MyReducer.class);
        
        job.setOutputKeyClass(Text.class);//指定reduce的输出类型
        job.setOutputValueClass(LongWritable.class);
        
        //2.3 指定写出到哪里
        FileOutputFormat.setOutputPath(job, outPath);        
        job.setOutputFormatClass(TextOutputFormat.class);//指定输出文件的格式化类
                
        job.waitForCompletion(true);//把job提交给JobTracker运行
    }
    
    /**
     * KEYIN    即k1        表示行的偏移量
     * VALUEIN    即v1        表示行文本内容
     * KEYOUT    即k2        表示行中出现的单词
     * VALUEOUT    即v2        表示行中出现的单词的次数，固定值1
     */
    static class MyMapper extends Mapper<LongWritable, Text, Text, LongWritable>{
        protected void map(LongWritable k1, Text v1, Context context) throws java.io.IOException ,InterruptedException {
            final Counter helloCounter = context.getCounter("Sensitive Words", "hello");
            
            final String line = v1.toString();
            if(line.contains("hello")){
                //记录敏感词出现在一行中
                helloCounter.increment(1L);
            }
            final String[] splited = line.split("\t");
            for (String word : splited) {
                context.write(new Text(word), new LongWritable(1));
            }
        };
    }
    
    /**
     * KEYIN    即k2        表示行中出现的单词
     * VALUEIN    即v2        表示行中出现的单词的次数
     * KEYOUT    即k3        表示文本中出现的不同单词
     * VALUEOUT    即v3        表示文本中出现的不同单词的总次数
     *
     */
    static class MyReducer extends Reducer<Text, LongWritable, Text, LongWritable>{
        protected void reduce(Text k2, java.lang.Iterable<LongWritable> v2s, Context ctx) throws java.io.IOException ,InterruptedException {
            long times = 0L;
            for (LongWritable count : v2s) {
                times += count.get();
            }
            ctx.write(k2, new LongWritable(times));
        };
    }
        
}
