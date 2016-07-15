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
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.input.TextInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;
import org.apache.hadoop.mapreduce.lib.output.TextOutputFormat;
import org.apache.hadoop.mapreduce.lib.partition.HashPartitioner;

public class WordCount {
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
    
    /**
     * KEYIN    即k1        表示行的偏移量
     * VALUEIN    即v1        表示行文本内容
     * KEYOUT    即k2        表示行中出现的单词
     * VALUEOUT    即v2        表示行中出现的单词的次数，固定值1
     */
    static class Map extends Mapper<LongWritable, Text, Text, LongWritable>{
        protected void map(LongWritable k1, Text v1, Context context) throws java.io.IOException ,InterruptedException {            
            final String line = v1.toString();        
            final String[] lineSplited = line.split("\t");
            for (String word : lineSplited) {
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
    static class Reduce extends Reducer<Text, LongWritable, Text, LongWritable>{
        protected void reduce(Text k2, java.lang.Iterable<LongWritable> values, Context ctx) throws java.io.IOException ,InterruptedException {
            long count = 0L;
            for (LongWritable val : values) {
                count+= val.get();
            }
            ctx.write(k2, new LongWritable(count));
        };
    }     
}
