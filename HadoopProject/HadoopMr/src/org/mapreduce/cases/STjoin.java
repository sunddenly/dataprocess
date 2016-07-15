package org.mapreduce.cases;

import java.io.IOException;
import java.net.URI;
import java.net.URISyntaxException;
import java.util.Iterator;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.Reducer;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;

public class STjoin {	 
    public static int time = 0;
    static final String INPUT_PATH="hdfs://hadoop:9000/STjoinInput";
    static final String OUT_PATH="hdfs://hadoop:9000/STjoinOutput";
    static final Path inpath=new Path(INPUT_PATH);
    static final Path outpath=new Path(OUT_PATH);
    /*
     * map将输出分割child和parent，然后正序输出一次作为右表，
     * 反序输出一次作为左表，需要注意的是在输出的value中必须
     * 加上左右表的区别标识。
     */
    public static class Map extends Mapper<Object,Text,Text,Text>{
    	//实现map
		@Override
		protected void map(Object key, Text value,Mapper<Object, Text, Text, Text>.Context context)throws IOException, InterruptedException {
			String childname = new String();//孩子名
			String parentname = new String();//父母名
			String relationtype = new String();//左右表标识
			
			String line = value.toString();//取出一行文本
			String[] lineSplited = line.split("\t");//对每行文本按Tap切分
			
			if(lineSplited[0].compareTo("child")!=0){
				childname=lineSplited[0];
				parentname=lineSplited[1];
				//输出左表
				relationtype="1";
				context.write(new Text(parentname),new Text(relationtype+"+"+childname+"+"+parentname));
				//输出右表
				relationtype="2";
				context.write(new Text(childname), new Text(relationtype+"+"+childname+"+"+parentname));
			}
		}
    	
    }
    public static class Reduce extends Reducer<Text,Text,Text,Text>{
    	//实现reduce
		@Override
		protected void reduce(Text key, Iterable<Text> values,Reducer<Text, Text, Text, Text>.Context context)throws IOException, InterruptedException {
			//输出表头
			if(time==0){
				context.write(new Text("grandchild"),new Text("grandparent"));
				time++;
			}						
			int grandchildnum=0;
			int grandparentnum=0;
			String grandchild[] =new String[10];			
			String grandparent[] =new String[10];
			//获取value-list
			Iterator<Text> ite = values.iterator();

			while(ite.hasNext()){
				String relationtype;
				String childname = new String();
				String parentname = new String();
				
				//从value-list中取出一条记录
				String record = ite.next().toString();	
				
				int len = record.length();
				if(len==0)continue;
				
				//对value-list中的每条record进行切分
				String[] recordSplited = record.split("[+]");				
				
				//获取value-list中的relationtype
				relationtype=recordSplited[0];
					
				//获取value-list中value的child和parent
				childname=recordSplited[1];			
				parentname=recordSplited[2];
					
				//左表,取出child放入grandchild
				if(relationtype.compareTo("1")==0){
					grandchild[grandchildnum]=childname;
					grandchildnum++;
				}
				//右表取出parent放入grandparent
				else {
					grandparent[grandparentnum]=parentname;
					grandparentnum++;
				}					
			}
			//grandchild和grandparent数组求笛卡尔积
			if(grandparentnum!=0&&grandchildnum!=0){
				for(int m=0;m<grandparentnum;m++){
					for(int n=0;n<grandparentnum;n++){
						context.write(new Text(grandchild[m]), new Text(grandparent[n]));
					}
				}
			}
			
		}
    }
    public static void main(String[] args) throws IOException, URISyntaxException, InterruptedException, ClassNotFoundException {
		Configuration conf = new Configuration();
		FileSystem fileSystem = FileSystem.get(new URI(INPUT_PATH),conf);
		if(fileSystem.exists(outpath)){
			fileSystem.delete(outpath,true);
		}
		Job job = new Job(conf,STjoin.class.getSimpleName());		
		job.setJarByClass(STjoin.class);
		//设置Map、ComBine、Reduce处理类
		job.setMapperClass(Map.class);
		//job.setCombinerClass(Reduce.class);
		job.setReducerClass(Reduce.class);
		//设置输出类型
		job.setOutputKeyClass(Text.class);
		job.setOutputValueClass(Text.class);
		//设置输入输出路径
		FileInputFormat.setInputPaths(job, inpath);
		FileOutputFormat.setOutputPath(job, outpath);
		
		job.waitForCompletion(true);	
	}
} 
