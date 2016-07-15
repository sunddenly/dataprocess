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
import org.mapreduce.cases.STjoin.Map;
import org.mapreduce.cases.STjoin.Reduce;

public class MTjoin {
	static final String INPUT_PATH="hdfs://hadoop:9000/MTjoinInput";
	static final String OUT_PATH="hdfs://hadoop:9000/MTjoinOutput";
	static final Path inpath=new Path(INPUT_PATH);
	static final Path outpath=new Path(OUT_PATH);
	public	static int time=0;
	public static class Map extends Mapper<Object,Text,Text,Text>{
		//实现map
		//在Map中先区分输入行属于左表还是右表,然后对两列值进行分割
		//连接列保存在key值,剩余的列和左右表标志保存在value中,最后输出
		@Override
		protected void map(Object key, Text value,Mapper<Object, Text, Text, Text>.Context context)throws IOException, InterruptedException {
			String line = value.toString();
			String[] lineSplited = line.split("\t");
			//首行不作处理
			if(line.contains("factoryname")==true||line.contains("aadressID")==true){
				return;
			}
			
			//左表
			if(line.charAt(0)>='9'||line.charAt(0)<='0'){
				context.write(new Text(lineSplited[1]),new Text("1+"+lineSplited[0]));
			}else{//右表
				context.write(new Text(lineSplited[0]), new Text("2+"+lineSplited[1]));
			}
		}
		public static class Reduce extends Reducer<Text,Text,Text,Text>{
			//reduce解析Map输出,将value中数据按左右表分别保存,然后求笛卡尔积
			@Override
			protected void reduce(Text key, Iterable<Text> values,Reducer<Text, Text, Text, Text>.Context context)throws IOException, InterruptedException {
				//输出文件第一行
				if(time==0){
					context.write(new Text("factoryname"), new Text("addressname"));
					time++;
				}
				
				int factorynum=0;
				int addressnum=0;
				String[] factory=new String[10];
				String[] address=new String[10];
				Iterator ite = values.iterator();
				while(ite.hasNext()){
					String record = ite.next().toString();
					String[] recordSplited = record.split("[+]");
					
					int len = record.length();
					if(len==0)	continue;
					
					String type;
					String factoryname = new String();
					String addressname = new String();
					
					type=recordSplited[0];
					//左表
					if(type.compareTo("1")==0){
						factoryname=recordSplited[1];
						factory[factorynum]=factoryname;
						factorynum++;
					}else{//右表
						addressname=recordSplited[1];
						address[addressnum]=addressname;
						addressnum++;
					}
				}
				//求笛卡尔积
				if(factorynum!=0&&addressnum!=0){
					for(int m=0;m<factorynum;m++){
						for(int n=0;n<addressnum;n++){
							context.write(new Text(factory[m]), new Text(address[n]));
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
			
			Job job = new Job(conf,MTjoin.class.getSimpleName());		
			job.setJarByClass(MTjoin.class);
			
			//设置Map、Reduce处理类
			job.setMapperClass(Map.class);
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
}
