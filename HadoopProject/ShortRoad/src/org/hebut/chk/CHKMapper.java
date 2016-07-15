package org.hebut.chk;
import java.io.IOException;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;
import org.hebut.graph.Color;
import org.hebut.graph.Node;
public class CHKMapper extends Mapper<LongWritable, Text, IntWritable,IntWritable >{
	public void map(LongWritable key, Text value,Context context) throws IOException, InterruptedException{
		Node node = new Node(value.toString());
		//if the color is gray , ouput<1,1>
		if(node.getColor() == Color.gray){
			context.write(new IntWritable(1), new IntWritable(1));
		}
		else{
			context.write(new IntWritable(1), new IntWritable(0));
		}
	}
}