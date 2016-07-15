package org.hebut.ssp;
import java.io.IOException;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Reducer;
import org.hebut.graph.Color;
import org.hebut.graph.Node;
/**
* A reducer class that select the min value of each key .
*/
public class SSPReducer extends Reducer<IntWritable, Text, IntWritable,Text>{
	public void reduce(IntWritable key, Iterable<Text> values, Context context) throws IOException,InterruptedException{
		Node node = new Node(key.get());
		for(Text value: values){
			Node tmpNode = new Node(node.getId()+"\t"+value.toString());
			if(tmpNode.getColor() > node.getColor()){
				node.setColor(tmpNode.getColor());
			}
			if(tmpNode.getDistance() < node.getDistance()){
				node.setDistance(tmpNode.getDistance());
				if(tmpNode.getColor() == Color.gray){
					node.setColor(Color.gray);
				}
			}
			if(tmpNode.getEdges().size() > 0){
				node.setEdges(tmpNode.getEdges());
			}
		}
		context.write(new IntWritable(node.getId()), new Text(node.getText()));
	}
}