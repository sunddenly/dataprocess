package org.hebut.ssp;
import java.io.IOException;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;
import org.hebut.graph.Color;
import org.hebut.graph.Edge;
import org.hebut.graph.Node;
/**
* the ssp map class
*/
public class SSPMapper extends Mapper<LongWritable, Text, IntWritable,Text > {
	public void map(LongWritable key, Text value,Context context) throws IOException, InterruptedException{
	//Get the logic Node object from the record string
	Node thisNode = new Node(value.toString());
	if(thisNode.getColor() == Color.gray){
		for(Edge e : thisNode.getEdges()){
			Node node = new Node(e.getEndPoint());
			node.setColor(Color.gray);
			node.setDistance(thisNode.getDistance() + e.getWeight());
			context.write(new IntWritable(node.getId()), new
			Text(node.getText()));
		}
		thisNode.setColor(Color.black);
	}
		context.write(new IntWritable(thisNode.getId()), new Text(thisNode.getText()));
	}
}
