package org.hebut.graph;
import java.util.*;

import org.hebut.util.MyStringUtil;
/**
* * The input record format is:
* ID Distance|Color|Edges
* the format of Edges is:
* endPoint1,weight1,endPoint2,weight2,...,endPointN,weightN,
* or just make it like
* ID Distance|Color|
* if there is no edges
* to make it simple,all the type is int
* 0:white,1:gray,2:black
*/
public class Node{
	int Id; //the id of this node
	int distance = Integer.MAX_VALUE; //the distance from the start node
	Vector<Edge> edges = new Vector<Edge>(); //the edges start from this node
	int color = Color.white;     // the color of this node
	public Node(String record) {  //get the Node from the record string
		String[] firstSplit = MyStringUtil.split(record, "\t");
		String[] secondSplit = MyStringUtil.split(firstSplit[1], "|");
		this.Id = Integer.valueOf(firstSplit[0]); //get the id
		this.color = Integer.valueOf(secondSplit[1]); //get the color
		if(secondSplit[0].equals("MAX")){
			this.distance = Integer.MAX_VALUE;
		}
		else{
			this.distance = Integer.valueOf(secondSplit[0]);
		}
		//get the edges
		if(secondSplit.length < 3) {
		//this.edges = null;
		}else{
			String[] thirdSplit = MyStringUtil.split(secondSplit[2], ",");
			int len = thirdSplit.length;
			for(int i = 0;i < len; i = i + 2){
				int endPoint = Integer.valueOf(thirdSplit[i]);
				int weight = Integer.valueOf(thirdSplit[i + 1]);
				this.edges.add(new Edge(this.Id,endPoint,weight));
			}
		}
	}
	public Node(int id){
		this.Id = id;
		this.setDistance(Integer.MAX_VALUE);
	}
	public Vector<Edge> getEdges(){
		return this.edges;
	}
	public void setEdges(Vector<Edge> edges){
		this.edges = edges;
	}
	public void setColor(int color){
		this.color = color;
	}
	public int getColor(){
		return this.color;
	}
	public void setDistance(int distance){
		this.distance = distance;
	}
	public int getDistance(){
		return this.distance;
	}
	public int getId(){
		return this.Id;
	}
	public String getText(){
		String col = String.valueOf(this.color);
		String dis;
		String edges = "";
		if(this.distance == Integer.MAX_VALUE){
			dis = "MAX";
		}else{
			dis = String.valueOf(this.distance);
		}
		if(this.getEdges().size() > 0){
			for(Edge e : this.getEdges()){
				String tem = e.endPoint+","+ e.weight+",";
				edges += tem;
			}
		}
		String nodeLine = dis+"|"+col+"|"+edges;
		return nodeLine;
	}
}
