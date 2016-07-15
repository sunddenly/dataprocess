package org.hebut.graph;
public class Edge{
	int startPoint; //the start point
	int endPoint; //the end point
	int weight; // the weight of this Edge
	public Edge(int start,int end,int weight){
		this.startPoint = start;
		this. endPoint = end;
		this.weight = weight;
	}
	public void setWeight(int weight){
		this.weight = weight;
	}
	public int getWeight(){
		return this.weight;
	}
	public int getEndPoint(){
		return this.endPoint;
	}
}