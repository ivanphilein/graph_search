package mapreduce;

import java.util.Comparator;

class CompareSetElement implements Comparator<Object>{
	
	public int compare(Object emp1, Object emp2) 
	{
		SetElement e1 = (SetElement)emp1;
		SetElement e2 = (SetElement)emp2;
	    int ret = 1;
	    
	    if(e1.getEdgeFrom()==e2.getEdgeFrom() && e1.getEdgeTo() == e2.getEdgeTo()){
	    	ret = 0;
	    }
	    else if(e1.getWeight()>e2.getWeight()){
	    	ret = -1;
	    }
	    return ret;
	}
}

//Here is the set element for the hashset used for calculate the real distance of subgraph
public class SetElement {
	private int edgeFrom;
	private int edgeTo;
	private double weight;
	
	public SetElement(int from, int to, double wei){
		edgeFrom = from;
		edgeTo = to;
		weight = wei;
	}
	
	public int getEdgeFrom(){
		return edgeFrom;
	}
	
	public void setEdgeFrom(int from){
		edgeFrom = from;
	}
	
	public int getEdgeTo(){
		return edgeTo;
	}
	
	public void setEdgeTo(int to) {
		edgeTo = to;
	}
	
	public double getWeight(){
		return weight;
	}
	
	public void setWeight(double wei) {
		weight = wei;
	}
	
}
