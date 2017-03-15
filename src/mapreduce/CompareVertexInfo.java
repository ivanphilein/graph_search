package mapreduce;

import java.util.Comparator;



public class CompareVertexInfo implements Comparator<Object>{
	
	public int compare(Object emp1, Object emp2) 
	{
		VertexInfo e1 = (VertexInfo)emp1;
		VertexInfo e2 = (VertexInfo)emp2;
	    int ret = 1;
	    
	    if(e1.getSumRMap()==e2.getSumRMap()){
	    	if(e1.getVertexId()==e2.getVertexId())
	    		ret = 0;
	    	else if(e1.getVertexId() < e2.getVertexId())
	    		ret = -1;
	    }
	    else if(e1.getSumRMap() < e2.getSumRMap()){
	    	ret = -1;
	    }
	    return ret;
	}
}