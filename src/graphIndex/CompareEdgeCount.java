package graphIndex;

import java.util.Comparator;

public class CompareEdgeCount implements Comparator<Object>{
	
	public int compare(Object emp1, Object emp2) 
	{
	    CompareElement e1 = (CompareElement)emp1;
	    CompareElement e2 = (CompareElement)emp2;
	    int ret = 1;

		if(e1.getVertexId()==e2.getVertexId())
			ret = 0;
	    if(e1.getCount() == e2.getCount()) {
	    	if(e1.getNumInBlock()==e2.getNumInBlock()){
	    		if((Integer)e1.getVertexId()<(Integer)e2.getVertexId())
	    			ret = -1;
	    	}
	    	else if((Integer)e1.getNumInBlock()<(Integer)e2.getNumInBlock())
	    		ret = -1;
	    }
	    if(e1.getCount() < e2.getCount()) ret = -1;

	    return ret;
	}
}