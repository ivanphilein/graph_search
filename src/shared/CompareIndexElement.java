package shared;

import java.util.Comparator;



public class CompareIndexElement implements Comparator<IndexElement>{
	
	public int compare(IndexElement e1, IndexElement e2) 
	{
	    int ret = 1;
	    if(e1.getStartVertex()==e2.getStartVertex() && e1.getEndVertex()==e2.getEndVertex())
	    	return 0;
	    /*if(e1.getLength() == e2.getLength() && e1.getStartVertex()==e2.getStartVertex() &&e1.getEndVertex()==e2.getEndVertex()&&e1.getNextVertex()==e2.getNextVertex()) {
	    	return 0;
	    }*/
	    if(e1.getLength() == e2.getLength()) {
	    	if((Integer)e1.getStartVertex()<(Integer)e2.getStartVertex())
	    		ret = -1;
	    	else if(e1.getStartVertex()==e2.getStartVertex()){
	    		if(e1.getNextVertex()<e2.getNextVertex()){
	    			ret = -1;
	    		}
	    		else if(e1.getNextVertex()==e2.getNextVertex()){
	    			if(e1.getEndVertex()<e2.getEndVertex()){
	    				ret = -1;
	    			}
	    		}
	    	}
	    }
	    if(e1.getLength() < e2.getLength()) ret = -1;

	    return ret;
	}
}