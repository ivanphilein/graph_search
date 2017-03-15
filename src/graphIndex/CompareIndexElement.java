package graphIndex;

import java.util.Comparator;

import shared.IndexElement;


public class CompareIndexElement implements Comparator<Object>{
	
	public int compare(Object emp1, Object emp2) 
	{
	    IndexElement e1 = (IndexElement)emp1;
	    IndexElement e2 = (IndexElement)emp2;
	    int ret = 1;
	        
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