package oneIteMR;

import java.util.Comparator;



public class CompareVertexOne implements Comparator<VertexOne>{
	
	public int compare(VertexOne e1, VertexOne e2) 
	{
	    /*if(e1.getVertexID()==e2.getVertexID())
	    	return 0;
	    if(e1.getActivation()>e2.getActivation())
	    	return 1;
	    return -1;*/
		return (int)(e1.getActivation()-e2.getActivation());
	}
}