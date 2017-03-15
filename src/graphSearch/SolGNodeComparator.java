package graphSearch;

import java.util.Comparator;


public class SolGNodeComparator implements Comparator<SolGraphNode>{
	public int compare(SolGraphNode e1, SolGraphNode e2) 
	{
		if (e1.getSum()<e2.getSum())
        {
            return -1;
        }
        if (e1.getSum()>e2.getSum())
        {
            return 1;
        }
        return 0;
	}
}
