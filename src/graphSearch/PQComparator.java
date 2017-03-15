package graphSearch;

import java.util.Comparator;

public class PQComparator implements Comparator<PQElement>{
	public int compare(PQElement e1, PQElement e2) 
	{
		if (e1.getPriority()<e2.getPriority())
        {
            return -1;
        }
        if (e1.getPriority()>e2.getPriority())
        {
            return 1;
        }
        return 0;
	}
}
