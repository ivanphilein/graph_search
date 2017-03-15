package shared;

import java.util.ArrayList;
import java.util.Comparator;
import java.util.Iterator;
import java.util.List;
import java.util.TreeSet;

class CompareQuerySet implements Comparator<TreeSet<Integer>>{
	
	public int compare(TreeSet<Integer> e1, TreeSet<Integer> e2) 
	{
		if(e1.size()<e2.size()){
			return -1;
		}
		if(e1.size()>e2.size()){
			return 1;
		}
		
		Iterator<Integer> iter1 = e1.iterator();
		Iterator<Integer> iter2 = e2.iterator();
		for(int i=0; i<e1.size(); i++){
			int key1 = iter1.next();
			int key2 = iter2.next();
			if(key1 < key2){
				return -1;
			}
			if(key1 > key2){
				return 1;
			}
		}
		return 0;
	}
}

public class ResultFileInfo {
	
	public String queryStr = "";
	public List<Double> sumList = new ArrayList<Double>();
	public List<Double> runTimeList = new ArrayList<Double>();
	
	String showFileInfo(){
		return "Q:"+queryStr+" sum:"+sumList.toString()+" time:"+runTimeList.toString();
	}

}
