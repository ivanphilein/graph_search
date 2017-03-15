package graphSearch;

import java.util.HashMap;
import java.util.Iterator;
import java.util.Map.Entry;


public class SolGraphNode {
	private HashMap<Integer, Integer> solGNodeMap = new HashMap<Integer, Integer>();
	private double sum=0;
	
	public void setSum(double s){
		sum = s;
	}
	
	public void addSum(double add){
		sum += add;
	}
	
	public double getSum(){
		return sum;
	}
	
	public void moveOneMore(int kid){
		solGNodeMap.put(kid, solGNodeMap.get(kid)+1);
		
	}
	
	public void addIterWithkeyword(int kid, int iter){
		solGNodeMap.put(kid, iter);
	}
	
	public HashMap<Integer, Integer> getSolNode(){
		return solGNodeMap;
	}
	
	public void cloneGNodeMap(SolGraphNode copyS, int kid){
		//solGNodeMap.putAll(copyS.getSolNode());
		
		Iterator<Entry<Integer, Integer>> iter = copyS.getSolNode().entrySet().iterator();
		while(iter.hasNext()){
			Entry<Integer, Integer> entry = iter.next();
			int originalkid = entry.getKey();
			if(originalkid == kid){
				solGNodeMap.put(kid, entry.getValue()+1);
			}
			else{
				solGNodeMap.put(originalkid, entry.getValue());
			}
		}
	}
	
	/*public String returnNode(){
		String retStr = "sum:";
		Iterator<Entry<Integer, Integer>> iter = solGNodeMap.entrySet().iterator();
		while(iter.hasNext()){
			Entry<Integer, Integer> entry = iter.next();
			Integer listIter = entry.getValue();
			if(listIter<){
				retStr += entry.getKey()+" "+listIter.next().getElement()+"   ";
				listIter.previous();
			}
			else{
				retStr += " no next!";
			}
		}
		
		return retStr;
	}*/
}
