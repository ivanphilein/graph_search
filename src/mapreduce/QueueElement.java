package mapreduce;

import java.util.ArrayList;
import java.util.Collections;
import java.util.HashMap;
import java.util.Iterator;
import java.util.List;
import java.util.Map.Entry;

public class QueueElement {
	//private int INFINITE = 10000000;
	//vertex id
	private int vertexID;
	//the result key map, key is keyword id, value is a list of the path, the path is Descending, the last one is this vertex, the first one is the vertex contains this keyword
	private HashMap<Integer,List<Integer>> keyMap = null;
	private double sum;
	
	public QueueElement(int vid){
		vertexID = vid;
		sum = 0;
	}
	//vertex id part
	public int getVertexID(){
		return vertexID;
	}
	//end of vertex id part
	//sum part
	public void setSum(double num){
		sum = num;
	}
	public double getSum(){
		return sum;
	}
	//end of sum part
	//key map part
	/**
	 * get key map
	 * @return
	 */
	public HashMap<Integer, List<Integer>> getKeyMap(){
		return keyMap;
	}
	/**
	 * Initial key map just based on this vertex, start vertexes
	 * @param keyList
	 * @return
	 */
	public boolean initialKeyMap(List<Integer> keyList){
		if(keyList!=null){
			Iterator<Integer> iterKey = keyList.iterator();
			while(iterKey.hasNext()){
				List<Integer> newList = new ArrayList<Integer>();
				newList.add(vertexID);
				keyMap.put(iterKey.next(), newList);
			}
		}
		return true;
	}
	/**
	 * giving a hashmap from parent node, then update and get a new hashmap for child node
	 * @param parentMap
	 * @return true if update correct, return false if there is loop in the path, please do free the memory of this element
	 */
	public boolean setKeyMap(QueueElement parentElement, double weight, List<Integer> keyList){
		if(keyMap == null){
			keyMap = new HashMap<Integer, List<Integer>>();
		}
		HashMap<Integer, List<Integer>> parentMap = parentElement.getKeyMap();
		if(parentMap == null)
			return false;
		//HashMap<Integer,Integer> fromNodeMap = new HashMap<Integer, Integer>();
		//update based on parent information
		Iterator<Entry<Integer, List<Integer>>> iter = parentMap.entrySet().iterator();
		while(iter.hasNext()){
			Entry<Integer, List<Integer>> entry = iter.next();
			int keyid = entry.getKey();
			List<Integer> pathList = entry.getValue();
			if(!pathList.contains(vertexID)){
				List<Integer> childList = new ArrayList<Integer>();
				/*int fromVID = pathList.get(pathList.size()-1);
				Object numOfFromID = fromNodeMap.get(fromVID);
				if(numOfFromID==null){
					fromNodeMap.put(fromVID, );
				}
				else{
					numOfFromID = (Integer)numOfFromID +1;
				}*/
				Collections.copy(childList, pathList);
				childList.add(vertexID);
				keyMap.put(keyid, childList);
				keyList.remove(keyid);
			}
			else{
				//fromNodeMap = null;
				keyMap = null;
				return false;
			}
		}
		//if there are some keywords this vertex has but its parent can not reach, add that part
		if(keyList.size()!=0){
			Iterator<Integer> iterKey = keyList.iterator();
			
			while(iterKey.hasNext()){
				List<Integer> newList = new ArrayList<Integer>();
				newList.add(vertexID);
				keyMap.put(iterKey.next(), newList);
			}
		}
		//total sum is the sum of parent + edge weight
		sum = parentElement.getSum()+weight;
		/*Iterator<Entry<Integer, Integer>> iterSum = fromNodeMap.entrySet().iterator();
		while(iterSum.hasNext()){
			sum 
		}*/
		return true;
	}
	//end of key map part
}
