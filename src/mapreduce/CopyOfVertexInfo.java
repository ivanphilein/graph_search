package mapreduce;

import java.util.ArrayList;
import java.util.HashMap;
import java.util.Iterator;
import java.util.List;
import java.util.Map;
import java.util.Map.Entry;
import java.util.TreeSet;

import shared.CompareIndexElement;
import shared.IndexElement;


public class CopyOfVertexInfo {
	private int vertexId;
	private double sumRMap;
	private List<Integer> sendingList = null;//the block ids for which this vertex are a out-portal node(we need to send to that block)
	private TreeSet<IndexElement> anceSet = null; // L_PN Object is  IndexElement
	private HashMap<Integer, IndexElement> resultMap = null;// a map with (kid, IndexElement) Object is  IndexElement
	
	private boolean update=false;
	public void setUpdate(boolean value){
		update = value;
	}
	public boolean getUpdate(){
		return update;
	}
	
	CopyOfVertexInfo(){
		vertexId=-1;
		sumRMap = 0;
	}
	
	CopyOfVertexInfo(int vid){
		vertexId=vid;
		sumRMap = 0;
	}

	
	//sending List part
	/**
	 * Add a block id to the sending list, sending list contains all the block ids for which this vertex are a out-portal node
	 * @param bid
	 */
	public void addToSendingList(int bid){
		if(sendingList == null)
			sendingList = new ArrayList<Integer>();
		sendingList.add(bid);
	}
	
	/**
	 * Return sending list, sending list contains all the block ids for which this vertex are a out-portal node
	 */
	public List<Integer> getSendingList(){
		return sendingList;
	}
	
	//end of sending List part
	
	//sumRMap part
	public double getSumRMap(){
		return sumRMap;
	}
	public void setSumRMap(double sum){
		sumRMap = sum;
	}
	//end of sumRMap part
	
	//vertexId part
	public int getVertexId(){
		return vertexId;
	}
	public void setVertexId(int id){
		vertexId = id;
	}
	//end of vertexId part
	
	//resultMap part
	/**
	 * get the resultMap map
	 * @return
	 */
	public HashMap<Integer, IndexElement> getResultMap(){
		return resultMap;
	}
	
	
	public HashMap<Integer, IndexElement> updateResultMap(int kid, IndexElement index){
		return resultMap;
	}
	
	/**
	 * write to result map
	 * @param kid
	 * @param resultStr string with all index element
	 * return if change, or insert, return true, if not change, return false
	 */
	public boolean writeResultMap(int kid, String resultStr){
		boolean update = false;
		String[] elementSet;
		String delimiter = " ";
		elementSet = resultStr.split(delimiter);
		IndexElement putElement = null;
		for(int i=0;i<elementSet.length;i++){
			putElement = new IndexElement(elementSet[i]);
			if(resultMap == null){
				resultMap = new HashMap<Integer, IndexElement>();
			}
			IndexElement oldElement = (IndexElement)resultMap.get(kid);
			if(oldElement == null){
				resultMap.put(kid, putElement);
				update = true;
				sumRMap += putElement.getLength();
				continue;
			}
			else if(putElement.getLength()<oldElement.getLength()){
				sumRMap = sumRMap-oldElement.getLength()+putElement.getLength();
				resultMap.remove(kid);
				resultMap.put(kid, putElement);
				update = true;
				sumRMap += putElement.getLength();
				continue;
			}
			
			//resultMap.put(kid, putElement);
		}
		return update;
	}
	/**
	 * write to result map
	 * @param kid
	 * @param resultElement
	 * @return
	 */
	public TreeSet<IndexElement> writeResultMap(int kid, IndexElement resultElement){
		if(resultMap == null){
			resultMap = new HashMap<Integer, IndexElement>();
		}
		IndexElement oldElement = (IndexElement)resultMap.get(kid);
		if(oldElement == null){
			resultMap.put(kid, resultElement);
		}
		else{
			//IndexElement oldElement = resultMap.get(kid);
			if(resultElement.getLength()<oldElement.getLength()){
				sumRMap = sumRMap-oldElement.getLength()+resultElement.getLength();
				//resultMap.remove(kid);
				resultMap.put(kid, resultElement);
			}
			return this.anceSet;
		}
		//resultMap.put(kid, resultElement);
		sumRMap += resultElement.getLength();
		return null;
	}
	/**
	 * get the IndexElement class based on vertex id and keyword id
	 * @param kid
	 * @return
	 */
	public IndexElement getElementBasedOnK(int kid){
		if(resultMap == null || !resultMap.containsKey(kid))
			return null;
		else{
			return (IndexElement)resultMap.get(kid);
		}
	}
		
	/**
	 * remove element based on key id
	 * @param key
	 * @return
	 */
	public HashMap<Integer, IndexElement> removeVertex2Result(Integer key){
		if(resultMap == null){
			return null;
		}
		resultMap.remove(key);
		return resultMap;
	}
	//end of v2ResultMap part
	
	//anceSet part
	public TreeSet<IndexElement> getAnceSet(){
		return anceSet;
	}
	/**
	 * Giving string of element, write to anceSet
	 * @param anceElement
	 */
	public void writeAnceSet(String anceElement){
		if(anceSet == null){
			anceSet = new TreeSet<IndexElement>(new CompareIndexElement());
		}
		if(!anceElement.trim().isEmpty()){
			String[] elementSet;
			String delimiter = " ";
			elementSet = anceElement.split(delimiter);
			IndexElement putElement = null;
			for(int i=0;i<elementSet.length;i++){
				putElement = new IndexElement(elementSet[i].toString());
				anceSet.add(putElement);
			}
		}
	}
	
	public void writeAnceSet(IndexElement anceElement){
		if(anceSet == null){
			anceSet = new TreeSet<IndexElement>(new CompareIndexElement());
		}
		anceSet.add(anceElement);
	}
	
	
	public boolean anceSetContain(Integer kid){
		return anceSet.contains(kid);
	}
	//end of vertex2Ance part
	
	/**
	 * Return a string which contain all the ance lise elements
	 * @return
	 */
	public String getAllAnceList(){
		if(anceSet == null)
			return null;
		String retStr = "";
		Iterator<IndexElement> iter = anceSet.iterator();
		IndexElement element = null;
		if(iter.hasNext()){
			element = iter.next();
			retStr += element.getElement();
		}
		while(iter.hasNext()){
			element = iter.next(); 
			retStr += " "+element.getElement();
		}
		
		return retStr;
	}
	
	/**
	 * Result example "RM 1:1.0,1,2,2" means to kid=1, dis=1.0, v_s=1, v_next=2, v_e=2
	 * @return
	 */
	public String getAllResultMap(){
		if(resultMap == null)
			return null;
		else{
			String retStr = "";
			Iterator<Entry<Integer,IndexElement>> iterMap = resultMap.entrySet().iterator();
			if(iterMap.hasNext()){
				@SuppressWarnings("rawtypes")
				Map.Entry entry = (Map.Entry) iterMap.next(); 
	    	    Object outkey = entry.getKey();
	    	    IndexElement tempElement = (IndexElement)resultMap.get(outkey);
	    	    retStr += outkey.toString()+":"+tempElement.getElement();
			}
    	    while(iterMap.hasNext()){
    	    	@SuppressWarnings("rawtypes")
				Map.Entry entry = (Map.Entry) iterMap.next(); 
	    	    Object outkey = entry.getKey();
	    	    IndexElement tempElement = (IndexElement)resultMap.get(outkey);
	    	    retStr += " "+outkey.toString()+":"+tempElement.getElement();
	    	    
    	    }
			return retStr;
		}
	}
	
	public String getFinalSubTree(){
		if(resultMap == null)
			return "";
		else{
			String retStr = this.vertexId+" - ";
			Iterator<Entry<Integer,IndexElement>> iterMap = resultMap.entrySet().iterator();
			boolean isFirst = true;
    	    while(iterMap.hasNext()){
    	    	@SuppressWarnings("rawtypes")
				Map.Entry entry = (Map.Entry) iterMap.next(); 
	    	    Object outkey = entry.getKey();
	    	    IndexElement tempElement = (IndexElement)resultMap.get(outkey);
	    	    if(isFirst){
	    	    	retStr += tempElement.getEndVertex();
	    	    	isFirst = false;
	    	    }
	    	    else{
	    	    	retStr += ","+tempElement.getEndVertex();
	    	    }
    	    }
			return retStr;
		}
	}
	/**
	 * Return the sum of score in the result map
	 * @return
	 */
	/*public int getSumResultMap(){
		if(resultMap == null)
			return 0;
		else{
			int sum = 0;
			Iterator<Entry<Integer,IndexElement>> iterMap = resultMap.entrySet().iterator();
    	    while(iterMap.hasNext()){
    	    	@SuppressWarnings("rawtypes")
				Map.Entry entry = (Map.Entry) iterMap.next(); 
	    	    Object outkey = entry.getKey();
	    	    IndexElement tempElement = resultMap.get(outkey);
	    	    sum += tempElement.getLength();
    	    }
			return sum;
		}
	}*/
	//end of show part
}
