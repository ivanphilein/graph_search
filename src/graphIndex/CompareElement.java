package graphIndex;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.Iterator;
import java.util.List;
import java.util.TreeSet;
import shared.IndexClass;




public class CompareElement {
	
	
	private int vertexId;
	private int count; 			// # of edges links to this vertex
	private int numInBlock; 	//# of nodes in the block that this vertex is in
	
	//private HashSet<Integer> outGoingSet = new HashSet<Integer>();
	//private HashSet<Integer> inComingSet = new HashSet<Integer>();
	//private HashSet<Integer> candInComingSet = new HashSet<Integer>();
	//private HashSet<Integer> candOutGoingSet = new HashSet<Integer>();
	//private int outgoingEdgeToAnotherBlock; //this vertex has an outgoing edge to another block  
	//private int incomingEdgeSameBlock; //this vertex has an incoming edge from the same block
	private List<Integer> updateList = null;
	//private List<Object> updateList = null;	// nodes from the adjacent edges 
	//private List<Object> sameBlock = null;	// nodes from the same block 
	public int blockID;
	
	
	/*public void addToOutGoingSet(int addbid){
		outGoingSet.add(addbid);
	}
	
	public HashSet<Integer> getOutGoingSet(){
		return outGoingSet;
	}
	
	public void addToInComingSet(int addbid){
		inComingSet.add(addbid);
	}
	
	public HashSet<Integer> getInComingSet(){
		return inComingSet;
	}
	
	public void addToCandInComingSet(int addbid){
		candInComingSet.add(addbid);
	}
	
	public HashSet<Integer> getCandInComingSet(){
		return candInComingSet;
	}
	
	public void addToCandoutGoingSet(int addbid){
		candOutGoingSet.add(addbid);
	}
	
	public HashSet<Integer> getCandoutGoingSet(){
		return candOutGoingSet;
	}*/
	
	
	//incoming edge part
	/*public void addNumIncomingEdgeFromSameBlock(int add){
		incomingEdgeSameBlock += add;
	}
	public void setNumIncomingEdgeFromSameBlock(int eID){
		incomingEdgeSameBlock = eID;
	}
	public int getIncomingEdgeFromSameBlock(){
		return incomingEdgeSameBlock;
	}*/
	//end of incoming edge part
	//outgoing edge part
	/*public void addNumOutgoingEdgeToAnotherBlock(int add){
		outgoingEdgeToAnotherBlock += add;
	}
	public void setNumOutgoingEdgeToAnotherBlock(int eID){
		outgoingEdgeToAnotherBlock = eID;
	}
	public int getOutgoingEdgeToAnotherBlock(){
		return outgoingEdgeToAnotherBlock;
	}*/
	//end of outgoing edge part
	//
	public CompareElement(){
		vertexId = -1;
		count = 0;
		numInBlock = 0;
		updateList = null;
	}
	public CompareElement(int vid){
		vertexId = vid;
		count = 0;
		numInBlock = 0;
		updateList = null;
	}
	public CompareElement(int vid, int bNum){
		vertexId = vid;
		count = 0;
		numInBlock = bNum;
		updateList = null;
	}
	public CompareElement(int vid, int bid, int bNum){
		vertexId = vid;
		count = 0;
		numInBlock = bNum;
		blockID = bid;
		updateList = null;
	}
	//set all element
	
	//end of setting all element
	//vertex id part
	public int getVertexId(){
		return vertexId;
	}
	public void setVertexId(int vid){
		vertexId = vid;
	}
	//end of vertex id part
	public int getNumInBlock(){
		return numInBlock;
	}
	public void setNumInBlock(int num){
		numInBlock=num;
	}
	//count part
	public int getCount(){
		return count;
	}
	public void addCountBy(int num){
		count = count+num;
	}
	public void setCountByNum(int num){
		count = num;
	}
	public void subCountBy(int num){
		count = count-num;
	}
	//end of count part
	//sameBlock part
	/*public void setSameBlcok(List<Object> tempList){
		 sameBlock = tempList;
	}
	public List<Object> getSameBlcok(){
		return sameBlock;
	}
	public void addToSameBlock(Object newO){
		if(sameBlock == null){
			sameBlock = new ArrayList<Object>();
		}
		//if(!sameBlock.contains((CompareElement)newO))
			sameBlock.add(newO);
	}*/
	//end of sameBlock part
	//updateList part
	/*public List<Object> getUpdateList(){
		return updateList;
	}
	public void addToUpdateList(Object newO){
		if(updateList == null){
			updateList = new ArrayList<Object>();
		}
		//if(!updateList.contains((CompareElement)newO))
			updateList.add(newO);
	}*/
	
	
	public List<Integer> getUpdateList(){
		return updateList;
	}
	public void addToUpdateList(Integer newO){
		if(updateList == null){
			updateList = new ArrayList<Integer>();
		}
		//if(!updateList.contains((CompareElement)newO))
			updateList.add(newO);
	}
	
	public void removeFromUpdateList(Object vid){
		if(updateList.contains(vid))
			updateList.remove(vid);
	}
	
	public void showUpdateList(){
		Iterator<Integer> it = updateList.iterator();  
        while (it.hasNext()){ 
        	int value = it.next();
        	System.out.print(value+" ");
        }
        System.out.println("");
	}
	
	
	public TreeSet<CompareElement> updateListBySub(int num, TreeSet<CompareElement>portalSet, KSearchGraph graph, IndexClass index,HashMap<Integer, CompareElement> storeMap){
		if(updateList!=null){
			Iterator<Integer> it = updateList.iterator();  
	        while (it.hasNext()){ 
	        	int value = it.next();
	        	CompareElement valueClass = (CompareElement)storeMap.get(value);
	        	//System.out.println("updateList:"+ valueClass.getVertexId()+" "+vertexId);
	        	if(portalSet.remove(valueClass)){
		        	valueClass.subCountBy(num);
		        	//valueClass.showUpdateList();
		        	valueClass.removeFromUpdateList(vertexId);
		        	portalSet.add(valueClass);
	        	}
	        }
		}
		return portalSet;
	}
	
	
	
	/*public TreeSet<Object> updateListBySub(int num, TreeSet<Object>portalSet, KSearchGraph graph, IndexClass index,HashMap<Integer, Object> storeMap){
		if(updateList!=null){
			Iterator<Object> it = updateList.iterator();  

			//cal = Calendar.getInstance();
			//System.out.println(Debugger.getCallerPosition()+updateList.size()+" update update List at"+" "+sdf.format(cal.getTime()));
			
	        while (it.hasNext()){ 
	        	Object value = it.next();
	        	CompareElement valueClass = (CompareElement)value;
	        	System.out.println("updateList:"+ valueClass.getVertexId());
	        	if(portalSet.remove(valueClass)){
		        	valueClass.subCountBy(num);
		        	valueClass.removeFromUpdateList((Integer)vertexId);
		        	portalSet.add(valueClass);
	        	}
	        }
	        //cal = Calendar.getInstance();
			//System.out.println(Debugger.getCallerPosition()+updateList.size()+" finish updating update List at"+" "+sdf.format(cal.getTime()));
		}
		/*if(sameBlock!=null){
			
	        cal = Calendar.getInstance();
			System.out.println(Debugger.getCallerPosition()+sameBlock.size()+"update sameBlock at"+" "+sdf.format(cal.getTime()));
			Iterator<Object> it = sameBlock.iterator();  
	        while (it.hasNext()){ 
	        	Object value = it.next();
	        	CompareElement valueClass = (CompareElement)value;
	        	System.out.println("sameBlock:"+ valueClass.getVertexId());
	        	if(portalSet.remove(valueClass)){
		        	valueClass.setNumInBlock(valueClass.getNumInBlock()-1);
		        	portalSet.add(valueClass);
	        	}
	        }
	        cal = Calendar.getInstance();
			System.out.println(Debugger.getCallerPosition()+sameBlock.size()+" finish updating sameBlock at"+" "+sdf.format(cal.getTime()));
		}*/
		//portalSet.remove(this);
		/*return portalSet;
	}*/
}
