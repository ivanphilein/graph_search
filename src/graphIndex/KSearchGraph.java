package graphIndex;

import java.io.IOException;
import java.util.HashMap;
import java.util.Map;
import java.util.Iterator;
import java.util.Map.Entry;



	
//store graph node information
public class KSearchGraph {
	
	private static HashMap<Integer, VertexClass> intToVertex = null;
	private static HashMap<Integer, EdgeClass> intToEdge = null;//from edge id to edge
	//private static HashMap<Integer, List<Object>> startVToEdge = null;//from start vertex id to its children
	//private static HashMap<Integer, List<Object>> endVToEdge =  null;//new HashMap<Integer, List<Object>>();//from end vertex id to its parents
	
	private static HashMap<Integer, Integer> blockNumMap = new HashMap<Integer, Integer>();//block num map, block id to number of nodes in this block
	//private static List<Integer> blockList = null;//block list
	//private static HashMap<Integer, String> intToKeyword = new HashMap<Integer, String>();
    private HashMap<String, Integer> keywordToInt = null;//new HashMap<String, Integer>();
    private int keywordNum=0;
    private int edgeNum = 0;
    
    public void addKeywordNum(int num){
    	keywordNum = keywordNum+num;
    }
    
    public void setKeywordNum(int num){
    	keywordNum=num;
    }
    
    public int getKeywordNum(){
    	return keywordNum;
    }
    public KSearchGraph(){
    	
    	
    }
    
    public void addBlockList(int bid){
        /*if(blockNumMap == null){
    		blockNumMap = new HashMap<Integer, Integer>();
    	}*/
        Object num = blockNumMap.get(bid);
        int retNum;
    	if(num == null){
    		retNum = 1;
    	}
    	else{
    		retNum = (Integer)num +1;
    	}
    	blockNumMap.put(bid, retNum);
    }
    
    public HashMap<Integer, Integer> getBlockNumMap(){
    	return blockNumMap;
    }
    
    public int getBlockNum(int bid){
    	return blockNumMap.get(bid);
    }
    
    //edge id to edge part
    public HashMap<Integer, EdgeClass> getEidToEdge(){
    	return intToEdge;
    }
    
    public int addEdgeInfo(EdgeClass edge){
    	if(intToEdge==null)
    		intToEdge = new HashMap<Integer, EdgeClass>();
    	edgeNum++;
    	edge.setEdgeID(edgeNum);
    	intToEdge.put(edgeNum, edge);
    	return edgeNum;
    }
    /**
     * Get edge based on vertex id
     * @return
     */
    public EdgeClass getEdgeFromID(int eid){
    	return intToEdge.get(eid);
    }
    //end of edge id to edge part
    
    /*public List<Object> getStartVToEdge(int vid){
    	if(startVToEdge==null)
    		return null;
    	return startVToEdge.get(vid);
    }*/
    
    /*public List<Object> getEndVToEdge(int vid){
    	if(endVToEdge==null)
    		return null;
    	return endVToEdge.get(vid);
    }*/
    //keyword to id part
    //write function from keyword to integer
  	public boolean writeKeywordToInt(String keyword, int kid){
  		if(keywordToInt==null)
  			keywordToInt = new HashMap<String, Integer>(); 
  		if(keywordToInt.containsKey(keyword)){
  			return false;
  		}
  		keywordToInt.put(keyword, kid);
  		return true;
  	}//end of writeKeywordToInt
    /**
     * return keywordToInt
     * @return HashMap<String, Integer>
     */
  	public HashMap<String, Integer> getKeywordToInt(){
  		return keywordToInt;
  	}//end of writeKeywordToInt
  	
  	//get id from keyword
  	public int getIDOfKeyword(String keyword){
  		Object num = keywordToInt.get(keyword);
  		if(num==null)
  			return -1;
  		else
  			return (Integer)num;
  	}
  	//end of keyword to id part
  	
    //start vertex to its children
    public void writeDirectedEdgeInfo(int eid, int from, int to, double weight) throws IOException{
    	//System.out.println(from+" "+to);
    	//List<Object> tempList = null;
    	EdgeClass tempEdge = new EdgeClass();
    	tempEdge.setEdgeID(eid);
    	tempEdge.setVFrom(from);
    	tempEdge.setVTo(to);
    	tempEdge.setWeight(weight);
    	addEdgeInfo(tempEdge);
    	VertexClass vertex = intToVertex.get(from);
    	if(vertex == null){
    		vertex = new VertexClass();
    	}
    	vertex.addOutGoingList(tempEdge);
    	vertex.addAdjEdgeList(tempEdge);
    	
    	vertex = intToVertex.get(to);
    	if(vertex == null){
    		vertex = new VertexClass();
    	}
    	vertex.addInComingList(tempEdge);
    	vertex.addAdjEdgeList(tempEdge);
    }
  	
  	/**
  	 * 
  	 * @param eid
  	 * @param from
  	 * @param to
  	 * @param weight
  	 * @throws IOException
  	 */
  	public void writeUnDirectededEdgeInfo(int id, int from, int to, double weight) throws IOException{
  		//System.out.println(from+" "+to+" "+weight);
    	//List<Object> tempList = null;
    	EdgeClass tempEdge = new EdgeClass();
    	tempEdge.setEdgeID(id);
    	tempEdge.setVFrom(from);
    	tempEdge.setVTo(to);
    	tempEdge.setWeight(weight);
    	//int eid = addEdgeInfo(tempEdge);
    	VertexClass vertex = intToVertex.get(from);
    	if(vertex == null){
    		vertex = new VertexClass();
    		intToVertex.put(from, vertex);
    	}
    	vertex.addAdjEdgeList(tempEdge);
    	addEdgeInfo(tempEdge);
    	vertex = intToVertex.get(to);
    	if(vertex == null){
    		vertex = new VertexClass();
    		intToVertex.put(to, vertex);
    	}
    	//eid = addEdgeInfo(tempEdge);
    	vertex.addAdjEdgeList(tempEdge);
    }
  	
    //end of writeStartV2Child
    
  //end vertex to its parents
   /* public void writeEndV2Parent(int eid, int parent, int vid, double weight) throws IOException{
    	List<Object> tempList = null;//new ArrayList<EdgeClass>();
    	EdgeClass tempEdge = new EdgeClass();
    	tempEdge.setEdgeID(eid);
    	tempEdge.setVFrom(parent);
    	tempEdge.setVTo(vid);
    	tempEdge.setWeight(weight);
    	
    	
    	if(endVToEdge == null)
    		endVToEdge = new HashMap<Integer, List<Object>>();
    	tempList = endVToEdge.get(vid);
    	if(tempList != null){
    		tempList.add(tempEdge);
    	}
    	else{
    		tempList = new ArrayList<Object>();
        	tempList.add(tempEdge);
        	endVToEdge.put(vid, tempList);
    	}
    }*/
    //end of writeStartV2Child
  	
 	//Write edge information, from "vid" to "child", weight is "weight", store in vertex class "vid"
    /*public boolean writeEdgeInfo(int eid, int vid, int child, double weight) throws IOException{
    	if(intToEdge == null)
    		intToEdge = new HashMap<Integer, Object>();
    	if(intToEdge.containsKey(eid)){
    		return false;
    	}
    	else{
    		EdgeClass newEdge = new EdgeClass();
    		newEdge.setEdgeID(eid);
    		newEdge.setVFrom(vid);
    		newEdge.setVTo(child);
    		newEdge.setWeight(weight);
    		intToEdge.put(eid, newEdge);
    		writeStartV2Child(eid,vid,child,weight);
    		writeEndV2Parent(eid,vid,child,weight);
    		return true;
    	}
    }*///end of writeEdgeInfo
    
  	//Write edge information, from "vid" to "child", weight is "weight", store in vertex class "vid"
   /* public boolean writeVertexChild(int parent, int vid, double weight) throws IOException{
    	if(intToVertex == null)
    		intToVertex = new HashMap<Integer, Object>();
    	VertexClass newVertex = (VertexClass)intToVertex.get(vid);
    	if(newVertex != null){
    		return newVertex.addParentList(parent, weight);
    	}
    	else{
    		newVertex = new VertexClass();
    		newVertex.setVertexID(vid);
    		newVertex.addParentList(parent, weight);
    		intToVertex.put(vid, newVertex);
    		return true;
    	}
    }*///end of writeVertexChild
    
    //Write keyword list to vertex class "vid"
    /**
     * Add a new vertex to intToVertex map
     * @param vid
     * @return
     * @throws IOException
     */
    public boolean addVertex(int vid) throws IOException{
    	if(intToVertex == null)
    		intToVertex = new HashMap<Integer, VertexClass>();
    	if(intToVertex.containsKey(vid)){
    		return false;
    	}
    	VertexClass newVertex = new VertexClass();//(VertexClass)intToVertex.get(vid);
    	newVertex.setVertexID(vid);
    	intToVertex.put(vid, newVertex);
    	return true;
    }
    
    public boolean writeKeywordList(int vid) throws IOException{
    	if(intToVertex == null)
    		intToVertex = new HashMap<Integer, VertexClass>();
    	
    	VertexClass ver = new VertexClass();
    	intToVertex.put(vid, ver);
    	
    	return true;
    	/*VertexClass newVertex = (VertexClass)intToVertex.get(vid);
    	if(newVertex != null){
    		return false;
    	}
    	else{
    		newVertex = new VertexClass();
    		newVertex.setVertexID(vid);
    		intToVertex.put(vid, newVertex);
    		return true;
    	}*/
    }
    
    /**
     * write keyword list to vertex
     * @param vid
     * @param keywordList first element is treated as weight
     * @return
     * @throws IOException
     */
    public boolean writeKeywordList(int vid, String keywordList) throws IOException{
    	if(intToVertex == null)
    		intToVertex = new HashMap<Integer, VertexClass>();

    	VertexClass newVertex = (VertexClass)intToVertex.get(vid);
    	if(newVertex != null){
    		newVertex.addKeyword(keywordList);//set keyword list and weight
    		this.addKeywordNum(newVertex.getKeyWord().size());
    		return false;
    	}
    	else{
    		newVertex = new VertexClass();
    		newVertex.setVertexID(vid);
    		newVertex.addKeyword(keywordList);
    		this.addKeywordNum(newVertex.getKeyWord().size());
    		intToVertex.put(vid, newVertex);
    		return true;
    	}
    }//end of writeKeywordList
    
    /**
     * return VertexClass from vid (Complexity: map get complexity = log) 
     * @param vid
     * @return
     */
    public VertexClass getVertex(int vid){
    	return  intToVertex.get(vid);
    }//end of getVertex
    
    //return VertexClass from vid
    public HashMap<Integer, VertexClass> getVertexMap(){
    	return intToVertex;
    }//end of getVertex
    
    //show all vertexes keywords and children
    public void showVertex() throws IOException{
    	Iterator<Entry<Integer, VertexClass>> iter = intToVertex.entrySet().iterator(); 
    	while (iter.hasNext()) { 
    	    @SuppressWarnings("rawtypes")
			Map.Entry entry = (Map.Entry) iter.next(); 
    	    VertexClass vertexkey = (VertexClass)entry.getValue(); 
    	    System.out.println("node "+vertexkey.getVertexID()+":");
    	    vertexkey.showKeywords();
    	    vertexkey.showParents();
    	    //vertexkey.showBid2OutPortalList();
    	} 
    }

}