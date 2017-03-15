package oneIteMR;

import java.io.IOException;
import java.util.HashMap;
import java.util.HashSet;

import org.apache.hadoop.io.DoubleWritable;
import org.apache.hadoop.io.Writable;


	
//store graph node information
public class GraphOne {
	
	private static HashMap<Integer, VertexOne> intToVertex = null;
	private static HashMap<Integer, EdgeOne> intToEdge = null;//from edge id to edge
    //private HashMap<String, Integer> keywordToInt = null;//new HashMap<String, Integer>();
    //private int keywordNum=0;
    private int edgeNum=0;
    
    //edge id to edge part
    public HashMap<Integer, EdgeOne> getEidToEdge(){
    	return intToEdge;
    }
    
    public int addEdgeInfo(EdgeOne edge){
    	if(intToEdge==null)
    		intToEdge = new HashMap<Integer, EdgeOne>();
    	edgeNum++;
    	edge.setEdgeID(edgeNum);
    	intToEdge.put(edgeNum, edge);
    	return edgeNum;
    }
    /**
     * Get edge based on vertex id
     * @return
     */
    public EdgeOne getEdgeFromID(int eid){
    	return intToEdge.get(eid);
    }
    //end of edge id to edge part
  	
    //start vertex to its children
    public void writeDirectedEdgeInfo(int eid, int from, int to, double weight) throws IOException{
    	//System.out.println("Edge:"+from+" "+to);
    	//List<Object> tempList = null;
    	EdgeOne tempEdge = new EdgeOne();
    	tempEdge.setEdgeID(eid);
    	tempEdge.setVFrom(from);
    	tempEdge.setVTo(to);
    	tempEdge.setWeight(weight);
    	addEdgeInfo(tempEdge);
    	VertexOne vertex = intToVertex.get(from);
    	if(vertex == null){
    		vertex = new VertexOne();
    	}
    	vertex.addOutGoingEdge(tempEdge);
    	vertex = intToVertex.get(to);
    	if(vertex == null){
    		vertex = new VertexOne();
    	}
    	vertex.addInComingEdge(tempEdge);
    }
  	
  	/**
  	 * 
  	 * @param eid
  	 * @param from
  	 * @param to
  	 * @param weight
  	 * @throws IOException
  	 */
  	public void writeUnDirectededEdgeInfo(int from, int to, double weight) throws IOException{
    	////System.out.println(from+" !!!!!! "+to);
    	//List<Object> tempList = null;
    	EdgeOne tempEdge = new EdgeOne();
    	tempEdge.setVFrom(from);
    	tempEdge.setVTo(to);
    	tempEdge.setWeight(weight);
    	//int eid = addEdgeInfo(tempEdge);
    	VertexOne vertex = intToVertex.get(from);
    	if(vertex == null){
    		vertex = new VertexOne();
    		intToVertex.put(from, vertex);
    	}
    	vertex.addInComingEdge(tempEdge);
    	
    	vertex = intToVertex.get(to);
    	if(vertex == null){
    		vertex = new VertexOne();
    		intToVertex.put(to, vertex);
    	}
    	tempEdge = new EdgeOne();
    	tempEdge.setVFrom(to);
    	tempEdge.setVTo(from);
    	tempEdge.setWeight(weight);
    	vertex.addOutGoingEdge(tempEdge);
    }
  	
    //end of writeStartV2Child
    
    //Write keyword list to vertex class "vid"
    /**
     * Add a new vertex to intToVertex map
     * @param vid
     * @return
     * @throws IOException
     */
    public boolean addVertex(int vid) throws IOException{
    	if(intToVertex == null)
    		intToVertex = new HashMap<Integer, VertexOne>();
    	if(intToVertex.containsKey(vid)){
    		return false;
    	}
    	VertexOne newVertex = new VertexOne();//(VertexOne)intToVertex.get(vid);
    	newVertex.setVertexID(vid);
    	intToVertex.put(vid, newVertex);
    	return true;
    }
    
    /*public boolean writeKeywordList(int vid, String[] keyArray) throws IOException{
    	if(intToVertex == null)
    		intToVertex = new HashMap<Integer, VertexOne>();
    	VertexOne newVertex = (VertexOne)intToVertex.get(vid);
    	if(newVertex == null){
    		newVertex = new VertexOne();
    		newVertex.setVertexID(vid);
    		intToVertex.put(vid, newVertex);
    	}
    	return true;//newVertex.addKeyword(keyArray);
    }*/
    
    /**
     * write keyword list to vertex
     * @param vid
     * @param keywordList first element is treated as weight
     * @return true if one whole solution found
     * @throws IOException
     */
    public boolean writeKeywordList(int vid, String keywordList, HashSet<Integer> querySet, HashMap<Integer, HashSet<VertexOne>> kidToNodes) throws IOException{
    	if(intToVertex == null)
    		intToVertex = new HashMap<Integer, VertexOne>();

    	VertexOne newVertex = (VertexOne)intToVertex.get(vid);
    	if(newVertex == null){
    		newVertex = new VertexOne();
    		newVertex.setVertexID(vid);
    		intToVertex.put(vid, newVertex);
    	}
		return newVertex.addKeyword(keywordList, querySet, kidToNodes);//set keyword list and weight
    }
    
    
    public boolean writeKeyArray(Writable[] array, HashSet<Integer> querySet, HashMap<Integer, HashSet<VertexOne>> kidToNodes) throws IOException{
    	if(intToVertex == null)
    		intToVertex = new HashMap<Integer, VertexOne>();
    	int vid = (int)((DoubleWritable)array[1]).get();
    	VertexOne newVertex = (VertexOne)intToVertex.get(vid);
    	if(newVertex == null){
    		newVertex = new VertexOne();
    		newVertex.setVertexID(vid);
    		intToVertex.put(vid, newVertex);
    	}
		return newVertex.addKeyword(array, querySet, kidToNodes);//set keyword list and weight
    }
    
    //end of writeKeywordList
    
    /**
     * return VertexOne from vid (Complexity: map get complexity = log) 
     * @param vid
     * @return
     */
    public VertexOne getVertex(int vid){
    	return  intToVertex.get(vid);
    }//end of getVertex
    
    //return VertexOne from vid
    public HashMap<Integer, VertexOne> getVertexMap(){
    	return intToVertex;
    }//end of getVertex
    
    
    ///////////////////////////////////////////////////////////////////
    //BiDir search part
    //End of BiDir search part
    ///////////////////////////////////////////////////////////////////

}