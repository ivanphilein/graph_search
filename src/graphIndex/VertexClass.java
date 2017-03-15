package graphIndex;
import java.io.IOException;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.HashSet;
import java.util.Iterator;
import java.util.List;
import java.util.Map.Entry;
import java.util.TreeSet;

import shared.IndexElement;
	
//store graph node information
public class VertexClass {
	
	private int vertexID;
	private double weight=1.0;
	private HashSet<Integer> keywordList = null;
	
	//following two lists are used for dericted graph
    private List<EdgeClass> outGoingList = null;//edge ids which are from this node
    private List<EdgeClass> inComingList = null;//new ArrayList<EdgeClass>();//edge ids which are to this node
    
    private List<EdgeClass> adjEdgeList = null;//edge ids which are related to this node(used for undericted graph), only contains the edge from this node
    
    //private TreeMap<Integer, Object> outPortalMap = null; // Integer is the from vertex id, for each vertex, at most one index element
    
    private HashSet<Integer> blockList = null;
    
    //In order to make the DFS or BFS works in directed graph and also graph with loop, we create three boolean in order to check one node is visited or expanded
    private final int WHITE = 0;//not visit yet
    private final int GRAY = 1;//in the open list
    private final int BLACK = 2;//in the close list
    private int state = WHITE;
    private IndexElement i_element = null;//this element is in the ance set, used to update when the state is GRAY, if state is WHITE or BLACK, this should be null
    
    private int portalBlock = -1;
    
    public void setPortalBlock(int pid){
    	portalBlock = pid;
    }
    
    public int getPortalBlock(){
    	return portalBlock;
    }
    
    //This part is the result map part, just for one iteration mapreduce
    private HashMap<Integer, HashSet<Integer>> resultMap = new HashMap<Integer, HashSet<Integer>>();
  //resultMap part
  	/**
  	 * get the resultMap map
  	 * @return
  	 */
  	public HashMap<Integer, HashSet<Integer>> getResultMap(){
  		return resultMap;
  	}
  	
  	/**
  	 * If there exsits solution for this keyword, Do not update
  	 * @param kid
  	 * @param pathSet
  	 */
  	public void addToResultMap(int kid, HashSet<Integer> pathSet){
  		if(resultMap.get(kid)==null){
  			resultMap.put(kid, pathSet);
  		}
  	}
  	
  	/**
  	 * Get size of resultmap
  	 * @return
  	 */
  	public int getSizeOfRM(){
  		return resultMap.size();
  	}
  	
  	
    //end of result map part
    
    
    
    /**
     * Return the indexelement
     * @return
     */
    public IndexElement getElement(){
    	return i_element;
    }
    /**
     * Set the i_element as element, it will be better set element as null when state is WHITE or BLACK
     * @param element
     */
    public void setElement(IndexElement element){
    	i_element = element;
    }
    
    public void setWeight(double w){
    	weight = w;
    }
    
    public double getWeight(){
    	return weight;
    }
    
    /**
     * Change the state to value, value only can be 0,1 or 2
     * @param value
     * @return
     */
    public boolean chanageState(int value){
    	if(value != WHITE && value!=GRAY && value!=BLACK)
    		return false;
    	state = value;
    	return true;
    }
    
    /**
     * Return the value of state
     * @return
     */
    public int getState(){
    	return state;
    }
    
    //private List<Integer> outBlockList = null;
    
    //private TreeSet<IndexElement> outPortalSet = null;
    //map from block id to the corresponding outportal set
    private HashMap<Integer, TreeSet<IndexElement> > outPortalMap = null;
    
    /**
     * Return out block list of this vertex
     * @return
     */
    /*public List<Integer> getOutBlockList(){
		return outBlockList;
	}*/
    /**
     * Add bid to out block list
     * @param bid
     */
    /*public void addToOutBlockList(int bid){
    	if(outBlockList == null){
    		outBlockList = new ArrayList<Integer>();
    	}
    	if(!outBlockList.contains(bid))
    		outBlockList.add(bid);
    }*/
    
    /**
     * O(1)
     * Return block list of this vertex
     * @return
     */
    public HashSet<Integer> getBlockList(){
		return blockList;
	}
    /**
     * Add bid to block list
     * @param bid
     */
    public void addToBlockList(int bid){
    	if(blockList == null){
    		blockList = new HashSet<Integer>();
    	}
    	blockList.add(bid);
    }
    
    
    //////////////////////////////////////////////////////////////////////////////////////
    //outportalMap part start
    /**
     * Add one element to the ourportal set
     * @param bid
     * @param outSetElement
     */
    public void addElementToOutportalMap(int bid, IndexElement outSetElement){
    	if(outPortalMap == null)
    		outPortalMap = new HashMap<Integer, TreeSet<IndexElement> >();
    	TreeSet<IndexElement> anceSet = outPortalMap.get(bid);
    	if(anceSet==null){
    		anceSet = new TreeSet<IndexElement>(new CompareIndexElement());
    		outPortalMap.put(bid, anceSet);
    	}
    	anceSet.add(outSetElement);
    }
    
    /**
     * Return the string of all blocks number use for mapreduce part, different blocks seperated by ":"
     * @return String
     */
    public String generateOutPortalBlock(){
    	if(outPortalMap == null)
    		return null;
    	Iterator<Entry<Integer, TreeSet<IndexElement>>> iter = outPortalMap.entrySet().iterator();
    	String retStr = "";
    	if(iter.hasNext()){
    		Entry<Integer, TreeSet<IndexElement>> entry = iter.next();
    		retStr += entry.getKey();
    	}
    	while(iter.hasNext()){
    		Entry<Integer, TreeSet<IndexElement>> entry = iter.next();
    		retStr += ":"+entry.getKey();
    	}
    	return retStr;
    }
    
    /**
     * Add to outportal map, from block id to corresponding L_PN set O(1)
     * @param bid
     * @param outSet
     */
    public void addToOutportalMap(int bid, TreeSet<IndexElement> outSet){
    	if(outPortalMap == null)
    		outPortalMap = new HashMap<Integer, TreeSet<IndexElement> >();
    	outPortalMap.put(bid, outSet);
    }
    
    /**
     * Return outPortal map O(1)
     * @return
     */
    public HashMap<Integer, TreeSet<IndexElement>> getOutPortalMap(){
    	return outPortalMap;
    }
    
    /**
     * Giving block id bid, return the corresponding out portal set, return null if there is not
     * @param bid
     * @return
     */
    public TreeSet<IndexElement> getOutPortalSetBasedOnBid(int bid){
    	if(outPortalMap==null)
    		return null;
    	return outPortalMap.get(bid);
    }
    
    /**
     * Giving block id bid, return the corresponding outportal set element (To a string)
     * @param bid
     * @return
     */
    public String getOutPortalListElement(int bid){
    	if(outPortalMap==null)
    		return null;
    	TreeSet<IndexElement> outPortalSet = outPortalMap.get(bid);
    	if(outPortalSet == null){
    		return null;
    	}
    	Iterator<IndexElement> iter = outPortalSet.iterator();
    	String outPortalList = "";
    	while (iter.hasNext()) {
    		if(outPortalList != ""){
    			outPortalList += " ";
    		}
    	    IndexElement showElement = (IndexElement)iter.next();
    	    outPortalList += showElement.getElement();
    	} 
    	return outPortalList;
    }
    
    //outportalMap part end
    //////////////////////////////////////////////////////////////////////////////////////
    
    
    
    
    /**
     * add one index element to out portal map
     * @param outIndex
     */
    /*public void addOutPortalSet( IndexElement outIndex){
    	if(outPortalSet == null){
    		outPortalSet = new TreeSet<IndexElement>(new CompareIndexElement());
    	}
    	outPortalSet.add(outIndex);
    }*/
    
    /**
     * Return out portal map
     * @return
     */
   /* public TreeSet<IndexElement> getOutPortalSet(){
    	return outPortalSet;
    }*/
    /**
     * Set out portal set from a map
     * @param portalSet
     */
    /*public void setOutPortalSet(TreeMap<Integer, IndexElement> portalMap){
    	if(outPortalSet == null)
    		outPortalSet = new TreeSet<IndexElement>(new CompareIndexElement());
    	Iterator<Entry<Integer, IndexElement>> iter = portalMap.entrySet().iterator();
    	while (iter.hasNext()) {
    	    @SuppressWarnings("rawtypes")
			Map.Entry entry = (Map.Entry) iter.next(); 
    	    IndexElement value = (IndexElement)entry.getValue();
    	    value.showElement();
    	    System.out.println(outPortalSet.contains(value));
    	    if(!outPortalSet.contains(value))
    	    outPortalSet.add(value);
    	    System.out.println(outPortalSet.contains(value));
    	}
    }*/
    
    /*public String getOutPortalListElement(){
    	if(outPortalSet==null)
    		return null;
    	Iterator<IndexElement> iter = outPortalSet.iterator();
    	String outPortalList = "";
    	while (iter.hasNext()) {
    		if(outPortalList != ""){
    			outPortalList += " ";
    		}
    	    IndexElement showElement = (IndexElement)iter.next();
    	    outPortalList += showElement.getElement();
    	} 
    	return outPortalList;
    }
    public void showBid2OutPortalList(){
    	Iterator<IndexElement> iter = outPortalSet.iterator();
    	while (iter.hasNext()) { 
    	    IndexElement showElement = (IndexElement)iter.next();
    	    showElement.showElement();
    	} 
    }*/
    /*public boolean setPortal(boolean b){
    	boolean temp = portal;
    	portal = b;
    	return temp;
    }
    
    public boolean getPortal(){
    	return portal;
    }
  	*/
  	//vertex id part
    //Set vertex id
    public void setVertexID(int id){
    	vertexID = id;
    }//end of setVertexID
    
    //Get vertex id
    public int getVertexID(){
    	return vertexID;
    }//end of getVertexID
    //end of vertex id part
    
    ///children part
  	//Add children id to vertex
    public boolean addOutGoingList(EdgeClass edgeClass)throws IOException{
    	if(outGoingList == null)
    		outGoingList = new ArrayList<EdgeClass>();
    	outGoingList.add(edgeClass);
    	return true;
    }//end of addChildList

    //Return children list
    public List<EdgeClass> getoutGoingList ()throws IOException{
        return outGoingList;
    }//end of getChildren
    ///end of children part
    
    ///parent part
  	//Add parent id to vertex
    public boolean addInComingList(EdgeClass edgeClass)throws IOException{
    	if(inComingList == null)
    		inComingList = new ArrayList<EdgeClass>();
    	inComingList.add(edgeClass);
    	//edgeWeight.put(parent, weight);
    	return true;
    }//end of addParentList

    /**
     * Return parents list O(1)
     * @return
     * @throws IOException
     */
    public List<EdgeClass> getInComingList()throws IOException{
        return inComingList;
    }//end of getParentList
    ///end of parents part
    
  ///undericted graph part
  	//Add edge id to vertex
    public boolean addAdjEdgeList(EdgeClass edge)throws IOException{
    	if(adjEdgeList == null)
    		adjEdgeList = new ArrayList<EdgeClass>();
    	adjEdgeList.add(edge);
    	return true;
    }//end of addChildList

    /**
     * Get the list of all adjacent edges, the element is edge id
     * @return
     * @throws IOException
     */
    public List<EdgeClass> getAdjEdgeList ()throws IOException{
        return adjEdgeList;
    }//end of getChildren
    ///end of undericted graph part
    
    
    ///keyword part
    //Add keyword id to vertex
    public boolean addKeyword(String keyword)throws IOException{
    	if(keywordList==null)
    		keywordList = new HashSet<Integer>();;
        String[] temp;
        String delimiter = " ";
        temp=keyword.split(delimiter);
        this.weight = Double.parseDouble(temp[0]);
        for(int i=1;i<temp.length;i++){
        	keywordList.add(Integer.parseInt(temp[i]));
        }
        return true;
    }//end of addKeyword
    
    //Search keyword by id
    public boolean searchKeyword(int keyword)throws IOException{
    	Iterator<Integer> it = keywordList.iterator();  
        while (it.hasNext()){  
        	if(it.next()==keyword)
        		return true;  
        }
    	return false;
    }//end of searchKeyword
    
    //Return keyword list
    public HashSet<Integer> getKeyWord()throws IOException{
        return keywordList;
    }//end of getKeyword
    ///end of keyword part
    
    public void showKeywords()throws IOException{
    	Iterator<Integer> it = keywordList.iterator();
    	if(it.hasNext()){
	    	System.out.print("keywords:");
	    	System.out.print(it.next());
	        for(; it.hasNext();) {
	        	System.out.print(",");
	            System.out.print(it.next());
	        }
	        System.out.println("");
    	}
    }
    public void showChildren()throws IOException{
    	Iterator<EdgeClass> it = outGoingList.iterator();
    	if(it.hasNext()){
        	System.out.print("children:");
        	System.out.print("(");
	    	System.out.print(it.next());
	        for(; it.hasNext();) {
	        	System.out.print(",");
	            System.out.print(it.next());
	        }
	        System.out.println(")");
    	}
    }
    
    public void showParents()throws IOException{
    	Iterator<EdgeClass> it = inComingList.iterator();
    	if(it.hasNext()){
        	System.out.print("parents:");
        	System.out.print("(");
	    	System.out.print(it.next());
	        for(; it.hasNext();) {
	        	System.out.print(",");
	            System.out.print(it.next());
	        }
	        System.out.println(")");
    	}
    }
    
}