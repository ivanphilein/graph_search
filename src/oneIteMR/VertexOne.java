package oneIteMR;

import java.io.IOException;
import java.util.HashMap;
import java.util.HashSet;
import java.util.Iterator;
import java.util.Map.Entry;

import org.apache.hadoop.io.DoubleWritable;
import org.apache.hadoop.io.Writable;
	
//store graph node information
public class VertexOne {
	private final Double Infinity = 10000.0;
	private int vertexID;
	private double weight;
	//private HashSet<Integer> keywordSet = null;
	
    private HashSet<EdgeOne> inComingSet = null;//incoming edges to this vertex
    private HashSet<EdgeOne> outGoingSet = null;//outgoing edges from this vertex
    
    private final int WHITE = 0;//not visit yet
    //private final int GRAY = 1;//in the open list
    //private final int BLACK = 2;//in the close list
    private int state = WHITE;
    
    //This part is the result map part, just for one iteration mapreduce
    private HashMap<Integer, HashSet<Integer>> resultMap = null;//new HashMap<Integer, HashSet<Integer>>();
    
    
    
    //seperateAct part
    /*public void initialSeperateAct(HashSet<Integer> querySet){
    	
    }*/
    //End of seperateAct part
  //resultMap part
  	/**
  	 * get the resultMap map
  	 * @return
  	 */
  	public HashMap<Integer, HashSet<Integer>> getResultMap(){
  		return resultMap;
  	}
  	
  	/**
  	 * This function is used for nodes which has keyword by itself
  	 * If there exsits solution for this keyword, Do not update
  	 * @param kid
  	 * @param pathSet
  	 */
  	public void addToResultMap(int kid, HashSet<Integer> pathSet){
  		if(resultMap==null)
  			resultMap = new HashMap<Integer, HashSet<Integer>>();
  		if(resultMap.get(kid)==null){
  			resultMap.put(kid, pathSet);
  		}
  	}
  	
  	/**
  	 * add to result map, based on one known vertex, the cost is the edge weight from this node to the known vertex
  	 * @param kid
  	 * @param vertex
  	 * @param cost
  	 * Return the size of result map, in case to check solution found or not
  	 */
  	public int addToResultMap(int kid, VertexOne vertex, double cost){
  		if(resultMap==null)
  			resultMap = new HashMap<Integer, HashSet<Integer>>();
  		HashSet<Integer> pathSet = resultMap.get(kid);
  		if(pathSet==null){
  			pathSet = new HashSet<Integer>();
  			HashSet<Integer> pSet = vertex.getResultMap().get(kid);
  			pathSet.add(vertex.getVertexID());
  			if(pSet!=null){
  				Iterator<Integer> iter = pSet.iterator();
  				while(iter.hasNext()){
  					pathSet.add(iter.next());
  				}
  			}
  			resultMap.put(kid, pathSet);
  		}
  		//this.updateDistMap(kid, vertex.getDistance(kid), cost);
  		return resultMap.size();
  	}
  	
  	/**
  	 * Return one string which has all information about this result map
  	 * @return
  	 */
  	public String showResultMap(){
  		String retStr = this.getActivation()+"-"+this.getVertexID()+"";
  		Iterator<Entry<Integer, HashSet<Integer>>> iter = resultMap.entrySet().iterator();
  		while(iter.hasNext()){
  			Entry<Integer, HashSet<Integer>> entry = iter.next();
  			retStr+=" "+entry.getKey()+":";
  			retStr+=entry.getValue().toString();
  		}
  		return retStr;
  	}
  	
  	public String showLeafMap(){
  		String retStr = this.getActivation()+"-"+this.getVertexID()+"";
  		Iterator<Entry<Integer, Integer>> iter = leafMap.entrySet().iterator();
  		if(iter.hasNext()){
  			Entry<Integer, Integer> entry = iter.next();
  			int kid = entry.getKey();
  			retStr+=",("+kid+":";
  			retStr+=entry.getValue()+"-"+this.distMap.get(kid)+")";
  		}
  		while(iter.hasNext()){
  			Entry<Integer, Integer> entry = iter.next();
  			int kid = entry.getKey();
  			retStr+=",("+kid+":";
  			retStr+=entry.getValue()+"-"+this.distMap.get(kid)+")";
  		}
  		return retStr;
  	}
  	
  	
  	/**
  	 * Get size of resultmap
  	 * @return
  	 */
  	public int getSizeOfRM(){
  		return distMap.size();
  	}
  	
  	
    //end of result map part
    
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
    	//if(value != WHITE && value!=GRAY && value!=BLACK)
    		//return false;
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
    
    //incoming edges part
    /**
     * Add incoming edge to this vertex
     * @param EdgeOne
     * @return
     * @throws IOException
     */
    public boolean addInComingEdge(EdgeOne EdgeOne)throws IOException{
    	if(inComingSet == null)
    		inComingSet = new HashSet<EdgeOne>();
    	return inComingSet.add(EdgeOne);
    }//end of addParentList

    /**
     * Return parents list O(1)
     * @return
     * @throws IOException
     */
    public HashSet<EdgeOne> getInComingSet()throws IOException{
        return inComingSet;
    }//end of getParentList
    //End of incoming edge part
    
    
  //outGoing edges part
    /**
     * Add outGoing edge to this vertex
     * @param EdgeOne
     * @return
     * @throws IOException
     */
    public boolean addOutGoingEdge(EdgeOne EdgeOne)throws IOException{
    	if(outGoingSet == null)
    		outGoingSet = new HashSet<EdgeOne>();
    	return outGoingSet.add(EdgeOne);
    }//end of addParentList

    /**
     * Return outGoing Set O(1)
     * @return
     * @throws IOException
     */
    public HashSet<EdgeOne> getOutGoingSet()throws IOException{
        return outGoingSet;
    }//end of getParentList
    //End of outGoing edge part
    
    /**
     * Read node information, test for bidirection search
     * @param keyArray
     * @return
     * @throws IOException
     */
    /*public boolean addKeyword(String[] keyArray)throws IOException{
    	if(keywordSet==null)
    		keywordSet = new HashSet<Integer>();
        this.weight = Double.parseDouble(keyArray[3]);
        for(int i=4;i<keyArray.length;i++){
        	int kid = Integer.parseInt(keyArray[i]);
        	keywordSet.add(kid);
        }
        return true;
    }*/
    /**
     * 
     * @param keyword
     * @param querySet
     * @param kidToNodes
     * @return
     * @throws IOException
     */
    public boolean addKeyword(String keyword, HashSet<Integer> querySet, HashMap<Integer, HashSet<VertexOne>> kidToNodes)throws IOException{
    	/*if(keywordSet==null)
    		keywordSet = new HashSet<Integer>();*/
        String[] temp;
        String delimiter = " ";
        temp=keyword.split(delimiter);
        this.weight = Double.parseDouble(temp[0]);
        for(int i=1;i<temp.length;i++){
        	int kid = Integer.parseInt(temp[i]);
        	if(querySet.contains(kid)){
        		HashSet<Integer> pathSet = new HashSet<Integer>();
        		pathSet.add(this.vertexID);
        		this.updateLeafMap(kid, this.vertexID);
        		HashSet<VertexOne> nodeSet = kidToNodes.get(kid);
        		if(nodeSet==null){
        			nodeSet = new HashSet<VertexOne>();
        			kidToNodes.put(kid, nodeSet);
        		}
        		nodeSet.add(this);
        		this.updateDistMap(kid, 0, 0);
        		this.setDepth(0);
        	}
        }
        if(this.getSizeOfRM()==querySet.size())
        	return true;
        else
        	return false;
    }
    
    public boolean addKeyword(Writable[] keyArray, HashSet<Integer> querySet, HashMap<Integer, HashSet<VertexOne>> kidToNodes)throws IOException{
    	/*if(keywordSet==null)
    		keywordSet = new HashSet<Integer>();*/
        this.weight = ((DoubleWritable)keyArray[2]).get();
        for(int i=3;i<keyArray.length;i++){
        	int kid = (int)((DoubleWritable)keyArray[i]).get();
        	//System.out.println("vid:"+this.vertexID+" kid:"+kid);
        	//keywordSet.add(kid);
        	if(querySet.contains(kid)){
        		HashSet<Integer> pathSet = new HashSet<Integer>();
        		pathSet.add(this.vertexID);
        		//this.addToResultMap(kid, pathSet);
        		this.updateLeafMap(kid, this.vertexID);
        		HashSet<VertexOne> nodeSet = kidToNodes.get(kid);
        		if(nodeSet==null){
        			nodeSet = new HashSet<VertexOne>();
        			kidToNodes.put(kid, nodeSet);
        		}
        		nodeSet.add(this);
        		this.updateDistMap(kid, 0, 0);
        		this.setDepth(0);
        	}
        }
        if(this.getSizeOfRM()==querySet.size())
        	return true;
        else
        	return false;
    }
    //end of addKeyword
    
    //Search keyword by id
    /*public boolean searchKeyword(int keyword)throws IOException{
    	Iterator<Integer> it = keywordSet.iterator();  
        while (it.hasNext()){  
        	if(it.next()==keyword)
        		return true;  
        }
    	return false;
    }*///end of searchKeyword
    
    //Return keyword list
    /*public HashSet<Integer> getKeyWord()throws IOException{
        return keywordSet;
    }*///end of getKeyword
    ///end of keyword part
    
    ////////////////////////////////////////////////////////////////
    //BiDir Search part
    //anceSet is the set of vertexes which are ancestors of this vertex, used to update if some better pathes found
    private HashMap<VertexOne, Double> anceMap = null;
    private VertexOne followV = null;
    private int depth = -1;//the level of search going, used to prune, do not get too deep
    private double activation = Infinity;
    private HashMap<Integer, Integer> leafMap = null;//new HashMap<Integer, Integer>();
    private HashMap<Integer, Double> distMap = new HashMap<Integer, Double>();
    
    public void setFollowV(VertexOne vertex){
    	followV = vertex;
    }
    
    public VertexOne getFollowV(){
    	return followV;
    }
    //leafMap part
    public void updateLeafMap(int kid, int leafId){
    	if(leafMap==null)
    		leafMap = new HashMap<Integer, Integer>();
    	leafMap.put(kid, leafId);
    }
    
    public HashMap<Integer, Integer> getLeafMap(){
    	
    	return leafMap;
    }
    
    public int getLeafByKid(int kid){
    	return leafMap.get(kid);
    }
    
    //end of leafMap part
    //distMap part
    /**
     * Update the distance to one keyword kid, if new distance is smaller, return true
     * @param kid
     * @param dist
     * @return
     */
    public boolean updateDistMap(int kid, double dist, double weight){
    	dist = dist + weight;
    	double oldDis = this.getDistance(kid);
    	if(oldDis==-1 || dist<oldDis){
    		distMap.put(kid, dist);
    		if(oldDis!=-1)
    			dist = dist-oldDis;
    		updateActivation(dist);
    		return true;
    	}
    	return false;
    }
    
    public String showDisMap(){
    	String outStr = this.getVertexID()+":";
    	Iterator<Entry<Integer, Double>> iter = distMap.entrySet().iterator();
    	while(iter.hasNext()){
    		Entry<Integer, Double> entry = iter.next();
    		outStr += "["+entry.getKey()+","+entry.getValue()+"]";
    	}
    	return outStr;
    }
    
    public boolean containDistance(int kid){
    	return distMap.containsKey(kid);
    }
    
    public HashMap<Integer, Double> getDisMap(){
    	return distMap;
    }
    
    public double getDistance(int kid){
    	if(!distMap.containsKey(kid))
    		return -1;
    	double wei = distMap.get(kid);
    	if(wei<0)
    		return 0-wei;
    	return wei;
    }
    //end of distance map part
    
    //end of distMap part
    //Activation part
    /*public void setActivation(double set){
    	activation = set;
    }*/
    /**
     * Default is 0, each time, if update, do a add operation
     * @param add
     */
    /*public void addActivation(int kid, double add){
    	if(activation==Infinity)
    		activation = 0;
    	activation += add;
    	//this.updateActMap(kid, add);
    }*/
    
    public void updateActivation(double add){
    	if(activation==Infinity)
    		activation = add;
    	else
    		activation = activation + add;
    }
    
    /*public void setActivation(double act){
    	activation  = act;
    }*/
    
    /**
     * original that is the sum of actMap, but now we change that to sum of distMap, we use distance as activation
     * @return
     */
    public double getActivation(){
    	/*if(distMap.isEmpty())
    		return Infinity;
    	Iterator<Entry<Integer, Double>> iter = distMap.entrySet().iterator();
    	double act = 0;
    	while(iter.hasNext()){
    		Entry<Integer, Double> entry = iter.next();
    		act+=entry.getValue();
    	}
    	return act;*/
    	return activation;
    }
    //End of Activation part
    
    public void insetAnceSet(VertexOne vertex, double dis){
    	if(anceMap==null)
    		anceMap = new HashMap<VertexOne, Double>();
    	anceMap.put(vertex, dis);
    	
    }
    
    public HashMap<VertexOne, Double> getAnceMap(){
    	return anceMap;
    }
    
    public double getAnceDis(VertexOne ver){
    	if(anceMap==null)
    		anceMap = new HashMap<VertexOne, Double>();
    	return anceMap.get(ver);
    }
    
    public void setDepth(int dep){
    	depth = dep;
    }
    
    public int getDepth(){
    	return depth;
    }
    //End of BiDir Search part
    ////////////////////////////////////////////////////////////////
    
}