package graphSearch;


import java.io.BufferedReader;
import java.io.DataInputStream;
import java.io.FileInputStream;
import java.io.IOException;
import java.io.InputStreamReader;
import java.util.HashMap;
import java.util.HashSet;
import java.util.Iterator;
import java.util.Map;
import java.util.TreeMap;
import java.util.Map.Entry;
import java.util.Set;

import org.jgrapht.UndirectedGraph;
import org.jgrapht.graph.AsWeightedGraph;
import org.jgrapht.graph.DefaultWeightedEdge;
import org.jgrapht.graph.SimpleGraph;

import shared.Debugger;

public class GraphClass {
	
	private UndirectedGraph<Integer, DefaultWeightedEdge> iniGraph = new SimpleGraph<Integer, DefaultWeightedEdge>(DefaultWeightedEdge.class);
	private Map<DefaultWeightedEdge, Double> weiMap = new HashMap<DefaultWeightedEdge, Double>();
	private AsWeightedGraph<Integer, DefaultWeightedEdge> graph = new AsWeightedGraph<Integer, DefaultWeightedEdge>(iniGraph, weiMap);
	
	private HashMap<Integer, Vertex> vidToVMap = new HashMap<Integer, Vertex>();//store from keyword id to vertex set
	private HashMap<Integer, HashSet<Integer>> keyToVMap = new HashMap<Integer, HashSet<Integer>>();//store from keyword id to vertex set

	private HashMap<Integer, HashSet<Integer>> bidToVMap = null;// new HashMap<Integer, HashSet<Integer>>();//store block id to all its vertexes in that block
	private HashSet<Integer> portalSet = null;
	
	public void addToPortalSet(int vid){
		if(portalSet == null){
			portalSet = new HashSet<Integer>();
		}
		portalSet.add(vid);
		
	}
	
	public HashSet<Integer> getPortalSet(){
		return portalSet;
	}
	
	public GraphClass(){
		
	}
	
	public GraphClass(String nodefile, String edgefile){
		this.readUnDirectedGraph(nodefile, edgefile, true);
	}
	
	public GraphClass(String nodefile, String edgefile, boolean storeKey){
		this.readUnDirectedGraph(nodefile, edgefile, storeKey);
	}
	
	public GraphClass(String nodefile, String edgefile, String partitionfile, String portalfile, boolean storeKey){
		try {
			this.readUnDirectedGraph(nodefile, edgefile, storeKey);
			this.readPartitionFile(partitionfile);
			this.getPortalMap(portalfile, true);
		} catch (IOException e) {
			// TODO Auto-generated catch block
			e.printStackTrace();
		}
		
	}
	///////////////////////////////////////////////////////////////////////////////////////////////
	//bidToVMap part
	/**
	 * Add vertex to corresponding block
	 * @param bid
	 * @param vid
	 */
	public void addVertexToBlock(int bid, int vid){
		if(bidToVMap == null){
			bidToVMap = new HashMap<Integer, HashSet<Integer>>();
		}
		HashSet<Integer> verSet = bidToVMap.get(bid);
		if(verSet==null){
			verSet = new HashSet<Integer>();
			bidToVMap.put(bid, verSet);
		}
		verSet.add(vid);
		Vertex ver = this.getVertexFromVid(vid);
		//ver.addToBlockList(bid);
		ver.setBlock(bid);
	}
	/**
	 * Get the set of vertexes from one block
	 * @param bid
	 * @return
	 */
	public HashSet<Integer> getVertexSetFromBid(int bid){
		return bidToVMap.get(bid);
	}
	
	
	public HashMap<Integer, HashSet<Integer>> getBidToVMap(){
		return bidToVMap;
	}
	//end of bidToVMap part
	///////////////////////////////////////////////////////////////////////////////////////////////
	public HashMap<Integer, Vertex> getVidToVMap(){
		return vidToVMap;
	}
	/**
	 * From vid, get the vertex class
	 * @param vid
	 * @return
	 */
	public Vertex getVertexFromVid(int vid){
		return vidToVMap.get(vid);
	}
	
	public HashSet<Integer> getVertexSetFromKey(int kid){
		return keyToVMap.get(kid);
	}
	/**
	 * After one search, resume vertex as inintial
	 */
	public void resumeVer(){
		Iterator<Entry<Integer, Vertex>> iter = vidToVMap.entrySet().iterator();
		while(iter.hasNext()){
			iter.next().getValue().resume();
		}
	}
	
	/**
     * Add vertex vid with key set
     * @param vid
     * @param keySet
     */
    public void addGraphVertex(int vid, double wei, HashSet<Integer> keySet){
    	if(!graph.containsVertex(vid)){
    		graph.addVertex(vid);
    		Vertex ver = vidToVMap.get(vid);
    		if(ver==null){
    			ver = new Vertex();
    			ver.setVertexId(vid);
    			vidToVMap.put(vid, ver);
    		}
    		ver.setVertexId(vid);
    		ver.setWeight(wei);
    		if(keySet != null){
	    		ver.addKeySet(keySet);
	    		//insert pair to keyToVMap
	    		Iterator<Integer> iter = keySet.iterator();
	    		while(iter.hasNext()){
	    			int kid = iter.next();
	    			//ver.initialPathMap(kid, 0, vid, vid);
	    			HashSet<Integer> vidSet = keyToVMap.get(kid);
	    			if(vidSet==null){
	    				vidSet = new HashSet<Integer>();
	    				keyToVMap.put(kid, vidSet);
	    			}
	    			vidSet.add(vid);
	    		}
    		}
    		//end of inserting pair to keyToVMap
    	}
    }
    
    public void addGraphVertexWithPortal(int vid, int portalid, double wei, HashSet<Integer> keySet){
    	this.addToPortalSet(vid);
    	if(!graph.containsVertex(vid)){
    		graph.addVertex(vid);
    		Vertex ver = vidToVMap.get(vid);
    		if(ver==null){
    			ver = new Vertex();
    			ver.setVertexId(vid);
    			vidToVMap.put(vid, ver);
    		}
    		ver.setVertexId(vid);
    		ver.setPortalBlock(portalid);
    		ver.setWeight(wei);
    		ver.addKeySet(keySet);
    		//insert pair to keyToVMap
    		Iterator<Integer> iter = keySet.iterator();
    		while(iter.hasNext()){
    			int kid = iter.next();
    			//ver.initialPathMap(kid, 0, vid, vid);
    			HashSet<Integer> vidSet = keyToVMap.get(kid);
    			if(vidSet==null){
    				vidSet = new HashSet<Integer>();
    				keyToVMap.put(kid, vidSet);
    			}
    			vidSet.add(vid);
    		}
    		//end of inserting pair to keyToVMap
    	}
    }
    
    /**
     * Add edge from v1 to v2, with weight weight
     * @param v1
     * @param v2
     * @param weight
     */
    public void addGrageEdge(int v1,int v2, Double weight) {
    	DefaultWeightedEdge edge = graph.addEdge(v1, v2); 
    	graph.setEdgeWeight(edge, weight);
    }
    
    public AsWeightedGraph<Integer, DefaultWeightedEdge> getGraph(){
    	return this.graph;
    }
    
    
    /**
     * Read graph based on node and edge files
     * @param nodeFile
     * @param edgeFile
     */
    public void readUnDirectedGraph(String nodeFile, String edgeFile, boolean storeKey){
    	if(nodeFile!=null){
    		System.out.println(Debugger.getCallerPosition()+"Read file: "+nodeFile +"...");
	  		try{
	  			  FileInputStream fstream = new FileInputStream(nodeFile);
	  			  // Get the object of DataInputStream
	  			  DataInputStream in = new DataInputStream(fstream);
	  			  BufferedReader br = new BufferedReader(new InputStreamReader(in));
	  			  String strLine;
			  	  String[] temp;
		  	      String delimiter = " ";
		  	      int read = 0;
		  	      while ((strLine = br.readLine()) != null)   {
		  	    	  if(strLine.startsWith("#"))
		  	    		  continue;
		  	    	  temp = strLine.split(delimiter);
		  	    	  int vid = Integer.parseInt(temp[0]);
		  	    	  double wei = Double.parseDouble(temp[1]);
		  	    	  HashSet<Integer> keySet = null;//
		  	    	  if(storeKey){
		  	    		  keySet = new HashSet<Integer>();
			  	    	  for(int i=2;i<temp.length;i++){
			  	    		  keySet.add(Integer.parseInt(temp[i]));
			  	    	  }
		  	    	  }
		  	    	  addGraphVertex(vid, wei, keySet);
		  	    	  read++;
		  	    	  if(read%100000==0)
		  	    		  System.out.println("read node num:"+read);
		  	      }
	  			  //Close the input stream
	  			  in.close();
	  		}catch (Exception e){//Catch exception if any
	  			  System.err.println("Error: " + e.getMessage());
	  		}	
    	}
  		
  		System.out.println(Debugger.getCallerPosition()+"Finish reading file: "+nodeFile +"...");
  		if(edgeFile!=null){
	  		System.out.println(Debugger.getCallerPosition()+"Read file: "+edgeFile +"...");
	  		try{
	  			  FileInputStream fstream = new FileInputStream(edgeFile);
	  			  // Get the object of DataInputStream
	  			  DataInputStream in = new DataInputStream(fstream);
	  			  BufferedReader br = new BufferedReader(new InputStreamReader(in));
	  			  String strLine;
			  	  String[] temp;
		  	      String delimiter = " ";
		  	      int read = 0;
		  	      while ((strLine = br.readLine()) != null)   {
		  	    	//System.out.println("read edge num:"+read);
		  	    	  if(strLine.startsWith("#"))
		  	    		  continue;
		  	    	  temp = strLine.split(delimiter);
		  	    	  if(temp.length==4){
		  	    		  addGrageEdge(Integer.parseInt(temp[1]), Integer.parseInt(temp[2]), Double.parseDouble(temp[3]));
		  	    	  }
		  	    	  read++;
		  	    	  if(read%100000==0)
		  	    		  System.out.println("read edge num:"+read);
		  	      }
	  			  //Close the input stream
	  			  in.close();
	  		}catch (Exception e){//Catch exception if any
	  			  System.err.println("Error: " + e.getMessage());
	  		}	
	  		
	  		System.out.println(Debugger.getCallerPosition()+"Finish reading file: "+edgeFile +"...");
  		}
  	}//end of Read functon
    
    /**
	 * Read portal node file and mark portal nodes
	 * @param graphClass
	 * @param portalFile
	 * @return
	 */
	public TreeMap<Integer, HashSet<Integer>> getPortalMap(String portalFile, boolean store){
		System.out.println("Read portal node file:"+portalFile);
		TreeMap<Integer, HashSet<Integer>> portalBlockMap = new TreeMap<Integer, HashSet<Integer>>();
		try{
			FileInputStream fstream = new FileInputStream(portalFile);
	  		// Get the object of DataInputStream
	  		DataInputStream in = new DataInputStream(fstream);
	  		BufferedReader br = new BufferedReader(new InputStreamReader(in));
			
			String strLine;
			while ((strLine = br.readLine()) != null)   {
	  			if(strLine.startsWith("#"))
	  				continue;
	  			this.addToPortalSet(Integer.parseInt(strLine));
	  			
			}
			br.close();
			Iterator<Integer> iter = portalSet.iterator();
			while(iter.hasNext()){
				int vid = iter.next();
				Vertex vertex = this.getVertexFromVid(vid);
				//int bid = graphClass.getVertexFromVid(vid).getBlockList().iterator().next();
				int bid = this.getVertexFromVid(vid).getBlock();
				vertex.setPortalBlock(bid);
				HashSet<Integer> blockSet = portalBlockMap.get(bid);
				if(blockSet == null){
					blockSet = new HashSet<Integer>();
					portalBlockMap.put(bid, blockSet);
				}
				blockSet.add(vid);
			}
			if(store==false){
				portalSet = null;
			}
		}
		catch (Exception e){//Catch exception if any
			  System.err.println("Error: " + e.getMessage());
		}
		System.out.println("Finish reading portal node file:"+portalFile+ " total block number with portal nodes:"+portalBlockMap.size());
		return portalBlockMap;
	}
    
	/**
	 * Read partition result file from Metis
	 * @param partitionFile
	 * @param graphClass
	 * @throws IOException
	 * Return the biggest block size
	 */
	public void readPartitionFile(String partitionFile) throws IOException{
  		FileInputStream fstream = new FileInputStream(partitionFile);
  		// Get the object of DataInputStream
  		DataInputStream in = new DataInputStream(fstream);
  		BufferedReader br = new BufferedReader(new InputStreamReader(in));
  		String strLine;
  		int vid = 1;
  		while ((strLine = br.readLine()) != null)   {
  			if(strLine.startsWith("#"))
  				continue;
  			int bid = Integer.parseInt(strLine);
  			this.addVertexToBlock(bid, vid);
  			if(vid %10000 == 0){
  			    System.out.println("Read :"+partitionFile+" "+vid+" "+bid);
  			}
  			vid++;
  		}
  		in.close();
  		//System.out.println("End of reading file: "+partitionFile +"...");
	}
    /**
     * show graph edges
     * @return
     */
    public String showGraph(){
    	String graphStr = "";
    	Set<DefaultWeightedEdge> edgeSet = graph.edgeSet();
    	Iterator<DefaultWeightedEdge> iter = edgeSet.iterator();
    	while(iter.hasNext()){
    		DefaultWeightedEdge edge = iter.next();
    		graphStr += graph.getEdgeSource(edge)+" "+graph.getEdgeTarget(edge)+" "+graph.getEdgeWeight(edge)+"\n";
    	}
    	return graphStr;
    }
    
}
