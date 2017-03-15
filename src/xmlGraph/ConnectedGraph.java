package xmlGraph;

import java.io.BufferedReader;
import java.io.BufferedWriter;
import java.io.DataInputStream;
import java.io.FileInputStream;
import java.io.FileWriter;
import java.io.InputStreamReader;
import java.text.DecimalFormat;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.HashSet;
import java.util.Iterator;
import java.util.List;
import java.util.Map;
import java.util.Random;
import java.util.TreeMap;
import java.util.Map.Entry;
import java.util.Stack;
import java.util.TreeSet;

import shared.Debugger;

class Node {
	   int id;
	   boolean visited;
	   boolean haveEdge;
	   List<Integer> adjList = new ArrayList<Integer>();
	   List<Integer> keyList = new ArrayList<Integer>();
 	   public Node(int num) {
	      this.id = num;
	      visited = false;
	      haveEdge = false;
	   }
}

class Edge {
	int srcid;
	int tgtid;
	   
}


public class ConnectedGraph {
	private static HashMap<Integer, Node> nodeMap = new HashMap<Integer, Node> ();
	private static HashMap<Integer, Edge> edgeMap = new HashMap<Integer, Edge> ();
	private static HashMap<Integer, TreeSet<Integer> > subGraphList = new HashMap<Integer, TreeSet<Integer>>();//sub graph id to all nodes in this sug-graph list
	private static HashMap<Integer, HashSet<Integer> > nodeAdj = new HashMap<Integer, HashSet<Integer>>();//from node id to all the adj list
	
	private static int nodeNum  = 0;
	private static int edgeNum = 0;
	
	//private static String folder = "data/compare/";
	//private static String folder = "data/DBLP/";
	private static String folder = "data/DBLPPaperWithAuthor/";
	//private static String folder = "data/testgraph/";
	//private static String folder = "data/DBLPSmallWithAuthor/";
	//private static String folder = "data/DBLPSmallOnlycrossRef/";
	private static String inputNode = folder+"nodes.txt";
	private static String inputEdge = folder+"edges.txt";
	//private static String inputMetis = folder+"metisGraphYifan.txt";
	//private static String inputMetis = folder+"test.txt";
	private static String outputNode = folder+"subnodes.txt";
	private static String outputEdge = folder+"subedges.txt";
	private static String outputError = folder+"suberror.txt";
	private static String outputNodeNum = folder+"subnodenum.txt";

	
	public static int generateRandomInteger(int aStart, int aEnd){
	    if ( aStart > aEnd ) {
	        throw new IllegalArgumentException("Start cannot exceed End.");
	      }
	      //get the range, casting to long to avoid overflow problems
	      long range = (long)aEnd - (long)aStart + 1;
	      // compute a fraction of the range, 0 <= frac < range
	      Random random = new Random();
	      long fraction = (long)(range * random.nextDouble());
	      int randomNumber =  (int)(fraction + aStart);  
	      return randomNumber;
	}
	
	public static boolean checkHaveSingleNode(String nodeFile, String edgeFile){
		System.out.println(Debugger.getCallerPosition()+"Read file: "+nodeFile +"...");
  		try{
  			//check node ids are continuous
  			FileInputStream fstream = new FileInputStream(nodeFile);
  			DataInputStream in = new DataInputStream(fstream);
  			BufferedReader br = new BufferedReader(new InputStreamReader(in));
  			String strLine;
		  	String[] temp;
	  	    String delimiter = ",";
	  	    while ((strLine = br.readLine()) != null)   {
	  			if(strLine.startsWith("#"))
	  				continue;
	  	        nodeNum++;
	  			int nid = Integer.parseInt(strLine.substring(0, strLine.indexOf(" ")));
	  			//System.out.println("nid "+nid);
	  			//remove nodeid
	  			strLine = strLine.substring(strLine.indexOf(" ")+1);
	  			//System.out.println(strLine);
	  			//remove weight
	  			strLine = strLine.substring(strLine.indexOf(" ")+1);
	  			//System.out.println(strLine);
	  	        Node newNode = new Node(nodeNum);
	  	        nodeMap.put(nid, newNode);
	  	        if(strLine.trim().isEmpty())
	  	        	continue;
	  	        temp = strLine.split(delimiter);
	  	        if(nodeNum != nid){
	  	        	System.out.println("not continues");
	  	        	return true;
	  	        }
	  	        for(int i=0;i<temp.length;i++){
	  	        	if(temp[i].trim()!="")
	  	        		newNode.keyList.add(Integer.parseInt(temp[i]));
	  	        }
	  	        if(nodeNum%10000==0)
	  	        	System.out.println("read node num:"+nodeNum);
	  		}
	  	    System.out.println("nodeNum "+nodeNum);
	  	    
	  	    //check node have edges or not
  			FileInputStream fstreamEdge = new FileInputStream(edgeFile);
  			DataInputStream inEdge = new DataInputStream(fstreamEdge);
  			BufferedReader brEdge = new BufferedReader(new InputStreamReader(inEdge));
  			String strLineEdge;
	  	    //int edgeID = 0;
	  	    while ((strLineEdge = brEdge.readLine()) != null)   {
	  	    	if(strLineEdge.startsWith("#")) continue;
			  	edgeNum++;
			  	//remove the edge ID
	  	    	strLineEdge = strLineEdge.substring(strLineEdge.indexOf(" ")+1);
			  	//get source node ID
			  	int srcid = Integer.parseInt(strLineEdge.substring(0, strLineEdge.indexOf(" ")));
			  	//get rid of srcid
			  	strLineEdge = strLineEdge.substring( strLineEdge.indexOf(" ") + 1);
			  	int tgtid = Integer.parseInt(strLineEdge.substring( 0, strLineEdge.indexOf(" ")));
			  	
			  	
			  	
			  	nodeMap.get(srcid).haveEdge = true;
			  	nodeMap.get(tgtid).haveEdge = true;
			  	nodeMap.get(srcid).adjList.add(tgtid);
				nodeMap.get(tgtid).adjList.add(srcid);
				Edge newEdge = new Edge();
				newEdge.srcid = srcid;
				newEdge.tgtid = tgtid;
				edgeMap.put(edgeNum,newEdge);
			  	//System.out.println("edge "+srcid+" "+tgtid);
	  	        if(edgeNum%100000==0)
	  	        	System.out.println("read edge num:"+edgeNum);
	  		}
	  	    
	  	    //check whether there are single node or not
	  	    /*int checkNode = 0;
	  	    for(Map.Entry<Integer,Node> e: nodeMap.entrySet()){
	  	    	checkNode++;
	  	        if(checkNode%100000==0)
	  	        	System.out.println("check node num:"+checkNode+" edgeMap size "+edgeMap.size());
				  if(e.getValue().haveEdge==false)
					  return true;
			}*/
	  	    
				  		
  		}catch (Exception e){//Catch exception if any
			  System.err.println("Error: " + e.getMessage());
		}
		return false;
		
	}
	/**
	 * Do DFS to generate the subgraph subID is the id of that subgraph
	 * @param root
	 * @param subID
	 */
	public static void dfs(int root, int subID) {
	    // DFS uses Stack data structure
	    Stack<Integer> stack = new Stack<Integer>();
	    stack.push(root);
	    nodeMap.get(root).visited = true;
	    subGraphList.get(subID).add(root);
	    while(!stack.isEmpty()) {
	        Node node = nodeMap.get(stack.pop());
		    //System.out.println(root+" "+stack.size()+" "+node.id);
	        List<Integer> adjList = node.adjList;
	  	    //System.out.println(node.id+" SIZE "+node.adjList.size());
	        Iterator<Integer> iter = adjList.iterator();
	        while(iter.hasNext()){
	            int adjN = iter.next();
	            //System.out.print(adjN+" ");
	            Node adjNode = nodeMap.get(adjN);
	            if(adjNode.visited==false){
	            	subGraphList.get(subID).add(adjN);
	            	stack.push(adjN);
	            	adjNode.visited=true;
	            }
	        }
	        //System.out.println("");
	    }
	}
	
	/**
	 * Break the loop in the graph//NOT FINISHED
	 * @return
	 */
	public static HashMap<Integer, Edge> breakLoop(){
		HashMap<Integer, Edge> retMap = new HashMap<Integer, Edge>();
		HashMap<Integer, HashSet<Integer>> reachMap = new HashMap<Integer, HashSet<Integer>>();
		for(Map.Entry<Integer, Edge> e: edgeMap.entrySet()){
			Edge tempEdge = e.getValue();
			int srcid = tempEdge.srcid;
			int tgtid = tempEdge.tgtid;
			HashSet<Integer> tgtSet = reachMap.get(tgtid);
			if(tgtSet != null){
				if(tgtSet.contains(tgtid))
					continue;
			}
			HashSet<Integer> srcSet = reachMap.get(srcid);
			if(srcSet == null){
				srcSet = new HashSet<Integer>();
				reachMap.put(srcid, srcSet);
			}
			srcSet.add(tgtid);
			retMap.put(e.getKey(),e.getValue());
		}
		return retMap;
	}
	
	/*public static void Read(String filename){
  		System.out.println(Debugger.getCallerPosition()+"Read file: "+filename +"...");
  		try{
  			FileInputStream fstream = new FileInputStream(filename);
  			// Get the object of DataInputStream
  			DataInputStream in = new DataInputStream(fstream);
  			BufferedReader br = new BufferedReader(new InputStreamReader(in));
  			String strLine;
		  	String[] temp;
	  	    String delimiter = " ";
	  	    int nodeID = 1;
	  	    if((strLine = br.readLine()) != null){
	  	    	temp = strLine.split(delimiter);
	  	    	nodeNum = Integer.parseInt(temp[0]);
	  	    	edgeNum = Integer.parseInt(temp[1]);
	  	    }
	  	    while ((strLine = br.readLine()) != null)   {
	  			if(strLine.startsWith("#"))
	  				continue;
	  	        temp = strLine.split(delimiter);
	  	        Node newNode = new Node(nodeID);
	  	        for(int i=0; i<temp.length;i++){
	  	        	newNode.adjList.add(Integer.parseInt(temp[i]));
	  	        }
	  	        nodeMap.put(nodeID, newNode);
	  	        nodeID++;
	  	        if(nodeID%100000==0)
	  	        	System.out.println("read node from metis graph num:"+nodeID);
	  		}
	  	    System.out.println(nodeNum+" "+nodeID);
				  		
  		}catch (Exception e){//Catch exception if any
			  System.err.println("Error: " + e.getMessage());
		}
	}*/
	
	public static void generateSubGraph(String nodefile, String edgefile, String errorfile){
		System.out.println("Start generate node to subgraph ");
		int node = 1;
		int subGraphID = 0;
		int biggest = 0;
		int subSize = 0;
		int totalsize = 0;
		while(node <= nodeNum){
			/*while(nodeMap.get(node).visited){
				if(node>nodeNum){
					break;
				}
				node++;
				if(node %100000==0)
						System.out.println("generate node to subgraph num: "+subGraphID+" "+node+" size: "+subGraphList.get(1).size());
			}*/
			for(; node<=nodeNum;node++){
				//System.out.println(node+" "+nodeMap.size());
				if(!nodeMap.get(node).visited){
					break;
				}
			}
			if(node<=nodeNum){
				subGraphID++;
				TreeSet<Integer> list = new TreeSet<Integer>();
				subGraphList.put(subGraphID, list);
				dfs(node, subGraphID);
				int newsize = subGraphList.get(subGraphID).size();
				if(newsize > subSize){
					biggest = subGraphID;
					subSize = newsize;
				}
				totalsize += newsize;
			}
		}
		
  		System.out.println(Debugger.getCallerPosition()+"Write file: "+nodefile +"...");
		HashMap<Integer, Integer> tranMap = new HashMap<Integer, Integer>();//from old id to new id
  		try{
  			FileWriter fstream = new FileWriter(nodefile);
  			BufferedWriter out = new BufferedWriter(fstream);
  			TreeSet<Integer> subGraph = subGraphList.get(biggest);
  			nodeNum = subGraph.size();
  			
  			System.out.println(biggest+" size: "+nodeNum);
  			int nodeID = 0;
  			while(!subGraph.isEmpty()){//nodeID<nodeNum){
  				nodeID++;
  				int oldID = subGraph.pollFirst();
  				if(nodeID != oldID){
  					tranMap.put(oldID, nodeID);
  				}
  				else{
  					tranMap.put(nodeID, nodeID);
  				}
  				out.write(nodeID+" 1.0");
  				Iterator<Integer> iter = nodeMap.get(oldID).keyList.iterator();
  				while(iter.hasNext()){
  					out.write(" "+iter.next());
  				}
  				out.write("\r\n");
  				/*if(nodeID %1000==0){
  					System.out.println("Generate new node file oldeid "+oldID+" newid "+nodeID+" total size in subgraph "+totalsize);
  				}*/
  			}
  			System.out.println("Finish output to node file, subgraph: "+subGraphID+" biggest subgraph "+biggest+" total nodes "+nodeNum+" edgeMap size "+edgeMap.size());
  			//Close the output node stream
  			out.close();
  			
  			FileWriter fstreamedge = new FileWriter(edgefile);
  			BufferedWriter outedge = new BufferedWriter(fstreamedge);
  			
  			int edgeID = 0;
  			//HashSet<Integer> visitedNode = new HashSet<Integer>();
  			
  			HashMap<Integer, HashSet<Integer>> edgeMapStore = new HashMap<Integer, HashSet<Integer>>();
  			for(Map.Entry<Integer, Edge> e: edgeMap.entrySet()){
  				Edge tempEdge = e.getValue();
  				int srcid = tempEdge.srcid;
  				int tgtid = tempEdge.tgtid;
  				
  				if(tranMap.containsKey(srcid) && tranMap.containsKey(tgtid)){
  					srcid = tranMap.get(srcid);
  					tgtid = tranMap.get(tgtid);
  					if(srcid==tgtid)
  						continue;
  					if(srcid>tgtid){
  						srcid = srcid+tgtid;
  						tgtid = srcid - tgtid;
  						srcid = srcid - tgtid;
  					}
  					HashSet<Integer> adjSet = nodeAdj.get(srcid);
					if(adjSet==null){
						adjSet = new HashSet<Integer>();
						adjSet.add(tgtid);
						nodeAdj.put(srcid, adjSet);
					}
					else if(!adjSet.contains(tgtid)){
						adjSet.add(tgtid);
					}
					else{
						continue;
					}
  					edgeID ++;
  					int range = 10;
  					int ranNum = generateRandomInteger(1,range);
  					DecimalFormat df = new DecimalFormat("#.#");
  					double wei = (ranNum *0.1);

  					outedge.write(edgeID+" "+srcid+" "+tgtid+" "+df.format(wei));
  					outedge.write("\n");
  					if(edgeID %10000==0){
  	  					System.out.println("Generate new edge file edgeID "+edgeID);
  	  				}
  				}
  			}
  			outedge.close();

  			FileWriter fstreamNum = new FileWriter(outputNodeNum);
  			BufferedWriter outNum = new BufferedWriter(fstreamNum);
  			outNum.write(nodeNum+"");
  			outNum.newLine();
  			outNum.write(edgeID+"");
  			outNum.close();

  			System.out.println(Debugger.getCallerPosition()+"Write file removed nodes : "+errorfile +"...");
  			FileWriter fstreamerror = new FileWriter(errorfile);
  			BufferedWriter outerror = new BufferedWriter(fstreamerror);
  			
  			int remnode = 0;
  			//subGraphList.get(subGraphID)
  			for(Map.Entry<Integer, TreeSet<Integer>> entry: subGraphList.entrySet()){
  				int subid = entry.getKey();
  				if(subid != biggest){
  					TreeSet<Integer> subSet = entry.getValue();
  					remnode+=subSet.size();
  				}
  				
  			}
  			outerror.write("removed nodes "+remnode+" sub nodes "+nodeNum);
  			outerror.close();
  			System.out.println(Debugger.getCallerPosition()+"end of Writing file removed nodes : "+nodefile +"...");
	  		
	  	}catch (Exception e){//Catch exception if any
			  System.err.println("Error: " + e.getMessage());
		}	
  		System.out.println(Debugger.getCallerPosition()+"Finish writing file: "+edgefile +"...");
  	}

	 public static void main(String argv[]) {
		 if(!checkHaveSingleNode(inputNode, inputEdge)){
			 //Read(inputMetis);
			 generateSubGraph(outputNode,outputEdge, outputError);
			 //WriteBiggestSubGraph(outputNode,outputEdge);
			 System.out.println("DONE");
		 }
	 }
}
