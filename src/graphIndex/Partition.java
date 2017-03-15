package graphIndex;



import graphIndex.EdgeClass;
import graphIndex.VertexClass;

import java.io.BufferedReader;
import java.io.BufferedWriter;
import java.io.DataInputStream;
import java.io.FileInputStream;
import java.io.FileWriter;
import java.io.IOException;
import java.io.InputStreamReader;
import java.text.SimpleDateFormat;
import java.util.ArrayList;
import java.util.Calendar;
import java.util.HashMap;
import java.util.HashSet;
import java.util.Iterator;
import java.util.Map;
import java.util.PriorityQueue;
import java.util.Stack;
import java.util.List;
import java.util.Map.Entry;
import java.util.TreeMap;
import java.util.TreeSet;

import shared.Debugger;
import shared.IndexClass;
import shared.IndexElement;



public class Partition {
	
	private final int WHITE = 0;//not visit yet
    private final int GRAY = 1;//in the open list
    private final int BLACK = 2;//in the close list

	private String DATE_FORMAT_NOW = "yyyy-MM-dd HH:mm:ss";
	private Calendar cal = Calendar.getInstance();
	private SimpleDateFormat sdf = new SimpleDateFormat(DATE_FORMAT_NOW);

	private HashMap<Integer, CompareElement> storeMap = new HashMap<Integer, CompareElement>();
	//block nodes number part
	private static HashMap<Integer, Integer> blockNumMap = null;//block num map, block id to number of nodes in this block

	//private HashMap<Integer, List<Object> > block2ElementMap = new HashMap<Integer, List<Object>>();//block id to a list of compareElement in this block 
	private HashMap<Integer, HashSet<CompareElement> > block2ElementMap = new HashMap<Integer, HashSet<CompareElement>>();
	public void addBlock2Num(int bid){
        if(blockNumMap == null){
    		blockNumMap = new HashMap<Integer, Integer>();
    	}
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
    //end of block nodes number part
    
    
    public void readVertexKeywordUpPortalBlock(String portlBlockFile, String readPortlBlockFile, KSearchGraph graphClass) throws IOException{
		FileWriter fstream;
		fstream = new FileWriter(readPortlBlockFile);
		BufferedWriter out = new BufferedWriter(fstream);
		
		FileInputStream readfstream = new FileInputStream(portlBlockFile);
		// Get the object of DataInputStream
		DataInputStream in = new DataInputStream(readfstream);
		BufferedReader br = new BufferedReader(new InputStreamReader(in));
		String strLine;
	  	String[] temp;
	    String delimiter = " ";
	    while ((strLine = br.readLine()) != null)   {
	    	if(strLine.contains("vertex")){
	    		temp = strLine.split(delimiter);
	    		int vid = Integer.parseInt(temp[2]);
	    		strLine += " "+graphClass.getVertex(vid).getKeyWord().toString().replace("[", "").replace("]", "").replace(" ", "").replace(",", " ");
	    	}
	    	else if(strLine.contains("portal")){
	    		temp = strLine.split(delimiter);
	    		int vid = Integer.parseInt(temp[3]);
	    		strLine += " "+graphClass.getVertex(vid).getKeyWord().toString().replace("[", "").replace("]", "").replace(" ", "").replace(",", " ");
	    	}
	    	out.write(strLine+"\n");
	    }
	    out.close();
	    br.close();
	}
    
    
public void updatePartInfoForIterMR(KSearchGraph graphC, IndexClass indexC, int bNum, String outputfile, String portalFile) throws IOException{
		
	TreeMap<Integer, HashSet<Integer>> portalBlockMap = GetPortalMap(graphC, indexC, portalFile);
		System.out.println("write to:"+outputfile);
		FileWriter fstream;
		fstream = new FileWriter(outputfile);
		BufferedWriter out = new BufferedWriter(fstream);
		while(!portalBlockMap.isEmpty()){
			Entry<Integer, HashSet<Integer>> entry = portalBlockMap.pollFirstEntry();
			int bid = entry.getKey();
			HashSet<Integer> portalNodeSet = entry.getValue();//portal node set
			Iterator<Integer> iterPortal = portalNodeSet.iterator();
			while(iterPortal.hasNext()){
				int portalId = iterPortal.next();
				List<EdgeClass> edgeList = graphC.getVertex(portalId).getAdjEdgeList();
				Iterator<EdgeClass> iterEdge = edgeList.iterator();
				while(iterEdge.hasNext()){
					EdgeClass edge = iterEdge.next();
					int from = edge.getVFrom();
					int to = edge.getVTo();
					//System.out.println("from:"+from+" to:"+to+" portalId:"+portalId+" bid:"+bid);
					if(from == portalId){
						VertexClass toVer = graphC.getVertex(to);
						int toBid = toVer.getBlockList().iterator().next();
						//System.out.println("bid:"+bid+"ver:"+to+" block:"+toBid+" portalBlock:"+toVer.getPortalBlock());
						if(toBid != bid){
							indexC.getNodeSetOfBlock(toBid).add(portalId);
						}
					}
					else if(to == portalId){
						VertexClass fromVer = graphC.getVertex(from);
						int fromBid = fromVer.getBlockList().iterator().next();
						//System.out.println("bid:"+bid+"ver:"+from+" block:"+fromBid+" portalBlock:"+fromVer.getPortalBlock());
						if(fromBid != bid){
							indexC.getNodeSetOfBlock(fromBid).add(portalId);
						}
					}
					
				}
			}
		}
		
		HashMap<Integer, HashSet<Integer>> bidToVMap = indexC.getBolckToNodeMap();
		
		Iterator<Entry<Integer, HashSet<Integer>>> iterBToV = bidToVMap.entrySet().iterator();
		while(iterBToV.hasNext()){
			Entry<Integer, HashSet<Integer>> entry = iterBToV.next();
			int bid = entry.getKey();
			HashSet<Integer> nodeSet = entry.getValue();
			//System.out.println("bid:"+bid+" nodes:"+nodeSet.toString());
			HashSet<EdgeClass> edgeSet = new HashSet<EdgeClass>();
			
			//generate edge set
			Iterator<Integer> iterNode = nodeSet.iterator();
			while(iterNode.hasNext()){
				int vid = iterNode.next();
				VertexClass vertex = graphC.getVertex(vid);
				int portalId = vertex.getPortalBlock();
				if(portalId == -1){
					HashSet<Integer> keySet = vertex.getKeyWord();
					if(keySet==null){
						out.write(bid+" vertex "+vid+" "+vertex.getWeight());
						out.write("\n");
					}
					else{
						out.write(bid+" vertex "+vid+" "+vertex.getWeight()+" "+keySet.toString().replace("[", "").replace("]", "").replace(" ", "").replace(",", " "));
						out.write("\n");
					}
				}
				else{
					HashSet<Integer> keySet = vertex.getKeyWord();
					if(keySet==null){
						out.write(bid+" portal "+ portalId +" "+vid+" "+vertex.getWeight());
						out.write("\n");
					}
					else{
						out.write(bid+" portal "+ portalId +" "+vid+" "+vertex.getWeight()+" "+keySet.toString().replace("[", "").replace("]", "").replace(" ", "").replace(",", " "));
						out.write("\n");
					}
				}
				List<EdgeClass> adjEdge = vertex.getAdjEdgeList();
				//System.out.println("vid:"+vid);
				Iterator<EdgeClass> iterEdge = adjEdge.iterator();
				while(iterEdge.hasNext()){
					EdgeClass tempEdge = iterEdge.next();
					int vfrom = tempEdge.getVFrom();
					int vto = tempEdge.getVTo();
					if(vfrom<vto){
						if(vfrom == vid){
							if(nodeSet.contains(vto)){
								edgeSet.add(tempEdge);
							}
						}
						else if(vto == vid){
							if(nodeSet.contains(vfrom)){
								edgeSet.add(tempEdge);
							}
						}
					}
				}
			}//end of this while, all nodes are written to file and all realted edges are stored in edgeSet
			
			Iterator<EdgeClass> iterEdge = edgeSet.iterator();
			while(iterEdge.hasNext()){
				EdgeClass edge = iterEdge.next();
				int vfrom = edge.getVFrom();
				int vto = edge.getVTo();
				double edgewei = edge.getWeight();
				out.write(bid+" edge "+vfrom+" "+vto+" "+edgewei);
				out.write("\n");
			}
			
			//this.writeFileForIterBFSMR(bid, nodeSet, edgeSet, portalSet,graphClass, out);
		}
		out.close();
	}
    
    
    
    public void showAnceSet(int bid,KSearchGraph sGraph, IndexClass indexC) throws IOException{
    	HashSet<Integer> vertexInBlock = indexC.getNodeSetOfBlock(bid);
		////System.out.println(bid+" "+vertexInBlock.size());
		Iterator<Integer> it = vertexInBlock.iterator();
		int vertexNum=0;
		//TreeSet<IndexElement> anceSet = null;
        while (it.hasNext()){  
			vertexNum++;
			if(vertexNum%10000==0){
				////System.out.println("Vertex num: "+vertexNum+" BlockNum "+bid);
			}
			int vid = it.next();
			//System.out.print(bid+" "+vid);
			VertexClass originalVertex = sGraph.getVertex(vid);
			TreeSet<IndexElement> anceSet = originalVertex.getOutPortalSetBasedOnBid(bid);
			if(anceSet==null)
				System.out.println(" null");
			else{
				Iterator<IndexElement> iter = anceSet.iterator();
				while(iter.hasNext()){
					IndexElement tempElement = iter.next();
					//System.out.print(" "+tempElement.getElement());
				}
				////System.out.println("");
			}
        }
    }
    
    
    /**
     * 
     * @param bid
     * @param sGraph
     * @param indexC
     * @throws IOException
     */
    public void generateAnceUnDirected(int bid,KSearchGraph sGraph, IndexClass indexC) throws IOException{
    	
    	HashSet<Integer> vertexInBlock = indexC.getNodeSetOfBlock(bid);
		Iterator<Integer> it = vertexInBlock.iterator();
		int vertexNum=0;
        while (it.hasNext()){  
			vertexNum++;
			////System.out.println(vertexNum);
			if(vertexNum%100==0){
				////System.out.println("Vertex num: "+vertexNum+" in generate Ance function");
			}
			int vid = it.next();
			IndexElement originalElement = new IndexElement();
	    	int tempID = vid;
	    	VertexClass originalVertex = sGraph.getVertex(vid);
	    	originalElement.setElement(0.0, tempID, tempID, tempID);
	    	
	    	//anceSet.add(originalElement);
	    	
	    	//BFS to find all the ancestors of vid in the block with id bid
	    	PriorityQueue<IndexElement> queue = 
	                new PriorityQueue<IndexElement>(10,new CompareIndexElement());
	    	//Queue<IndexElement> tempStack = new LinkedList<IndexElement>();
	    	//tempStack.add(originalElement);
	    	queue.add(originalElement);
	    	//HashSet<Integer> visitedSet = new HashSet<Integer>();
	    	//visitedSet.add(tempID);
    		originalVertex.addElementToOutportalMap(bid, originalElement);
    		int num = 0;
	    	//while(!tempStack.isEmpty()){
    		while(!queue.isEmpty()){
	    		//IndexElement parentElement = tempStack.poll();
    			IndexElement parentElement = queue.poll();
    			num++;
    			if(num%100==0){
    				////System.out.println("num="+num+" "+queue.size());
    			}
    			int popID = parentElement.getStartVertex();
	    		VertexClass tempVertex = sGraph.getVertex(popID);
	    		tempVertex.setElement(null);
	    		tempVertex.chanageState(BLACK);
	    		List<EdgeClass> adjList = tempVertex.getAdjEdgeList();
	    		if(adjList == null)
	    			break;
	    		Iterator<EdgeClass> itParent = adjList.iterator();
	    		
	        	while (itParent.hasNext()){//all the incoming edges of vid
	        		EdgeClass edge = (EdgeClass) itParent.next();
	        		int pID = edge.getVFrom();
	        		if(pID==popID)
	        			pID = edge.getVTo();
	        		VertexClass parentVertex = sGraph.getVertex(pID);
        			////System.out.println(pID+" ========= "+parentVertex.getState());
	        		//if this parent vertex is not totally new
	        		if(parentVertex.getBlockList().contains(bid)){//O(n) the maximum number of n is the number of partitions.
		        		if(parentVertex.getState()==WHITE){
	    					IndexElement newElement = null;//new IndexElement();
			            	double dist = parentElement.getLength()+edge.getWeight();
			            	newElement= new IndexElement(dist, pID, parentElement.getStartVertex(), tempID);
			            	parentVertex.setElement(newElement);
			            	parentVertex.chanageState(GRAY);
			            	originalVertex.addElementToOutportalMap(bid, newElement);
			            	queue.add(newElement);
		        		}
		        		else if(parentVertex.getState()==GRAY){
		        			IndexElement tempElement = parentVertex.getElement();
		        			double newdis = parentElement.getLength()+edge.getWeight();
		        			if(tempElement.getLength()>newdis){
		        				tempElement.setLength(newdis);
		        				tempElement.setStartVertex(pID);
		        				tempElement.setNextVertex(parentElement.getStartVertex());
		        				tempElement.setEndVertex(tempID);
		        			}
		        		}
	        		}//end of "the parent vertex is in this block"
	        	}//end of processing all the edges of vid
		    	
	    	}//end of while with "itParent.hasNext()"
	    	
	    	//chagen all the vertex state to "WHITE"
	    	Iterator<Integer> itCopy = vertexInBlock.iterator();
	    	while(itCopy.hasNext()){
	    		int tempVid = itCopy.next();
	    		VertexClass tempVertex = sGraph.getVertex(tempVid);
	    		tempVertex.chanageState(WHITE);
	    	}
	    	
	    }//end of while with "!tempStack.isEmpty()"
    	////System.out.println("end while loop 1");
        //showAnceSet(bid, sGraph, indexC);                               
	}
    
    
	/**
	 * Use BFS search to find the ancestors of vid in the block with id bid
	 * O(n) where n is the number of nodes in the partition bid
	 * There should not loop in the partition
	 * @param bid
	 * @param vid
	 * @param sGraph
	 * @param indexC
	 * @return
	 * @throws IOException
	 */
    public void generateAnceDirected(int bid,KSearchGraph sGraph, IndexClass indexC) throws IOException{
    	
    	HashSet<Integer> vertexInBlock = indexC.getNodeSetOfBlock(bid);
		////System.out.println(bid+" "+vertexInBlock.size());
		Iterator<Integer> it = vertexInBlock.iterator();
		int vertexNum=0;
		//TreeSet<IndexElement> anceSet = null;
    	////System.out.println("start while loop 1");
        while (it.hasNext()){  
			vertexNum++;
			////System.out.println(vertexNum);
			if(vertexNum%1000==0){
				////System.out.println("Vertex num: "+vertexNum+" in generate Ance function");
			}
			int vid = it.next();
			
			//TreeSet<IndexElement> anceSet = new TreeSet<IndexElement>(new CompareIndexElement());//key is the ancestor vertex id, value is IndexElement class
	    	IndexElement originalElement = new IndexElement();
	    	int tempID = vid;
	    	VertexClass originalVertex = sGraph.getVertex(vid);
	    	//the first element (dist=0.0, vs=tempId, v_{next}=tempID, ve=empID)
	    	//add the initial index from vid to vid, will be remove later
	    	originalElement.setElement(0.0, tempID, tempID, tempID);
	    	
	    	//anceSet.add(originalElement);
	    	
	    	//BFS to find all the ancestors of vid in the block with id bid
	    	Stack<IndexElement> tempStack = new Stack<IndexElement>();
	    	tempStack.push(originalElement);
	    	HashSet<Integer> visitedSet = new HashSet<Integer>();
	    	visitedSet.add(tempID);
    		originalVertex.addElementToOutportalMap(bid, originalElement);
	    	while(!tempStack.isEmpty()){
	    		IndexElement parentElement = tempStack.pop();
	    		int popID = parentElement.getStartVertex();
	    		VertexClass tempVertex = sGraph.getVertex(popID);
	    		List<EdgeClass> parentList = tempVertex.getInComingList();
	    		if(parentList == null)
	    			break;
	    		Iterator<EdgeClass> itParent = parentList.iterator();
		    	////System.out.println("start while loop 3 vid= "+vid+" size: "+tempStack.size());
		    	//HashSet<Integer> parentSet = new HashSet<Integer>();
	        	while (itParent.hasNext()){//all the incoming edges of vid
	        		EdgeClass edge = (EdgeClass)itParent.next();
	        		int pID = edge.getVFrom();
	        		/*if(parentSet.contains(pID))
	        			continue;
	        		parentSet.add(pID);*/
	        		VertexClass parentVertex = sGraph.getVertex(pID);
	        		//the parent vertex is in this block
	        		if(parentVertex.getBlockList().contains(bid)){//O(n) the maximum number of n is the number of partitions.
	        			TreeSet<IndexElement> anceSet = parentVertex.getOutPortalSetBasedOnBid(bid);
    					IndexElement newElement = null;//new IndexElement();
	        			if(anceSet != null){
	        				////System.out.println(" ininini child "+vid+" parent "+pID);
	        				//Object[] array = anceSet.toArray();
	        				//for(int i=0;i<array.length;i++){
	        				//	IndexElement fromelement = (IndexElement)array[i];
	        				//	if(fromelement.getLength()==0)
	        				//		newElement = new IndexElement(fromelement.getLength()+edge.getWeight(), fromelement.getStartVertex(), tempID, tempID);
	        				//	else
	        				//		newElement= new IndexElement(fromelement.getLength()+edge.getWeight(), fromelement.getStartVertex(), fromelement.getNextVertex(), tempID);
	        					
	        				//	originalVertex.addElementToOutportalMap(bid, newElement);
	        				//}
	        				Iterator<IndexElement> iterAnce = anceSet.iterator();
	        				while(iterAnce.hasNext()){
	        					IndexElement fromelement = iterAnce.next();
	        					//fromelement.showElement();
	        					if(fromelement.getLength()==0)
	        						newElement = new IndexElement(fromelement.getLength()+edge.getWeight(), fromelement.getStartVertex(), tempID, tempID);
	        					else
	        						newElement= new IndexElement(fromelement.getLength()+edge.getWeight(), fromelement.getStartVertex(), fromelement.getNextVertex(), tempID);
	        					//newElement.showElement();
	        					originalVertex.addElementToOutportalMap(bid, newElement);
	        				}
		        			visitedSet.add(pID);
	        			}
	        			else{
	        				////System.out.println(" outout child "+vid+" parent "+pID);
		            		double dist = parentElement.getLength()+edge.getWeight();
		            		newElement= new IndexElement(dist, pID, parentElement.getStartVertex(), tempID);
		            		originalVertex.addElementToOutportalMap(bid, newElement);
		            		if(!visitedSet.contains(pID)){
		            			tempStack.add(newElement);
		        				visitedSet.add(pID);
		        			}
	        			}
	        		}//end of "the paretn vertex is in this block"
	        	}//end of processing all the incoming edges of vid
		    	////System.out.println("end while loop 3 vid= "+vid);
	    	}//remove the index point to itself
	    	////System.out.println("end while loop 2 vid= "+vid);
	    }
    	////System.out.println("end while loop 1");
        //showAnceSet(bid, sGraph, indexC);
    	//end of generate all Ancestor nodes
    	//return anceSet;                                      
	}
    
    
    /**
	 * Use BFS search to find the ancestors of vid in the block with id bid
	 * O(n) where n is the number of nodes in the partition bid
	 * 
	 * @param bid
	 * @param vid
	 * @param sGraph
	 * @param indexC
	 * @return
	 * @throws IOException
	 */
	public TreeSet<IndexElement> generateLPN(int bid, int vid, KSearchGraph sGraph, IndexClass indexC) throws IOException{
		//VertexClass findVertex = sGraph.getVertex(vid);
		//generate all the Ancestor nodes
    	TreeSet<IndexElement> anceSet = new TreeSet<IndexElement>(new CompareIndexElement());//key is the ancestor vertex id, value is IndexElement class
    	IndexElement originalElement = new IndexElement();
    	//int tempID = findVertex.getVertexID();
    	int tempID = vid;
    	
    	//the first element (dist=0.0, vs=tempId, v_{next}=tempID, ve=empID)
    	//add the initial index from vid to vid, will be remove later
    	originalElement.setElement(0.0, tempID, tempID, tempID);
    	
    	anceSet.add(originalElement);
    	
    	//BFS to find all the ancestors of vid in the block with id bid
    	Stack<IndexElement> tempStack = new Stack<IndexElement>();
    	tempStack.push(originalElement);
    	HashSet<Integer> visitedSet = new HashSet<Integer>();
    	visitedSet.add(tempID);
    	while(!tempStack.isEmpty()){
    		IndexElement parentElement = tempStack.pop();
    		VertexClass tempVertex = sGraph.getVertex(parentElement.getStartVertex());
    		
    		List<EdgeClass> parentList = tempVertex.getInComingList();
    		if(parentList == null)
    			break;
    		Iterator<EdgeClass> itParent = parentList.iterator();
        	while (itParent.hasNext()){//all the incoming edges of vid
        		EdgeClass edge = (EdgeClass)itParent.next();
        		int pID = edge.getVFrom();
        		
        		VertexClass parentVertex = sGraph.getVertex(pID);
        		
        		//the parent vertex is in this block
        		if(parentVertex.getBlockList().contains(bid)){//O(n) the maximum number of n is the number of partitions.
        			IndexElement newElement = new IndexElement();
        			
            		double dist = parentElement.getLength()+edge.getWeight();
            		newElement.setElement(dist, pID, parentElement.getStartVertex(), tempID);
            		//IndexElement parentElement = (IndexElement)anceMap.get(pID);//O(log) complexity 
            		anceSet.add(newElement);
            		if(!visitedSet.contains(pID)){
            			tempStack.add(newElement);
            			visitedSet.add(pID);
            		}
        		}//end of "the paretn vertex is in this block"
        	}//end of processing all the incoming edges of vid
    	}//remove the index point to itself
    	//anceMap.remove(tempID);
    	//showAnceSet(bid, sGraph, indexC);
    	//end of generate all Ancestor nodes
    	return anceSet;
	}
	
	/**
	 * Write the L_KN information, from block id to keyword
	 * @param sGraph
	 * @param indexC
	 * @throws IOException
	 */
	public void generateLKN(KSearchGraph sGraph, IndexClass indexC, boolean dericted) throws IOException{
		HashMap<Integer, Integer> bidMap = getBlockNumMap();
		////System.out.println("start generate LKN "+bidMap.size());
		Iterator<Entry<Integer, Integer>> bidIter = bidMap.entrySet().iterator();
		int blockNum=0;
		int vertexNum=0;
		while(bidIter.hasNext()){
			@SuppressWarnings("rawtypes")
			Map.Entry entryBMap = (Map.Entry) bidIter.next(); 
			int blockID = (Integer)entryBMap.getKey(); 
			/*if(dericted)
				generateAnceDirected(blockID, sGraph, indexC);
			else
				generateAnceUnDirected(blockID, sGraph, indexC);*/
			HashSet<Integer> vertexInBlock = indexC.getNodeSetOfBlock(blockID);
			////System.out.println(blockID+" "+vertexInBlock.size());
			Iterator<Integer> it = vertexInBlock.iterator();
			blockNum++;
	        while (it.hasNext()){  
				vertexNum++;
				if(vertexNum%1000==0){
					////System.out.println("Vertex num: "+vertexNum+" BlockNum "+blockNum);
				}
	        	int vid = it.next();
	        	VertexClass findVertex = sGraph.getVertex(vid);
	        	findVertex.addToOutportalMap(blockID, generateLPN(blockID, vid, sGraph, indexC));
	        	TreeSet<IndexElement> anceSet = findVertex.getOutPortalSetBasedOnBid(blockID);
	        	HashSet<Integer> keyList = findVertex.getKeyWord();
	        	if(keyList==null){
	        		continue;
	        	}
	        	Iterator<Integer> itKey = keyList.iterator();
	        	
	        	while (itKey.hasNext()){
	        		int kid = itKey.next();
	        		/*String keyStr = blockID+","+itKey.next();
		        	Iterator<IndexElement> iter = anceSet.iterator();
		            while (iter.hasNext()) { 
		            	Object value = iter.next();
		            	indexC.putListBToK(keyStr, (IndexElement)value);
		            } */
	        		Iterator<IndexElement> iter = anceSet.iterator();
		            while (iter.hasNext()) { 
		            	Object value = iter.next();
		        		if(vid == 1){
		        			////System.out.println("vid:"+vid+" kid:"+kid+" index:"+((IndexElement)value).getElement());
		        		}
		            	indexC.addIndexBToK(blockID, kid, (IndexElement)value);
		            } 
	        	}
	        }
		}
	}//end of writeIndexBToK
	
	//get portal nodes
	

	public void readPartitionFile(String partitionFile, KSearchGraph graphC, IndexClass indexC) throws IOException{
		cal = Calendar.getInstance();
  		// command line parameter
  		FileInputStream fstream = new FileInputStream(partitionFile);
  		// Get the object of DataInputStream
  		DataInputStream in = new DataInputStream(fstream);
  		BufferedReader br = new BufferedReader(new InputStreamReader(in));
  		String strLine;
  		int numOfLine = 1;
  		while ((strLine = br.readLine()) != null)   {
  			if(strLine.startsWith("#"))
  				continue;
  			int bid = Integer.parseInt(strLine);
  			graphC.getVertex(numOfLine).addToBlockList(bid);
  			indexC.addBlockWithNodeID(bid, numOfLine);
  			graphC.addBlockList(bid);
  			//HashMap<Integer, List<Integer>> tempB2VMap = indexC.getBlockOfNode();
  			addBlock2Num(bid);
  			HashSet<Integer> vertexList = indexC.getNodeSetOfBlock(bid);
  			if(vertexList != null){
  				vertexList.add(numOfLine);
  			}
  			else{
  				indexC.addBlockWithNodeID(bid, numOfLine);
  			}
  			numOfLine++;
  			if(numOfLine %10000 == 0){
  				cal = Calendar.getInstance();
  			    //System.out.println(Debugger.getCallerPosition()+"Read :"+partitionFile+" "+numOfLine+" "+sdf.format(cal.getTime()));
  			}
  		}
  		in.close();
  		//System.out.println("End of reading file: "+partitionFile +"...");
	}
	
	
	/**
	 * select one portal node, then update the block list 
	 * @param graph
	 * @param indexC
	 */
	public void updateBlockID(CompareElement element, KSearchGraph graphC, IndexClass indexC){
		//List<Object> updateList = element.getUpdateList();
		List<Integer> updateList = element.getUpdateList();
		int vid = element.getVertexId();
		VertexClass portalVertex = graphC.getVertex(vid);
		if(updateList!=null){
			int num = 0;
			Iterator<Integer> it = updateList.iterator();  
	        while (it.hasNext()){
	        	num++;
	        	/*if(num%100000==0)
	        		System.out.println("updateList "+num);*/
	        	CompareElement valueElement = (CompareElement)storeMap.get(it.next());

	        	int value = valueElement.getVertexId();
	        	HashSet<Integer> blockList = graphC.getVertex(value).getBlockList();
	        	Iterator<Integer> blockIter = blockList.iterator();
	        	while(blockIter.hasNext()){
	        		int bid = blockIter.next();
	        		//indexC.addBlockWithNodeID(bid, vid);
	        		portalVertex.addToBlockList(bid);
	        	}
	    		
	        }
		}
		////System.out.println("end of updating blcok id start...."+updateList.size());
	}

	/**
	 * From all the edge information, generate a map which contains the vid to compareelement(used to do sorting and get the portal node) 
	 * @param vidfrom
	 * @param vidto
	 * @param graphC
	 */
	public boolean generateCompareElementMap(int vidfrom, int vidto, KSearchGraph graphC){
		
		/*cal = Calendar.getInstance();
		////System.out.println(Debugger.getCallerPosition()+"end of putting edges into map"+" "+sdf.format(cal.getTime()));*/
		int bidfrom = graphC.getVertex(vidfrom).getBlockList().iterator().next();
    	CompareElement fromElement = (CompareElement)storeMap.get(vidfrom);
    	if(fromElement == null){
    		//fromElement = new CompareElement(vidfrom,getBlockNum(bidfrom));
    		fromElement = new CompareElement(vidfrom,bidfrom,getBlockNum(bidfrom));
    		storeMap.put(vidfrom, fromElement);
    	
    	}
    	int bidto = graphC.getVertex(vidto).getBlockList().iterator().next();
    	CompareElement toElement = (CompareElement)storeMap.get(vidto);
    	if(toElement == null){
    		//toElement = new CompareElement(vidto, getBlockNum(bidto));
    		toElement = new CompareElement(vidto, bidto, getBlockNum(bidto));
    		storeMap.put(vidto, toElement);
    	}
	    if(bidfrom != bidto){
	    	fromElement.addCountBy(1);
	    	toElement.addCountBy(1);
	    	
	    	fromElement.addToUpdateList(vidto);
	    	toElement.addToUpdateList(vidfrom);
	    	return true;
	    	//toElement.addToCandInComingSet(bidfrom);
	    }
	    else{
	    	HashSet<CompareElement> elementList = block2ElementMap.get(bidfrom);
	    	if(elementList == null){
	    		elementList = new HashSet<CompareElement>();
	    		elementList.add(fromElement);
	    		elementList.add(toElement);
	    		block2ElementMap.put(bidfrom, elementList);
	    	}
	    	else{
	    		if(!elementList.contains(fromElement)){
	    			elementList.add(fromElement);
	    		}
	    		if(!elementList.contains(toElement)){
	    			elementList.add(toElement);
	    		}
	    	}

	    }
	    return false;
	}

	

	/**
	 * generatePortalNode
	 * @param graphC graph class
	 * @param indexC index class
	 * generate the portal node from graphC
	 */
	public void generatePortalNode(KSearchGraph graphC, IndexClass indexC){
		//HashMap<Integer, List<Object>> bid2CompareElement = new HashMap<Integer, List<Object>>();
  		int edgeNum = 0;
  		HashMap<Integer, EdgeClass> tempEdgeMap = graphC.getEidToEdge();
  		Iterator<Entry<Integer, EdgeClass>> iter = tempEdgeMap.entrySet().iterator();
  		//put all edges into map first
    	while (iter.hasNext()){
    		@SuppressWarnings("rawtypes")
			Map.Entry entry = (Map.Entry) iter.next();
    	    EdgeClass tempEdge = (EdgeClass)entry.getValue();
    	    edgeNum++;
    	    if(edgeNum % 100000==0){
    	    	cal = Calendar.getInstance();
  			    ////System.out.println(Debugger.getCallerPosition()+"Store edges to Map:"+edgeNum+" "+sdf.format(cal.getTime()));
    	    }
	    	int vidfrom = tempEdge.getVFrom();
	    	int vidto = tempEdge.getVTo();
	    	if(vidfrom<vidto){
	    		generateCompareElementMap(vidfrom,vidto,graphC);
	    	}
	    	
    	}
    	cal = Calendar.getInstance();
		////System.out.println(Debugger.getCallerPosition()+"end of putting edges into map"+" "+sdf.format(cal.getTime()));
		
    	//read map information to TreeSet
    	Iterator<Entry<Integer, CompareElement>> iterMap = storeMap.entrySet().iterator();
    	edgeNum = 0;
		TreeSet<CompareElement> portalSet = new TreeSet<CompareElement>(new CompareEdgeCount());
    	while(iterMap.hasNext()){
    		@SuppressWarnings("rawtypes")
			Map.Entry entry = (Map.Entry) iterMap.next(); 
    	    CompareElement tempElement = (CompareElement)entry.getValue();
    	    if(tempElement.getCount()>0){
	    	    portalSet.add(tempElement);
	    	    //storeMap.remove(tempElement);
	    	    edgeNum++;
	    	    if(edgeNum % 100000==0){
	    	    	cal = Calendar.getInstance();
	    			////System.out.println(Debugger.getCallerPosition()+"Store map to set:"+edgeNum+" "+sdf.format(cal.getTime()));
	    	    }
    	    }
    	}
    	//storeMap.clear();
    	cal = Calendar.getInstance();
		////System.out.println(Debugger.getCallerPosition()+"END OF Storing map to set:"+edgeNum+" "+sdf.format(cal.getTime()));
    	//end of reading map information to TreeSet

    	//select largest element from TreeSet and update
    	List<Integer> portalList = new ArrayList<Integer>();
		int loop=0;
		
		while(!portalSet.isEmpty()){
			CompareElement maxElement = (CompareElement)portalSet.pollLast();
			//int bid = indexC.getNodeWithBlockList((Integer)maxElement.getVertexId()).get(0);
			if(maxElement.getCount()<=0){
				break;
			}
			portalList.add((Integer)maxElement.getVertexId());
			//updateBlockID(maxElement,graphC, indexC);
			////System.out.println(maxElement.blockID);
			HashSet<CompareElement> blockList = block2ElementMap.get(maxElement.blockID);
			Iterator<CompareElement> it = blockList.iterator();  
	        while (it.hasNext()){ 
	        	CompareElement valueClass = it.next();
	        	////System.out.println("sameBlock:"+ valueClass.getVertexId());
	        	if(portalSet.remove(valueClass)){
		        	valueClass.setNumInBlock(valueClass.getNumInBlock()-1);
		        	portalSet.add(valueClass);
	        	}
	        }
			portalSet = maxElement.updateListBySub(1,portalSet,graphC, indexC, storeMap);
			loop++;
			if(loop % 1000 ==0){
				//showPortalSet(portalSet);
				cal = Calendar.getInstance();
				////System.out.println(Debugger.getCallerPosition()+portalSet.size()+" count:"+maxElement.getCount()+" at loop"+loop+sdf.format(cal.getTime()));
			}
			
    	}
		indexC.setPortalList(portalList);
		//System.out.println("End of generatePortalNode function ");
    	//end of select largest element from TreeSet and update
	}
	
	
	
	/**
	 * generatePortalNode
	 * @param graphC graph class
	 * @param indexC index class
	 * generate the portal node from graphC
	 * @throws IOException 
	 */
	public void generatePortalNode(KSearchGraph graphC, IndexClass indexC, String portalFile) throws IOException{
		//HashMap<Integer, List<Object>> bid2CompareElement = new HashMap<Integer, List<Object>>();
  		int edgeNum = 0;
  		HashMap<Integer, EdgeClass> tempEdgeMap = graphC.getEidToEdge();
  		Iterator<Entry<Integer, EdgeClass>> iter = tempEdgeMap.entrySet().iterator();
  		//put all edges into map first
  		int totalPortalEdge = 0;
  		HashSet<Integer> candPortalSet = new HashSet<Integer>();
  		List<Integer> candList = new ArrayList<Integer>();
    	while (iter.hasNext()){
    		@SuppressWarnings("rawtypes")
			Map.Entry entry = (Map.Entry) iter.next();
    	    EdgeClass tempEdge = (EdgeClass)entry.getValue();
    	    edgeNum++;
    	    if(edgeNum % 100000==0){
    	    	cal = Calendar.getInstance();
  			    ////System.out.println(Debugger.getCallerPosition()+"Store edges to Map:"+edgeNum+" "+sdf.format(cal.getTime()));
    	    }
	    	int vidfrom = tempEdge.getVFrom();
	    	int vidto = tempEdge.getVTo();
	    	//if(vidfrom<vidto){
		    	if(generateCompareElementMap(vidfrom,vidto,graphC)){
		    		//System.out.println("pedge:"+vidfrom+" "+vidto);
	    			totalPortalEdge++;
	    			candPortalSet.add(vidfrom);
	    			candPortalSet.add(vidto);
	    			candList.add(vidfrom);
	    			candList.add(vidto);
	    		}
	    	//}
    	}
    	int portalsize = candPortalSet.size();
    	
    	FileWriter fstreamNum = new FileWriter("List.txt");
		BufferedWriter outNum = new BufferedWriter(fstreamNum);
		Iterator<Integer> iterList = candList.iterator();
		while(iterList.hasNext()){
			outNum.write(iterList.next()+"\n");
		}
		outNum.close();
		
		fstreamNum = new FileWriter("Set.txt");
		outNum = new BufferedWriter(fstreamNum);
		iterList = candPortalSet.iterator();
		while(iterList.hasNext()){
			outNum.write(iterList.next()+"\n");
		}
		outNum.close();
    	
    	candPortalSet = null;
    	cal = Calendar.getInstance();
		////System.out.println(Debugger.getCallerPosition()+"end of putting edges into map"+" "+sdf.format(cal.getTime()));
		
    	//read map information to TreeSet
    	Iterator<Entry<Integer, CompareElement>> iterMap = storeMap.entrySet().iterator();
    	edgeNum = 0;
		TreeSet<CompareElement> portalSet = new TreeSet<CompareElement>(new CompareEdgeCount());
    	while(iterMap.hasNext()){
    		@SuppressWarnings("rawtypes")
			Map.Entry entry = (Map.Entry) iterMap.next(); 
    	    CompareElement tempElement = (CompareElement)entry.getValue();
    	    if(tempElement.getCount()>0){
	    	    portalSet.add(tempElement);
	    	    //storeMap.remove(tempElement);
	    	    edgeNum++;
	    	    if(edgeNum % 100000==0){
	    	    	cal = Calendar.getInstance();
	    			System.out.println(Debugger.getCallerPosition()+"Store map to set:"+edgeNum+" "+sdf.format(cal.getTime()));
	    	    }
    	    }
    	}
    	//storeMap.clear();
    	cal = Calendar.getInstance();
		////System.out.println(Debugger.getCallerPosition()+"END OF Storing map to set:"+edgeNum+" "+sdf.format(cal.getTime()));
    	//end of reading map information to TreeSet

    	//select largest element from TreeSet and update
    	List<Integer> portalList = new ArrayList<Integer>();
		int loop=0;
		//showPortalSet(portalSet);
		while(!portalSet.isEmpty()){
			CompareElement maxElement = (CompareElement)portalSet.pollLast();
			//int bid = indexC.getNodeWithBlockList((Integer)maxElement.getVertexId()).get(0);
			if(maxElement.getCount()<=0){
				break;
			}
			portalList.add((Integer)maxElement.getVertexId());
			updateBlockID(maxElement,graphC, indexC);
			//System.out.println(maxElement.blockID);
			
			//update the block size
			HashSet<CompareElement> blockList = block2ElementMap.get(maxElement.blockID);
			Iterator<CompareElement> it = blockList.iterator();  
	        while (it.hasNext()){ 
	        	CompareElement valueClass = it.next();
	        	//System.out.println("sameBlock:"+ valueClass.getVertexId());
	        	if(portalSet.remove(valueClass)){
		        	valueClass.setNumInBlock(valueClass.getNumInBlock()-1);
		        	portalSet.add(valueClass);
	        	}
	        }
	        //end of updating the block size
	        
			portalSet = maxElement.updateListBySub(1,portalSet,graphC, indexC, storeMap);
			loop++;
			if(loop % 1000 ==0){
				//showPortalSet(portalSet);
				cal = Calendar.getInstance();
				System.out.println(Debugger.getCallerPosition()+portalSet.size()+" count:"+maxElement.getCount()+" at loop"+loop+sdf.format(cal.getTime()));
			}
			
    	}
		//indexC.setPortalList(portalList);
		////System.out.println("End of generatePortalNode function with portal nodes:"+portalList.size());
		//output portal nodes to a file
				loop=0;
				FileWriter fstream = new FileWriter(portalFile);
				BufferedWriter out = new BufferedWriter(fstream);
				Iterator<Integer> iterPortal = portalList.iterator();
				out.write("#candidata:"+portalsize+" portaledge:"+totalPortalEdge+" portal:"+portalList.size()+"\n");
				while(iterPortal.hasNext()){
					int portalInt = iterPortal.next();
		 			out.write(portalInt+"");
		 			out.write("\n");
		 			loop++;
					if(loop % 1000 ==0){
						cal = Calendar.getInstance();
						System.out.println(Debugger.getCallerPosition()+"write to File with portalList size:"+portalList.size()+" at loop"+loop+sdf.format(cal.getTime()));
					}
				}
				out.close();
    	//end of output portal node information
	}
	
	/**
	 * This function is used to initial the portal-node-block, return portal map with key is the block id of portal node
	 * @param graphC class used to find the portal node partition information
	 * @param indexC index class used to find the portal node set
	 */
	@SuppressWarnings("resource")
	public TreeMap<Integer, HashSet<Integer>> GetPortalMap(KSearchGraph graphC, IndexClass indexC, String portalFile){
		TreeMap<Integer, HashSet<Integer>> portalBlockMap = new TreeMap<Integer, HashSet<Integer>>();
		try{
			List<Integer> portalList = new ArrayList<Integer>();
			FileInputStream fstream = new FileInputStream(portalFile);
	  		// Get the object of DataInputStream
	  		DataInputStream in = new DataInputStream(fstream);
	  		BufferedReader br = new BufferedReader(new InputStreamReader(in));
			
			String strLine;
			while ((strLine = br.readLine()) != null)   {
	  			if(strLine.startsWith("#"))
	  				continue;
	  			portalList.add(Integer.parseInt(strLine));
			}
			Iterator<Integer> iter = portalList.iterator();
			while(iter.hasNext()){
				int vid = iter.next();
				int bid = graphC.getVertex(vid).getBlockList().iterator().next();
				graphC.getVertex(vid).setPortalBlock(bid);
				HashSet<Integer> blockSet = portalBlockMap.get(bid);
				if(blockSet == null){
					blockSet = new HashSet<Integer>();
					portalBlockMap.put(bid, blockSet);
				}
				blockSet.add(vid);
			}
		}
		catch (Exception e){//Catch exception if any
			  System.err.println("Error: " + e.getMessage());
		}
		return portalBlockMap;
	}
	
	public void generateNAndEForBlock(KSearchGraph graphC, IndexClass indexC, int bid, String blockFile){
		try {
			FileWriter fstream;
			fstream = new FileWriter(blockFile);
			BufferedWriter out = new BufferedWriter(fstream);
			HashSet<Integer> nodeSet = indexC.getNodeSetOfBlock(bid);
			HashSet<EdgeClass> edgeSet = new HashSet<EdgeClass>();
			Iterator<Integer> iter = nodeSet.iterator();
			out.write(nodeSet.size()+"");
			out.write("\n");
			while(iter.hasNext()){
				int vid = iter.next();
				VertexClass vertex = graphC.getVertex(vid);
				out.write(vid+" "+vertex.getWeight()+" "+vertex.getKeyWord().toString().replace("[", "").replace("]", "").replace(" ", "").replace(",", ""));
				out.write("\n");
				List<EdgeClass> adjEdge = vertex.getAdjEdgeList();
				Iterator<EdgeClass> iterEdge = adjEdge.iterator();
				while(iterEdge.hasNext()){
					EdgeClass tempEdge = iterEdge.next();
					int vfrom = tempEdge.getVFrom();
					int vto = tempEdge.getVTo();
					if(vfrom == vid){
						VertexClass secVer = graphC.getVertex(vto);
						if(secVer.getBlockList().contains(bid)){
							edgeSet.add(tempEdge);
						}
					}
					else if(vto == vid){
						VertexClass secVer = graphC.getVertex(vfrom);
						if(secVer.getBlockList().contains(bid)){
							edgeSet.add(tempEdge);
						}
					}
				}
			}//end of this while, all nodes are written to file and all realted edges are stored in edgeSet
			
			Iterator<EdgeClass> iterEdge = edgeSet.iterator();
			while(iterEdge.hasNext()){
				EdgeClass tempEdge = iterEdge.next();
				out.write(tempEdge.showEdge());
				out.write("\n");
			}
			out.close();
		} catch (IOException e) {
			// TODO Auto-generated catch block
			e.printStackTrace();
		}
		
	}
	
	/**
	 * Update the block partition information, in order to contain the blocks with portal-node-blocks
	 * @param graphC
	 * @param indexC
	 * @param numB
	 * @throws IOException 
	 */
	public void updatePartitionInfo(KSearchGraph graphC, IndexClass indexC, int level, String outputFile, String portalFile, boolean seperate) throws IOException{
		////System.out.println(outputFile);
		TreeMap<Integer, HashSet<Integer>> portalBlockMap = GetPortalMap(graphC, indexC, portalFile);
		int iniBlockS = indexC.getBlockNum();
		int storeLevel = level;
		//bNum is the number of blocks
		int bNum = iniBlockS;
		//bigSize is the biggest block size
		//int bigSize = (int)(indexC.getBiggestBlockSise()*1.6);
		//Iterator<Entry<Integer, HashSet<Integer>>> iter = portalBlockMap.entrySet().iterator();
		
		//write original block to file
		FileWriter fstream;
		fstream = new FileWriter(outputFile);
		BufferedWriter out = new BufferedWriter(fstream);
		for(int i=0;i<bNum;i++){
			HashSet<Integer> nodeSet = indexC.getNodeSetOfBlock(i);
			HashSet<EdgeClass> edgeSet = new HashSet<EdgeClass>();
			Iterator<Integer> iterNode = nodeSet.iterator();
			while(iterNode.hasNext()){
				int vid = iterNode.next();
				VertexClass vertex = graphC.getVertex(vid);
				int pid = vertex.getPortalBlock();
				if(pid==-1){
					if(vertex.getKeyWord()==null){
						out.write(i+" vertex "+vid+" "+vertex.getWeight());
						out.write("\n");
					}
					else{
						out.write(i+" vertex "+vid+" "+vertex.getWeight()+" "+vertex.getKeyWord().toString().replace("[", "").replace("]", "").replace(" ", "").replace(",", " "));
						out.write("\n");
					}
				}
				else{
					if(vertex.getKeyWord()==null){
						out.write(i+" portal "+pid+" "+vid+" "+vertex.getWeight());
						out.write("\n");
					}
					else{
						out.write(i+" portal "+pid+" "+vid+" "+vertex.getWeight()+" "+vertex.getKeyWord().toString().replace("[", "").replace("]", "").replace(" ", "").replace(",", " "));
						out.write("\n");
					}
				}
				List<EdgeClass> adjEdge = vertex.getAdjEdgeList();
				Iterator<EdgeClass> iterEdge = adjEdge.iterator();
				while(iterEdge.hasNext()){
					EdgeClass tempEdge = iterEdge.next();
					int vfrom = tempEdge.getVFrom();
					int vto = tempEdge.getVTo();
					if(vfrom == vid){
						VertexClass secVer = graphC.getVertex(vto);
						if(secVer.getBlockList().contains(i)){
							edgeSet.add(tempEdge);
						}
					}
					else if(vto == vid){
						VertexClass secVer = graphC.getVertex(vfrom);
						if(secVer.getBlockList().contains(i)){
							edgeSet.add(tempEdge);
						}
					}
				}
			}//end of this while, all nodes are written to file and all realted edges are stored in edgeSet
			
			Iterator<EdgeClass> iterEdge = edgeSet.iterator();
			while(iterEdge.hasNext()){
				EdgeClass tempEdge = iterEdge.next();
				out.write(i+" edge "+tempEdge.showEdge());
				out.write("\n");
			}
		}
		

		int bigSize = (int)(indexC.getBiggestBlockSise()*2);
		//while(iter.hasNext()){
		while(!portalBlockMap.isEmpty()){
			//Entry<Integer, HashSet<Integer>> entry = iter.next();
			//HashSet<Integer> nodeSet = entry.getValue();
			Entry<Integer, HashSet<Integer>> entry = portalBlockMap.pollFirstEntry();
			HashSet<Integer> nodeSet = entry.getValue();
			int size = nodeSet.size();
			//initial new block with bid=bNum
			indexC.addBlockWithNodeSet(bNum, entry.getValue());
			//Based on nodeSet, add all the edges related with the nodes in that set, the value is a edge list with same distance value
			TreeMap<Double, List<EdgeClass>> edgeMap = new TreeMap<Double, List<EdgeClass>>();
			Iterator<Integer> iterNode = nodeSet.iterator();
			//this while is used to initial added all the edges directly related with all portal-nodes in this new block
			while(iterNode.hasNext()){
				int nodeid = iterNode.next();
				VertexClass vertex = graphC.getVertex(nodeid);
				List<EdgeClass> adjList = vertex.getAdjEdgeList();
				if(adjList!=null){
					Iterator<EdgeClass> iterEdge = adjList.iterator();
					while(iterEdge.hasNext()){
						EdgeClass edge = iterEdge.next();
						double dis = edge.getWeight();
						List<EdgeClass> edgeList = edgeMap.get(dis);
						if(edgeList==null){
							edgeList = new ArrayList<EdgeClass>();
							edgeMap.put(dis, edgeList);
						}
						edgeList.add(edge);
					}
				}
			}//after this while loop, all the adj edges based on original portal nodes in this new block are added to edgeMap
			double dis=0;
			
			////System.out.println("dis:"+dis+" edgeMap:"+edgeMap.size()+" nodeset:"+nodeSet.size());
			boolean run = true;
			HashSet<Integer> visitedSet = new HashSet<Integer>();
			//this while is used to add all related nodes to this block
			int iter = 0;
			while(run && edgeMap.size()!=0){
				////System.out.println("visited:"+visitedSet.toString());
				Entry<Double, List<EdgeClass>> listEntry = edgeMap.pollFirstEntry();
				dis = (double)listEntry.getKey();
				////System.out.println("bid:"+bNum+" size:"+size+" dis:"+dis+" edgeMap:"+edgeMap.size());
				level = storeLevel-(int)dis;
				if(level<0){
					//System.out.println("level break:"+bNum);
					break;
				}
				if(size>=bigSize){
					run=false;
                    //System.out.println("size break:"+size+" biggest:"+bigSize+" bid:"+bNum+" level:"+level);
                    break;
                }
				int add = 0;
				List<EdgeClass> listEdge = listEntry.getValue();
				Iterator<EdgeClass> iterEdge = listEdge.iterator();
				while(iterEdge.hasNext()){
					EdgeClass tempEdge = iterEdge.next();
					visitedSet.add(tempEdge.getEdgeID());
					////System.out.println("add visited:"+tempEdge.showEdge());
					int vfrom = tempEdge.getVFrom();
					int vto = tempEdge.getVTo();
					double wei = tempEdge.getWeight();
					if(nodeSet.add(vfrom)){
						add++;
						//indexC.addBlockWithNodeID(bNum, vfrom);
						VertexClass vertexF = graphC.getVertex(vfrom);
						vertexF.addToBlockList(bNum);
						size++;
						/*if(size>=bigSize){
							run=false;
							//System.out.println("size break:"+bigSize+" "+bNum+" level:"+level);
							break;
						}*/
						List<EdgeClass> adjList = vertexF.getAdjEdgeList();
						if(adjList!=null){
							Iterator<EdgeClass> iterFrom = adjList.iterator();
							while(iterFrom.hasNext()){
								EdgeClass edge = iterFrom.next();
								////System.out.println("check edge:"+edge.showEdge());
								if(!visitedSet.contains(edge.getEdgeID())){
									double disFrom = wei+edge.getWeight();
									List<EdgeClass> edgeList = edgeMap.get(disFrom);
									if(edgeList==null){
										edgeList = new ArrayList<EdgeClass>();
										edgeMap.put(disFrom, edgeList);
									}
									edgeList.add(edge);
								}
							}
						}
					}
					if(nodeSet.add(vto)){
						add ++;
						VertexClass vertexT = graphC.getVertex(vto);
						vertexT.addToBlockList(bNum);
						size++;
						/*if(size>=bigSize){
							run=false;
							//System.out.println("size break:"+bigSize+" "+bNum+" level:"+level);
							break;
						}*/
						List<EdgeClass> adjList = vertexT.getAdjEdgeList();
						////System.out.println("vid:"+vto+"adj edge:"+adjList.size());
						if(adjList!=null){
							Iterator<EdgeClass> iterTo = adjList.iterator();
							while(iterTo.hasNext()){
								EdgeClass edge = iterTo.next();
								////System.out.println("check edge:"+edge.showEdge());
								if(!visitedSet.contains(edge.getEdgeID())){
									double disTo = wei+edge.getWeight();
									List<EdgeClass> edgeList = edgeMap.get(disTo);
									if(edgeList==null){
										edgeList = new ArrayList<EdgeClass>();
										edgeMap.put(disTo, edgeList);
									}
									edgeList.add(edge);
								}
							}
						}
					}//end of if
				}//end of while(iterEdge.hasNext())
				
				//System.out.println("add:"+add+" edgeMap:"+edgeMap.size()+" iter:"+ ++iter);
			}//After this while, for this new block with bid=bNum, it is done!
			
			//store this block
			//indexC.addBlockWithNodeSet(bNum, nodeSet);
			//
			
			//not sotre, write to file
			HashSet<EdgeClass> edgeSet = new HashSet<EdgeClass>();
			iterNode = nodeSet.iterator();
			while(iterNode.hasNext()){
				int vid = iterNode.next();
				VertexClass vertex = graphC.getVertex(vid);
				////System.out.println("vid weight "+vertex.getWeight()+"|"+vertex.getKeyWord());
				out.write(bNum+" vertex "+vid+" "+vertex.getWeight());//+" "+vertex.getKeyWord().toString().replace("[", "").replace("]", "").replace(" ", "").replace(",", " "));
				out.write("\n");
				List<EdgeClass> adjEdge = vertex.getAdjEdgeList();
				Iterator<EdgeClass> iterEdge = adjEdge.iterator();
				while(iterEdge.hasNext()){
					EdgeClass tempEdge = iterEdge.next();
					int vfrom = tempEdge.getVFrom();
					int vto = tempEdge.getVTo();
					if(vfrom == vid){
						VertexClass secVer = graphC.getVertex(vto);
						if(secVer.getBlockList().contains(bNum)){
							edgeSet.add(tempEdge);
						}
					}
					else if(vto == vid){
						VertexClass secVer = graphC.getVertex(vfrom);
						if(secVer.getBlockList().contains(bNum)){
							edgeSet.add(tempEdge);
						}
					}
				}
			}//end of this while, all nodes are written to file and all realted edges are stored in edgeSet
			Iterator<EdgeClass> iterEdge = edgeSet.iterator();
			while(iterEdge.hasNext()){
				EdgeClass tempEdge = iterEdge.next();
				out.write(bNum+" edge "+tempEdge.showEdge());
				out.write("\n");
			}
			//
			nodeSet = null;
			edgeSet = null;
			size=0;
			bNum++;
			level = storeLevel;
		}
		out.close();
		/*if(seperate==false){
			try {
				FileWriter fstream;
				fstream = new FileWriter(outputFile);
				BufferedWriter out = new BufferedWriter(fstream);
				for(int i=0;i<bNum;i++){
					HashSet<Integer> nodeSet = indexC.getNodeSetOfBlock(i);
					HashSet<EdgeClass> edgeSet = new HashSet<EdgeClass>();
					Iterator<Integer> iterNode = nodeSet.iterator();
					while(iterNode.hasNext()){
						int vid = iterNode.next();
						VertexClass vertex = graphC.getVertex(vid);
						////System.out.println("vid weight "+vertex.getWeight()+"|"+vertex.getKeyWord());
						out.write(i+" vertex "+vid+" "+vertex.getWeight()+" "+vertex.getKeyWord().toString().replace("[", "").replace("]", "").replace(" ", "").replace(",", " "));
						out.write("\n");
						List<EdgeClass> adjEdge = vertex.getAdjEdgeList();
						Iterator<EdgeClass> iterEdge = adjEdge.iterator();
						while(iterEdge.hasNext()){
							EdgeClass tempEdge = iterEdge.next();
							int vfrom = tempEdge.getVFrom();
							int vto = tempEdge.getVTo();
							if(vfrom == vid){
								VertexClass secVer = graphC.getVertex(vto);
								if(secVer.getBlockList().contains(i)){
									edgeSet.add(tempEdge);
								}
							}
							else if(vto == vid){
								VertexClass secVer = graphC.getVertex(vfrom);
								if(secVer.getBlockList().contains(i)){
									edgeSet.add(tempEdge);
								}
							}
						}
					}//end of this while, all nodes are written to file and all realted edges are stored in edgeSet
					
					Iterator<EdgeClass> iterEdge = edgeSet.iterator();
					while(iterEdge.hasNext()){
						EdgeClass tempEdge = iterEdge.next();
						out.write(i+" edge "+tempEdge.showEdge());
						out.write("\n");
					}
				}
				out.close();
			} catch (IOException e) {
				// TODO Auto-generated catch block
				e.printStackTrace();
			}
		}
		else if(seperate){
			for(int i=0;i<bNum;i++){
				generateNAndEForBlock(graphC,indexC,i,outputFile+"_"+i);
				
			}
		}*/
	}
	
	
	//test for portal set
	public void showPortalSet(TreeSet<CompareElement> portalSet){
		Iterator<CompareElement> iter = portalSet.iterator();
		int total = 0;
		while(iter.hasNext()){
			CompareElement tempC = (CompareElement)iter.next();
			total += tempC.getCount();
			System.out.println("vid: "+tempC.getVertexId()+", count: "+tempC.getCount()+", numB: "+tempC.getNumInBlock());
		
		}
		System.out.println("total:"+total);
	}
	public void showPortalNode(KSearchGraph graph, IndexClass indexC){
		List<Integer> portalList = indexC.getPortalList();
		Iterator<Integer> iter = portalList.iterator();
		////System.out.println("portal nodes:");
		while(iter.hasNext()){
			int pvid = iter.next();
			//System.out.print(pvid);
			HashSet<Integer> blockList = graph.getVertex(pvid).getBlockList();
			Iterator<Integer> iterBList = blockList.iterator();
			while(iterBList.hasNext()){
				//System.out.print(" "+iterBList.next());
			}
			////System.out.println("");
		}
	}
	//end of test
	/**
	 * generate out-portal nodes from all the portal nodes
	 * @param sGraph
	 * @param indexC
	 * @throws IOException
	 */
	public void generateOutPortalAnce(KSearchGraph sGraph, IndexClass indexC) throws IOException{
		cal = Calendar.getInstance();
		////System.out.println(Debugger.getCallerPosition()+"Start generate out portal nodes : "+sdf.format(cal.getTime()));
		List<Integer> portalList = indexC.getPortalList();
		Iterator<Integer> iter = portalList.iterator();
		int loop = 0;
		//int bloop = 0;
		int total = portalList.size();
		int numOutPortal = 0;
		while(iter.hasNext()){
			loop++;
			if(loop %10000 ==0){
				cal = Calendar.getInstance();
				////System.out.println(Debugger.getCallerPosition()+" at loop "+loop + " out of total "+total+" "+sdf.format(cal.getTime()));
			}
			int vid = iter.next();
			VertexClass vertex = sGraph.getVertex(vid);
			
			List<EdgeClass> edgeFromV = vertex.getoutGoingList();
			List<EdgeClass> edgeToV = vertex.getInComingList();
			if(edgeFromV==null || edgeToV==null)
				continue;
			HashSet<Integer> bidList = vertex.getBlockList();//indexC.getNodeWithBlockList(vid);
			Iterator<Integer> itBid = bidList.iterator();
			while(itBid.hasNext()){
				int bid = itBid.next();
				boolean fromBool = false;
				boolean toBool = false;
				Iterator<EdgeClass> iterTo = edgeToV.iterator();
				while(iterTo.hasNext()&&!fromBool){
					EdgeClass toClass = (EdgeClass)iterTo.next();
					HashSet<Integer> blockFrom = sGraph.getVertex(toClass.getVFrom()).getBlockList();//indexC.getNodeWithBlockList(toClass.getVFrom());
					if(blockFrom.contains(bid)){
						fromBool = true;
						break;
					}
				}
				Iterator<EdgeClass> iterFrom = edgeFromV.iterator();
				while(iterFrom.hasNext()&&!toBool){
					EdgeClass fromClass = (EdgeClass)iterFrom.next();
					HashSet<Integer> blockTo = sGraph.getVertex(fromClass.getVTo()).getBlockList();//indexC.getNodeWithBlockList(fromClass.getVTo());
					Iterator<Integer> blockIter = blockTo.iterator();
					while(blockIter.hasNext()){
						if(blockIter.next()!=bid){
							toBool = true;
							break;
						}
					}
				}
				if(fromBool && toBool){
					numOutPortal++;
					TreeSet<IndexElement> anceSet = generateLPN(bid, vid, sGraph, indexC);
					////System.out.println("!!!!!!!!!!!!!!!!!!!!end of  generateLPN "+" block "+bid+" vid "+vid+" size "+anceSet.size());
					sGraph.getVertex(vid).addToOutportalMap(bid, anceSet);
				}
			}
		}
		cal = Calendar.getInstance();
		////System.out.println(Debugger.getCallerPosition()+"END OF generate out portal nodes : "+sdf.format(cal.getTime()));
		////System.out.println("num of out-portal nodes "+numOutPortal);
		//showPortalNode(sGraph,indexC);
	}
  	//end of generate indexes part

	public Partition(){;}
}
