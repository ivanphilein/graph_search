package oneIteMR;

import java.io.BufferedReader;
import java.io.BufferedWriter;
import java.io.DataInputStream;
import java.io.FileInputStream;
import java.io.FileWriter;
import java.io.IOException;
import java.io.InputStreamReader;
import java.util.ArrayList;
import java.util.Date;
import java.util.HashMap;
import java.util.HashSet;
import java.util.Iterator;
import java.util.List;
import java.util.Map.Entry;
import java.util.PriorityQueue;
import java.util.TreeMap;


public class BiDirSearch {
	
	private final int WHITE=0;
	private final int GRAY=1;
	private final int BLACK=2;
	//kidToNodeSet is map from kid to a set with all nodes which can connect with this keyword
    private PriorityQueue<VertexOne> inVertexSet = new PriorityQueue<VertexOne>(10, new CompareVertexOne());
    private PriorityQueue<VertexOne> outVertexSet = new PriorityQueue<VertexOne>(10, new CompareVertexOne());
    private HashSet<VertexOne> inCheckedSet = new HashSet<VertexOne>();
    private HashSet<VertexOne> outCheckedSet = new HashSet<VertexOne>();
    private int depthMax = 8;
    private static HashSet<VertexOne> finalRM = new HashSet<VertexOne>();
    
    public static HashSet<Integer> getQueryList(String queryStr){
		if(queryStr.isEmpty()){
			return null;
		}
		HashSet<Integer> queryList = new HashSet<Integer>();
		String[] temp;
		String delimiter = ":";
		temp=queryStr.split(delimiter);
		for(int i=0;i<temp.length;i++){
			queryList.add(Integer.parseInt(temp[i]));
			
		}
		return queryList;
	}
   
    
    //test
    public String showVertexSet(PriorityQueue<VertexOne> vertexSet) throws IOException{
    	if(vertexSet==null)
    		return "Empty";
    	Iterator<VertexOne> iter = vertexSet.iterator();
    	String outStr = "";
    	
    	while(iter.hasNext()){
    		VertexOne vertex = iter.next();
    		outStr += " vid:"+vertex.getVertexID();
    		outStr += "act:" + vertex.getActivation();
    		Iterator<Entry<Integer, Double>> iterRM = vertex.getDisMap().entrySet().iterator();
    		while(iterRM.hasNext()){
    			Entry<Integer, Double> entry = iterRM.next();
    			outStr += "("+entry.getKey()+","+entry.getValue()+")";
    		}
    	}
    	return outStr;
    }
    //end of test
    
    public void initialInVertexSet(HashMap<Integer, HashSet<VertexOne>> kidToNodeSet){
    	Iterator<Entry<Integer, HashSet<VertexOne>>> iter = kidToNodeSet.entrySet().iterator();
    	while(iter.hasNext()){
    		Entry<Integer, HashSet<VertexOne>> entry = iter.next();
    		//int kid = entry.getKey();
    		HashSet<VertexOne> vertexSet = entry.getValue();
    		//int size = vertexSet.size();
    		Iterator<VertexOne> iterVertex = vertexSet.iterator();
    		while(iterVertex.hasNext()){
    			VertexOne vertex = iterVertex.next();
    			vertex.updateActivation(0);
    			//HashMap<Integer, Double> disMap = vertex.getDisMap();
    			if(vertex.getState()==WHITE){
    				vertex.chanageState(GRAY);
    				inVertexSet.add(vertex);
    			}
    			
    		}
    	}
    	kidToNodeSet=null;
    }
    
    public boolean runBiDirSearch(HashSet<Integer> querySet, GraphOne graph, int topK) throws IOException{
    	boolean stop = false;
    	int num = querySet.size();
    	//System.out.println(num);
    	//boolean outEmpty = outVertexSet.isEmpty();
    	//int i = 0;
    	//while(!inEmpty || !outEmpty){
    	while(!inVertexSet.isEmpty() || !outVertexSet.isEmpty()){
    		VertexOne popV = inVertexSet.peek();
    		VertexOne secPopV = outVertexSet.peek();
    		if(popV==null && secPopV==null)
    			break;
    		//System.out.println("inset:"+i+":"+showVertexSet(inVertexSet));
        	//System.out.println("outset:"+i+":"+showVertexSet(outVertexSet));
        	boolean runIn = false;
    		if(secPopV==null){
    			runIn = true;
    		}
    		else if(popV != null && popV.getActivation()<=secPopV.getActivation()){
    			runIn = true;
    		}
    		if(runIn){
    			//System.out.println("in");
    			inVertexSet.poll();
    			outVertexSet.add(popV);
    			
				//System.out.println("pop vid:"+popV.getVertexID());
				if(popV.getState()!=BLACK){
					inCheckedSet.add(popV);
					if(complete(popV, num)){
						finalRM.add(popV);
						if(finalRM.size()>=topK)
							return true;
						//continue;
					}
					if(popV.getDepth()<depthMax){
						HashSet<EdgeOne> incomSet = popV.getInComingSet();
						if(incomSet!=null){
		    				Iterator<EdgeOne> iterInCom = incomSet.iterator();
		    				while(iterInCom.hasNext()){
		    					EdgeOne edge = iterInCom.next();
		    					//System.out.println("income:"+edge.showEdge());
		    					stop = exploreEdge(edge, graph, querySet, topK);
		    					if(stop)
		    						return true;
		    					VertexOne fromV = graph.getVertex(edge.getVFrom());
			    				if(!inCheckedSet.contains(fromV) && !inVertexSet.contains(fromV)){
			    					inVertexSet.add(fromV);
			    				}
		    				}
						}
						
					}
					
				}
    		}
    		else{
    			//System.out.println("out");
    			outVertexSet.poll();
    			outCheckedSet.add(secPopV);
    			if(secPopV.getState()!=BLACK){
	    			if(complete(secPopV, num)){
						finalRM.add(secPopV);
						if(finalRM.size()>=topK)
							return true;
						//continue;
					}
					//System.out.println("pop vid:"+secPopV.getVertexID());
	    			HashSet<EdgeOne> outGoSet = secPopV.getOutGoingSet();
					if(outGoSet!=null){
	    				Iterator<EdgeOne> iterInCom = outGoSet.iterator();
	    				while(iterInCom.hasNext()){
	    					EdgeOne edge = iterInCom.next();
	    					//System.out.println("outgoint:"+edge.showEdge());
	    					stop = exploreEdge(edge, graph, querySet, topK);
	    					if(stop)
	    						return true;
	    					VertexOne toV = graph.getVertex(edge.getVTo());
		    				if(!outCheckedSet.contains(toV)&& !outVertexSet.contains(toV)){
		    					outVertexSet.add(toV);
		    				}
	    				}
					}
    			}
    		}
    	}
    	return stop;
    }
    /**
     * This is the real backward-expanding search method, not real bi-directional search
     * @param querySet
     * @param graph
     * @param topK
     * @return
     * @throws IOException
     */
    /*public boolean runBiDirSearch(HashSet<Integer> querySet, GraphOne graph, int topK) throws IOException{
    	boolean stop = false;
    	int num = querySet.size();
    	boolean inEmpty = inVertexSet.isEmpty();
    	boolean outEmpty = true;//outVertexSet.isEmpty();
    	int i = 0;
    	//while(!inEmpty || !outEmpty){
    	while(!inVertexSet.isEmpty()){
    		//System.out.println("inset:"+i+":"+showVertexSet(inVertexSet));
        	//System.out.println("outset:"+i+":"+showVertexSet(outVertexSet));
    		//if(!inEmpty){
	    		VertexOne vertex = inVertexSet.poll();
				//System.out.println("pop vid:"+vertex.getVertexID());
				if(vertex.getState()!=BLACK){
					//inCheckedSet.add(vertex);
					if(complete(vertex, num)){
						//System.out.println("found:"+vertex.getVertexID()+" "+num);
						finalRM.add(vertex);
						if(finalRM.size()>=topK)
							return true;
						continue;
					}
					//if(vertex.getDepth()<depthMax){
						HashSet<EdgeOne> incomSet = vertex.getInComingSet();
						if(incomSet!=null){
							//System.out.println("read incoming edges:"+incomSet.size());
		    				Iterator<EdgeOne> iterInCom = incomSet.iterator();
		    				while(iterInCom.hasNext()){
		    					EdgeOne edge = iterInCom.next();
		    					//System.out.println("income:"+edge.showEdge());
		    					stop = exploreInComEdge(edge, graph, num, topK);
		    					if(stop)
		    						return true;
		    				}
						}
					//}
				}
    		//}
    	}
    	return stop;
    }*/
    
    /**
     * Check one vertex contains one whole solution or not
     * @param vertex
     * @return
     */
    public boolean complete(VertexOne vertex, int num){
    	//System.out.print("Check "+vertex.getVertexID());
    	if(vertex.getSizeOfRM()==num){
    		//System.out.println("found:"+vertex.getVertexID()+" "+ vertex.showLeafMap());
    		vertex.chanageState(BLACK);
    		return true;
    	}
		//System.out.println(" false");
    	return false;
    }
    
    public boolean exploreEdge(EdgeOne edge, GraphOne graph, HashSet<Integer> querySet, int topK) throws IOException{
    	//System.out.println(edge.showEdge());
    	VertexOne fromV = graph.getVertex(edge.getVFrom());
    	VertexOne toV = graph.getVertex(edge.getVTo());
    	toV.insetAnceSet(fromV, edge.getWeight());
    	Iterator<Integer> iterQ = querySet.iterator();
    	int num = querySet.size();
    	while(iterQ.hasNext()){
    		int kid = iterQ.next();
    		if(toV.containDistance(kid)){
	    		double wToV = toV.getDistance(kid);
	    		double weight = edge.getWeight();
	    		if(fromV.containDistance(kid)){
		    		double wFromV = fromV.getDistance(kid);
	    			//System.out.println(fromV.getVertexID()+" "+kid+" "+wFromV);
		    		if(wFromV>wToV+weight){
		    			fromV.setFollowV(toV);
		    			fromV.updateDistMap(kid, wToV, weight);
		    			fromV.updateLeafMap(kid, toV.getLeafByKid(kid));
		    			if(complete(fromV, num)){
		    				if(finalRM.add(fromV)){
		    					if(finalRM.size()>=topK){
		    						return true;
		    					}
		    				}
		    			}
		    			if(attach(fromV, kid, num)>=topK){
		    				return true;
		    			}
		    		}
	    		}
	    		else{
	    			//System.out.println(fromV.getVertexID()+" "+kid+" NULL");
	    			fromV.setFollowV(toV);
	    			fromV.updateDistMap(kid, wToV, weight);
	    			fromV.updateLeafMap(kid, toV.getLeafByKid(kid));
	    			if(complete(fromV, num)){
	    				if(finalRM.add(fromV)){
	    					if(finalRM.size()>=topK){
	    						return true;
	    					}
	    				}
	    			}
	    			if(attach(fromV, kid, num)>=topK){
	    				return true;
	    			}
	    		}
    		}
    	}
    	return false;
    }
    
    public int attach(VertexOne vertex, int kid, int num){
    	HashMap<VertexOne, Double> ansMap = vertex.getAnceMap();
    	int size = finalRM.size();
    	if(ansMap!=null){
			double dis = vertex.getDistance(kid);
    		Iterator<Entry<VertexOne, Double>> iter = ansMap.entrySet().iterator();
    		while(iter.hasNext()){
    			Entry<VertexOne, Double> entry = iter.next();
    			double disToAns = entry.getValue();
    			VertexOne ansVer = entry.getKey();
    			if(ansVer.updateDistMap(kid, dis, disToAns)){
    				ansVer.updateLeafMap(kid, vertex.getLeafByKid(kid));
    				if(complete(ansVer, num)){
    					//System.out.println("ans "+ansVer.getVertexID()+" "+ansVer.getState());
    					if(finalRM.add(ansVer)){
    						size++;
    					}
    				}
    			}
    		}
    	}
    	return size;
    }
    
    
    public String showResult(){
    	//System.out.println("results "+ finalRM.size());
    	String outStr = "";
    	TreeMap<Double, TreeMap<Integer, List<String>>> map = new TreeMap<Double, TreeMap<Integer, List<String>>>();
    	Iterator<VertexOne> iterFinal = finalRM.iterator();
    	/*if(iterFinal.hasNext()){
	    	VertexOne ver = iterFinal.next();
	    	int vid = ver.getVertexID();
	    	Double sum = ver.getActivation();
	    	TreeMap<Integer, List<String>> secMap = map.get(sum);
	    	if(secMap==null){
	    		secMap = new TreeMap<Integer, List<String>>();
	    		map.put(sum, secMap);
	    	}
	    	List<String> ansList = secMap.get(vid);
	    	if(ansList==null){
	    		ansList = new ArrayList<String>();
	    		secMap.put(vid, ansList);
	    	}
	    	ansList.add(ver.showLeafMap());
    	}*/
	    while(iterFinal.hasNext()){
	    	VertexOne ver = iterFinal.next();
	    	int vid = ver.getVertexID();
	    	Double sum = ver.getActivation();
	    	TreeMap<Integer, List<String>> secMap = map.get(sum);
	    	if(secMap==null){
	    		secMap = new TreeMap<Integer, List<String>>();
	    		map.put(sum, secMap);
	    	}
	    	List<String> ansList = secMap.get(vid);
	    	if(ansList==null){
	    		ansList = new ArrayList<String>();
	    		secMap.put(vid, ansList);
	    	}
	    	//System.out.println(ver.showLeafMap());
	    	ansList.add(ver.showLeafMap());
	    }
	    Iterator<Entry<Double, TreeMap<Integer, List<String>>>> iterMap = map.entrySet().iterator();
	    while(iterMap.hasNext()){
	    	Entry<Double, TreeMap<Integer, List<String>>> entry = iterMap.next();
	    	TreeMap<Integer, List<String>> secMap = entry.getValue();
	    	Iterator<Entry<Integer, List<String>>> iterSec = secMap.entrySet().iterator();
	    	while(iterSec.hasNext()){
	    		Entry<Integer, List<String>> secEntry = iterSec.next();
	    		List<String> thiList = secEntry.getValue();
	    		Iterator<String> thiIter = thiList.iterator();
	    		if(thiIter.hasNext()){
	    			outStr += "\n"+thiIter.next();
	    		}
	    		while(thiIter.hasNext()){
	    			outStr += "\n"+thiIter.next();
	    		}
	    	}
	    }
	    return outStr;
    }
    
    public static void main(String[] args) throws Exception {

    	long lDateTimeStart=0;
    	long lDateTimeFinish=0;
	    lDateTimeStart = new Date().getTime();
    	BiDirSearch bdsearch = new BiDirSearch();
    	String nodefile = args[0];
    	String edgefile = args[1];
    	String query = args[2];
  		HashSet<Integer> querySet = getQueryList(query);
  		int topK = Integer.parseInt(args[3]);
		GraphOne graph = new GraphOne();
    	FileInputStream fstream = new FileInputStream(nodefile);
    	HashMap<Integer, HashSet<VertexOne>> kidToNodeSet = new HashMap<Integer, HashSet<VertexOne>>();
		// Get the object of DataInputStream
		DataInputStream in = new DataInputStream(fstream);
		BufferedReader br = new BufferedReader(new InputStreamReader(in));
		String strLine;
	  	String[] temp;
	    String delimiter = " ";
	    while ((strLine = br.readLine()) != null)   {
	    	if(strLine.startsWith("#"))
	  			continue;
	  		temp = strLine.split(delimiter);
	  		int vid = Integer.parseInt(temp[0]);
		  	strLine = strLine.substring(strLine.indexOf(delimiter)+1);
		  	//System.out.println(vid+" "+strLine);
		  	graph.writeKeywordList(vid, strLine, querySet, kidToNodeSet);
	    }
	    in.close();
	    //System.out.println("Finish reading node file");
	    fstream = new FileInputStream(edgefile);
	    in = new DataInputStream(fstream);
		br = new BufferedReader(new InputStreamReader(in));
	    while ((strLine = br.readLine()) != null)   {
	    	if(strLine.startsWith("#"))
	  			continue;
	  		temp = strLine.split(delimiter);
	  	    if(temp.length>=4){
	  	        graph.writeDirectedEdgeInfo(Integer.parseInt(temp[0]), Integer.parseInt(temp[1]), Integer.parseInt(temp[2]), Double.parseDouble(temp[3]));
	  	    }
			
	    }
	    in.close();
	    //System.out.println("Finish reading edge file");
	    
	    Date begin = new Date();
	    bdsearch.initialInVertexSet(kidToNodeSet);
	    bdsearch.runBiDirSearch(querySet, graph, topK);
	    Date end = new Date();
	    //String folderTo = "results/20130305_results/bidirection/";
	    String folderTo = "";
	    String outName = "query_"+querySet.size()+"_"+query.replace(":", "_")+"_bidirectional.txt";
	    FileWriter outfstream = new FileWriter(folderTo+outName); //true tells to append data.
		BufferedWriter out = new BufferedWriter(outfstream);
		out.write("Query:"+query);
		out.write("\n");
	    out.write(bdsearch.showResult());
		out.write("\n");
		out.write("\n");
        lDateTimeFinish = new Date().getTime();
		out.write("Bidirection Search:"+(end.getTime()-begin.getTime())+"ms	total:"+(lDateTimeFinish-lDateTimeStart)+" ms");
		out.close();
	}
}
