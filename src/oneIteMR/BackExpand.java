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


public class BackExpand {
	
	private final int WHITE=0;
	private final int GRAY=1;
	//private final int BLACK=2;
	//kidToNodeSet is map from kid to a set with all nodes which can connect with this keyword
    private PriorityQueue<VertexOne> inVertexSet = new PriorityQueue<VertexOne>(10, new CompareVertexOne());
    //private PriorityQueue<VertexOne> outVertexSet = new PriorityQueue<VertexOne>(10, new CompareVertexOne());
    //private HashSet<VertexOne> inCheckedSet = new HashSet<VertexOne>();
    //private HashSet<VertexOne> outCheckedSet = new HashSet<VertexOne>();
    //private int depthMax = 8;
    private HashSet<VertexOne> finalRM = new HashSet<VertexOne>();
    
    public HashSet<VertexOne> getFinalRM(){
    	return finalRM;
    }
    
    public HashSet<Integer> getQueryList(String queryStr){
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
    	while(!inVertexSet.isEmpty()){
    			//System.out.println(showVertexSet(inVertexSet));
	    		VertexOne vertex = inVertexSet.poll();
				//System.out.println("pop vid:"+vertex.getVertexID());
				//if(vertex.getState()!=BLACK){
					//inCheckedSet.add(vertex);
					if(complete(vertex, num)){
						//System.out.println("found:"+vertex.getVertexID()+" "+num);
						finalRM.add(vertex);
						if(finalRM.size()>=topK)
							return true;
						//continue;
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
				//}
    		
    	}
    	return stop;
    }
 
    public boolean haveSolution(){
    	if(finalRM.size()==0)
    		return false;
    	return true;
    }
    /**
     * Check one vertex contains one whole solution or not
     * @param vertex
     * @return
     */
    public boolean complete(VertexOne vertex, int num){
    	//System.out.print("Check "+vertex.getVertexID());
    	if(vertex.getSizeOfRM()==num){
			//vertex.chanageState(BLACK);
    		//System.out.println(" true");
    		return true;
    	}
		//System.out.println(" false");
    	return false;
    }
    
    public boolean exploreInComEdge(EdgeOne edge, GraphOne graph, int size, int topK) throws IOException{
    	//System.out.println(edge.showEdge());
    	VertexOne fromV = graph.getVertex(edge.getVFrom());
    	//if(fromV.getState()!=BLACK){
	    	VertexOne toV = graph.getVertex(edge.getVTo());
	    	HashMap<Integer, Double> toRM = toV.getDisMap();
	    	Iterator<Entry<Integer, Double>> iter = toRM.entrySet().iterator();
	    	double weight = edge.getWeight();
	    	boolean update = false;
	    	//System.out.println("toRM size:"+toRM.size());
	    	while(iter.hasNext()){
	    		Entry<Integer, Double> entry = iter.next();
	    		int kid = entry.getKey();
	    		double toDis = entry.getValue();
	    		if(update){
	    			fromV.updateDistMap(kid, toDis, weight);
	    		}
	    		else{
	    			update = fromV.updateDistMap(kid, toDis, weight);
	    		}
		    	//System.out.println("vid:"+fromV.getVertexID()+" toDis:"+toDis+" toVid:"+toV.getVertexID()+" update:"+update);
	   			if(update)
	   				fromV.updateLeafMap(kid, toV.getLeafByKid(kid));
	   			/*if(complete(fromV, size)){
	   				//System.out.println(fromV.getVertexID()+" FOUND");
	   				//fromV.chanageState(BLACK);
	    			finalRM.add(fromV);
					if(finalRM.size()>=topK)
						return true;
	    			//inVertexSet.remove(fromV);
	    		}*/
	    		
	    	}
	    	if(fromV.getState()==WHITE){
	    		//System.out.println("WHITE:"+fromV.getVertexID());
	    		inVertexSet.add(fromV);
	    		fromV.chanageState(GRAY);
	    	}
	    	else if(fromV.getState()==GRAY && update){
	    		//System.out.println("GRAY:"+fromV.getVertexID());
	    		inVertexSet.remove(fromV);
	    		inVertexSet.add(fromV);
	    	}
    	//}
    	return false;
    }
    
    public boolean exploreOutGoEdge(EdgeOne edge, GraphOne graph, int size, int topK) throws IOException{
    	VertexOne toV = graph.getVertex(edge.getVTo());
    	Double weight = edge.getWeight();
    	if(toV.getState()==WHITE){
        	VertexOne fromV = graph.getVertex(edge.getVFrom());
	    	HashMap<Integer, Double> toRM = toV.getDisMap();
	    	Iterator<Entry<Integer, Double>> iter = toRM.entrySet().iterator();
	    	while(iter.hasNext()){
	    		Entry<Integer, Double> entry = iter.next();
	    		int kid = entry.getKey();
	    		double toDis = entry.getValue();
	    		double fromDis = fromV.getDistance(kid);
	    		if(fromDis==-1 || fromDis> toDis){
	    			fromV.updateDistMap(kid, toDis, weight);
	    			fromV.updateLeafMap(kid, toV.getLeafByKid(kid));
	    			if(complete(fromV, size)){
	    				finalRM.add(fromV);
						if(finalRM.size()>=topK)
							return true;
	    			}
	    		}
	    	}
	    	
	    	/*HashMap<Integer, Double> fromRM = fromV.getDisMap();
	    	iter = fromRM.entrySet().iterator();
	    	boolean update = false;
	    	while(iter.hasNext()){
	    		Entry<Integer, Double> entry = iter.next();
	    		int kid = entry.getKey();
	    		toV.updateDistMap(kid, entry.getValue(), weight);
	    	}
	    	if(toV.getState()==WHITE){
	    		inVertexSet.add(toV);
	    		toV.chanageState(GRAY);
	    	}
	    	else if(toV.getState()==GRAY && update){
	    		inVertexSet.remove(toV);
	    		inVertexSet.add(toV);
	    		toV.chanageState(GRAY);
	    	}*/
    	}
    	return false;
    }
    
    public void exploreEdge(EdgeOne edge, GraphOne graph, HashSet<Integer> querySet) throws IOException{
    	VertexOne fromV = graph.getVertex(edge.getVFrom());
    	VertexOne toV = graph.getVertex(edge.getVTo());
    	Iterator<Integer> iterQ = querySet.iterator();
    	while(iterQ.hasNext()){
    		int kid = iterQ.next();
    		if(toV.containDistance(kid)){
	    		double wToV = toV.getDistance(kid);
	    		double weight = edge.getWeight();
	    		if(fromV.containDistance(kid)){
		    		double wFromV = fromV.getDistance(kid);
		    		if(wFromV>wToV){
		    			fromV.setFollowV(toV);
		    			fromV.updateDistMap(kid, wToV, weight);
		    			fromV.updateLeafMap(kid, toV.getLeafByKid(kid));
		    			//attach(fromV, kid, graph, less);
		    			//fromV.updateActMap(kid, toV.getActivityByKid(kid)*factor);
		    			//System.out.println("from v:"+fromV.getVertexID()+" "+fromV.getActivation());
		    			//activate(fromV, kid, graph);
		    		}
	    		}
	    		else{
	    			fromV.setFollowV(toV);
	    			fromV.updateDistMap(kid, wToV, weight);
	    			fromV.updateLeafMap(kid, toV.getLeafByKid(kid));
	    			//attach(fromV, kid, graph);
	    			//System.out.println("from v:"+fromV.getVertexID()+" to v:"+toV.getVertexID()+" "+kid);
	    			//fromV.updateActMap(kid, toV.getActivityByKid(kid)*factor);
	    			//System.out.println("from v:"+fromV.getVertexID()+" "+fromV.getActivation());
	    			//activate(fromV, kid, graph);
	    		}
    		}
    	}
    }
    
    public void activate(VertexOne vertex, int kid, GraphOne graph) throws IOException{
    	if(inVertexSet.contains(vertex)){
    		inVertexSet.remove(vertex);
    		inVertexSet.add(vertex);
    	}
    	HashSet<EdgeOne> anceSet = vertex.getInComingSet();
    	Iterator<EdgeOne> iterAnce = anceSet.iterator();
    	while(iterAnce.hasNext()){
    		EdgeOne edge = iterAnce.next();
    		VertexOne anceVer = graph.getVertex(edge.getVFrom());
    		//anceVer.updateActMap(kid, vertex.getActivation()*factor);
    		if(inVertexSet.contains(anceVer)){
        		inVertexSet.remove(anceVer);
        		inVertexSet.add(anceVer);
        	}
    	}
    }
    
    public String showResult(){
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
    
    public String showResult(GraphOne graph){
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
	    	ansList.add(ver.showLeafMap());
	    }
	    
	    for(int i=1;i<=2;i++){
	    	int vid = i;
	    	VertexOne ver = graph.getVertex(vid);
	    	if(ver==null){
	    		continue;
	    	}
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
	    	if(ver.getLeafMap()!=null)
	    		
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
    	BackExpand backClass = new BackExpand();
    	long lDateTimeStart=0;
    	long lDateTimeFinish=0;
	    lDateTimeStart = new Date().getTime();
    	BackExpand bdsearch = new BackExpand();
    	String nodefile = args[0];
    	String query = args[1];
  		HashSet<Integer> querySet = backClass.getQueryList(query);
  		int topK = Integer.parseInt(args[2]);
		GraphOne graph = new GraphOne();
    	FileInputStream fstream = new FileInputStream(nodefile);
    	HashMap<Integer, HashSet<VertexOne>> kidToNodeSet = new HashMap<Integer, HashSet<VertexOne>>();
		// Get the object of DataInputStream
		DataInputStream in = new DataInputStream(fstream);
		BufferedReader br = new BufferedReader(new InputStreamReader(in));
		String strLine;
	  	String[] temp;
	    String delimiter = " ";
	    int nodeNum = Integer.parseInt(br.readLine());
	    for(int i=0;i<nodeNum;i++){
	    	strLine = br.readLine();
	    	if(strLine.startsWith("#"))
	  			continue;
	  		temp = strLine.split(delimiter);
	  		int vid = Integer.parseInt(temp[0]);
		  	strLine = strLine.substring(strLine.indexOf(delimiter)+1);
		  	graph.writeKeywordList(vid, strLine, querySet, kidToNodeSet);
	    }
	    int i=0;
	    while ((strLine = br.readLine()) != null)   {
	    	if(strLine.startsWith("#"))
	  			continue;
	  		temp = strLine.split(delimiter);
	  	    if(temp.length>=3){
	  	    	i++;
	  	        graph.writeDirectedEdgeInfo(i, Integer.parseInt(temp[0]), Integer.parseInt(temp[1]), Double.parseDouble(temp[2]));
		  	    
	  	    }
			
	    }
	    in.close();
	    System.out.println("Finish reading graph file");
	    
	    Date begin = new Date();
	    bdsearch.initialInVertexSet(kidToNodeSet);
	    bdsearch.runBiDirSearch(querySet, graph, topK);
	    Date end = new Date();
	    String folderTo = "results/20130305_results/backExpandRepeat/";
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
    /*public static void main(String[] args) throws Exception {

    	long lDateTimeStart=0;
    	long lDateTimeFinish=0;
	    lDateTimeStart = new Date().getTime();
    	BackExpand bdsearch = new BackExpand();
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
	    System.out.println("Finish reading node file");
	    fstream = new FileInputStream(edgefile);
	    in = new DataInputStream(fstream);
		br = new BufferedReader(new InputStreamReader(in));
	    while ((strLine = br.readLine()) != null)   {
	    	if(strLine.startsWith("#"))
	  			continue;
	  		temp = strLine.split(delimiter);
	  	    if(temp.length>=4){
	  	        graph.writeDirectedEdgeInfo(i, Integer.parseInt(temp[0]), Integer.parseInt(temp[1]), Double.parseDouble(temp[2]));
		  	    
	  	    }
			
	    }
	    in.close();
	    System.out.println("Finish reading edge file");
	    
	    Date begin = new Date();
	    bdsearch.initialInVertexSet(kidToNodeSet);
	    bdsearch.runBiDirSearch(querySet, graph, topK);
	    Date end = new Date();
	    String folderTo = "results/20130305_results/backExpandRepeat/";
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
	}*/
}
