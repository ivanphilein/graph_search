package graphSearch;

import java.io.BufferedReader;
import java.io.BufferedWriter;
import java.io.DataInputStream;
import java.io.File;
import java.io.FileInputStream;
import java.io.FileWriter;
import java.io.IOException;
import java.io.InputStreamReader;
import java.text.DecimalFormat;
import java.util.Date;
import java.util.HashSet;
import java.util.Iterator;
import java.util.PriorityQueue;
import java.util.Set;

import org.jgrapht.graph.*;
import org.kohsuke.args4j.CmdLineException;
import org.kohsuke.args4j.CmdLineParser;

import shared.CmdOption;
import shared.IndexElement;

public class SearchUndirected {
	private GraphClass graphClass = null;//new GraphClass();
	//private UndirectedGraph<Integer, DefaultWeightedEdge> iniGraph = new SimpleGraph<Integer, DefaultWeightedEdge>(DefaultWeightedEdge.class);
	//Map<DefaultWeightedEdge, Double> weiMap = new HashMap<DefaultWeightedEdge, Double>();
	//AsWeightedGraph<Integer, DefaultWeightedEdge> graph = new AsWeightedGraph<Integer, DefaultWeightedEdge>(iniGraph, weiMap);
	//FinalSolutions solutionList = new FinalSolutions();
	
	
	//HashMap<Integer, Vertex> vidToVMap = new HashMap<Integer, Vertex>();//store from keyword id to vertex set
	//HashMap<Integer, HashSet<Integer>> keyToVMap = new HashMap<Integer, HashSet<Integer>>();//store from keyword id to vertex set

	/*public void resumeVer(){
		Iterator<Entry<Integer, Vertex>> iter = vidToVMap.entrySet().iterator();
		while(iter.hasNext()){
			iter.next().getValue().resume();
		}
	}*/
	
	public SearchUndirected(GraphClass graph){
		graphClass = graph;
	}
	
	public SearchUndirected(String nodefile, String edgefile){
		graphClass = new GraphClass();
		graphClass.readUnDirectedGraph(nodefile, edgefile, true);
	}
	
	public SearchUndirected(String nodefile, String edgefile, String partition,  String portalfile, boolean storeKey){
		graphClass = new GraphClass(nodefile, edgefile, partition, portalfile, storeKey);
	}
	
	public SearchUndirected(String nodefile, String edgefile, boolean storeKey){
		graphClass = new GraphClass();
		graphClass.readUnDirectedGraph(nodefile, edgefile, storeKey);
	}
	
	public SearchUndirected(String nodefile, String edgefile, String partition,  String portalfile){
		graphClass = new GraphClass(nodefile, edgefile, partition, portalfile, true);
	}
	
	public void writeToFile(String outFolder, String queryStr, int size, int topK, FinalSolutions solution, double time){
		String filename = outFolder+"query_"+size+"_"+queryStr+"_BestFS.txt_top"+topK;
		//System.out.println("Write file: "+filename +"...");
  		try{
  			 FileWriter fstream = new FileWriter(filename);
  			 BufferedWriter out = new BufferedWriter(fstream);
  			 out.write("Query:"+queryStr+" topK:"+topK);
  			 out.write("\n");
  	    	 out.write(solution.returnSolutions());
  	    	 out.write("Time:"+time+" ms"+"	"+time/1000+" s");
  			 //Close the output stream
  			 out.close();
	  	}catch (Exception e){//Catch exception if any
			  System.err.println("Error: " + e.getMessage());
		}	
  		//System.out.println("Finish writing file: "+filename +"...");
	}
	
	public void runQuery(String folderFile, String outFolder, int topK){
		//System.out.println("Read folder: "+folderFile +"...");
  		try{
  			File folder = new File(folderFile);
  			//System.out.println("folder:"+folderFile);
  			File[] listOfFiles = folder.listFiles();
  			//TreeMap<String, String> resultMap = new TreeMap<String, String>(new CompareString());
  			//TreeSet<String> fileSet = new TreeSet<String>(new CompareString());
  			for (File file : listOfFiles) {
  			    if (file.isFile()) {
  			    	String filename = file.getName();
  			    	//String runTime;
		  			FileInputStream fstream = new FileInputStream(folderFile+filename);
		  			// Get the object of DataInputStream
		  			DataInputStream in = new DataInputStream(fstream);
		  		    BufferedReader br = new BufferedReader(new InputStreamReader(in));
		  			String strLine;
			  	    while ((strLine = br.readLine()) != null)   {
			  	    	if(strLine.startsWith("#"))
			  	    		continue;
			  	    	//strLine = strLine.substring(strLine.indexOf(" ")+1);
			  	    	HashSet<Integer> querySet = iniQuerySet(strLine);
			  	    	long lDateTimeStart=0;
			  	    	long lDateTimeFinish=0;
			  	    	lDateTimeStart = new Date().getTime();
			  	    	FinalSolutions solution = this.runBestFirstSearch(querySet, topK);
			  	    	lDateTimeFinish = new Date().getTime();
			  	    	double time = lDateTimeFinish-lDateTimeStart;
			  	    	writeToFile(outFolder, strLine.replace(" ", "_"), querySet.size(), topK, solution, time);
			  	    	
			  	    	solution = null;
			  	    }
	  			    
		  			//Close the input stream
			  	    in.close();
  			    }
  			}
  		}catch (Exception e){//Catch exception if any
  			  System.err.println("Error: " + e.getMessage());
  		}	
	}
	
	
	
    /////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////
    //querySet part
    private HashSet<Integer> iniQuerySet(String queryStr){
		if(queryStr.isEmpty()){
			return null;
		}
		HashSet<Integer> querySet = new HashSet<Integer>();
		String[] temp;
		String delimiter = " ";
		temp=queryStr.split(delimiter);
		for(int i=0;i<temp.length;i++){
			querySet.add(Integer.parseInt(temp[i]));
		}
		return querySet;
	}
    
    /////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////
	/**
     * Add edge from v1 to v2, with weight weight
     * @param v1
     * @param v2
     * @param weight
     */
    /*public void addGrageEdge(int v1,int v2, Double weight) {
    	DefaultWeightedEdge edge = graph.addEdge(v1, v2); 
    	graph.setEdgeWeight(edge, weight);
    }*/
    
    public FinalSolutions runBestFirstSearch(HashSet<Integer> querySet, int topK){
    	DecimalFormat df = new DecimalFormat("#.##");
    	long lDateTimeStart=0;
    	lDateTimeStart = new Date().getTime();
    	AsWeightedGraph<Integer, DefaultWeightedEdge> graph = graphClass.getGraph();
    	////System.out.println("search step:"+1);
    	FinalSolutions solutionList = new FinalSolutions();
    	int querySize = querySet.size();
    	////System.out.println(queryStr+" "+querySet.size());
    	Iterator<Integer> iterQuery = querySet.iterator();
    	PriorityQueue<PQElement> queue = new PriorityQueue<PQElement>(10, new PQComparator());
    	//int smallSize = 0;
    	//int smallKid = -1;
    	int total = 1;
    	int found = 0;
    	while(iterQuery.hasNext()){
    		int qid = iterQuery.next();
    		HashSet<Integer> verSet = graphClass.getVertexSetFromKey(qid);
    		if(verSet != null){
    			////System.out.println("total:"+total+" key:"+qid+" size:"+verSet.size());
    			if(total<topK){
    				total = total*verSet.size();
    			}
    			/*if(smallKid <0 || verSet.size()<smallSize){
    				smallKid = qid;
    				smallSize = verSet.size();
    			}*/
    			Iterator<Integer> iterVer = verSet.iterator();
    			while(iterVer.hasNext()){
    				int vid = iterVer.next();
    				Vertex ver = graphClass.getVertexFromVid(vid);
    				IndexElement index = ver.initialPathMap(qid, 0.0, vid, vid, querySize);
    				////System.out.println(qid+" "+vid+" "+querySize);
	    			if(index!=null){
	    				////System.out.println("solutions before:"+solutionList.returnSolutions());
	    				//ver.findSolution(qid, index, querySet, topK, solutionList);
	    				ver.addSouPath(ver, qid, vid, vid, 0.0, querySet, topK-found, solutionList);
	    				found = solutionList.getSize();
	    				////System.out.println("solutions after:"+solutionList.returnSolutions());
	    				if(found>=topK){
    	    				break;
    	    			}
	    			}
	    			
	    			PQElement element = new PQElement();
					element.setVid(vid);
					element.setSource(vid);
					element.setPriority(0.0);
					element.setKeyword(qid);
					queue.add(element);
    			}
    		}
    		else{
    			total = 0;
    			return solutionList;
    		}
    	}
    	/*if(total != 0){
    		HashSet<Integer> verSet = graphClass.getVertexSetFromKey(smallKid);
    		Iterator<Integer> iterVer = verSet.iterator();
			while(iterVer.hasNext()){
				int vid = iterVer.next();
				PQElement element = new PQElement();
				element.setVid(vid);
				element.setSource(vid);
				element.setPriority(0.0);
				element.setKeyword(smallKid);
				queue.add(element);
			}
    	}*/
    	//queue contians all vertex with at least one keyword, with priority 0.0
    	////System.out.println(getDetailPriorityQ(queue)+"  "+queue.size());
    	////System.out.println("search step:"+2);
    	double stopPriority = 0.0;
    	int i=0;
    	////System.out.println("total:"+total);
    	if(total<topK)
    		topK = total;
    	boolean stop = false;
    	int popNum = 0;
    	while(!queue.isEmpty()&&stop == false){
    		long lDateTimeRun=0;
    		lDateTimeRun = new Date().getTime();
    		if((lDateTimeRun-lDateTimeStart)>120000){
    			//System.out.println("Time break");
    			break;
    		}
			i++;
    		PQElement element = queue.poll();
    		////System.out.println("pop:"+element.returnElement()+" num:"+popNum+++" found:"+found);
    		int vid = element.getVertexID();
    		double priority = element.getPriority();
			if(i%100000==0){
				//System.out.println("stop:"+stop+" found:"+found+" size:"+solutionList.getSize()+" total:"+total+" queue size:"+queue.size());
	    		//System.out.println("priority:"+priority+" stop:"+stopPriority);
			}
    		int source = element.getSource();
    		int key = element.getKeyword();
    		Vertex vertex = graphClass.getVertexFromVid(vid);
    		Vertex souVer = graphClass.getVertexFromVid(source);//update source one
    		vertex.addVisited(source);
    		if(vid != source){
    			////System.out.println("before vertex update: "+souVer.returnSolutionStr());
    			//boolean update = souVer.updateSouPath(vertex, key,  element.getNextID(),vid, element.getPriority(), querySize);
    			souVer.addSouPath(vertex, key,  element.getNextID(),vid, element.getPriority(), querySet, topK-found, solutionList);
    			found = solutionList.getSize();
    			////System.out.println("solution:\n"+solutionList.returnSolutions(true));
				////System.out.println("vertex update: "+souVer.returnSolutionStr());
    			//if(update && souVer.found(querySize)){
	    			//int add = souVer.findSolution(querySet, topK, solutionList);

	    			//total = total-add;
		    		//if(stop==false && found>=topK){
	    			if(found>=topK){
		    			stop = true;
		    			//System.out.println("found: "+found+" topK:"+topK);
		    			stopPriority = element.getPriority();
		    			//System.out.println("break");
		    			break;
		    		}
    			//}
    		}
    		Set<DefaultWeightedEdge> edgeSet = graph.edgesOf(vid);
    		Iterator<DefaultWeightedEdge> iterEdge = edgeSet.iterator();
    		////System.out.println("search step:"+3);
    		while(iterEdge.hasNext()){
    			DefaultWeightedEdge edge = iterEdge.next();
    			int newid = graph.getEdgeSource(edge);
    			int tid = graph.getEdgeTarget(edge);
    			double weight = graph.getEdgeWeight(edge);
    			////System.out.println("edge:"+newid+" "+tid+" "+weight);
    			if(newid==vid){
    				newid = tid;
    			}
    			Vertex newVer = graphClass.getVertexFromVid(newid);
    			////System.out.println("newid:"+newid+" source:"+source+" visited:"+ newVer.getVisitedSet().toString() );
    			if(newid!=source && !newVer.visited(source)){
        			////System.out.println("newid:"+newid+" not visited in "+source);
	    			PQElement newElement = new PQElement();
	    			newElement.setVid(newid);
	    			if(vid == source){
	    				newElement.setNextID(newid);
	    			}
	    			else
	    				newElement.setNextID(element.getNextID());
	    			newElement.setPriority(Double.parseDouble(df.format(priority+weight)));
	    			//newElement.setCost(priority+weight);
	    			newElement.setSource(source);
	    			newElement.setKeyword(key);
	    			queue.add(newElement);
    			}
    		}
    		element = null;
    		////System.out.println("search step:"+4);
    	}
    	queue = null;
		solutionList.returnSolutions(topK);
		graphClass.resumeVer();
		return solutionList;
    }
    
    
    /*public FinalSolutions runBestFirstSearchWithPortal(HashSet<Integer> querySet, int topK, int blockId){
    	FinalSolutions solution = this.runBestFirstSearch(querySet, topK);
    	searchDisPortalNode(querySet, solution.getTopKSum(), blockId, topK-solution.getSize(), solution);
    	solution.returnSolutions(topK);
    	return solution;
    }
    
    public void searchDisPortalNode(HashSet<Integer> querySet, double topKSum, int blockId, int topK,  FinalSolutions solutionList){
    	AsWeightedGraph<Integer, DefaultWeightedEdge> graph = graphClass.getGraph();
    	PriorityQueue<PQElement> queue = new PriorityQueue<PQElement>(10, new PQComparator());
    	HashSet<Integer> portalSet = graphClass.getPortalSet();
    	if(portalSet != null){
    		Iterator<Integer> iterPortal = portalSet.iterator();
			while(iterPortal.hasNext()){
				int vid = iterPortal.next();
				PQElement element = new PQElement();
				element.setVid(vid);
				element.setNextID(vid);
				element.setSource(vid);
				element.setPriority(0.0);
				queue.add(element);
				
			}
    	}
    	while(!queue.isEmpty()){
    		PQElement element = queue.poll();
    		//////System.out.println("pop:"+element.returnElement());
    		int vid = element.getVertexID();
    		double priority = element.getPriority();
    		int source = element.getSource();
    		double prioirty = element.getPriority();
    		if(topKSum>=0 && prioirty>topKSum){
    			break;
    		}
    		Vertex vertex = graphClass.getVertexFromVid(vid);
    		Vertex souVer = graphClass.getVertexFromVid(source);//update source one
    		vertex.addVisited(source);
    		int found = 0;
    		if(vid != source){
    			found += souVer.addSouPathWithPortal(vertex, element.getNextID(),vid, element.getPriority(), querySet, topK-found, solutionList, blockId);
    			
    		}
    		Set<DefaultWeightedEdge> edgeSet = graph.edgesOf(vid);
    		Iterator<DefaultWeightedEdge> iterEdge = edgeSet.iterator();
    		//////System.out.println("search step:"+3);
    		while(iterEdge.hasNext()){
    			DefaultWeightedEdge edge = iterEdge.next();
    			int newid = graph.getEdgeSource(edge);
    			int tid = graph.getEdgeTarget(edge);
    			double weight = graph.getEdgeWeight(edge);
    			////System.out.println("s:"+newid+" t:"+tid+" wei:"+weight+" p:"+priority);
    			if(newid==vid){
    				newid = tid;
    			}
    			Vertex newVer = graphClass.getVertexFromVid(newid);
    			if(newid!=source && !newVer.visited(source)){
        			//////System.out.println("newid:"+newid+" not visited in "+source);
	    			PQElement newElement = new PQElement();
	    			newElement.setVid(newid);
	    			if(vid == source){
	    				newElement.setNextID(newid);
	    			}
	    			else
	    				newElement.setNextID(element.getNextID());
	    			weight+=priority;
	    			newElement.setPriority(weight);
	    			newElement.setSource(source);
	    			//////System.out.println("new element:"+newElement.returnElement());
	    			queue.add(newElement);
	    			//newVer.addVisited(source);
    			}
    		}
    		element = null;
    	}
    	//System.out.println(solutionList.getPortalDisMapFromVid(2));
		//System.out.println(solutionList.getPortalDisMapFromVid(3));
		//System.out.println(graphClass.getVertexFromVid(2).returnSolutionStr());
		//System.out.println(graphClass.getVertexFromVid(3).returnSolutionStr());
		//System.out.println(graphClass.getVertexFromVid(7).returnSolutionStr());
    	queue = null;
		////System.out.println(solutionList.getPortalDisMapFromVid(2));
		////System.out.println(solutionList.getPortalDisMapFromVid(3));
		//graphClass.resumeVer();
    }*/
    
    
    public FinalSolutions runBestFirstSearchWithPortal(HashSet<Integer> querySet, int topK){
    	//DecimalFormat df = new DecimalFormat("#.##");
    	long lDateTimeStart=0;
    	lDateTimeStart = new Date().getTime();
    	AsWeightedGraph<Integer, DefaultWeightedEdge> graph = graphClass.getGraph();
    	////System.out.println("search step:"+1);
    	FinalSolutions solutionList = new FinalSolutions();
    	int querySize = querySet.size();
    	////System.out.println(queryStr+" "+querySet.size());
    	Iterator<Integer> iterQuery = querySet.iterator();
    	PriorityQueue<PQElement> queue = new PriorityQueue<PQElement>(10, new PQComparator());
    	int smallSize = 0;
    	int smallKid = -1;
    	int total = 1;
    	int found = 0;

    	HashSet<Integer> portalSet = graphClass.getPortalSet();
    	while(iterQuery.hasNext()){
    		int qid = iterQuery.next();
    		HashSet<Integer> verSet = graphClass.getVertexSetFromKey(qid);
    		if(verSet != null){
    			////System.out.println("total:"+total+" key:"+qid+" size:"+verSet.size());
    			if(total<topK){
    				total = total*verSet.size();
    			}
    			if(smallKid <0 || verSet.size()<smallSize){
    				smallKid = qid;
    				smallSize = verSet.size();
    			}
    			Iterator<Integer> iterVer = verSet.iterator();
    			while(iterVer.hasNext()){
    				int vid = iterVer.next();
    				Vertex ver = graphClass.getVertexFromVid(vid);
    				IndexElement index = ver.initialPathMap(qid, 0.0, vid, vid, querySize);
    				////System.out.println(qid+" "+vid+" "+querySize);
	    			if(index!=null){
	    				////System.out.println("solutions before:"+solutionList.returnSolutions());
	    				//ver.findSolution(qid, index, querySet, topK, solutionList);
	    				ver.addSouPath(ver, qid, vid, vid, 0.0, querySet, topK-found, solutionList);
	    				found = solutionList.getSize();
	    				////System.out.println("solutions after:"+solutionList.returnSolutions());
	    				if(found>=topK){
    	    				break;
    	    			}
	    			}
    				//portalSet.remove(vid);
    				PQElement element = new PQElement();
    				element.setVid(vid);
    				element.setNextID(vid);
    				element.setSource(vid);
    				element.setPriority(0.0);
    				element.setKeyword(qid);
    				queue.add(element);
    			}
    		}
    		else{
    			total = 0;
    		}
    	}
    	if(portalSet != null){
    		Iterator<Integer> iterPortal = portalSet.iterator();
			while(iterPortal.hasNext()){
				int vid = iterPortal.next();
				PQElement element = new PQElement();
				element.setVid(vid);
				element.setNextID(vid);
				element.setSource(vid);
				element.setPriority(0.0);
				element.setKeyword(-1);
				queue.add(element);
			}
    	}
    	int i=0;
    	////System.out.println("total:"+total);
    	double biggestSum = -1;
    	if(found>=topK){
    		biggestSum = solutionList.returnSolutions(topK);
    		return solutionList;
    	}
    	while(!queue.isEmpty()){
    		long lDateTimeRun=0;
    		lDateTimeRun = new Date().getTime();
    		if((lDateTimeRun-lDateTimeStart)>120000){
    			//System.out.println("Time break");
    			break;
    		}
			i++;
    		PQElement element = queue.poll();
    		////System.out.println("pop:"+element.returnElement());
    		int vid = element.getVertexID();
    		double priority = element.getPriority();
			if(i%100000==0){
				//System.out.println("found:"+found+" size:"+solutionList.getSize()+" total:"+total+" queue size:"+queue.size());
	    		//System.out.println("priority:"+priority);
			}
    		int source = element.getSource();
    		int key = element.getKeyword();
    		double prioirty = element.getPriority();
    		////System.out.println("priority:"+priority+" biggest:"+biggestSum);
    		if(biggestSum>=0 && prioirty>biggestSum){
    			//System.out.println("biggest:"+biggestSum+" BREAK");
    			break;
    		}
    		Vertex vertex = graphClass.getVertexFromVid(vid);
    		Vertex souVer = graphClass.getVertexFromVid(source);//update source one
    		if(found >= topK && souVer.getPortalBlock() == -1){
    			////System.out.println("continue "+topK);
    			continue;
    		}
    		if(vertex.visited(source)){
    			continue;
    		}
    		
    		vertex.addVisited(source);
    		//candidate part
    		//end of candidate part
    		if(vid != source){
    			////System.out.println("before vertex update: "+souVer.returnSolutionStr());
    			if(found<topK){
    				souVer.addSouPathWithPortal(vertex, key,  element.getNextID(),vid, element.getPriority(), querySet, topK, solutionList);
    	    		found = solutionList.getSize();
    				if(found >= topK){
    					//System.out.println("Found:"+found+" topK:"+topK);
    					biggestSum = solutionList.returnSolutions(topK);
    				}
    			}
    				
    		}
    		Set<DefaultWeightedEdge> edgeSet = graph.edgesOf(vid);
    		Iterator<DefaultWeightedEdge> iterEdge = edgeSet.iterator();
    		////System.out.println("search step:"+3);
    		while(iterEdge.hasNext()){
    			DefaultWeightedEdge edge = iterEdge.next();
    			int newid = graph.getEdgeSource(edge);
    			int tid = graph.getEdgeTarget(edge);
    			double weight = graph.getEdgeWeight(edge);
    			////System.out.println("edge:"+newid+" "+tid+" "+weight);
    			if(newid==vid){
    				newid = tid;
    			}
    			Vertex newVer = graphClass.getVertexFromVid(newid);
    			////System.out.println("newid:"+newid+" source:"+source+" visited:"+ newVer.getVisitedSet().toString() );
    			if(newid!=source && !newVer.visited(source)){
        			////System.out.println("newid:"+newid+" not visited in "+source);
	    			PQElement newElement = new PQElement();
	    			newElement.setVid(newid);
	    			if(vid == source){
	    				newElement.setNextID(newid);
	    			}
	    			else
	    				newElement.setNextID(element.getNextID());
	    			newElement.setPriority((priority+weight));
	    			//newElement.setCost(priority+weight);
	    			newElement.setSource(source);
	    			newElement.setKeyword(key);
	    			queue.add(newElement);
    			}
    		}
    		element = null;
    	}
    	queue = null;
		solutionList.returnSolutions(topK);

		/*System.out.println(solutionList.getPortalDisMapFromVid(2));
		System.out.println(solutionList.getPortalDisMapFromVid(3));
		System.out.println(graphClass.getVertexFromVid(2).returnSolutionStr());
		System.out.println(graphClass.getVertexFromVid(3).returnSolutionStr());
		System.out.println(graphClass.getVertexFromVid(7).returnSolutionStr());*/
		//graphClass.resumeVer();
		return solutionList;
    }
    
    
    public String getDetailPriorityQ(PriorityQueue<PQElement> queue){
    	String retStr = "";
    	Iterator<PQElement> iter = queue.iterator();
    	while(iter.hasNext()){
    		PQElement ele = iter.next();
    		retStr += "||vid:"+ele.getVertexID()+" p:"+ele.getPriority()+" s:"+ele.getSource()+" k:"+ele.getKeyword()+"     ";
    	}
    	return retStr;
    }
    
    /*public static void main(String[] args) throws IOException {
    	CmdOption option = new CmdOption();
		CmdLineParser parser = new CmdLineParser(option);
		
		//1. get command line parameters 
		try {
			parser.parseArgument(args);
		} catch (CmdLineException e) {
		}
    	SearchUndirected search = new SearchUndirected(option.folder+option.nodefile, option.folder+option.edgefile);
    	//search.readUnDirectedGraph(option.folder+option.nodefile, option.folder+option.edgefile);
    	//System.out.println("folder:"+option.queryfolder+" topK!!!!!!:"+option.topK);
    	search.runQuery(option.queryfolder,"", option.topK);
    }*/
    public static void main(String[] args) throws IOException {
    	CmdOption option = new CmdOption();
		CmdLineParser parser = new CmdLineParser(option);
		
		//1. get command line parameters 
		try {
			parser.parseArgument(args);
		} catch (CmdLineException e) {
		}
    	SearchUndirected search = new SearchUndirected(option.folder+option.nodefile, option.folder+option.edgefile, option.folder+option.partitionfile, option.folder+option.portalF);
    	
    	HashSet<Integer> querySet = search.iniQuerySet(option.query.replace(":", " "));
    	//FinalSolutions solution = search.runBestFirstSearch(querySet, option.topK);
    	FinalSolutions solution = search.runBestFirstSearchWithPortal(querySet, option.topK);
    	System.out.println("solution:\n"+solution.returnSolutions());
    }
    
}
