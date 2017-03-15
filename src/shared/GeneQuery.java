package shared;


import graphSearch.GraphClass;
import graphSearch.Vertex;

import java.util.Random;
import java.io.BufferedWriter;
import java.io.FileWriter;
import java.io.IOException;
import java.util.HashSet;
import java.util.Iterator;
import java.util.LinkedList;
import java.util.Set;
import java.util.Map.Entry;
import java.util.Queue;
import java.util.TreeMap;
import java.util.TreeSet;

import org.jgrapht.graph.AsWeightedGraph;
import org.jgrapht.graph.DefaultWeightedEdge;
import org.kohsuke.args4j.CmdLineException;
import org.kohsuke.args4j.CmdLineParser;

public class GeneQuery {
	private static int black=2;
	private static int gray=1;
	private static GraphClass graphClass = new GraphClass();
	/**
	 * Generate random number from aStart to aEnd
	 * @param aStart
	 * @param aEnd
	 * @param aRandom
	 */
	public int generateRandomInteger(int aStart, int aEnd){
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
	
	/**
	 * Read graph based on BFSCopyOfGeneQuery and then generate random query
	 * Return a hashset with all random query
	 * @param nodeFile
	 * @param edgeFile
	 * @param level
	 */
	public HashSet<HashSet<Integer>> runGraphBFS(int level, int numKey, int numQuery){
		try{
			  Iterator<Entry<Integer, Vertex>> iter = graphClass.getVidToVMap().entrySet().iterator();
			  //////////////////////////
			  //test
			  /*while(iter.hasNext()){
				  vertex = iter.next().getValue();
				  Iterator<Integer> iterkey = vertex.getKeySet().iterator();
				  //System.out.println("VID:"+vertex.getVid());
				  //System.out.print("key:");
				  while(iterkey.hasNext()){
					  //System.out.print(iterkey.next()+" ");
				  }
				  //System.out.println("");
				  //System.out.print("adj:");
				  Iterator<Vertex> iterVer = vertex.getAdjSet().iterator();
				  while(iterVer.hasNext()){
					  Vertex tempV = iterVer.next();
					  //System.out.print(tempV.getVid()+" ");
				  }
				  //System.out.println("");
			  }*/
			  //end of test
			  //////////////////////////
			  HashSet<Integer> allKeySet = new HashSet<Integer>();
			  Vertex vertex = iter.next().getValue();
			  Queue<Vertex> queueV = new LinkedList<Vertex>();
			  queueV.add(vertex);
			  vertex.setLevel(0);
			  int lev=1;
			  while(!queueV.isEmpty()){
				  vertex = queueV.poll();
				  int vid = vertex.getVertexId();
				  //System.out.println("vid:"+vertex.getVertexId());
				  vertex.setColor(black);
				  if(vertex.getLevel()!=lev && level!=0){
					  //System.out.println("Level:"+lev);
					  vertex.setLevel(lev);
					  lev++;
					  if(lev>level+1)
						  break;
				  }
				  //System.out.println("key size:"+vertex.getKeySet().size());
				  Iterator<Integer> keyIter = vertex.getKeySet().iterator();
				  while(keyIter.hasNext()){
					  int kid=keyIter.next();
					  //System.out.println("kid:"+kid);
					  allKeySet.add(kid);
				  }
				  //System.out.println("begin");
				  AsWeightedGraph<Integer, DefaultWeightedEdge> graph = graphClass.getGraph();
				  Set<DefaultWeightedEdge> edgeSet = graph.edgesOf(vid);
				  Iterator<DefaultWeightedEdge> edgeIter = edgeSet.iterator();
				  while(edgeIter.hasNext()){
					  DefaultWeightedEdge edge = edgeIter.next();
					  int sid = graph.getEdgeSource(edge);
					  if(sid==vid){
						  sid = graph.getEdgeTarget(edge);
					  }

					  Vertex newVer = graphClass.getVertexFromVid(sid);
					  newVer.setLevel(lev+1);
					  int color = newVer.getColor();
					  //System.out.println("sid:"+sid+" color:"+color);
					  if(color!=gray && color!=black){
						  //System.out.println("add vid");
						  queueV.add(newVer);
						  newVer.setColor(gray);
					  }
				  }
				  //System.out.println("end");
			  }
			  //generate random query
			  HashSet<HashSet<Integer>> queryList = new HashSet<HashSet<Integer>>();//used to store random query
			  int range = allKeySet.size()-1;
			  System.out.println("range:"+range);
			  if(range<numKey){
				  System.out.println("Not enough keywords");
				  return null;
			  }
			  for(int num=0;num<numQuery;num++){
				  HashSet<Integer> keySet = new HashSet<Integer>(numKey);
		    	  //generate one random number during each iterator, query part done after this loop
		    	  for(int i=0;i<numKey;i++){
					  ////System.out.println("range size"+range+" "+i);
			    	  int ranNum = generateRandomInteger(0,range);
					  //System.out.println("random number "+ranNum+" "+allKeySet.size());
			    	  Iterator<Integer> iterSet = allKeySet.iterator();
			    	  for(int t=0;t<ranNum;t++){
			    		  iterSet.next();
			    	  }
			    	  int ranKey = iterSet.next();
			    	  boolean addNew = false;
			    	  if(graphClass.getVertexSetFromKey(ranKey).size()>=100){
			    		  addNew = keySet.add(ranKey);
			    	  }
			    	  if(addNew==false)
			    		  i--;
		    	  }
		    	  boolean newQuery = queryList.add(keySet);
		    	  if(newQuery==false)
		    		  num--;
			  }
			  return queryList;
		}
		catch (Exception e){//Catch exception if any
			  System.err.println("Error: " + e.getMessage());
		}
		return null;
	}
	
	public String retQueryAndNumOfVer(HashSet<HashSet<Integer>> querySet){
		String retStr = "";
		Iterator<HashSet<Integer>> iter = querySet.iterator();
		int num = 0;
		while(iter.hasNext()){
			HashSet<Integer> query = iter.next();
			retStr +=  "Query"+ ++num +": \n";
			Iterator<Integer> iterKey = query.iterator();
			while(iterKey.hasNext()){
				int key = iterKey.next();
				retStr += key+":"+graphClass.getVertexSetFromKey(key).size()+"\n";
			}
		}
		return retStr;
	}
	
	
	/**
	 * Generate query for a set of keyword numbers, number of query should be the same
	 * @param numQSet
	 * @param nodeFile
	 * @param edgeFile
	 * @param level
	 * @param numQuery
	 * @return
	 */
	public TreeMap<Integer, HashSet<HashSet<Integer>>> growRandomQuery(TreeSet<Integer> numQSet, int level, int numQuery){
		int bigNum = numQSet.last();
		TreeMap<Integer, HashSet<HashSet<Integer>>> retMap = new TreeMap<Integer, HashSet<HashSet<Integer>>>(); 
		System.out.println("start:"+numQSet.size()+" "+level+" "+numQuery );
		HashSet<HashSet<Integer>> bigQuerySet = this.runGraphBFS(level, bigNum, numQuery);
		System.out.println(retQueryAndNumOfVer(bigQuerySet));
		//System.out.println("big:"+bigQuerySet.size());
		if(bigQuerySet != null){
			retMap.put(bigNum, bigQuerySet);
			int previousInt = bigNum;
			while(!numQSet.isEmpty()){
				int numQ = numQSet.pollLast();//iterNum.next();
				System.out.println("numq:"+numQ);
				HashSet<HashSet<Integer>> priviousSet = retMap.get(previousInt);
				Iterator<HashSet<Integer>> iter = priviousSet.iterator();
				HashSet<HashSet<Integer>> querySet = new HashSet<HashSet<Integer>>();
				//for(int num=0;num<numQuery;num++){
				while(iter.hasNext()){
					HashSet<Integer> query = iter.next();//get the random query for biggest keywords number
					
					HashSet<Integer> newQuery = new HashSet<Integer>();
					Iterator<Integer> iterKey = query.iterator();
					for(int i=0; i<numQ; i++){
						newQuery.add(iterKey.next());
					}
					querySet.add(newQuery);
					/*boolean success = false;
					while(success==false){
						HashSet<Integer> newQuery = new HashSet<Integer>();
						for(int nKey=0; nKey<numQ; nKey++){
							Iterator<Integer> iterKey = query.iterator();
							for(int i=0; i<nKey; i++){
								iterKey.next();
							}
							int newKey = iterKey.next();
							boolean add = newQuery.add(newKey);
							if(add == false)
								nKey--;
						}
						boolean addQuery = querySet.add(newQuery);
						
						if(addQuery != false){
							success = true;
						}
					}*/
				}
				retMap.put(numQ, querySet);
				previousInt = numQ;
			}
		}
		return retMap;
	}
	/**
	 * Write generated query to file
	 * type 0: file for best first search (memory level for check)
	 * @param queryMap
	 * @param type
	 * @throws IOException
	 */
	public void writeToFile(TreeMap<Integer, HashSet<HashSet<Integer>>> queryMap, String outputFolder, int type) throws IOException{
			switch(type){
			case 0://file for best first search(memory level for check)
				Iterator<Entry<Integer, HashSet<HashSet<Integer>>>> iter = queryMap.entrySet().iterator();
				while(iter.hasNext()){
					Entry<Integer, HashSet<HashSet<Integer>>> entry = iter.next();
					int numKey = entry.getKey();
					String filename = outputFolder+"bestQuery_"+numKey;
					HashSet<HashSet<Integer>> querySet = entry.getValue();//10 query
					FileWriter fstream = new FileWriter(filename);
				    BufferedWriter out = new BufferedWriter(fstream);
				    Iterator<HashSet<Integer>> iterQSet = querySet.iterator();
		    		//System.out.println("Size:"+querySet.size());
				    TreeMap<Integer, HashSet<Integer>> queryTree = new TreeMap<Integer,HashSet<Integer>>();
				    while(iterQSet.hasNext()){
				    	HashSet<Integer> query = iterQSet.next();
				    	Iterator<Integer> iterQKey = query.iterator();
				    	queryTree.put(iterQKey.next(), query);
				    }
				    
				    
				    Iterator<Entry<Integer, HashSet<Integer>>> iterF = queryTree.entrySet().iterator();
				    while(iterF.hasNext()){
				    	HashSet<Integer> query = iterF.next().getValue();
				    	Iterator<Integer> iterQKey = query.iterator();
				    	if(iterQKey.hasNext()){
				    		////System.out.println(iterQKey.next()+" !!!");
				    		out.write(iterQKey.next()+"");
				    	}
				    	while(iterQKey.hasNext()){
				    		////System.out.println(iterQKey.next()+" ~");
				    		out.write(" "+iterQKey.next());
				    	}
				    	out.write("\n");
				    }
				    out.close();
				}
				break;
			case 1://file for best first search(MapReduce with one iteration)
				break;
			}
	}
	
	/**
	 * From input string, return one HashSet with all numbers of keyword size
	 * @param numQStr
	 * @return
	 */
	public TreeSet<Integer> getNumQSet(String numQStr){
		String deli = " ";
		String[] temp = numQStr.split(deli);
		int size = temp.length;
		TreeSet<Integer> numQSet = new TreeSet<Integer>();
		for(int i=0; i<size; i++){
			numQSet.add(Integer.parseInt(temp[i]));
		}
		return numQSet;
	}
	/**
	 * Transfer from string to hash set
	 * @param str
	 * @return
	 */
	public HashSet<Integer> strToHashSet(String str){
		String deli = " ";
		String[] temp = str.split(deli);
		int size = temp.length;
		HashSet<Integer> numQSet = new HashSet<Integer>();
		for(int i=0; i<size; i++){
			numQSet.add(Integer.parseInt(temp[i]));
		}
		return numQSet;
	}
	
	/*public void writeForOneMR(HashSet<Integer> allKeySet){
			  // Create file 
			  //output file for Mapreduce
			  FileWriter fstream = new FileWriter(outfile);
		      BufferedWriter out = new BufferedWriter(fstream);
		      
		      //output file for Blinks
		      FileWriter fstreamBlink = new FileWriter(outBlink);
		      BufferedWriter outB = new BufferedWriter(fstreamBlink);
		      
		      //output file for bidirectional search on Mapreduce
		      FileWriter fstreamBiMR = new FileWriter(strBiMR);
		      BufferedWriter outBiMR = new BufferedWriter(fstreamBiMR);

		      //output file for bidirectional search on Memory
		      FileWriter fstreamBiDir = new FileWriter(strBiDir);
		      BufferedWriter outBiDir = new BufferedWriter(fstreamBiDir);
		      
		      out.write("rm log"+numKey+".txt");//remove the old log file if exist
		      out.write("\n");
		      
		      outBiMR.write("rm log"+numKey+".txt");//remove the old log file if exist
		      outBiMR.write("\n");
		      
		      for(int num=0;num<numQuery;num++){
		    	  Vector<Integer> keyVec = new Vector<Integer>(numKey);
		    	  String outKey="";
		    	  //generate one random number during each iterator, query part done after this loop
		    	  for(int i=0;i<numKey;i++){
					  ////System.out.println("range size"+range+" "+i);
			    	  int ranNum = generateRandomInteger(0,range);
					  ////System.out.println("random number "+ranNum);
			    	  Iterator<Integer> iterSet = allKeySet.iterator();
			    	  for(int t=0;t<ranNum;t++){
			    		  iterSet.next();
			    	  }
			    	  int ranKey = iterSet.next();
			    	  boolean same=false;
			    	  Iterator<Integer> iterVec = keyVec.iterator();
			    	  while(iterVec.hasNext()){
			    		  if(ranKey == iterVec.next()){
			    			  same=true;
			    			  break;
			    		  }
			    	  }
			    	  if(same==true){
			    		  i--;
			    	  }
			    	  else{
			    		  keyVec.add(ranKey);
			    		  if(i==0){
			    			  outKey += ranKey+"";
			    		  }
			    		  else{
			    			  outKey += ":"+ranKey;
			    		  }
			    	  }
		    	  }//end of for(keyVec stores the query)
				  ////System.out.println("range size"+range);
		    	  String outStr = "../hadoop-1.0.3/bin/hadoop jar /home/hadoop/mapreduce/keywordSearch/keywordsearch.jar mapreduce.Main -input /user/hadoop/input -output /user/hadoop/output/output";
		          
		          outStr+=num;
		          outStr+=" -filetype 1 -topK ";
		          outStr+=topK;
		          outStr+=" -query ";
		          outStr+=outKey;
		          outStr+=">> log";
		          outStr+=numKey;
		          outStr+= ".txt";
		          out.write(outStr);
		          out.write("\n");
		          
		          outB.write(numKey+" "+outKey.replace(":", " ")+" ");
		          outB.write("\n");
		          
		          String outForSecMR = "../hadoop-1.0.3/bin/hadoop jar /home/hadoop/mapreduce/oneIterSearch/keywordsearch.jar oneIteMR.Main -input /user/hadoop/input -output /user/hadoop/output/output";
		          
		          outForSecMR+=num;
		          outForSecMR+=" -filetype 1 -topK ";
		          outForSecMR+=topK;
		          outForSecMR+=" -query ";
		          outForSecMR+=outKey;
		          outForSecMR+=">> log";
		          outForSecMR+=numKey;
		          outForSecMR+= ".txt";
		          outBiMR.write(outForSecMR);
		          outBiMR.write("\n");
		          
		          String outForBiDir = "java -Xmx20G -cp keywordsearch.jar oneIteMR.BiDirSearch data/DBLPPaperWithAuthor/subnodes.txt data/DBLPPaperWithAuthor/subedges.txt ";
		          outForBiDir += outKey;
		          outForBiDir += " "+topK;
		          outBiDir.write(outForBiDir);
		          outBiDir.write("\n");
		      }
		      
		      String secoutput="../hadoop-1.0.3/bin/hadoop fs -get output";
		      secoutput+=" ../hadoop-1.0.3/iterOutput/output"+numKey;
		      out.write(secoutput);
		      out.write("\n");
			  out.close();
			  outB.close();
			  String thioutput="../hadoop-1.0.3/bin/hadoop fs -get output";
			  thioutput+=" ../hadoop-1.0.3/oneOutput/output"+numKey;
			  outBiMR.write(thioutput);
			  outBiMR.write("\n");
			  outBiMR.close();
			  outBiDir.close();
		
	}*/
	
	
	public static void main(String [] args) throws IOException{
		CmdOption option = new CmdOption();
		CmdLineParser parser = new CmdLineParser(option);
		
		//1. get command line parameters 
		try {
			parser.parseArgument(args);
		
			GeneQuery geneQuery = new GeneQuery();
			//System.out.println("1");
			TreeSet<Integer> numQSizeSet = geneQuery.getNumQSet(option.numQSizeStr);

			graphClass.readUnDirectedGraph(option.folder+option.nodefile, option.folder+option.edgefile, true);
			//System.out.println("2"+ graphClass.showGraph());
			TreeMap<Integer, HashSet<HashSet<Integer>>> queryMap = geneQuery.growRandomQuery(numQSizeSet, 10, option.numQ);
			//System.out.println("3:"+queryMap.size());
			geneQuery.writeToFile(queryMap, option.queryfolder, 0);
			//System.out.println("4");
		} 
		catch (CmdLineException e) {
		}
	}
}
