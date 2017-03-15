package iteration;
import graphSearch.FinalSolutions;
import graphSearch.SolGNodeComparator;
import graphSearch.SolGraphNode;
import graphSearch.SolutionClass;

import java.io.IOException;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.HashSet;
import java.util.Iterator;
import java.util.List;
import java.util.Map.Entry;
import java.util.PriorityQueue;
import java.util.TreeMap;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Reducer;

import shared.IndexElement;

public class IterReducer extends Reducer<IntWritable, Text, IntWritable, Text> 
{
	private final static int solReducer = -2;
	private String original = "original";
	private String verLabel = "vertex";
	private String connLable = "connect";
	private String tarLable = "target";
	
	public static enum State {
	    UPDATED;
	}
	
	public static enum Sum {
	    TOPKSUM;
	}
	
	public String returnSolutionStr(int vid, TreeMap<Integer, List<IndexElement>> pathMap, String label){
		if(pathMap==null || pathMap.isEmpty()){
			return null;
		}
		String retStr = label+" "+vid;
		Iterator<Entry<Integer, List<IndexElement>>> iter = pathMap.entrySet().iterator();
		while(iter.hasNext()){
			Entry<Integer, List<IndexElement>> entry = iter.next();
			int kid = entry.getKey();
			List<IndexElement> indexSet = entry.getValue();
			Iterator<IndexElement> iterList = indexSet.iterator();
			retStr += " "+kid+":";
			if(iterList.hasNext()){
				IndexElement index = iterList.next();
				retStr += index.getElement();
			}
			while(iterList.hasNext()){
				IndexElement index = iterList.next();
				retStr += "-"+index.getElement();
			}
		}
		return retStr;
	}
	
	public int findSolution(int vid, TreeMap<Integer, List<IndexElement>> pathMap, int topK, FinalSolutions solutionList){
		int add = 0;

		Iterator<Entry<Integer, List<IndexElement>>> iter = pathMap.entrySet().iterator();
		SolGraphNode node = new SolGraphNode();
		while(iter.hasNext()){

			Entry<Integer, List<IndexElement>> entry = iter.next();
			int otherkid = entry.getKey();
			node.addIterWithkeyword(otherkid, 0);
		}
		
		PriorityQueue<SolGraphNode> queue = 
                new PriorityQueue<SolGraphNode>(10, new SolGNodeComparator());
		queue.add(node);
		while(!queue.isEmpty() && topK>0){
			SolGraphNode popNode = queue.poll();
			HashMap<Integer, Integer> solGNodeMap = popNode.getSolNode();
			SolutionClass solution = new SolutionClass();
			double sum = 0;
			Iterator<Entry<Integer, Integer>> iterNodeMap = solGNodeMap.entrySet().iterator();
			while(iterNodeMap.hasNext()){
				Entry<Integer, Integer> entry = iterNodeMap.next();
				int key = entry.getKey();
				int number = entry.getValue();
				List<IndexElement> keyList = pathMap.get(key);
				IndexElement newIndex = keyList.get(number);
				solution.addSolution(entry.getKey(), newIndex);
				sum += newIndex.getLength();
			}
			solution.setSum(sum);
			solution.setVid(vid);
			if(solutionList.addSolution(solution)){
				add ++;
				topK--;
			}
			//add new element
			iterNodeMap = solGNodeMap.entrySet().iterator();
			while(iterNodeMap.hasNext()){
				Entry<Integer, Integer> entry = iterNodeMap.next();
				int newkid = entry.getKey();
				int number = entry.getValue();
				List<IndexElement> list = pathMap.get(newkid);
				if(number<list.size()-1){
					IndexElement first = list.get(number);
					IndexElement second = list.get(number+1);
					double diff = second.getLength()-first.getLength();
					SolGraphNode newNode = new SolGraphNode();
					newNode.setSum(diff);
					newNode.cloneGNodeMap(popNode, newkid);
					queue.add(newNode);
				}
			}
		}
		return add;
	}
	
	private int getQuerySize(String queryStr){
		String[] temp;
		String delimiter = ":";
		temp=queryStr.split(delimiter);
		return temp.length;
	}
	
	public IndexElement listContain(List<IndexElement> list, IndexElement index){
		Iterator<IndexElement> iter = list.iterator();
		while(iter.hasNext()){
			IndexElement oldIndex = iter.next();
			if(oldIndex.getStartVertex()==index.getStartVertex() && oldIndex.getEndVertex()==index.getEndVertex()){
				return oldIndex;
			}
		}
		return null;
	}
	
	
	/**
	 * Add index with kid to the corresponding vertex vid
	 * @param vid
	 * @param kid
	 * @param index
	 * @param vidPathMap
	 * @return
	 */
	public boolean addVidKidIndexToMap(int vid, int kid, int querySize, int topK, FinalSolutions solution, IndexElement index, HashMap<Integer, TreeMap<Integer, List<IndexElement>>> vidPathMap){
		TreeMap<Integer, List<IndexElement>> pathMap = vidPathMap.get(vid);
		if(pathMap==null){
			pathMap = new TreeMap<Integer, List<IndexElement>>();
			vidPathMap.put(vid, pathMap);
		}
		List<IndexElement> indexSet = pathMap.get(kid);
		if(indexSet == null){
			indexSet = new ArrayList<IndexElement>();
			pathMap.put(kid, indexSet);
			indexSet.add(index);
			if(pathMap.size()==querySize)
				this.findSolution(vid, pathMap, topK, solution);
			return true;
		}
		else{
			IndexElement oldIndex = listContain(indexSet, index);
			if(oldIndex == null){
				indexSet.add(index);
				if(pathMap.size()==querySize)
					this.findSolution(vid, pathMap, topK, solution);
				return true;
			}
			else{
				if(oldIndex.getLength()>index.getLength()){
					oldIndex.setLength(index.getLength());

					if(pathMap.size()==querySize)
						this.findSolution(vid, pathMap, topK, solution);
					return true;
				}
				else{
					return false;
				}
			}
		}

	}
	
	/*public void updateFromSetToList(HashMap<Integer, TreeMap<Integer, TreeSet<IndexElement>>> vidPathMapSet, HashMap<Integer, TreeMap<Integer, List<IndexElement>>> vidPathMap){
		Iterator<Entry<Integer, TreeMap<Integer, TreeSet<IndexElement>>>> iter = vidPathMapSet.entrySet().iterator();
		while(iter.hasNext()){
			Entry<Integer, TreeMap<Integer, TreeSet<IndexElement>>> entry = iter.next();
			int vid = entry.getKey();
			TreeMap<Integer, List<IndexElement>> treeMapList = new TreeMap<Integer, List<IndexElement>>();
			TreeMap<Integer, TreeSet<IndexElement>> treeMap = entry.getValue();
			Iterator<Entry<Integer, TreeSet<IndexElement>>> secIter = treeMap.entrySet().iterator();
			while(secIter.hasNext()){
				Entry<Integer, TreeSet<IndexElement>> secEntry = secIter.next();
				int kid = secEntry.getKey();
				TreeSet<IndexElement> indexSet = secEntry.getValue();
				List<IndexElement> list = new ArrayList<IndexElement>();
				list.addAll(indexSet);
				treeMapList.put(kid, list);
			}
			vidPathMap.put(vid, treeMapList);
		}
		vidPathMapSet = null;
	}*/
	
	public int updateAnceVertex(int vid, int anceVid, int querySize, int topK, FinalSolutions solution, double distance, HashMap<Integer, TreeMap<Integer, List<IndexElement>>> vidPathMap, long topKSum){
		boolean update = false;
		TreeMap<Integer, List<IndexElement>> pathMap = vidPathMap.get(vid);
		Iterator<Entry<Integer, List<IndexElement>>> iter = pathMap.entrySet().iterator();
		while(iter.hasNext()){
			Entry<Integer, List<IndexElement>> entry = iter.next();
			int kid = entry.getKey();
			List<IndexElement> indexList = entry.getValue();
			Iterator<IndexElement> iterList = indexList.iterator();
			while(iterList.hasNext()){
				IndexElement element = iterList.next();
				if(topKSum>0 && topKSum<(element.getLength()+distance))
					continue;
				IndexElement newIndex = new IndexElement();
				newIndex.setElement(distance+element.getLength(), anceVid, vid, element.getEndVertex());
				if(update==false)
					update = this.addVidKidIndexToMap(anceVid, kid, querySize, topK, solution, newIndex, vidPathMap);
				else{
					this.addVidKidIndexToMap(anceVid, kid,querySize, topK, solution, newIndex, vidPathMap);
				}
			}
		}
		if(update)
			return anceVid;
		else
			return -1;
	}
	
	public void updateConnectedPortal(HashSet<String> connSet, String firstDelimiter, String secDelimiter, FinalSolutions solution, long topKSum){
		//update distance between portal nodes connection 
		Iterator<String> iterConn = connSet.iterator();
		while(iterConn.hasNext()){
			String connStr = iterConn.next();
			String[] temp = connStr.split(firstDelimiter);
			int vid = Integer.parseInt(temp[0]);
			for(int i=1; i<temp.length; i++){
				int disIndex = temp[i].indexOf(secDelimiter);
				int portalid = Integer.parseInt(temp[i].substring(0, disIndex));
				double distance = Double.parseDouble(temp[i].substring(disIndex+1));
				if(topKSum>0 && topKSum<distance)
					continue;
				solution.addToPortalDisMapForIterMR(vid, portalid, distance);
				solution.addToPortalDisMapForIterMR(portalid, vid, distance);
			}
		}
		//End of updating distance between portal nodes connection
	}
	
	
	public void reduce(IntWritable key, Iterable<Text> values, Context context) throws InterruptedException, IOException {
		
		//System.out.println("Start Reading block:"+key.get());
		Configuration conf = context.getConfiguration();
		String query = conf.get("QUERY");
		int querySize = this.getQuerySize(query);
		int topK = Integer.parseInt(conf.get("TOPK"));
		
		int mapReduceKey = key.get();
		if(mapReduceKey==solReducer){
			FinalSolutions solution = new FinalSolutions();
			for(Text val : values){
				SolutionClass solClass = new SolutionClass(val.toString());
				solution.addSolution(solClass);
				//context.write(key, new Text(val.toString()));
			}
			//context.write(key, new Text(solution.getSize()+":SIZE"));
			double topKSum = solution.returnSolutions(topK);
			List<SolutionClass> topKSolList = solution.getTopKSol();
			if(topKSolList!=null){
				Iterator<SolutionClass> iterList = topKSolList.iterator();
				while(iterList.hasNext()){
					String solStr = iterList.next().getSolution();
					context.write(key, new Text(solStr));
				}
			}
			//context.write(new IntWritable(-10), new Text(topKSum+""));
			context.getCounter(Sum.TOPKSUM).setValue((long)topKSum);
			/*if(rmMap.size()>topK){
		  		context.write(key, new Text("query:"+query));
		  		double sum = -1;
				while(topK>0){
					Entry<Double, HashSet<String>> entry = rmMap.pollFirstEntry();
	  				HashSet<String> solSet = entry.getValue();
	  				Iterator<String> iter = solSet.iterator();
	  				while(iter.hasNext()){
	  					String solStr = iter.next();
	  					context.write(key, new Text(solStr));
	  					topK--;
	  					if(topK==0){
	  						sum = Double.parseDouble(solStr.substring(0, solStr.indexOf(" ")));
	  						break;
	  					}
	  				}	
		  			
		  		}
				context.getCounter(Sum.TOPKSUM).setValue((long) sum);
	  			context.getCounter(State.UPDATED).setValue(-100);
			}
			else{
				context.getCounter(Sum.TOPKSUM).setValue(-2);
			}*/
		}
		else{
			int bid = key.get();
			long topKSum =Long.parseLong(conf.get("TOPKSUM"));
			HashMap<Integer, TreeMap<Integer, List<IndexElement>>> vidPathMap = new HashMap<Integer, TreeMap<Integer, List<IndexElement>>>();
			String firstDelimiter = " ";//vid kid_1 kid_2 kid_3....
			String secDelimiter = ":"; //for each path, kid:indexes
			String thirdDelimiter = "-";//for all indexes, index_1-index_2-...
			HashMap<Integer, List<String>> storeListMap = new HashMap<Integer, List<String>>();//used to store multiple strings with same vertex id
			HashSet<Integer> duplicateVer = new HashSet<Integer>();
			HashSet<String> connSet = new HashSet<String>();
			FinalSolutions solution = new FinalSolutions();
			for(Text value : values){
				String candidateStr = value.toString();
				int index = candidateStr.indexOf(firstDelimiter);
				if(index==-1)
					continue;
				String label = candidateStr.substring(0, index);
				//context.write(key, new Text(label));
				if(label.equals(original)){
					candidateStr = candidateStr.substring(index+1);
					String[] temp = candidateStr.split(firstDelimiter);
					int vid = Integer.parseInt(candidateStr.substring(0, candidateStr.indexOf(firstDelimiter)));
					for(int i=1; i<temp.length; i++){
						String kidToIndex = temp[i];
						int indexOrigi = kidToIndex.indexOf(secDelimiter);
						int kid = Integer.parseInt(temp[i].substring(0, indexOrigi));
						kidToIndex = kidToIndex.substring(indexOrigi+1);
						String[] indexArray = kidToIndex.split(thirdDelimiter);
						for(int j=0; j<indexArray.length; j++){
							String indexStr = indexArray[j];
							IndexElement indexElement = new IndexElement(indexStr);
							this.addVidKidIndexToMap(vid, kid, querySize, topK, solution,indexElement, vidPathMap);
						}
					}
				}
				else if(label.equals(verLabel)){
					candidateStr = candidateStr.substring(index+1);
					int vid = Integer.parseInt(candidateStr.substring(0, candidateStr.indexOf(firstDelimiter)));
					
					//if(vidPathMap.containsKey(vid)){
					duplicateVer.add(vid);
					//}
					List<String> strList = storeListMap.get(vid);
					if(strList==null){
						strList = new ArrayList<String>();
						storeListMap.put(vid, strList);
					}
					strList.add(candidateStr);
				}
				else if(label.equals(connLable)){
					//context.write(key, new Text(candidateStr));
					candidateStr = candidateStr.substring(index+1);
					connSet.add(candidateStr);
				}
				else if(label.equals(tarLable)){
					candidateStr = candidateStr.substring(index+1);
					solution.readTargetInfo(candidateStr);
					context.write(key, value);
				}
			}
			
			context.write(key, new Text("topKSum:"+topKSum));
			this.updateConnectedPortal(connSet, firstDelimiter, secDelimiter, solution, topKSum);
			
			boolean update = false;
			Iterator<Integer> iter = duplicateVer.iterator();
			while(iter.hasNext()){
				int vid = iter.next();
				List<String> candList = storeListMap.get(vid);
				Iterator<String> candIter = candList.iterator();
				
				//merge multiple solutions
				boolean updateVer = false;
				while(candIter.hasNext()){
					String candStr = candIter.next();
					String[] temp = candStr.split(firstDelimiter);
					for(int i=1; i<temp.length; i++){
						String kidToIndex = temp[i];
						int index = kidToIndex.indexOf(secDelimiter);
						int kid = Integer.parseInt(temp[i].substring(0, index));
						kidToIndex = kidToIndex.substring(index+1);
						String[] indexArray = kidToIndex.split(thirdDelimiter);
						for(int j=0; j<indexArray.length; j++){
							String indexStr = indexArray[j];
							IndexElement indexElement = new IndexElement(indexStr);
							if(topKSum>0 && topKSum<indexElement.getLength())
								continue;
							if(!vidPathMap.containsKey(vid)){
								continue;
							}
							if(updateVer == false){
								updateVer = this.addVidKidIndexToMap(vid, kid, querySize, topK, solution, indexElement, vidPathMap);
							}
							else{
								this.addVidKidIndexToMap(vid, kid, querySize, topK, solution, indexElement, vidPathMap);
							}
						}
					}
				}
				//end of merging multiple solutions
				if(updateVer){
					update = true;
					//updateSet.add(vid);
					//update portal distance map
					HashMap<Integer, Double> porDisMap = solution.getPortalDisMapFromVid(vid);
					if(porDisMap!=null){
						Iterator<Entry<Integer, Double>> iterPDMap = porDisMap.entrySet().iterator();
						while(iterPDMap.hasNext()){
							Entry<Integer, Double> entryPDMap = iterPDMap.next();
							int anceId = entryPDMap.getKey();
							int upId = this.updateAnceVertex(vid, anceId,querySize, topK, solution, entryPDMap.getValue(), vidPathMap, topKSum);
							/*if(upId != -1){
								updateSet.add(anceId);
							}*/
							/*int targetId = solution.getTargetBlock(anceId);
							String outStr = "";
							if(targetId == bid){
								outStr += original+this.returnSolutionStr(anceId, vidPathMap.get(anceId));
							}
							else{
								outStr += verLabel+this.returnSolutionStr(anceId, vidPathMap.get(anceId));
							}
							if(outStr != null)
								context.write(new IntWritable(targetId), new Text(outStr));*/
						}
					}
					//end of updating portal distance map
						
					//send candidate solution
					/*TreeMap<Integer, List<IndexElement>> pathMap = vidPathMap.get(vid);
					String outStr = original + this.returnSolutionStr(vid, pathMap);
					if(outStr != null){
						context.write(key, new Text(outStr));
						//System.out.println("key:"+entry.getKey()+" pathMap:"+());
					}	
					//update whole solution	
					if(pathMap.size()==querySize)
						this.findSolution(vid, pathMap, topK, solution);*/
				}
				/*else{
					//send candidate solution
					TreeMap<Integer, List<IndexElement>> pathMap = vidPathMap.get(vid);
					String outStr = this.returnSolutionStr(vid, pathMap, original);
					if(outStr != null){
						context.write(key, new Text(outStr));
						//System.out.println("key:"+entry.getKey()+" pathMap:"+());
					}
				}*/
			}
			
			if(solution.getPortalDisMap()!=null){
				Iterator<Entry<Integer, HashMap<Integer, Double>>> iterDis = solution.getPortalDisMap().entrySet().iterator();
				while(iterDis.hasNext()){
					int vid = iterDis.next().getKey();
					String connStr = solution.retPDisMapFromVidStr(vid);
					if(connStr != null)
						context.write(key, new Text(connStr));
				}
			}
			
			if(update){
			//if(!updateSet.isEmpty()){
				context.getCounter(State.UPDATED).setValue(-100);
				solution.returnSolutions(topK);
				//generate top K solutions
				List<SolutionClass> topKSolList = solution.getTopKSol();
				if(topKSolList!=null){
					Iterator<SolutionClass> iterList = topKSolList.iterator();
					while(iterList.hasNext()){
						String solStr = iterList.next().getSolution();
						context.write(new IntWritable(solReducer), new Text(solStr));
					}
				}
				
				/*Iterator<Integer> iterUp = updateSet.iterator();
				while(iterUp.hasNext()){
					int upid = iterUp.next();
					int targetId = solution.getTargetBlock(upid);
					String outStr = "";
					if(targetId == bid){
						outStr += original+this.returnSolutionStr(upid, vidPathMap.get(upid));
					}
					else{
						outStr += verLabel+this.returnSolutionStr(upid, vidPathMap.get(upid));
					}
					if(outStr != null)
						context.write(new IntWritable(targetId), new Text(outStr));
				}*/
			}
			else{
				context.getCounter(State.UPDATED).increment(1);
			}
			
			Iterator<Entry<Integer, TreeMap<Integer, List<IndexElement>>>> iterPath = vidPathMap.entrySet().iterator();
			while(iterPath.hasNext()){
				Entry<Integer, TreeMap<Integer, List<IndexElement>>> entry = iterPath.next();
				int vid = entry.getKey();
				int targetId = solution.getTargetBlock(vid);
				String outStr = "";
				if(targetId == bid){
					outStr += this.returnSolutionStr(vid, entry.getValue(),original);
				}
				else{
					outStr += this.returnSolutionStr(vid, entry.getValue(), verLabel);
				}
				if(outStr != null)
					context.write(new IntWritable(targetId), new Text(outStr));
			}
			//context.getCounter(Sum.TOPKSUM).setValue((long)topKSum);
			//context.write(key, new Text("update:"+updateSet.size()));
			/*if(!updateVSet.isEmpty()){
				context.getCounter(State.UPDATED).increment(1);
				//update vertexes with multiple candidates and sent its path map
				Iterator<Entry<Integer, TreeMap<Integer, List<IndexElement>>>> iterSol = vidPathMap.entrySet().iterator();
				while(iterSol.hasNext()){
					Entry<Integer, TreeMap<Integer, List<IndexElement>>> entry = iterSol.next();
 					TreeMap<Integer, List<IndexElement>> pathMap = entry.getValue();
 					int vid = entry.getKey();
 					String outStr = this.returnSolutionStr(vid, pathMap);
 					if(outStr != null){
 						context.write(key, new Text(outStr));
 						//System.out.println("key:"+entry.getKey()+" pathMap:"+());
 					}
					if(pathMap.size()==querySize)
						this.findSolution(vid, pathMap, topK, solution);
				}
				//end of updating vertexes with multiple candidates
				
				//update portal node ances...
				iterSol = vidPathMap.entrySet().iterator();
				while(iterSol.hasNext()){
					Entry<Integer, TreeMap<Integer, List<IndexElement>>> entry = iterSol.next();
 					int vid = entry.getKey();
 					HashMap<Integer, Double> porDisMap = solution.getPortalDisMapFromVid(vid);
 					Iterator<Entry<Integer, Double>> iterPDMap = porDisMap.entrySet().iterator();
 					while(iterPDMap.hasNext()){
 						Entry<Integer, Double> entryPDMap = iterPDMap.next();
 						int anceId = entryPDMap.getKey();
 						this.updateAnceVertex(vid, anceId, entryPDMap.getValue(), vidPathMap, topKSum);
 						int targetId = solution.getTargetBlock(anceId);
 						String outStr = this.returnSolutionStr(anceId, vidPathMap.get(anceId));
 						if(outStr != null)
 							context.write(new IntWritable(targetId), new Text(outStr));
 					}
				}
				//end of updating portal node ances...
				
				//generate top K solutions
				double newSum = solution.returnSolutions(topK);
				if(newSum<topKSum){
					topKSum = (long) newSum;
				}
				List<SolutionClass> topKSolList = solution.getTopKSol();
				if(topKSolList!=null){
					Iterator<SolutionClass> iterList = topKSolList.iterator();
					while(iterList.hasNext()){
						String solStr = iterList.next().getSolution();
						context.write(new IntWritable(solReducer), new Text(solStr));
					}
				}
				
			}
			else
				context.getCounter(State.UPDATED).setValue(-100);*/
		}
	}
}
