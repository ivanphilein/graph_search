package graphSearch;


import java.util.HashMap;
import java.util.HashSet;
import java.util.Iterator;
import java.util.LinkedList;
import java.util.List;
import java.util.PriorityQueue;
import java.util.Map.Entry;
import java.util.TreeMap;

import shared.IndexElement;


public class Vertex {
	private int vid;
	private HashSet<Integer> keywordSet = null;//store keyword set
	private HashSet<Integer> visitedSet = new HashSet<Integer>();//store keyword set
	private TreeMap<Integer, List<IndexElement>> pathMap = null;// new TreeMap<Integer, List<IndexElement>>();
	private int priority = 0;
	//private HashSet<Integer> blockList = null;
	private int block = -1;
	private double weight = 0;
	
	private int portalBlock = -1;//set portal block information if this vertex is one portal node. Set the value as original block id
	
	////////////////////////////////////////////////////////////////////////////////////////////////////////
	//portal block part
	public int getPortalBlock(){
		return portalBlock;
	}
	public void setPortalBlock(int portal){
		portalBlock = portal;
	}
	//End of portal block part
	////////////////////////////////////////////////////////////////////////////////////////////////////////
	
	////////////////////////////////////////////////////////////////////////////////////////////////////////
	//weight
	public double getWeight(){
		return weight;
	}
	public void setWeight(double wei){
		weight = wei;
	}
	//End of weight	
	////////////////////////////////////////////////////////////////////////////////////////////////////////

	
	////////////////////////////////////////////////////////////////////////////////////////////////////////
	//block list part
	/**
     * O(1)
     * Return block list of this vertex
     * @return
     */
    /*public HashSet<Integer> getBlockList(){
		return blockList;
	}*/
    /**
     * Add bid to block list
     * @param bid
     */
    /*public void addToBlockList(int bid){
    	if(blockList == null){
    		blockList = new HashSet<Integer>();
    	}
    	blockList.add(bid);
    }*/
    //end of blcok list part
	////////////////////////////////////////////////////////////////////////////////////////////////////////
	
	////////////////////////////////////////////////////////////////////////////////////////////////////////
	/**
	 * Set block id
	 * @param bid
	 */
	public void setBlock(int bid){
		block = bid;
	}
	/**
	 * Get block id
	 * @return
	 */
	public int getBlock(){
		return block;
	}
	////////////////////////////////////////////////////////////////////////////////////////////////////////
	
	////////////////////////////////////////////////////////////////////////////////////////////////////////
	//part used for generating random query
	private int color=0;
	private int level=0;
	/*
	 * get the value of level
	 */
	public int getLevel(){
		return level;
	}
	/*
	 * Set the value of level
	 */
	public void setLevel(int lev){
		level = lev;
	}
	/*
	 * Set value of color
	 */
	public void setColor(int col){
		color=col;
	}
	/*
	 * Get the color value
	 */
	public int getColor(){
		return color;
	}
	//End of part used for generating random query
	////////////////////////////////////////////////////////////////////////////////////////////////////////
	
	public void resume(){
		visitedSet = new HashSet<Integer>();
		//////System.out.println(this.returnSolutionStr());
		pathMap = null;//new TreeMap<Integer, List<IndexElement>>();
		priority = 0;
	}
	
	public double getPriority(){
		return priority;
	}
	
	public void addPriority(double add){
		priority += add;
	}
	
	public int getVertexId(){
		return vid;
	}
	
	public void setVertexId(int id){
		vid = id;
	}
	
	////////////////////////////////////////////////////////////////////////////////////////////////////////
	//keywordSet part
	public void addKeySet(HashSet<Integer> keySet){
		keywordSet = keySet;
	}
	
	public HashSet<Integer> getKeySet(){
		return keywordSet;
	}
	//end of keywordSet part
	////////////////////////////////////////////////////////////////////////////////////////////////////////
	////////////////////////////////////////////////////////////////////////////////////////////////////////
	//visitedSet part
	public void addVisited(int visited){
		visitedSet.add(visited);
	}
	/**
	 * Check one source if visited or not
	 * @param visited
	 * @return
	 */
	public boolean visited(int visited){
		if(visitedSet.contains(visited)){
			return true;
		}
		return false;
	}
	
	public HashSet<Integer> getVisitedSet(){
		return visitedSet;
	}
	//end of visitedSet part
	////////////////////////////////////////////////////////////////////////////////////////////////////////
	////////////////////////////////////////////////////////////////////////////////////////////////////////
	//pathMap part
	public int getPathSize(){
		if(pathMap==null)
			return 0;
		return pathMap.size();
	}
	
	
	
	public IndexElement initialPathMap(int kid, double weight, int nextid, int endid, int querySize){
		IndexElement index = new IndexElement(weight, vid, nextid, endid);
		if(pathMap==null)
			pathMap = new TreeMap<Integer, List<IndexElement>>();
		List<IndexElement> indexList = pathMap.get(kid);
		if(indexList == null){
			indexList = new LinkedList<IndexElement>();
			pathMap.put(kid, indexList);
		}
		indexList.add(index);
		priority += weight;
		if(pathMap.size()>=querySize)
			return index;
		return null;
	}
	/**
	 * Add one index for keyword kid to pathmap, return index if there is one whole solution found, otherwise, return null
	 * @param kid
	 * @param weight
	 * @param nextid
	 * @param endid
	 * @param querySize
	 * @return
	 */
	/*public IndexElement addPathMap(int kid, double weight, int nextid, int endid, int querySize){
		IndexElement index = new IndexElement(weight, vid, nextid, endid);
		List<IndexElement> indexList = pathMap.get(kid);
		if(indexList == null){
			indexList = new LinkedList<IndexElement>();
			pathMap.put(kid, indexList);
		}
		indexList.add(index);
		priority += weight;
		if(pathMap.size()>=querySize)
			return index;
		return null;
	}*/
	
	/**
	 * Add index to kid, return true if one whole solution found
	 * @param kid
	 * @param index
	 * @param querySize
	 * @return
	 */
	public boolean addPathMap(int kid, IndexElement index, int querySize){
		if(pathMap==null)
			pathMap = new TreeMap<Integer, List<IndexElement>>();
		List<IndexElement> indexList = pathMap.get(kid);
		if(indexList == null){
			indexList = new LinkedList<IndexElement>();
			pathMap.put(kid, indexList);
		}
		indexList.add(index);
		priority += index.getLength();
		if(pathMap.size()>=querySize)
			return true;
		return false;
	}
	
	public TreeMap<Integer, List<IndexElement>> getPathMap(){
		return this.pathMap;
	}
	
	public boolean found(int size){
		return this.getPathSize()>=size;
	}
	
	/*public int addSolution(IndexElement index, int kid, int topK, FinalSolutions solutionList){
		int add = 0;
		if(solutionList.getSize()==0){
			return this.findSolution(kid, index, topK, solutionList);
		}
		List<SolutionClass> solList = solutionList.getInterSolutions();
		Iterator<SolutionClass> iterSol = solList.iterator();
		while(iterSol.hasNext()){
			SolutionClass original = iterSol.next();
			if(original.getVid() == this.vid){
				SolutionClass sol = iterSol.next().geneAndUpSolution(kid, index);
				if(solutionList.addSolution(sol)){
					add++;
				}
			}
		}
		return add;
	}*/
	
	public int addSouPath(Vertex newVer, int kid, int nextid, int endid, double weight, HashSet<Integer> querySet, int topK, FinalSolutions solutionList){
		//////System.out.println("update:"+newVer.getVertexId()+" key:"+kid+" nextid:"+nextid+" endid:"+endid+" weight:"+weight);
		int add = 0;
		boolean update = false;
		HashSet<Integer> keySet = newVer.getKeySet();
		Iterator<Integer> iter = querySet.iterator();
		while(iter.hasNext()){
			int qid = iter.next();
			if(qid == kid){
				continue;
			}
			//////System.out.println("qid:"+qid);
			if(keySet.contains(qid)){
				IndexElement newIndex = new IndexElement(weight, this.getVertexId(), nextid, endid);
				update = this.addPathMap(qid, newIndex, querySet.size());
				if(update){
					add += this.findSolution(qid, newIndex, topK, solutionList);
					if(add >= topK){
						break;
					}
				}
			}
		}
		
		return add;
	}
	
	
	/*public int addSouPathWithPortal(Vertex newVer, int nextid, int endid, double weight, HashSet<Integer> querySet, int topK, FinalSolutions solutionList, int bid){
		////////System.out.println("update:"+newVer.getVertexId()+" key:"+kid+" nextid:"+nextid+" endid:"+endid+" weight:"+weight);
		
		int firstP = this.getPortalBlock();
		int secP = newVer.getPortalBlock();
		if(firstP==bid){
			//if(this.getVertexId()<newVer.getVertexId() && firstP!= -1 && secP != -1){
			if(secP != -1){
				solutionList.addToPortalDisMap(this.getVertexId(), newVer.getVertexId(), weight, false);
			}
		}
		if(secP == bid){
			//if(this.getVertexId()<newVer.getVertexId() && firstP!= -1 && secP != -1){
			if(firstP!= -1){
				solutionList.addToPortalDisMap(newVer.getVertexId(), this.getVertexId(), weight, false);
			}
		}
		int add = 0;
		boolean update = false;
		HashSet<Integer> keySet = newVer.getKeySet();
		Iterator<Integer> iter = querySet.iterator();
		while(iter.hasNext()){
			int qid = iter.next();
			////////System.out.println("qid:"+qid);
			if(keySet.contains(qid)){
				IndexElement newIndex = new IndexElement(weight, this.getVertexId(), nextid, endid);
				////////System.out.println("qid:"+qid+" index:"+newIndex.getElement());
				update = this.addPathMap(qid, newIndex, querySet.size());
				if(update){
					add += this.findSolution(qid, newIndex, topK, solutionList);
				}
			}
		}
		
		return add;
	}*/
	
	public int addSouPathWithPortal(Vertex newVer, int kid, int nextid, int endid, double weight, HashSet<Integer> querySet, int topK, FinalSolutions solutionList){
		//////System.out.println("update:"+newVer.getVertexId()+" key:"+kid+" nextid:"+nextid+" endid:"+endid+" weight:"+weight);
		int add = addSouPath( newVer,  kid,  nextid,  endid,  weight, querySet, topK,  solutionList);
		if(this.getPortalBlock()!=-1){
		
			if(this.getVertexId()<newVer.getVertexId() && this.getPortalBlock()!= -1 && newVer.getPortalBlock() != -1){
				solutionList.addToPortalDisMap(this.getVertexId(), newVer.getVertexId(), weight, false);
				//////System.out.println("!!!!!!!!!!!"+solutionList.returnPortalDisMap());
			}
			HashSet<Integer> keySet = newVer.getKeySet();
			Iterator<Integer> iter = querySet.iterator();
			while(iter.hasNext()){
				int qid = iter.next();
				if(qid!=kid)
					continue;
				//////System.out.println("qid:"+qid);
				if(keySet.contains(qid)){
					IndexElement newIndex = new IndexElement(weight, this.getVertexId(), nextid, endid);
					//////System.out.println("qid:"+qid+" index:"+newIndex.getElement());
					this.addPathMap(qid, newIndex, querySet.size());
				}
			}
		}
		return add;
	}
	

	public int findSolution(int kid, IndexElement index, int topK, FinalSolutions solutionList){
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
		int i=0;
		while(!queue.isEmpty() && topK>0){
			SolGraphNode popNode = queue.poll();
			//////System.out.println("node11111:"+popNode.returnNode());
			HashMap<Integer, Integer> solGNodeMap = popNode.getSolNode();
			SolutionClass solution = new SolutionClass();
			solution.addSolution(kid, index);
			double sum = index.getLength();
			Iterator<Entry<Integer, Integer>> iterNodeMap = solGNodeMap.entrySet().iterator();
			while(iterNodeMap.hasNext()){
				Entry<Integer, Integer> entry = iterNodeMap.next();
				int key = entry.getKey();
				if(key == kid)
					continue;
				int number = entry.getValue();
				List<IndexElement> keyList = pathMap.get(key);
				IndexElement newIndex = keyList.get(number);
				solution.addSolution(entry.getKey(), newIndex);
				sum += newIndex.getLength();
			}
			solution.setSum(sum);
			if(solutionList.addSolution(solution)){
				////System.out.println(i++ +":\n" +solutionList.returnSolutions(true));
				add ++;
				topK--;
			}
			//add new element
			iterNodeMap = solGNodeMap.entrySet().iterator();
			while(iterNodeMap.hasNext()){
				Entry<Integer, Integer> entry = iterNodeMap.next();
				int newkid = entry.getKey();
				if(newkid == kid)
					continue;
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
	/*public int findSolution(int kid, IndexElement index, int topK, FinalSolutions solutionList){
		int add = 0;
		//////System.out.println(this.returnSolutionStr());
		//////System.out.println(kid+" "+index.getElement());
		TreeMap<Integer, List<IndexElement>> copyMap = new TreeMap<Integer, List<IndexElement>>();
		copyMap.putAll(pathMap);
		copyMap.remove(kid);
		Iterator<Entry<Integer, List<IndexElement>>> iter = copyMap.entrySet().iterator();
		SolGraphNode node = new SolGraphNode();
		while(iter.hasNext()){
			Entry<Integer, List<IndexElement>> entry = iter.next();
			int otherkid = entry.getKey();
			List<IndexElement> indexSet = entry.getValue();
			ListIterator<IndexElement> listIter = indexSet.listIterator();
			node.addIterWithkeyword(otherkid, listIter);
		}
		
		PriorityQueue<SolGraphNode> queue = 
                new PriorityQueue<SolGraphNode>(10, new SolGNodeComparator());
		queue.add(node);
		
		while(!queue.isEmpty() && topK>0){
			SolGraphNode popNode = queue.poll();
			//////System.out.println("node11111:"+popNode.returnNode());
			HashMap<Integer, ListIterator<IndexElement>> solGNodeMap = popNode.getSolNode();
			//////System.out.println("size:"+solGNodeMap.size());
			SolutionClass solution = new SolutionClass();
			solution.addSolution(kid, index);
			//solution.setVid(this.getVertexId());
			double sum = index.getLength();
			Iterator<Entry<Integer, ListIterator<IndexElement>>> iterNodeMap = solGNodeMap.entrySet().iterator();
			while(iterNodeMap.hasNext()){
				Entry<Integer, ListIterator<IndexElement>> entry = iterNodeMap.next();
				ListIterator<IndexElement> listIter = entry.getValue();
				IndexElement newIndex = listIter.next();
				//////System.out.println("ssss "+newIndex.getElement());
				solution.addSolution(entry.getKey(), newIndex);
				sum += newIndex.getLength();
				listIter.previous();
			}
			solution.setSum(sum);
			if(solutionList.addSolution(solution)){
				add ++;
				topK--;
			}
			//add new element
			iterNodeMap = solGNodeMap.entrySet().iterator();
			while(iterNodeMap.hasNext()){

				Entry<Integer, ListIterator<IndexElement>> entry = iterNodeMap.next();
				int newkid = entry.getKey();
				if(newkid == kid)
					continue;
				ListIterator<IndexElement> listIter = entry.getValue();
				IndexElement indexE = listIter.next();
				if(listIter.hasNext()){
					IndexElement nextIndex = listIter.next();
					////System.out.println("|||||||| "+indexE.getElement()+" newIndex:"+nextIndex.getElement());
					double diff = nextIndex.getLength()-indexE.getLength();
					SolGraphNode newNode = new SolGraphNode();
					newNode.setSum(diff);
					listIter.previous();
					listIter.previous();
					newNode.cloneGNodeMap(popNode);
					newNode.moveOneMore(entry.getKey());
					queue.add(newNode);
					////System.out.println("size:"+queue.size());
				}
				else{
					listIter.previous();
				}
			}
		}
		return add;
	}*/
	
	/*public String showSolutionStr(){
		String retStr = "vid:"+vid;
		if(pathMap==null){
			retStr += " NULL";
			return retStr;
		}
		Iterator<Entry<Integer, List<IndexElement>>> iter = pathMap.entrySet().iterator();
		while(iter.hasNext()){
			Entry<Integer, List<IndexElement>> entry = iter.next();
			int kid = entry.getKey();
			List<IndexElement> indexSet = entry.getValue();
			Iterator<IndexElement> iterList = indexSet.iterator();
			retStr += " "+kid+":";
			while(iterList.hasNext()){
				IndexElement index = iterList.next();
				retStr += index.getElement()+"-";
			}
			retStr += " ";
		}
		return retStr;
	}*/
	
	public String returnSolutionStr(){
		if(pathMap==null){
			return null;
		}
		String retStr = " "+vid;
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
	//end of pathMap part
	////////////////////////////////////////////////////////////////////////////////////////////////////////
}
