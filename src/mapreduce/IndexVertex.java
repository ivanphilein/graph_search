package mapreduce;

import java.util.HashMap;
import java.util.HashSet;
import java.util.Iterator;
import java.util.Map.Entry;
import java.util.Stack;
import java.util.TreeSet;

import shared.IndexClass;
import shared.IndexElement;

public class IndexVertex {
	private HashMap<Integer, Object> id2Vertex = new HashMap<Integer, Object> (); // Object is VertexInfo vertex id to vertex map
	//private HashMap<Integer, TreeSet<IndexElement> > vertex2Ance = null; // L_PN
	private HashSet<Integer> resultSet = null;//result set used to store all the vertex in this iteration which can generate a whole solution, element in this set is vertex id

	private HashMap<Double,TreeSet<SetElement>> pathMap = null; //From the root vertex id, to the solutions set contains all the pathes
	/**
	 * Return the path map
	 * @return HashMap<Double,TreeSet<SetElement>>
	 */
	public HashMap<Double,TreeSet<SetElement>> getPathMap(){
		return pathMap;
	}
	/**
	 * Based on the path map, return a string which contains all the vertexes on that path, used for final reducer
	 */
	public String getPathMapStr(){
		////System.out.println("function start");
		if(pathMap==null){
			return null;
		}
		String retStr = "";
		Iterator<Entry<Double, TreeSet<SetElement>>> iter = pathMap.entrySet().iterator();
		////System.out.println("SIZE: "+pathMap.size());
		HashSet<Integer> storePath = new HashSet<Integer>();
		while(iter.hasNext()){
			Entry<Double, TreeSet<SetElement>> entry = iter.next();

			////System.out.println("2");
			TreeSet<SetElement> pathSet = entry.getValue();
			if(pathSet!=null){
				Iterator<SetElement> iterSet = pathSet.iterator();
				if(iterSet.hasNext()){
					SetElement temp = iterSet.next();
					storePath.add(temp.getEdgeFrom());
					storePath.add(temp.getEdgeTo());
				}
				while(iterSet.hasNext()){
					SetElement temp = iterSet.next();
					storePath.add(temp.getEdgeTo());
				}
			}
		}
		Iterator<Integer> iterSet = storePath.iterator();
		retStr += "sum: "+(storePath.size()-1);
		if(iterSet.hasNext()){
			retStr += " "+iterSet.next();
		}
		while(iterSet.hasNext()){
			retStr += "-"+iterSet.next();
		}
		/*while(iter.hasNext()){
			Entry<Double, TreeSet<SetElement>> entry = iter.next();
			retStr += " sum:"+entry.getKey()+" ";

			////System.out.println("2");
			TreeSet<SetElement> pathSet = entry.getValue();
			if(pathSet!=null){
				Iterator<SetElement> iterSet = pathSet.iterator();
				if(iterSet.hasNext()){
					SetElement temp = iterSet.next();
					if(temp.getEdgeFrom()==temp.getEdgeTo())
						retStr += temp.getEdgeFrom();
					else{
						retStr += temp.getEdgeFrom()+"-"+temp.getEdgeTo();
					}
				}
				while(iterSet.hasNext()){
					retStr += "-"+iterSet.next().getEdgeTo();
				}
			}
		}*/
		return retStr;
	}
	
	private int numOfSolution = -1;
	public int total = -1;

	//public HashSet<Integer> 
	
	/**
	 * This function is used on the iteration mapreduce part, just include some repeat edges
	 * @param vertex
	 */
	public void updateSumRMap(VertexInfo vertex){
		HashMap<Integer, TreeSet<IndexElement>> resultMap = vertex.getResultMap();
		Iterator<Entry<Integer, TreeSet<IndexElement>>> iter = resultMap.entrySet().iterator();
		int sum = 0;
		int total = 0;
		//HashMap<Integer, Double> tempMap = new HashMap<Integer, Double>();
		while(iter.hasNext()){
			Entry<Integer, TreeSet<IndexElement>> entry = iter.next();
			//int kid = entry.getKey();
			TreeSet<IndexElement> pqueue = entry.getValue();
			if(!pqueue.isEmpty()){
				if(total <= 0){
					total = pqueue.size();
				}
				else{
					total = total*pqueue.size();
				}
				sum += pqueue.first().getLength();
			}
		}
		total=1;
		//System.out.println("UUUUUUUUUUUUUUUUUPDATE THRESHOLD "+sum+" Total number:"+total);
		updateThreshold(sum);
		addNumOfSolution(total);
	}
	/**
	 * Based on the resultmap, calculate the real cost of the solution, should not run if resultmap is null, here we just use all weight as 1(need to change stack if weight is not same)
	 * @param vid vertex id
	 * @param indexC LKN index class
	 */
	public boolean updateSumRMapFinal(int vid, IndexClass indexC){
		// THIS PART IS USDED FOR THE REAL DISTANCE PART, NOT FINISH YET
		if(pathMap == null)
			pathMap = new HashMap<Double,TreeSet<SetElement>>();//this set is used to store all the edges in the path, one edge store only once
		//create the path set used to store the information of all pathes of that vertex id
		TreeSet<SetElement> pathSet = new TreeSet<SetElement>(new CompareSetElement());
		VertexInfo vertex = this.getVertex(vid);
		HashMap<Integer, TreeSet<IndexElement>> resultMap = vertex.getResultMap();
		////System.out.println("RM:"+vertex.getAllResultMap());
		Iterator<Entry<Integer, TreeSet<IndexElement>>> iter = resultMap.entrySet().iterator();
		Stack<Integer> storeStack = new Stack<Integer>();//used to store all the information of next vertex id
		double sum = 0;
		while(iter.hasNext()){
			Entry<Integer, TreeSet<IndexElement>> entry = iter.next();
			int kid = entry.getKey();
			TreeSet<IndexElement> pqueue = entry.getValue();
			if(!pqueue.isEmpty()){
				//Giving an index(0.0, S, N, E), generate the 
				IndexElement firstElement = pqueue.pollFirst();
				int start = firstElement.getStartVertex();
				int next = firstElement.getNextVertex();
				int end = firstElement.getEndVertex();
				SetElement edgeElement = new SetElement(start, next, firstElement.getLength());
				pathSet.add(edgeElement);
				sum += firstElement.getLength();
				storeStack.push(next);
				while(!storeStack.isEmpty()){
					int pop = storeStack.pop();
					if(pop == end){
						break;
					}
					//indexC.g
					VertexInfo nextVertex = this.getVertex(pop);
					if(nextVertex == null)
						return false;
					TreeSet<IndexElement> nextpqueue = nextVertex.getResultMap().get(kid);
					Iterator<IndexElement> iterElement = nextpqueue.iterator();
					while(iterElement.hasNext()){
						IndexElement tempElement = iterElement.next();
						if(tempElement.getEndVertex() == end){
							int addVid = tempElement.getNextVertex();
							storeStack.push(addVid);
							edgeElement = new SetElement(tempElement.getStartVertex(), tempElement.getNextVertex(), tempElement.getLength());
							pathSet.add(edgeElement);
						}
					}
				}
			}
		}
		pathMap.put(sum, pathSet);
		//////System.out.println("UUUUUUUUUUUUUUUUUPDATE THRESHOLD "+sum);
		//updateThreshold(sum);
		addNumOfSolution(1);
		return true;
	}
	
	
	//numOfSolution part
	/**
	 * get the value of numOfSolution
	 * @return
	 */
	public int getNumOfSolution(){
		return numOfSolution;
	}
	/**
	 * set numOfSolution to value num
	 * @param num
	 */
	public void setNumOfSolution(int num){
		numOfSolution = num;
	}
	/**
	 * add value add to numOfSolution
	 * @param add
	 */
	public void addNumOfSolution(int add){
		numOfSolution += add;
	}
	//end of numOfSolution part
	
	
	private double threshold = -1;
	//threshold part
	public double getThreshold(){
		return threshold;
	}
	
	public void setThreshold(double set){
		threshold = set;
	}
	
	public void updateThreshold(double update){
		if(threshold < update || threshold<0){
			threshold = update;
		}
	}
	/**
	 * based on one vertex information, update the threshold
	 * @param vertex
	 * @param threshold
	 */
	public void updateThreshold(VertexInfo vertex, double newThres){
		threshold = newThres;
	}
	//end of threshold part
	//resultSet part
	
	public void setResultSetNull(){
		resultSet = null;
	}
	/**
	 * Add vertex id to result set
	 * @param vid
	 */
	public void addToResultSet(int vid){
		if(resultSet == null){
			resultSet = new HashSet<Integer>();
		}
		resultSet.add(vid);
	}
	
	public HashSet<Integer> getResultSet(){
		return resultSet;
	}
	/**
	 * output the result map as string format, used for next iteration
	 * @return
	 */
	public String writeableResultSet(){
		if(resultSet==null){
			return null;
		}
		Iterator<Integer> iter = resultSet.iterator();
		String retStr = "";
		if(iter.hasNext()){
			int vid = iter.next();
			retStr += " vid:"+vid+" ";
			VertexInfo vertex = this.getVertex(vid);
			retStr += vertex.getAllResultMap();
			this.deleteVertex(vid);
		}
		while(iter.hasNext()){
			int vid = iter.next();
			VertexInfo vertex = this.getVertex(vid);
			retStr += " vid:"+vid+" "+vertex.getAllResultMap();
			this.deleteVertex(vid);
		}
		return retStr;
	}
	
	
	public String showResultSet(){
		if(resultSet==null){
			return null;
		}
		Iterator<Integer> iter = resultSet.iterator();
		String retStr = "";
		while(iter.hasNext()){
			int vid = iter.next();
			retStr += " "+vid;
		}
		return retStr;
	}
	//end of resultSet part
	
	
	//id2Vertex part
	public HashMap<Integer, Object> getId2Vertex(){
		return id2Vertex;
	}
	/**
	 * check whether id2Vertex contains key vid or not
	 * @param vid
	 * @return
	 */
	public boolean id2VertexContains(int vid){
		if(id2Vertex == null)
			return false;
		else
			return id2Vertex.containsKey(vid);
	}
	
	/**
	 * Add a new vertex to the id2Vertex map
	 * @param vid
	 * @param newVertex
	 */
	public void addVertex(int vid, VertexInfo newVertex){
		/*if(id2Vertex == null)
			id2Vertex = new HashMap<Integer, Object> ();*/
		id2Vertex.put(vid, newVertex);
	}
	
	/**
	 * Get vertex class based on vid
	 * @param vid
	 * @return vertex class
	 */
	public VertexInfo getVertex(int vid){
		/*if(id2Vertex == null)
			return null;*/
		return (VertexInfo)id2Vertex.get(vid);
	}
	
	
	/**
	 * Delete vertex from map based on vid
	 * @param vid
	 */
	public void deleteVertex(int vid){
		if(id2Vertex != null)
			id2Vertex.remove(vid);
	}
	
	//end of id2Vertex part
	//v2ResultMap part
	
	/**
	 * The Write result map function for iteration part
	 * @param vid
	 * @param inputStr
	 * @param total
	 * @return true if some nodes update
	 */
	public boolean writeV2ResultMapForIteration(String inputStr){
		////////System.out.println("result "+inputStr);
		boolean retBool = false;
		String[] temp;
		String delimiter = " ";
		temp = inputStr.split(delimiter);
		int vid = Integer.parseInt(temp[0]);
		String[] secondTemp;
		for(int i=1;i<temp.length;i=i+2){
			int kid = Integer.parseInt(temp[i]);
			delimiter = "-";
			secondTemp = temp[i+1].toString().split(delimiter);
			for(int j=0;j<secondTemp.length;j++){
				IndexElement putElement = new IndexElement(secondTemp[j]);
				if(retBool == false)
					retBool = writeV2ResultMap(vid, kid,putElement, false);
				else
					writeV2ResultMap(vid, kid,putElement, false);
			}
			
		}
		
		return retBool;
	}
	
	public void writeLKNForFinal(String inputStr){
		String[] temp;
		String delimiter = " ";
		temp=inputStr.split(delimiter);
		IndexElement putElement = null;
		int sum = 0;
		if(temp.length>=2){
			int keyid = (Integer.parseInt(temp[0]));
			//VertexInfo vertex = null;
			for(int i=1;i<temp.length;i++){
				putElement = new IndexElement(temp[i]);
				sum += putElement.getLength();
				if(this.getThreshold()>0 && sum>this.getThreshold())
					break;
				if(this.getNumOfSolution()>=total)
					break;
				this.writeV2ResultMap(putElement.getStartVertex(), keyid, putElement, true);
			}
		}
	}
	
	/**
	 * Write vertex id to result map in the first map based on query
	 * @param temp
	 * return true if update else false
	 */
	public boolean writeV2ResultMapForInitial(String inputStr){
		boolean retBool = false;
		String[] temp;
		String delimiter = " ";
		temp=inputStr.split(delimiter);
		IndexElement putElement = null;
		int sum = 0;
		if(temp.length>=2){
			int keyid = (Integer.parseInt(temp[0]));
			//VertexInfo vertex = null;
			for(int i=1;i<temp.length;i++){
				putElement = new IndexElement(temp[i]);
				sum += putElement.getLength();
				if(this.getThreshold()>0 && sum>this.getThreshold())
					break;
				if(this.getNumOfSolution()>=total)
					break;
				retBool = this.writeV2ResultMap(putElement.getStartVertex(), keyid, putElement, true);
			}
			
		}
		return retBool;
	}
	/**
	 * Add element to vertex result map, used at final reducer
	 * @param vid
	 * @param kid
	 * @param putElement
	 */
	public void writeSolution(int vid, int kid, IndexElement putElement){
		VertexInfo tempVertex = (VertexInfo)id2Vertex.get(vid);
		//
		if(tempVertex != null){
			//////System.out.println("NOT NULL");
			tempVertex.addToResultMap(kid, putElement, total);
		}
		else{
			//////System.out.println("NULL");
			tempVertex = new VertexInfo(vid);
			tempVertex.addToResultMap(kid, putElement, total);
			addVertex(vid, tempVertex);
		}
	}
	
	/**
	 * write element to map
	 * @param vid vertex id
	 * @param kid keyword id
	 * @param putElement indexElement
	 * @param oneblock only update the LPN if oneblock is false, as if that is in one block, the LKN must contains, so do not need to update
	 * return true if update else false
	 */
	public boolean writeV2ResultMap(int vid, int kid, IndexElement putElement, boolean oneblock){
		System.out.println("write RM: vid:"+vid+" kid: "+kid+ "elelment: "+putElement.getElement());
		VertexInfo tempVertex = (VertexInfo)id2Vertex.get(vid);
		//
		boolean retBool = false;
		boolean found = false;
		if(tempVertex != null){
			//////System.out.println("NOT NULL");
			found = tempVertex.addToResultMap(kid, putElement, total);
			retBool = true;
		}
		else{
			//////System.out.println("NULL");
			tempVertex = new VertexInfo(vid);
			found = tempVertex.addToResultMap(kid, putElement, total);
			addVertex(vid, tempVertex);
		}
		if(found){
			System.out.println("FOOOOOOOOOOOOOOOUND "+vid);
			this.addToResultSet(vid);
			this.updateSumRMap(tempVertex);
		}
		if(!oneblock){
			TreeSet<IndexElement> retSet = tempVertex.getAnceSet();
			updateAnceVertex(tempVertex, retSet);
			/*if(retSet != null){
				//////System.out.println("start of write function!!!!!!!!!! "+retSet.size()+"::::"+putElement.getElement());
				Iterator<IndexElement> iterSet = retSet.iterator();
				while(iterSet.hasNext()){
					IndexElement tempElement  =  iterSet.next();
					if(tempElement==null){
						continue;
					}
					int anceID = tempElement.getStartVertex();
					if(anceID==vid)
						continue;
					double length = putElement.getLength()+tempElement.getLength();
					VertexInfo anceVertex = getVertex(anceID);
					if(anceVertex==null){
						anceVertex = new VertexInfo(anceID);
					}
					////////System.out.println("putElement:"+putElement.getElement());
					////////System.out.println("tempElement:"+tempElement.getElement());
					IndexElement newElement = new IndexElement();
					newElement.setStartVertex(tempElement.getStartVertex());
					newElement.setLength(length);
					newElement.setNextVertex(tempElement.getEndVertex());
					newElement.setEndVertex(putElement.getEndVertex());
					////////System.out.println("tempElement:"+tempElement.getElement()+" kid:"+kid);
					writeV2ResultMap(tempElement.getStartVertex(),kid, newElement, total, false);
					//retSet.remove(retSet.first());
				}
				////////System.out.println("end of write function!!!!!!!!!!");
				return true;
			}*/
		}
		return retBool;
	}
	
	/**
	 * Update the result map, return true if there is a whole solution found
	 * @param vid
	 * @param kid
	 * @param putElement
	 * @param queryList
	 * @return
	 */
	/*public boolean writeV2ResultMap(int vid, int kid, IndexElement putElement, HashSet<Integer> queryList){
		////////System.out.println("write RM: vid:"+vid+" kid: "+kid+ "elelment: "+putElement.getElement());
		VertexInfo tempVertex = (VertexInfo)id2Vertex.get(vid);
		int total = queryList.size();
		if(tempVertex == null){
			tempVertex = new VertexInfo(vid);
			addVertex(vid, tempVertex);
		}
		return tempVertex.addToResultMap(kid, putElement,total);
	}
	*/
	/**
	 * based on each input string, store vertex and its Ancestors information
	 * @param temp string array, the array contains all the element if ance list
	 */
	public void writeVertex2Ance(String[] temp, int bid){
		if(temp.length>1){
			int vid = Integer.parseInt(temp[0].toString());
			////////System.out.print("write ance set of "+vid+" ");
			IndexElement putElement = null;
			VertexInfo tempVertex = getVertex(vid);
			if(tempVertex==null){
				tempVertex = new VertexInfo(vid);
				addVertex(vid,tempVertex);
			}
			for(int i=1;i<temp.length;i++){
				////////System.out.println("temp[i] "+temp[i]);
				putElement = new IndexElement(temp[i].toString());
				putElement.showElement();
				tempVertex.writeAnceSet(putElement);
				//update ance vertex sending list part
				int anceVid = putElement.getStartVertex();
				VertexInfo anceVertex = this.getVertex(anceVid);
				if(anceVertex==null){
					anceVertex = new VertexInfo(anceVid);
					this.addVertex(putElement.getStartVertex(), anceVertex);
				}
				anceVertex.addToSendingList(bid);
				//end of updating ance vertex sending list part
			}
		}
	}
	/**
	 * based on each input string, store vertex and its Ancestors information
	 * @param vertexStr input string
	 */
	public void writeVertex2Ance(int vid, String vertexStr){
		////////System.out.println("ance "+vertexStr);
		String[] elementSet;
		String delimiter = " ";
		elementSet = vertexStr.toString().split(delimiter);
		IndexElement putElement = null;
		for(int i=0;i<elementSet.length;i++){
			putElement = new IndexElement(elementSet[i].toString());
			VertexInfo tempVertex = getVertex(vid);
			if(tempVertex==null){
				tempVertex = new VertexInfo(vid);
			}
			tempVertex.writeAnceSet(putElement);
			addVertex(vid,tempVertex);
		}
		
	}
	
	
	//update the ance vertexes part
	/**
	 * Based on one vertex which needs to update the ance vertexes, do the update
	 * @param vertex
	 * @param anceSet
	 * @return true if update
	 */
	public boolean updateAnceVertex(VertexInfo vertex, TreeSet<IndexElement> anceSet){
		//////System.out.println("Vertex "+vertex.getVertexId()+" ance "+vertex.getAllAnceList());
		//////System.out.println("RESULTTTTTTTTTTT  "+vertex.getAllResultMap());
		//TreeSet<IndexElement> updateAnceSet = null;
		boolean retBool = false;
		if(anceSet!=null){
			HashMap<Integer, TreeSet<IndexElement>> rmMap = vertex.getResultMap();
			int vid = vertex.getVertexId();
			Iterator<IndexElement> iterAnce = anceSet.iterator();
			while(iterAnce.hasNext()){
				IndexElement anceElement = iterAnce.next();
				int anceVid = anceElement.getStartVertex();
				if(anceVid != vid){
					int num = 0;
					VertexInfo anceVertex = getVertex(anceVid);
					if(anceVertex==null){
						anceVertex = new VertexInfo(anceVid);
						addVertex(anceVid, anceVertex);
					}
					Iterator<Entry<Integer, TreeSet<IndexElement>>> iterRM = rmMap.entrySet().iterator();
					while(iterRM.hasNext()){
						Entry<Integer, TreeSet<IndexElement>> entry = iterRM.next();
						//int kid = entry.getKey();
						TreeSet<IndexElement> pQueue = entry.getValue();
						if(!pQueue.isEmpty()){
							num++;
							Iterator<IndexElement> iterQueue = pQueue.iterator();
							while(iterQueue.hasNext()){
								IndexElement element = iterQueue.next();
								IndexElement newElement = new IndexElement(element.getLength()+anceElement.getLength(), anceElement.getStartVertex(), anceElement.getNextVertex(), element.getEndVertex());
								retBool = anceVertex.addToResultMap(entry.getKey(), newElement);
							}
						}
					}
					//////System.out.println("number "+num+" total "+total);
					if(num==total){
						//////System.out.println("FOOOOOOOOOOOOOOOUND "+anceVertex.getVertexId());
						this.addToResultSet(anceVertex.getVertexId());
						this.updateSumRMap(anceVertex);
					}
					//System.out.println("ANCE Vertex "+anceVertex.getVertexId()+" ance ResultMap "+anceVertex.getAllResultMap());
					/*anceVertex.setUpdate(true);
					if(updateAnceSet!=null){
						updateAnceVertex(anceVertex ,updateAnceSet);
					}*/
				}
			}
		}
		return true;
	}
	//end of updating the ance vertexes part

}
