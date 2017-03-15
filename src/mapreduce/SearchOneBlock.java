package mapreduce;

import java.io.IOException;
import java.util.HashMap;
import java.util.HashSet;
import java.util.Iterator;
import java.util.LinkedList;
import java.util.List;
import java.util.Queue;

import graphIndex.EdgeClass;
import graphIndex.KSearchGraph;
//import graphIndex.KSearchGraph;
import graphIndex.VertexClass;

public class SearchOneBlock {
	//vertex information in this block, could be move to other classes
	private static HashMap<Integer, VertexClass> intToVertex = null;
	private static HashMap<Integer, EdgeClass> intToEdge = null;//from edge id to edge
	private Queue<QueueElement> resutlQueue = new LinkedList<QueueElement>();
	
	//private String Query = null;
	//A queue of visited nodes, queueElement contains vid and a map, each node has a map from keyword id to a list with path, the size of map should be the total keywords in query
	private Queue<QueueElement> searchQueue = null;
	
	//
	public SearchOneBlock(KSearchGraph graph){
		intToVertex = graph.getVertexMap();
		intToEdge = graph.getEidToEdge();
	}
	
	//searchQueue part
	/**
	 * Add QueueElement to searchQueue
	 * @param element
	 */
	public void addToQueue(QueueElement element){
		if(searchQueue==null){
			searchQueue = new LinkedList<QueueElement>();
		}
		searchQueue.add(element);
	}
	//end of searchQueue part
	
	//search function part
	/**
	 * search query based on on graph/block
	 * @param queryList
	 * @param topK
	 * @return
	 * @throws IOException
	 */
	public boolean searchInBlock(HashSet<Integer> queryList, int topK) throws IOException{
		if(searchQueue==null || intToVertex==null){
			System.out.println("Initialization error or query is empty");
			return false;
		}
		int totalQuery = queryList.size();
		int tempVid = 0;
		VertexClass tempVertex = null;
		List<Integer> keyList = null;
		List<EdgeClass> adjList = null;//only contains the edge from this node
		QueueElement element = null;
		while(!searchQueue.isEmpty() && topK>0){
			element = searchQueue.poll();
			//if find a solution, then store that, do not consider its adj list anymore
			if(element.getKeyMap().size() == totalQuery){
				System.out.println("find solution");
				resutlQueue.add(element);
				topK--;
				continue;
			}
			tempVid = element.getVertexID();
			tempVertex = intToVertex.get(tempVid);
			if(tempVertex==null){
				/*System.out.println("Vertex found error!");
				return false;*/
				continue;//can be improved here
			}
			adjList = tempVertex.getAdjEdgeList();
			Iterator<EdgeClass> iterAdj = adjList.iterator();
			EdgeClass adjEdge = null;
			int adjId = 0;
			double adjWeight = 0;
			while(iterAdj.hasNext()){
				adjEdge = iterAdj.next();
				adjId = adjEdge.getEdgeID();
				adjWeight = adjEdge.getWeight();
				QueueElement newElement = new QueueElement(adjId);
				boolean retBool = newElement.setKeyMap(element, adjWeight, keyList);
				if(retBool == false){
					newElement = null;
					continue;
				}
			}
			
		}
		return true;
	}
}
