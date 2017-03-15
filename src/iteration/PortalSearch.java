package iteration;

import graphSearch.GraphClass;
import graphSearch.PQComparator;
import graphSearch.PQElement;
import graphSearch.Vertex;

import java.io.BufferedWriter;
import java.io.File;
import java.io.FileWriter;
import java.io.IOException;
import java.util.ArrayList;
import java.util.Collections;
import java.util.HashSet;
import java.util.Iterator;
import java.util.List;
import java.util.PriorityQueue;
import java.util.Set;

import org.jgrapht.graph.AsWeightedGraph;
import org.jgrapht.graph.DefaultWeightedEdge;
import org.kohsuke.args4j.CmdLineException;
import org.kohsuke.args4j.CmdLineParser;

import shared.CmdOption;

public class PortalSearch 
{
	private GraphClass graphClass = new GraphClass();
	static HashSet<ResultElement> solution = new HashSet<ResultElement>(); // Solution stored

	public PortalSearch(String nodefile, String edgefile)
	{
		graphClass.readUnDirectedGraph(nodefile, edgefile, true);
	}

	public void PSearch(HashSet<Integer> querySet, HashSet<Integer> PortalNodes)
	{
		
		System.out.println("Q = "+querySet);
		System.out.println("---------------------");
		System.out.println("P = " + PortalNodes);
		
		AsWeightedGraph<Integer, DefaultWeightedEdge> graph = graphClass.getGraph();
		
		// Priority Node Iterator
		Iterator<Integer> iterProNode = PortalNodes.iterator();
		
		while(iterProNode.hasNext())
		{
			// Priority Queue
			PriorityQueue<PQElement> queue = new PriorityQueue<PQElement>(1000, new PQComparator());
			queue.removeAll(queue);
			// Visited Node and Edges
			HashSet<DefaultWeightedEdge> visitededge = new HashSet<DefaultWeightedEdge>();
			HashSet<Integer> visitednode = new HashSet<Integer>();
			
			visitededge.removeAll(visitededge);
			visitednode.removeAll(visitednode);
			
			int pnodeid = iterProNode.next(); //  get the first portal node
			System.out.println("****************************");
			System.out.println("Pnode = " + pnodeid);
			System.out.println("****************************");
			int Pnext=pnodeid; // The next node
	
			//initialize the queue
			PQElement element1 = new PQElement();
			element1.setVid(pnodeid);
			element1.setSource(pnodeid);
			element1.setPriority(1.0);
			//element1.setKeyword();// 22 for the test
			queue.add(element1);
	
			int firstverid=pnodeid;
			while(!queue.isEmpty())
			{
				PQElement element = queue.poll();
				int vid = element.getVertexID();
				double priority = element.getPriority();
				
				HashSet<Integer> keySet = new HashSet<Integer>();
				Vertex m = new Vertex();
				m = graphClass.getVertexFromVid(vid);
				keySet = m.getKeySet();			
	
				int source = element.getSource();
	
				// if the Portalnode is a solution
				if (!Collections.disjoint(querySet, keySet) && vid == pnodeid)
				{
					//System.out.println("****************");
					for (Iterator<Integer> matchkey = keySet.iterator(); matchkey.hasNext();)
					{
						int key1 = matchkey.next();
						querySet.contains(key1);
						ResultElement result = new ResultElement(key1,0.0, vid, vid, vid);
						solution.add(result);
						System.out.println("result = "+ result.getElement());
					}
		//			System.out.println("****************");
					
					visitednode.add(vid); // This node is visited
					firstverid=vid;
					//System.out.println("solution!!");
				}
	
				Set<DefaultWeightedEdge> edgeSet = graph.edgesOf(vid);
				Iterator<DefaultWeightedEdge> iterEdge = edgeSet.iterator();
				
	//			System.out.println("algo1");
				while(iterEdge.hasNext())
				{
	//				System.out.println("algo2");
					DefaultWeightedEdge edge = iterEdge.next();
					int newid = graph.getEdgeSource(edge);
					int tid = graph.getEdgeTarget(edge);
					
					double weight = graph.getEdgeWeight(edge);
					
					if (newid==pnodeid)
					{
							Pnext = tid;
					}
					
					//if(!visitednode.contains(tid))
					if(!visitededge.contains(edge)) // edges are visited
					{
			    		PQElement newElement = new PQElement();
			    		newElement.setVid(tid);
			    		//newElement.setNextID(element.getNextID());
			    		newElement.setPriority(priority+weight);
				   		newElement.setSource(source);
				   		//newElement.setKeyword(key);
				   		queue.add(newElement);
				   		
				   		Vertex m1 = new Vertex();
						m1 = graphClass.getVertexFromVid(tid);
						keySet = m1.getKeySet();	
				   		if (!Collections.disjoint(querySet, keySet))
						{
				   			//System.out.println("****************");
							for (Iterator<Integer> matchkey = keySet.iterator(); matchkey.hasNext();)
							{
								int key1 = matchkey.next();
								if (querySet.contains(key1))
								{
									ResultElement result = new ResultElement(key1,priority, firstverid, Pnext, tid);			
									solution.add(result);
									//System.out.println("solution!!");
									System.out.println("from "+newid+" -- to "+tid);
									System.out.println("result = "+ result.getElement());
								}
								
							}
							//System.out.println("****************");
						}
				   		visitednode.add(tid);
					}
					visitededge.add(edge); // This edge is visited
				}
			}
		}

	}

	public static void main(String[] args) throws IOException {
    	CmdOption option = new CmdOption();
		CmdLineParser parser = new CmdLineParser(option);
		//System.out.println("hi\n");
		//get command line parameters 
		try {
			parser.parseArgument(args);
			
		} catch (CmdLineException e) {
			System.out.println("Wrong\n");
		}
		
		//System.out.println("Step2");
		PortalSearch psearch = new PortalSearch(option.folder+option.nodefile, option.folder+option.edgefile);
		
		// Input for test
		HashSet<Integer> inp_q = new HashSet<Integer>(); // input query elements
		inp_q.add(1); // keyword 1
		inp_q.add(2); // keyword 2
		HashSet<Integer> inp_p = new HashSet<Integer>(); // input portal elements
		inp_p.add(2); // portal node 2
		inp_p.add(3); // portal node 3
		inp_p.add(7); // portal node 7

		// Call the function
		psearch.PSearch(inp_q, inp_p);
		//System.out.println("Step3");
		// Write the result
		
		File file = new File("Omar.txt");   
		FileWriter fw = new FileWriter(file.getAbsoluteFile());
		BufferedWriter bw = new BufferedWriter(fw);
		
		Iterator<ResultElement> iter =  solution.iterator();
		List<ResultElement> myList = new ArrayList<ResultElement>(); 
		List<ResultElement> myListend = new ArrayList<ResultElement>();
		
		while(iter.hasNext())
		{           
			int ch1=1;
			ResultElement temp = (ResultElement)iter.next();
			if (!myList.contains(temp))
			{
				for (ResultElement ch : myList)
				{
					if (temp.getkeyword()==ch.getkeyword() && temp.getStartVertex()==ch.getStartVertex() && 
							temp.getNextVertex()==ch.getNextVertex() && temp.getEndVertex()==ch.getEndVertex())
					{
						//myList.remove(ch);
						if (temp.getLength()>ch.getLength()) 
						{
							ch1 = 0;
							continue;
						}
						else {
							myListend.remove(ch);
						}
					}
				}
				if (ch1==1)
				{
					myList.add(temp);
					myListend.add(temp);
				}
			}
		}

		for (ResultElement ch : myListend)
		{
			bw.write("("+ ch.getElement() +")\n");
		}
		
		bw.close();
		//bw.flush();
		System.out.println("Done!!!");
    }

}