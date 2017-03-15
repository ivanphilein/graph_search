package oneIteMR;

import java.io.BufferedReader;
import java.io.DataInputStream;
import java.io.FileInputStream;
import java.io.IOException;
import java.io.InputStreamReader;
import java.util.HashMap;
import java.util.HashSet;
import java.util.Iterator;
import java.util.LinkedList;

public class CheckExist {
	
	public boolean checkEdges(int from, int to, double wei, int key, GraphOne graph) throws IOException{
		VertexOne toV = graph.getVertex(to);
		if(toV.getDistance(key)!=0){
			return false;
		}
		HashSet<VertexOne> verSet =  new HashSet<VertexOne>();//toV.getInComingSet();
		LinkedList<VertexOne> list = new LinkedList<VertexOne>();
		list.add(toV);
		toV.chanageState(0);
		while(!list.isEmpty()){
			VertexOne vertex = list.pollFirst();
			int dis = vertex.getState();
			HashSet<EdgeOne> edgeSet = vertex.getInComingSet();
			if(edgeSet != null){
				Iterator<EdgeOne> iter = edgeSet.iterator();
				while(iter.hasNext()){
					EdgeOne edge = iter.next();
					VertexOne newV = graph.getVertex(edge.getVFrom());
					int newDis = (int)(dis+edge.getWeight());
					newV.chanageState(newDis);
					if(newDis==wei){
						verSet.add(newV);
					}
					if(newDis < wei){
						list.add(newV);
					}
				}
			}
		}
		Iterator<VertexOne> iterV = verSet.iterator();
		while(iterV.hasNext()){
			VertexOne ver = iterV.next();
			if(ver.getVertexID()==from){
				return true;
			}
		}
		return false;
	}
	
	public static void main(String[] args) throws Exception {
    	BackExpand bdsearch = new BackExpand();
    	String nodefile = args[0];
    	String query = args[1];
  		HashSet<Integer> querySet = bdsearch.getQueryList(query);
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
	    CheckExist checkClass = new CheckExist();
	    int from = Integer.parseInt(args[2]);
	    int to = Integer.parseInt(args[3]);
	    double wei = Double.parseDouble(args[4]);
	    int key = Integer.parseInt(args[5]);
	    System.out.println(checkClass.checkEdges(from, to, wei, key, graph));
	}

}
