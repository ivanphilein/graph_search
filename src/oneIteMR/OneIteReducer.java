package oneIteMR;

import java.io.IOException;
import java.util.HashMap;
import java.util.HashSet;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.io.DoubleWritable;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.io.Writable;
import org.apache.hadoop.mapreduce.Reducer;

public class OneIteReducer extends Reducer<IntWritable,DouArrayWritable,IntWritable,Text>{
	private final static double v = 0.0;
    //private final int BLACK = 2;//in the close list
	
	/**
	 * based on query string, return query list
	 * @return
	 */
	private HashSet<Integer> getQueryList(String queryStr){
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
	
	public void reduce(IntWritable key, Iterable<DouArrayWritable> values, Context context) throws InterruptedException {
		try {
			//System.out.println("Start Reading block:"+key.get());
			Configuration conf = context.getConfiguration();
	  		int dir = Integer.parseInt(conf.get("DIR"));
	  		String query = conf.get("QUERY");
	  		//context.write(new IntWritable(0), new Text("query_"+query+"_bidirMR.txt"));
	  		HashSet<Integer> querySet = getQueryList(query);
	  		int total = querySet.size();
	  		int topK = Integer.parseInt(conf.get("TOPK"));
	  		//used to store the found solutions vertexes
	  		//HashSet<VertexOne> topKVertex = new HashSet<VertexOne>();
	  		HashMap<Integer, HashSet<VertexOne>> kidToNodes = new  HashMap<Integer, HashSet<VertexOne>>();
			GraphOne graph = new GraphOne();
			for(DouArrayWritable val : values){
				//System.out.println("read="+(read++));
				Writable[] array = val.get();
				if(((DoubleWritable)array[0]).get()==v){
					/*for(int i = 0;i<array.length;i++){
						System.out.print(array[i]+" ");
					}*/
					//System.out.println("vertex");
					//boolean found = graph.writeKeyArray(array, querySet, kidToNodes);
					//int vid = (int)((DoubleWritable)array[1]).get();
					graph.writeKeyArray(array, querySet, kidToNodes);
					
					/*if(vid==1 || vid==2 || vid==1128042 || vid ==8 || vid==1314648){
						String testStr = "";
						for(int i=0;i<array.length;i++){
							testStr += array[i].toString()+" ";
						}
						context.write(key, new Text(testStr));
					}*/
					/*if(found==true){
						int vid = (int)((DoubleWritable)array[1]).get();
						VertexOne tempV = graph.getVertex(vid);
						tempV.chanageState(BLACK);
						topKVertex.add(tempV);
						if(topKVertex.size()==topK){
							run=false;
							break;
						}
					}*/
				}
				else{
					
		  	        if(array.length>=5){
		  	        	if(dir==0)
		  	        		graph.writeUnDirectededEdgeInfo(((int)((DoubleWritable)array[2]).get()), ((int)((DoubleWritable)array[3]).get()),((int)((DoubleWritable)array[4]).get()));
		  	        	else{
		  	        		graph.writeDirectedEdgeInfo(((int)((DoubleWritable)array[1]).get()),((int)((DoubleWritable)array[2]).get()), ((int)((DoubleWritable)array[3]).get()),((int)((DoubleWritable)array[4]).get()));
		  	        	}
		  	        }
				}
			}//finish reading all nodes and edges
			
			//System.out.println("Finish Reading block:"+key.get()+" "+kidToNodes.size()+" "+total);
			
	  		if(kidToNodes.size()>=total){
	  			/*BiDirSearch search = new BiDirSearch();
	  			search.initialInVertexSet(kidToNodes);
	  			search.runBiDirSearch(querySet, graph, topK);*/
	  			BackExpand search = new BackExpand();
	  			search.initialInVertexSet(kidToNodes);
	  			search.runBiDirSearch(querySet, graph, topK);
	  			
	  			//System.out.println(search.showResult());
	  			//if(search.haveSolution()){
		  			context.write(new IntWritable(0), new Text(search.showResult()));
	  			//}
				/*Iterator<Entry<Integer, HashSet<VertexOne>>> iterKMap = kidToNodes.entrySet().iterator();
				//for nodes have each keyword, grow, while 1
				int i=0;
				while(iterKMap.hasNext() && run){
					//System.out.println("i="+(i++));
					Entry<Integer, HashSet<VertexOne>> entry = iterKMap.next();
					int kid = entry.getKey();
					HashSet<VertexOne> nodeSet = entry.getValue();
					System.out.print(kid+":");
					HashSet<VertexOne> newNodeSet = new HashSet<VertexOne>();
					Iterator<VertexOne> iterNode = nodeSet.iterator();
					//for one keyword, grow based on the node set, while 2
					int j=0;
					while(iterNode.hasNext()){
						//System.out.println("j="+(j++));
						VertexOne vertex = iterNode.next();
						System.out.print(" "+vertex.getVertexID());
						if(vertex.getState()!=BLACK){
							//int vid = vertex.getVertexID();
							HashSet<EdgeOne> edgeSet = vertex.getInComingSet();
							if(edgeSet!=null){
								Iterator<EdgeOne> iterEdge = edgeSet.iterator();
								while(iterEdge.hasNext()){
									EdgeOne edge = iterEdge.next();
									int newVid = edge.getVFrom();
									VertexOne newVertex = graph.getVertex(newVid);
									int size = newVertex.addToResultMap(kid, vertex, edge.getWeight());
									if(size==total){
										newVertex.chanageState(BLACK);
										topKVertex.add(newVertex);
										if(topKVertex.size()==topK){
											run=false;
											break;
										}
									}
									else{
										newNodeSet.add(newVertex);
									}
								}
							}
						}
					}//end of while 2
					//System.out.println("");
					if(!newNodeSet.isEmpty()){
						kidToNodes.put(kid, newNodeSet);
					}
					nodeSet=null;
				}//end of while 1
				
				Iterator<VertexOne> iter = topKVertex.iterator();
				while(iter.hasNext()){
					context.write(new IntWritable(0), new Text(iter.next().showResultMap()));
				}*/
	  		}
			
		} catch (IOException e) {
			// TODO Auto-generated catch block
			e.printStackTrace();
		}
	}
	
}
