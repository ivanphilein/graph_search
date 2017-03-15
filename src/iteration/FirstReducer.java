package iteration;

import graphSearch.FinalSolutions;
import graphSearch.GraphClass;
import graphSearch.SearchUndirected;
import graphSearch.SolutionClass;
import graphSearch.Vertex;

import java.io.IOException;
import java.util.HashMap;
import java.util.HashSet;
import java.util.Iterator;
import java.util.List;
import java.util.Map.Entry;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Reducer;

public class FirstReducer extends Reducer<IntWritable,Text,IntWritable,Text>{
	private final static String vertex = "vertex";
	private final static String original = "original";
	private final static String portal = "portal";
	private final static String delimiter = " ";
	private final static int solReducer = -2;
    //private final int BLACK = 2;//in the close list
	
	/**
	 * based on query string, return query list
	 * @return
	 */
	private HashSet<Integer> getQueryList(String queryStr){
		if(queryStr==null || queryStr.isEmpty()){
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
	
	public void reduce(IntWritable key, Iterable<Text> values, Context context) throws InterruptedException, IOException {
		
		int bid = key.get();
		//System.out.println("Start Reading block:"+key.get());
		Configuration conf = context.getConfiguration();
		String query = conf.get("QUERY");
		HashSet<Integer> querySet = getQueryList(query);
		int topK = Integer.parseInt(conf.get("TOPK"));
		GraphClass graphClass = new GraphClass();
		HashSet<String> edgeSet = new HashSet<String>();
		HashSet<Integer> portalSet = new HashSet<Integer>(); 
		for(Text val : values){
			String strLine = val.toString();
			String[] temp = strLine.split(delimiter);
			if(temp[0].equals(vertex)){
				HashSet<Integer> keySet = new HashSet<Integer>();
				int vid = Integer.parseInt(temp[1]);
				double weight = Double.parseDouble(temp[2]);
				for(int i=3;i<temp.length;i++){
					keySet.add(Integer.parseInt(temp[i]));
				}
				graphClass.addGraphVertex(vid, weight, keySet);
			}
			else if(temp[0].equals(portal)){
				HashSet<Integer> keySet = new HashSet<Integer>();
				int portalid = Integer.parseInt(temp[1]);
				int vid = Integer.parseInt(temp[2]);
				double weight = Double.parseDouble(temp[3]);
				for(int i=4;i<temp.length;i++){
					keySet.add(Integer.parseInt(temp[i]));
				}
				graphClass.addGraphVertexWithPortal(vid, portalid, weight, keySet);
				portalSet.add(vid);
			}
			else{
				if(temp.length>=4){
					edgeSet.add(strLine);
				}
			}
		}//finish reading all nodes and edges
		Iterator<String> iterEdge = edgeSet.iterator();
		while(iterEdge.hasNext()){
			String edgeStr = iterEdge.next();
			String[] temp = edgeStr.split(delimiter);
			int vfrom = Integer.parseInt(temp[1]);
			int vto = Integer.parseInt(temp[2]);
			double weight = Double.parseDouble(temp[3]);
			graphClass.addGrageEdge(vfrom, vto, weight);
		}
		edgeSet = null;
		SearchUndirected search = new SearchUndirected(graphClass);
		FinalSolutions solution = search.runBestFirstSearchWithPortal(querySet, topK);
		List<SolutionClass> topKSolList = solution.getTopKSol();
		if(topKSolList!=null){
			Iterator<SolutionClass> iterList = topKSolList.iterator();
			while(iterList.hasNext()){
				String solStr = iterList.next().getSolution();
				context.write(new IntWritable(solReducer), new Text(solStr));
			}
		}
		
		Iterator<Integer> portalIter = portalSet.iterator();
		while(portalIter.hasNext()){
			int porId = portalIter.next();
			Vertex porVer = graphClass.getVertexFromVid(porId);
			int porBlock = porVer.getPortalBlock();
			String candidateStr = "";
			if(porBlock == bid){
				candidateStr += original + porVer.returnSolutionStr();
			}
			else{
				candidateStr += vertex + porVer.returnSolutionStr();
			}
			if(candidateStr != null){
				context.write(new IntWritable(porBlock), new Text(candidateStr));
				solution.addToTargetMap(porId, porVer.getPortalBlock());
			}
		}
		context.write(key, new Text(solution.returnTargetMap()));
		if(solution.getPortalDisMap()!=null){
			Iterator<Entry<Integer, HashMap<Integer, Double>>> iterDis = solution.getPortalDisMap().entrySet().iterator();
			while(iterDis.hasNext()){
				int vid = iterDis.next().getKey();
				context.write(key, new Text(solution.retPDisMapFromVidStr(vid)));
			}
		}
	}
	
}
