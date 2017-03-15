package graphSearch;

import java.util.HashSet;
import java.util.Iterator;
import java.util.Map.Entry;
import java.util.Set;
import java.util.TreeMap;

import org.jgrapht.graph.AsWeightedGraph;
import org.jgrapht.graph.DefaultWeightedEdge;
import org.kohsuke.args4j.CmdLineException;
import org.kohsuke.args4j.CmdLineParser;

import shared.CmdOption;

public class GenePath {
	public boolean getPath(GraphClass graphClass, int from, int to, double weight){
		AsWeightedGraph<Integer, DefaultWeightedEdge> graph = graphClass.getGraph();
		TreeMap<Double, HashSet<Integer>> disMap = new TreeMap<Double, HashSet<Integer>>();
		HashSet<Integer> checkedSet = new HashSet<Integer>();
		//List<Integer> pathList = new ArrayList<Integer>();
		HashSet<Integer> iniSet = new HashSet<Integer>();
		iniSet.add(from);
		disMap.put(0.0, iniSet);
		while(!disMap.isEmpty()){
			Entry<Double, HashSet<Integer>> entry = disMap.pollFirstEntry();
			Double dis = entry.getKey();
			HashSet<Integer> verSet = entry.getValue();
			if(dis==weight){
				if(verSet.contains(to)){
					return true;
				}
				break;
			}
			if(dis>weight){
				break;
			}
			Iterator<Integer> iterVSet = verSet.iterator();
			while(iterVSet.hasNext()){
				int pop = iterVSet.next();
				checkedSet.add(pop);
				Set<DefaultWeightedEdge> edgeSet = graph.edgesOf(pop);
				Iterator<DefaultWeightedEdge> iter = edgeSet.iterator();
				while(iter.hasNext()){
					DefaultWeightedEdge edge = iter.next();
					int newid = graph.getEdgeSource(edge);
					if(newid == pop){
						newid = graph.getEdgeTarget(edge);
					}
					if(!checkedSet.contains(newid)){
						double edgewei = dis+graph.getEdgeWeight(edge);
						HashSet<Integer> addSet = disMap.get(edgewei);
						if(addSet==null){
							addSet = new HashSet<Integer>();
							disMap.put(edgewei, addSet);
						}
						addSet.add(newid);
					}
				}
			}
			//Vertex popV = graphClass.getVertexFromVid(from);
			
		}
		return false;
	}
	
	public static void main(String[] args) throws Exception {
		CmdOption option = new CmdOption();
		CmdLineParser parser = new CmdLineParser(option);
		
		//1. get command line parameters 
		try {
			parser.parseArgument(args);
		} catch (CmdLineException e) {
		}
		
		GraphClass graphClass = new GraphClass();
		graphClass.readUnDirectedGraph(option.folder+option.nodefile, option.folder+option.edgefile, false);
		GenePath genePath = new GenePath();
		int from = 1127613;
		int to = 855186;
		double weight = 3.0;
		System.out.println("from:"+from+" to:"+to+" weight:"+weight+" "+genePath.getPath(graphClass, from, to, weight));
	}
}
