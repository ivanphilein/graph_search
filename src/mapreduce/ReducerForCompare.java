package mapreduce;

import java.io.IOException;
import java.util.ArrayList;
import java.util.Comparator;
import java.util.HashMap;
import java.util.Iterator;
import java.util.List;
import java.util.TreeSet;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.io.DoubleWritable;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Reducer;

import shared.IndexElement;


class SolutionSet{
	String solution;
	Double sum;
};
 
class SolCompare implements Comparator<SolutionSet>{
	
	public int compare(SolutionSet e1, SolutionSet e2) 
	{
	    if(e1.sum<e2.sum)
	    	return -1;
	    /*if(e1.sum==e2.sum && e1.solution.equalsIgnoreCase(e2.solution)){
	    	return 0;
	    }*/
	    return 1;
	    
	}
}

public class ReducerForCompare extends Reducer<IntWritable,Text,DoubleWritable,Text>{

	private static String SOLUTION = "Solution:";
	private static String solSum = "SUM:";
	private static String LKNFile = "L_KN";
	private static String splitStr = " ";
	private static String SOLUNUM = "SOLUNUM:";
	private static String VID = "vid:";
	//map from distance from one map which start from node id to indexes start from this node
	private static HashMap<Double, HashMap<Integer, HashMap<Integer,IndexElement>>> indexC =  new HashMap<Double, HashMap<Integer, HashMap<Integer,IndexElement>>>();
	
	/**
	 * Based on distance, start node, and end noded, return the next node id
	 * @param dis
	 * @param start
	 * @param end
	 * @return
	 */
	private int getNextNode(Double dis, int start, int end){
		HashMap<Integer, HashMap<Integer,IndexElement>> mapSameDis = indexC.get(dis);
		if(mapSameDis!=null){
			HashMap<Integer,IndexElement> mapFromEnd = mapSameDis.get(start);
			if(mapFromEnd!=null){
				IndexElement index = mapFromEnd.get(end);
				if(index!=null){
					return index.getNextVertex();
				}
			}
		}
		return -1;
	}
	//private static String THRESHOLD = "Threshold:";
	public void reduce(IntWritable key, Iterable<Text> values, Context context) throws IOException, InterruptedException{
  		int total = 0;
  		Configuration conf = context.getConfiguration();
		//vClass.total = Integer.parseInt(conf.get("TOTAL"));
  		int topK = Integer.parseInt(conf.get("TOPK"));
  		String query = conf.get("QUERY");
  		context.write(new DoubleWritable(0), new Text("query:"+query));
  		TreeSet<SolutionSet> solSet = new TreeSet<SolutionSet>(new SolCompare());
  		for(Text val : values){
  			String inputString = val.toString();
  			String label = inputString.substring(0, inputString.indexOf(" "));
  			//remove label
  			inputString = inputString.substring(inputString.indexOf(" ")+1);
  			if(label.equals(SOLUNUM)){
  				int numTotal = Integer.parseInt(inputString);
  				if(numTotal>0)
  					total += numTotal;
  				continue;
  			}
  			if(label.equals(LKNFile)){
				IndexElement index = new IndexElement(inputString);
				HashMap<Integer, HashMap<Integer,IndexElement>> mapSameDis = indexC.get(index.getLength());
				if(mapSameDis==null){
					mapSameDis = new HashMap<Integer, HashMap<Integer,IndexElement>>();
					indexC.put(index.getLength(), mapSameDis);
				}
				HashMap<Integer,IndexElement> mapSameEnd = mapSameDis.get(index.getStartVertex());
				if(mapSameEnd==null){
					mapSameEnd = new HashMap<Integer,IndexElement>();
					mapSameDis.put(index.getStartVertex(), mapSameEnd);
				}
				mapSameEnd.put(index.getEndVertex(), index);
				//context.write(new DoubleWritable(key.get()), new Text(LKNFile+inputString));
  			}
  			else if(label.equals(SOLUTION)){
  				String[] sol = inputString.split(VID);
  				for(int i=0;i<sol.length;i++){
  					String[] detail = sol[i].split(solSum);
					if(detail.length>1){
		  				SolutionSet newSet = new SolutionSet();
		  				Double sum = Double.parseDouble(detail[1]);
		  				newSet.sum = sum;
		  				newSet.solution = detail[0].replace("(", "").replace(")", "");
		  				solSet.add(newSet);
					}
  				}
  				//context.write(new DoubleWritable(key.get()), new Text(inputString));
			}//end of if for solution
  		}//end of for
  		
  		if(total>0){
  			context.write(new DoubleWritable(key.get()), new Text("Total Found:"+total));
  		}
  		int num=0;
  		while(!solSet.isEmpty()&&num<topK){
  		//while(!solSet.isEmpty()){
  			SolutionSet resultSet = solSet.pollFirst();
 			context.write(new DoubleWritable(key.get()),new Text(resultSet.solution+" "+solSum+resultSet.sum));
  			//String solution = solSet.pollFirst().solution;
  			/////////////////////////////////////////////////////////////////////////////////////////////Works for real path
  			/*String[] solPath = resultSet.solution.split(splitStr);
  			int from=0;
  			int next=0;
  			int to=0;
  			Double dis=0.0;
  			String[] nodeA;
  			String resultSol=solPath[0]+" ";
			for(int i=1;i<solPath.length;i++){
	  			List<Integer> pathSet = new ArrayList<Integer>();
				String pathStr = "";
				nodeA = solPath[i].split(",");
				pathStr += nodeA[0];
				from = Integer.parseInt(nodeA[1]);
				next = Integer.parseInt(nodeA[2]);
				to = Integer.parseInt(nodeA[3]);
				pathSet.add(from);
				String[] disT = nodeA[0].split(":");
				dis = Double.parseDouble(disT[1]);
				while(next!=to && next!=-1){
				//while(next!=to){
					pathSet.add(next);
					dis = dis-1.0;
					next = getNextNode(dis,next,to);
				}
				pathSet.add(to);
				Iterator<Integer> iterPath = pathSet.iterator();
				while(iterPath.hasNext()){
					pathStr += "-"+iterPath.next();
				}
				resultSol += pathStr+"; ";
			}
  			context.write(new DoubleWritable(key.get()),new Text(resultSol+" "+solSum+resultSet.sum));*/
  			/////////////////////////////////////////////////////////////////////////////////////////////
  			num++;
  		}
  	}
}
