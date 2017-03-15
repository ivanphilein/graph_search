package mapreduce;


import graphIndex.KSearchGraph;

import java.io.IOException;
//import java.util.HashMap;
import java.util.HashSet;
/*import java.util.Iterator;
import java.util.List;
import java.util.Map;
import java.util.PriorityQueue;
import java.util.TreeSet;
import java.util.Map.Entry;*/

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Reducer;




public  class KSearchReducerOnce extends Reducer<IntWritable,Text,IntWritable,Text> {
	//private final int MAX=100000;
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
			if(!queryList.contains(temp[i])){
				queryList.add(Integer.parseInt(temp[i]));
			}
		}
		return queryList;
	}
  	public void reduce(IntWritable key, Iterable<Text> values, Context context) throws IOException, InterruptedException{
		Configuration conf = context.getConfiguration();
  		int num=5;// = conf.getInt("TopK",0);
		String query = conf.get("QUERY");
		System.out.println("TopK "+num+" QUERY:"+query);
		HashSet<Integer> queryList = getQueryList(query);
		if(queryList==null){
			System.exit(0);
		}
		KSearchGraph graph = new KSearchGraph();
		String[] temp;
		String delimiter = ":";
		for(Text val : values){	
			temp=val.toString().split(delimiter);
			if(temp.length==4){
				int vid = Integer.parseInt(temp[0]);
				double weight = Double.parseDouble(temp[1]);
				
			}
		}
  		
  		
  	}

}