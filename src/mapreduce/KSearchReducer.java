//NOT USING NOW
package mapreduce;


import java.io.IOException;
import java.util.Date;
import java.util.HashMap;
import java.util.HashSet;
import java.util.Iterator;
import java.util.Map;
import java.util.Map.Entry;
import java.util.TreeSet;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Reducer;

import shared.IndexElement;




public  class KSearchReducer extends Reducer<IntWritable,Text,IntWritable,Text> {
	private final static String LKNFile = "L_KN";
	private final static String LPNFile = "L_PN";	
	private final int MAX=100000;	
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

    	long SumTime = 0;
    	long lDateTimeStart=0;
    	long lDateTimeFinish=0;
  		
  		
		Configuration conf = context.getConfiguration();
		int num=5;// = conf.getInt("TopK",0);
		String query = conf.get("QUERY");
		System.out.println("TopK "+num);
		System.out.println("QUERY:"+query);
		int Max = MAX;
		System.out.println("QUERY:"+query);
		HashSet<Integer> queryList = getQueryList(query);
		IndexVertex vClass = new IndexVertex();
  		for(Text val : values){	
  			String inputString = val.toString();
  			System.out.println("input "+inputString);
  			String label = inputString.substring(0, inputString.indexOf(" "));
  			//remove label
  			inputString = inputString.substring(inputString.indexOf(" ")+1);
			
  			if(label.equals(LKNFile)){
	  			boolean retBool = false;
				String[] temp;
				String delimiter = " ";
				temp=inputString.split(delimiter);
				IndexElement putElement = null;
				if(temp.length>=2){
					/////////count time start point
					lDateTimeStart = new Date().getTime();
			        System.out.println("Date() - Time in milliseconds: " + lDateTimeStart);
					//end of count time start point
					int keyid = (Integer.parseInt(temp[0]));
					int total = queryList.size();
					if(queryList.contains(keyid)){
						VertexInfo vertex = null;
						for(int i=1;i<temp.length;i++){
							putElement = new IndexElement(temp[i]);
							int vid = putElement.getStartVertex();
							vertex = vClass.getVertex(vid);
							if(vertex==null){
								vertex = new VertexInfo(vid);
								vClass.addVertex(vid, vertex);
							}
							retBool = vertex.addToResultMap(keyid, putElement, total);
							if(retBool){
								if(num>0){
									num--;
								if(num==0)
									break;
							}
								TreeSet<Integer> costSet = vertex.getSubGraphCost(num, Max, queryList);
								if(num>0 && costSet!=null){
									Max = costSet.last();
									num -= costSet.size();
									if(num==0)
										break;
								}
							}
						}
					}
					/////end running time
					lDateTimeFinish = new Date().getTime();
			        System.out.println("Date() - Time in milliseconds: " + lDateTimeFinish);
			        SumTime += lDateTimeFinish-lDateTimeStart;
			        //end of ending running time
				}
  			}
  			else if(label.equals(LPNFile)){
  				System.out.println("LPN "+inputString);
				String[] temp;
				String delimiter = " ";
				temp=inputString.split(delimiter);
				vClass.writeVertex2Ance(temp, key.get());
  			}
  		}//end of for
  		HashMap<Integer, Object> allVertex = vClass.getId2Vertex();
  		
  		//
  		if(allVertex!=null){
	  		Iterator<Entry<Integer, Object>> iter = allVertex.entrySet().iterator(); 
	    	while (iter.hasNext()) { 
	    	    @SuppressWarnings("rawtypes")
				Map.Entry entry = (Map.Entry) iter.next(); 
	    	    int outkey = (Integer)entry.getKey();
	    	    VertexInfo outVertex = (VertexInfo)allVertex.get(outkey);
	    	    String retStr = outVertex.getAllAnceList();
	    	    String resultMapStr = outVertex.getAllResultMap();
	    	    if(retStr != null){
	    	    	retStr = "AL:"+retStr;
		    	    if(resultMapStr != null){
		    	    	retStr += " RM:"+resultMapStr;
		    	    }
	    	    	System.out.println("kid "+outkey+" |send "+retStr);
	    	    	context.write(key, new Text(outkey+" "+retStr));
	    	    }
	    	    else if(resultMapStr != null){
	    	    	retStr = "RM:"+resultMapStr;
	    	    	System.out.println("kid "+outkey+" |send "+retStr);
	    	    	context.write(key, new Text(outkey+" "+retStr));
	    	    }
	    	} 
  		}
  		
  		
  		
  	}

}