package mapreduce;

/*import java.io.IOException;
import java.util.HashMap;
import java.util.HashSet;
import java.util.Iterator;
import java.util.List;
import java.util.Map;
import java.util.Map.Entry;

import org.apache.hadoop.conf.Configuration;*/
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Reducer;

public class LoopReducerWithVID extends Reducer<IntWritable,Text,IntWritable,Text>{
//	private static int num=0;
//	private static String SOLUTION = "Solution:";
//	private static String THRESHOLD = "Threshold:";
//	
//	public static enum State {
//	    UPDATED;
//	}
//	private IndexVertex vClass = new IndexVertex();
//	
//	/**
//	 * update the results we have already now
//	 * @param solution
//	 * @param vClass
//	 */
//	public void setSolution(String solution, IndexVertex vClass){
//		String[] temp;
//		String delimiter = "-";
//		temp = solution.split(delimiter);
//		for(int i=0;i<temp.length;i++){
//			
//		}
//	}
//  	public void reduce(IntWritable key, Iterable<Text> values, Context context) throws IOException, InterruptedException{
//
//  		num++;
//  		Configuration conf = context.getConfiguration();
//		int total = Integer.parseInt(conf.get("TOTAL"));
//		int threshold = 0;
//  		System.out.println("num "+num);
//		boolean update = false;
//  		for(Text val : values){	
//  			System.out.println("LoopReduce key:"+key.get()+"loopReduce value:"+val.toString());
//  			//context.write(key, val);
//  			String inputString = val.toString();
//  			if(inputString.startsWith(SOLUTION)){
//  				context.write(key, new Text(inputString));
//  			}
//  			
//  			if(inputString.startsWith(THRESHOLD)){
//  				inputString = inputString.substring(inputString.indexOf(":")+1);
//  				threshold = Integer.parseInt(inputString);
//  			}
//  			int vid = Integer.parseInt(inputString.substring(0,inputString.indexOf(" ")));
//  			//get rid of vid
//  			inputString = inputString.substring(inputString.indexOf(" ")+1);
//  			if(vClass.getVertex(vid)!=null){
//  				update = true;
//  			}
//  			if(inputString.startsWith("AL:")){
//				vClass.writeVertex2Ance(vid, inputString.substring(0,inputString.indexOf("RM:")).replace("AL:", ""));
//				inputString = inputString.substring(inputString.indexOf("RM:"));
//			}
//  			/*if(inputString.startsWith("RM:")){
//				if(update==false)
//					update = vClass.writeV2ResultMapForIteration(vid,inputString.replace("RM:", "").toString(), total);
//				else
//					vClass.writeV2ResultMapForIteration(vid,inputString.replace("RM:", "").toString(), total);
//			}*/
//			
//  		}
//  		
//  		System.out.println("update");
//  		if(update){
//  			context.getCounter(State.UPDATED).increment(1);
//  		}
//  		HashMap<Integer, Object> allVertex = vClass.getId2Vertex();
//  		System.out.println("size "+allVertex.size());
//  		if(allVertex!=null){
//	  		Iterator<Entry<Integer, Object>> iter = allVertex.entrySet().iterator(); 
//	    	while (iter.hasNext()) { 
//	    	    @SuppressWarnings("rawtypes")
//				Map.Entry entry = (Map.Entry) iter.next(); 
//	    	    int outkey = (Integer)entry.getKey();
//	    	    VertexInfo outVertex = (VertexInfo)allVertex.get(outkey);
//	    	    String retStr = outVertex.getAllAnceList();
//	    	    String resultMapStr = outVertex.getAllResultMap();
//	    	    if(retStr != null){
//	    	    	retStr = "AL:"+retStr;
//		    	    if(resultMapStr != null){
//		    	    	retStr += " RM:"+resultMapStr;
//		    	    }
//	    	    	System.out.println("kid "+outkey+" |first "+retStr);
//	    	    	context.write(key, new Text(outkey+" "+retStr));
//	    	    }
//	    	    else if(resultMapStr != null){
//	    	    	retStr = "RM:"+resultMapStr;
//	    	    	System.out.println("kid "+outkey+" |first "+retStr);
//	    	    	context.write(key, new Text(outkey+" "+retStr));
//	    	    }
//	    	    List<Integer> sendingList = outVertex.getSendingList();
//	    	    if(sendingList != null && resultMapStr != null){
//	    	    	resultMapStr = outkey+" RM:"+resultMapStr;
//	    	    	Iterator<Integer> sendIter = sendingList.iterator();
//	    	    	while(sendIter.hasNext()){
//	    	    		int keybid = sendIter.next();
//		    	    	System.out.println("kid "+outkey+" |second "+retStr);
//	    	    		context.write(new IntWritable(keybid), new Text(resultMapStr));
//	    	    	}
//	    	    }
//	    	} 
//  		}
//  	}
}
