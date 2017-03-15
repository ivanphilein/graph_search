package mapreduce;


import java.io.IOException;
import java.util.HashMap;
import java.util.HashSet;
import java.util.Iterator;
import java.util.Map;
import java.util.Map.Entry;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Reducer;

import shared.IndexElement;




public  class KSearchReducerWithBID extends Reducer<IntWritable,Text,IntWritable,Text> {
	private final static String LKNFile = "L_KN";
	private final static String LPNFile = "L_PN";	
	private static String SOLUTION = "Solution:";
	private static String SOLUNUM = "SOLUNUM:";
	
	public static enum State {
	    UPDATED;
	}
	
  	public void reduce(IntWritable key, Iterable<Text> values, Context context) throws IOException, InterruptedException{
  		////System.out.println("INITIAL REDUCER~~");
		Configuration conf = context.getConfiguration();
		IndexVertex vClass = new IndexVertex();
  		vClass.total = Integer.parseInt(conf.get("TOTAL"));
  		//String query = conf.get("QUERY");
		//HashSet<Integer> queryList = getQueryList(query);
		boolean update = true;
  		for(Text val : values){	
  			String inputString = val.toString();
  			String label = inputString.substring(0, inputString.indexOf(" "));
  			//remove label
  			inputString = inputString.substring(inputString.indexOf(" ")+1);
  			if(label.equals(LKNFile)){
				vClass.writeV2ResultMapForInitial(inputString);
  			}
  			else if(label.equals(LPNFile)){
				String[] temp;
				String delimiter = " ";
				temp=inputString.split(delimiter);
				//if temp.length<=1 means that this vertex is not out-portal node, its information should be included in out-portal nodes
				if(temp.length>1){
					int vid = Integer.parseInt(temp[0].toString());
					String[] bidArray;
					delimiter = ":";
					bidArray = temp[1].toString().split(delimiter);
					////System.out.println("length of bidArray "+bidArray.length+" temp[1] "+temp[1]);
					VertexInfo vertex = vClass.getVertex(vid);
					if(vertex==null){
					//	////System.out.println("vid null "+vid);
						vertex = new VertexInfo(vid);
						vClass.addVertex(vid, vertex);
					}
					//write the sending part
					for(int i=0;i<bidArray.length;i++){
						////System.out.println("bidArray"+i+" "+bidArray[i]);
						vertex.addToSendingList(Integer.parseInt(bidArray[i]));
					}
					//write the ance part
					for(int i=2;i<temp.length;i++){
						////System.out.println("temp[i] "+temp[i]);
						IndexElement putElement = new IndexElement(temp[i]);
						putElement = new IndexElement(temp[i].toString());
						//putElement.showElement();
						vertex.writeAnceSet(putElement);

						//update ance vertex sending list part
						int anceVid = putElement.getStartVertex();
						VertexInfo anceVertex = vClass.getVertex(anceVid);
						if(anceVertex==null){
							anceVertex = new VertexInfo(anceVid);
							vClass.addVertex(putElement.getStartVertex(), anceVertex);
						}
						anceVertex.addToSendingList(key.get());
						//end of updating ance vertex sending list part
					}
					if(temp.length>2){
						////System.out.println("!!!!!"+key+" LPN:"+inputString);
						context.write(key, new Text("LPN:"+inputString));
					}
				}
				
  			}
  		}//end of for
  		//System.out.println("update:"+update);
  		//System.out.println("Total number:"+vClass.getNumOfSolution()+" "+vClass.total);
  		int total = vClass.getNumOfSolution();
  		if(total>0)
  			context.write(new IntWritable(0), new Text(SOLUNUM+" "+total));
  		if(total>=vClass.total){
  			update = false;
  		}
  		if(update){
  			context.getCounter(State.UPDATED).increment(1);
  			HashSet<Integer> resultSet = vClass.getResultSet();
  	  		if(resultSet != null){
  	  			context.write(new IntWritable(0), new Text(SOLUTION+vClass.writeableResultSet()));
  	  		}
  	  		
	  	  	HashMap<Integer, Object> allVertex = vClass.getId2Vertex();
	  		//////System.out.println("size "+allVertex.size());
	  		if(allVertex!=null){
		  		Iterator<Entry<Integer, Object>> iter = allVertex.entrySet().iterator(); 
		    	while (iter.hasNext()) { 
		    	    @SuppressWarnings("rawtypes")
					Map.Entry entry = (Map.Entry) iter.next(); 
		    	    int outkey = (Integer)entry.getKey();
		    	    VertexInfo outVertex = (VertexInfo)allVertex.get(outkey);
		    	    //String retStr = outVertex.getAllAnceList();
		    	    String resultMapStr = outVertex.getAllResultMap();
		    	    
		    	    HashSet<Integer> sendingList = outVertex.getSendingList();
		    	    if(sendingList != null && resultMapStr != null){
		    	    	resultMapStr = "RM:"+outkey+" "+resultMapStr;
		    	    	Iterator<Integer> sendIter = sendingList.iterator();
		    	    	while(sendIter.hasNext()){
		    	    		int sendbid = sendIter.next();
			    	    	////System.out.println("!!!!!!!!!!key "+sendbid+" |second "+resultMapStr);
		    	    		context.write(new IntWritable(sendbid), new Text(resultMapStr));
		    	    	}
		    	    }
		    	} 
	  		}
  		}
  		else{
  			context.getCounter(State.UPDATED).setValue(-100);
	  		//output the found whole solutions first
	  		HashSet<Integer> resultSet = vClass.getResultSet();
	  		if(resultSet != null){
	  			////System.out.println("!!!!!!!!!!!!!!"+SOLUTION+" threshold:"+vClass.getThreshold());
	  			context.write(key, new Text(SOLUTION+vClass.writeableResultSet()));
	  		}
	  		
	  		/*HashMap<Integer, Object> allVertex = vClass.getId2Vertex();
	  		////System.out.println("size "+allVertex.size());
	  		if(allVertex!=null){
		  		Iterator<Entry<Integer, Object>> iter = allVertex.entrySet().iterator(); 
		    	while (iter.hasNext()) { 
		    	    @SuppressWarnings("rawtypes")
					Map.Entry entry = (Map.Entry) iter.next(); 
		    	    int outkey = (Integer)entry.getKey();
		    	    VertexInfo outVertex = (VertexInfo)allVertex.get(outkey);
		    	    //String retStr = outVertex.getAllAnceList();
		    	    String resultMapStr = outVertex.getAllResultMap();
		    	    
		    	    HashSet<Integer> sendingList = outVertex.getSendingList();
		    	    if(sendingList != null && resultMapStr != null){
		    	    	resultMapStr = "RM:"+outkey+" "+resultMapStr;
		    	    	Iterator<Integer> sendIter = sendingList.iterator();
		    	    	while(sendIter.hasNext()){
		    	    		int sendbid = sendIter.next();
			    	    	////System.out.println("!!!!!!!!!!key "+sendbid+" |second "+resultMapStr);
		    	    		context.write(new IntWritable(sendbid), new Text(resultMapStr));
		    	    	}
		    	    }
		    	} 
	  		}*/
  		}
  		
  		
  		
  	}

}
