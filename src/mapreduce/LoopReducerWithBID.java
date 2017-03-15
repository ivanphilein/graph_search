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
public class LoopReducerWithBID extends Reducer<IntWritable,Text,IntWritable,Text>{
	//private static int num=0;
	private final static String RESULTMAP = "RM:";
	private final static String LPN = "LPN:";	
	private static String SOLUTION = "Solution:";
	private static String SOLUNUM = "SOLUNUM:";
	private static String solSum = "SUM:";
	
	public static enum State {
	    UPDATED;
	}
	
  	public void reduce(IntWritable key, Iterable<Text> values, Context context) throws IOException, InterruptedException {

  		int total = 0;
  		//System.out.println("THIS IS LOOPREDUCERWITHBID");
  		IndexVertex vClass = new IndexVertex();
  		Configuration conf = context.getConfiguration();
  		vClass.total = Integer.parseInt(conf.get("TOTAL"));
		boolean update = false;
  		for(Text val : values){
  			String inputString = val.toString().trim();
  			//System.out.println("IIIIIIIIIIIIIInput "+inputString);
  			if(inputString.startsWith(SOLUNUM)){
  				total += Integer.parseInt(inputString.replace(SOLUNUM+" ", ""));
  				continue;
  			}
  	  		if(inputString.startsWith(SOLUTION)){
  	  			//System.out.println("!!!!!!!!!!!!!!"+SOLUTION+"1111111 threshold:"+vClass.getThreshold());
				context.write(key, new Text(inputString));
				continue;
			}
			
			/*if(inputString.startsWith(THRESHOLD)){
				inputString = inputString.substring(inputString.indexOf(":")+1);
				threshold = Integer.parseInt(inputString);
				vClass.setThreshold(threshold);
				continue;
			}*/
  			if(inputString.startsWith(LPN)){//LPN information format: LPN:vid sending block list(bid1:bid2) indexes
  				inputString = inputString.replace(LPN, "");
  				String[] temp;
				String delimiter = " ";
				temp=inputString.split(delimiter);
				//if temp.length<=1 means that this vertex is not out-portal node, its information should be included in out-portal nodes
				if(temp.length>1){
					//////System.out.println("!!!!!!!!!!LPN:"+inputString);
					int vid = Integer.parseInt(temp[0].toString());
					String[] bidArray;
					delimiter = ":";
					bidArray = temp[1].toString().split(delimiter);
					//////System.out.println("length of bidArray "+bidArray.length+" temp[1] "+temp[1]);
					VertexInfo vertex = vClass.getVertex(vid);
					if(vertex==null){
						//////System.out.println("vid null "+vid);
						vertex = new VertexInfo(vid);
						vClass.addVertex(vid, vertex);
						//write the sending part
						for(int i=0;i<bidArray.length;i++){
							//////System.out.println("bidArray"+i+" "+bidArray[i]);
							vertex.addToSendingList(Integer.parseInt(bidArray[i]));
						}
						//write the ance part
						for(int i=2;i<temp.length;i++){
							IndexElement putElement = new IndexElement(temp[i]);
							putElement = new IndexElement(temp[i].toString());
							//putElement.showElement();
							vertex.writeAnceSet(putElement);
						}
					}
					else{
						//write the ance part
						for(int i=2;i<temp.length;i++){
							IndexElement putElement = new IndexElement(temp[i]);
							putElement = new IndexElement(temp[i].toString());
							//putElement.showElement();
							vertex.writeAnceSet(putElement);
						}
						vClass.updateAnceVertex(vertex, vertex.getAnceSet());
					}
					////System.out.println("!!!!!!!!!!!!!"+"LPN:"+inputString);
					context.write(key, new Text("LPN:"+inputString));
				}
  			}
  			else if(inputString.startsWith(RESULTMAP)){//information of part solution
  				inputString = inputString.replace(RESULTMAP, "");
  				String[] sol = inputString.split(solSum);
  				String[] temp;
  				String delimiter = " ";
  				temp = inputString.split(delimiter);
  				int vid = Integer.parseInt(temp[0]);
				if(sol.length>1){
	  				Double sum = Double.parseDouble(sol[1]);
	  				update = vClass.writeV2ResultMapForIteration(sol[0].replace("(", "").replace(")", "").replace(":", " "));
				}
  			}
			
  		}
  		//System.out.println("Total number:"+vClass.getNumOfSolution()+" "+vClass.total);
  		if(vClass.getNumOfSolution()>0)
  			total += vClass.getNumOfSolution();
  		//context.write(new IntWritable(0), new Text(SOLUNUM+" "+total));
  		if(vClass.getNumOfSolution()>=vClass.total){
  			update = false;
  		}
  		/*else{
  			update = true;
  		}*/
  		if(update==false){

  			context.getCounter(State.UPDATED).setValue(-100);
  			////System.out.println("Update "+update);
  			HashSet<Integer> resultSet = vClass.getResultSet();
  	  		if(resultSet != null){
  	  			String tempStr = vClass.writeableResultSet();
  	  			//System.out.println("!!!!!!!!!!!!!!"+SOLUTION+" block:"+key.get()+" results:"+tempStr);
	  			context.write(key, new Text(SOLUTION+tempStr));
  	  		}
  		}
  		else{
  			////System.out.println("Update "+update);
  			context.getCounter(State.UPDATED).increment(1);
	  		//output the found whole solutions first
	  		HashSet<Integer> resultSet = vClass.getResultSet();
	  		if(resultSet != null){
	  			String tempStr = vClass.writeableResultSet();
  	  			//System.out.println("!!!!!!!!!!!!!!"+SOLUTION+" block:"+key.get()+" results:"+tempStr);
	  			context.write(key, new Text(SOLUTION+tempStr));
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
  		////System.out.println("ENDENDEND");
  		//vClass.setResultSetNull();
  	}
}
