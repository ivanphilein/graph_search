package mapreduce;

import java.io.IOException;
import java.util.HashMap;
import java.util.Iterator;
import java.util.Map.Entry;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.io.DoubleWritable;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Reducer;

import shared.IndexClass;
import shared.IndexElement;

public class FinalReducer extends Reducer<IntWritable,Text,DoubleWritable,Text>{
	private static String SOLUTION = "Solution:";
	private final static String LKNFile = "L_KN";
	private static String THRESHOLD = "Threshold:";
	
  	public void reduce(IntWritable key, Iterable<Text> values, Context context) throws IOException, InterruptedException{
  		System.out.println("THIS IS FINALREDUCERWITHBID");
  		int bid = key.get();
		IndexVertex vClass = new IndexVertex();
		IndexClass indexC = new IndexClass();
  		Configuration conf = context.getConfiguration();
		vClass.total = Integer.parseInt(conf.get("TOTAL"));
  		int topK = Integer.parseInt(conf.get("TOPK"));
  		//String sunTime = conf.get("SumTime");
  		//the reading value part
  		for(Text val : values){
  			String inputString = val.toString();
  			//String label = inputString.substring(0, inputString.indexOf(" "));
  			//remove label
  			//inputString = inputString.substring(inputString.indexOf(" ")+1);
			//if(label.equals(SOLUTION)){
			if(inputString.startsWith(SOLUTION)){
				context.write(new DoubleWritable(key.get()), new Text(inputString));
				/*String[] temp;
				String delimiter = " ";
				temp = inputString.split(delimiter);
				if(temp.length>0){
					int vid = -1;
					int kid = -1;
					String elementStr = null;
					for(int i=0; i<temp.length;i++){
						elementStr = temp[i].trim();
						if(elementStr.startsWith(THRESHOLD)){
							continue;
						}
						if(elementStr.startsWith("vid:")){
							
							elementStr = elementStr.replace("vid:", "");
							vid = Integer.parseInt(elementStr);
						}
						else if(!elementStr.contains(",")){
							kid = Integer.parseInt(elementStr);
						}
						else{
							String[] secTemp;
							String secDel = "-";
							secTemp = elementStr.split(secDel);
							for(int j=0;j<secTemp.length;j++){
								IndexElement element = new IndexElement(secTemp[j]);
								//here we set the boolean as true, as we do not need to update LPN any more
								vClass.writeV2ResultMap(vid, kid, element, true);
								//vClass.writeSolution(vid, kid, element);
							}
						}
					}
				}*/
				//context.write(new DoubleWritable(key.get()), new Text(inputString));
			}
			//read the LKN part
			/*else if(label.equals(LKNFile)){
				//System.out.println("LKN "+inputString);
				String[] temp;
				String delimiter = " ";
				temp = inputString.split(delimiter);
				if(temp.length>0){
					//int kid = Integer.parseInt(temp[0]);
					for(int i=1;i<temp.length;i++){
						//indexC.addIndexBToK(bid, kid, new IndexElement(temp[i].toString()));
						vClass.writeV2ResultMapForInitial(inputString);
					}
				}
  			}*/
  		}
  		//end of reading value part
  		//vClass.setNumOfSolution(0);
  		//calculation part and get the top k based on real distance
  		/*HashMap<Integer, Object> allVertex = vClass.getId2Vertex();
  		System.out.println("size: "+allVertex.size());
  		if(allVertex!=null){
  			int num = 0;
  			Iterator<Entry<Integer, Object>> iter = allVertex.entrySet().iterator(); 
	  		//HashMap<Integer,TreeSet<SetElement>> resultMap = new HashMap<Integer,TreeSet<SetElement>>();
    	    //VertexInfo tempVertex = null;
	    	while (iter.hasNext()) { 
				Entry<Integer, Object> entry = iter.next(); 
	    	    int vid = entry.getKey();
	    	    System.out.println("VID before find "+vid);
	    	    boolean find = vClass.updateSumRMapFinal(vid, indexC);
	    	    if(find==false)
	    	    	continue;
	    	    System.out.println("VID found "+vid);
	    	    num++;
	    	    if(num>=topK){
	    	    	System.out.println("num "+num+" topk "+topK);
	    	    	break;
	    	    }*/
	    	    //here is the resultSet with all the path
	    	    /*TreeSet<SetElement> resultSet = new TreeSet<SetElement>(new CompareSetElement());
	    	    VertexInfo outVertex = (VertexInfo) allVertex.get(vid);
	    	    HashMap<Integer, TreeSet<IndexElement>> rmMap = outVertex.getResultMap();
	    	    if(rmMap != null){
	    	    	Iterator<Entry<Integer, TreeSet<IndexElement>>> iterRM = rmMap.entrySet().iterator();
	    	    	while(iterRM.hasNext()){
	    	    		Entry<Integer, TreeSet<IndexElement>> entryRM = iterRM.next();
	    	    		int kid = entryRM.getKey();
	    	    		TreeSet<IndexElement> rmSet = entryRM.getValue();
	    	    		if(rmSet!=null){
	    	    			Iterator<IndexElement> iterSet = rmSet.iterator();
	    	    			while(iterSet.hasNext()){
	    	    				IndexElement indexelement = iterSet.next();
	    	    				SetElement newElement = new SetElement(indexelement.getStartVertex(), indexelement.getNextVertex(), indexelement.getLength());
	    	    				resultSet.add(newElement);
	    	    			}
	    	    		}
	    	    	}
	    	    	resultMap.put(vid, resultSet);
	    	    }*/
	    	/*} 
	    	//output all the information in resultMap and also check the real distance
	    	System.out.println(bid+" NOT NULL?????");
	    	//context.write(new DoubleWritable(bid), new Text("sdfds"));
	    	String retStr = vClass.getPathMapStr();
	    	if(retStr != null)
	    		context.write(new DoubleWritable(bid), new Text(retStr));
	    	
  		}*/
  		// end of calculation part and get the top k based on real distance
  	}
}
