package oneIteMR;

import java.io.IOException;
import java.util.HashSet;
import java.util.Iterator;
import java.util.Map.Entry;
import java.util.TreeMap;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Reducer;

public class FinalReducer extends Reducer<IntWritable,Text,IntWritable,Text>{
	public void reduce(IntWritable key, Iterable<Text> values, Context context) throws InterruptedException, IOException {
		//System.out.println("Reducer");
		String firSplit = "-";
		//String detStr = "-";
		TreeMap<Double, HashSet<String>> rmMap = new TreeMap<Double, HashSet<String>>();
		Configuration conf = context.getConfiguration();
		String query = conf.get("QUERY");
  		context.write(new IntWritable(1), new Text("query:"+query));
		for(Text val : values){
			String inputStr = val.toString();
			//System.out.println(inputStr);
	  		
			String[] store = inputStr.split(firSplit);
			double sum = Double.parseDouble(store[0]);
			HashSet<String> strSet = rmMap.get(sum);
			if(strSet==null){
				strSet = new HashSet<String>();
				rmMap.put(sum,strSet);
			}
			strSet.add(inputStr);
			
			/*for(int i=0;i<store.length;i++){
				String[] temp = store[i].split(detStr);
				double sum = Double.parseDouble(temp[0]);
				HashSet<String> strSet = rmMap.get(sum);
				if(strSet==null){
					strSet = new HashSet<String>();
					rmMap.put(sum,strSet);
				}
				strSet.add(temp[1]);
			}*/
		}
		
  		int topK = Integer.parseInt(conf.get("TOPK"));
  		while(topK>0){
  			if(!rmMap.isEmpty()){
  				Entry<Double, HashSet<String>> entry = rmMap.pollFirstEntry();
  				HashSet<String> solSet = entry.getValue();
  				double sum = entry.getKey();
  				Iterator<String> iter = solSet.iterator();
  				while(iter.hasNext()){
  					context.write(key, new Text(iter.next()+" Sum:"+sum));
  					topK--;
  					if(topK==0)
  						break;
  				}
  				
  			}
  			else
  				break;
  		}
	}

}
