package oneIteMR;

import java.io.IOException;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.io.DoubleWritable;

public class OneIteMapper extends Mapper<LongWritable, Text, IntWritable, DouArrayWritable>{
	private final static String vertex = "vertex";
	private final static int v = 0;
	private final static int e = 1;
	
	/*private HashSet<Integer> getQueryList(String queryStr){
		if(queryStr.isEmpty()){
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
	}*/
	
	public void map(LongWritable key, Text value, Context context) throws IOException,  InterruptedException {
		String strLine = value.toString();
		//Configuration conf = context.getConfiguration();
  		//String query = conf.get("QUERY");
  		//HashSet<Integer> querySet = getQueryList(query);
		if(!strLine.startsWith("#")&&!strLine.isEmpty()){
			String[] temp = strLine.split(" ");
			int bid = Integer.parseInt(temp[0]);
			int size = temp.length;
			DouArrayWritable keyArray = new DouArrayWritable();
			DoubleWritable[] array = new DoubleWritable[size-1];
	  		if(temp[1].equals(vertex)){
	  			array[0] = new DoubleWritable(v);
	  			//store weight first
	  			/*for(int i=1;i<3;i++){
	  				array[i] = new DoubleWritable(Double.parseDouble(temp[i+1]));
	  			}
	  			int j=2;
	  			for(int i=3;i<size-1;i++){
		  			if(querySet.contains(Integer.parseInt(temp[i+1]))){
		  				j++;
		  				array[j] = new DoubleWritable(Double.parseDouble(temp[i+1]));
		  			}
	  				
	  			}*/
	  			for(int i=1;i<size-1;i++){
	  				array[i] = new DoubleWritable(Double.parseDouble(temp[i+1]));
	  				
	  			}
	  		}
	  		else{
	  			array[0] = new DoubleWritable(e);
	  			for(int i=1;i<size-1;i++){
	  				array[i] = new DoubleWritable(Double.parseDouble(temp[i+1]));
	  				
	  			}
	  		}
	  		
  			keyArray.set(array);
	  		context.write(new IntWritable(bid),keyArray);
		}
	}

}
