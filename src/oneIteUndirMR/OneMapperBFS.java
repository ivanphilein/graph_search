package oneIteUndirMR;

import java.io.IOException;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;

public class OneMapperBFS extends Mapper<LongWritable, Text, IntWritable, Text>{
	private final static String delimiter = " ";
	
	/*public void map(LongWritable key, Text value, Context context) throws IOException,  InterruptedException {
		String strLine = value.toString();
		if(!strLine.startsWith("#")&&!strLine.isEmpty()){
			String[] temp = strLine.split(" ");
			int bid = Integer.parseInt(temp[0]);
			int size = temp.length;
			DouArrayWritable keyArray = new DouArrayWritable();
			DoubleWritable[] array = new DoubleWritable[size-1];
	  		if(temp[1].equals(vertex)){
	  			array[0] = new DoubleWritable(v);
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
	}*/

	public void map(LongWritable key, Text value, Context context) throws IOException,  InterruptedException {
		String strLine = value.toString();
		//if(!strLine.startsWith("#")&&!strLine.isEmpty()){
			int index = strLine.indexOf(delimiter);
			int bid = Integer.parseInt(strLine.substring(0, index));
			strLine = strLine.substring(index+1);
	  		context.write(new IntWritable(bid),new Text(strLine));
		//}
	}
}
