package oneIteUndirMR;

import java.io.IOException;

import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;

public class FinalMapper extends Mapper<LongWritable, Text, IntWritable, Text>{
	public void map(LongWritable key, Text value, Context context) throws IOException,  InterruptedException {
		String delimiter = "	";
		String[] temp = value.toString().split(delimiter);
		//System.out.println(value.toString());
		if(temp.length>1){
			//System.out.println("Key: "+key.get()+"value: "+value.toString()+" temp[0] "+temp[0]+" temp[1] "+temp[1]);
			context.write(new IntWritable(0), new Text(temp[1]));
		}
		/*if(value.toString().trim().length()>1){
			context.write(new IntWritable(0),value);
		}*/
	}

}