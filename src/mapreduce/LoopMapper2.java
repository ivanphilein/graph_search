package mapreduce;

import java.io.IOException;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;

public class LoopMapper2 extends Mapper<IntWritable, Text, IntWritable, Text> {
	
	public void map(IntWritable key, Text value, Context context) throws IOException,  InterruptedException {
		/*String[] temp;
		String delimiter = "	";
		temp=value.toString().split(delimiter);*/
		System.out.println("loopmap Key: "+key.get()+"value: "+value.toString());
		context.write(new IntWritable(key.get()), value);
	}
}

