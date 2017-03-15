package iteration;

import java.io.IOException;

import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;

public class IterMapper extends Mapper<LongWritable, Text, IntWritable, Text> 
{ 
	public void map(LongWritable key, Text value, Context context) throws IOException, InterruptedException {
		String[] temp;
		String delimiter = "	";
		temp=value.toString().split(delimiter);
		context.write(new IntWritable(Integer.parseInt(temp[0])), new Text(temp[1]));
   }
}

 