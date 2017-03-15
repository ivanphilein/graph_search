package mapreduce;

import java.io.IOException;
import java.util.HashSet;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.InputSplit;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.lib.input.FileSplit;

public class LoopMapper1 extends Mapper<LongWritable, Text, IntWritable, Text> {
	
	/*public void map(LongWritable key, Text value, Context context) throws IOException,  InterruptedException {
		if(!value.toString().startsWith("#")){
			String[] temp;
			String delimiter = "	";
			temp=value.toString().split(delimiter);
			if(temp.length>1){
				System.out.println("loopmap Key: "+key.get()+"value: "+value.toString()+" temp[0] "+temp[0]+" temp[1] "+temp[1]);
				context.write(new IntWritable(Integer.parseInt(temp[0])), new Text(temp[1]));
			}
		}
	}*/
	public void map(LongWritable key, Text value, Context context) throws IOException,  InterruptedException {
		Configuration conf = context.getConfiguration();
		String query = conf.get("QUERY");
		////////////////////////////////////////////////
		InputSplit inputSplit = context.getInputSplit();
		String fileName = ((FileSplit) inputSplit).getPath().getName();
		
		int sendID = 0;
		
		//System.out.println("input "+value);
		////////////////////////////////////////////////
		if(fileName.toString().startsWith("part")){
			if(!value.toString().startsWith("#")){
				String[] temp;
				String delimiter = "	";
				temp=value.toString().split(delimiter);
				if(temp.length>1){
					//System.out.println("loopmap Key: "+key.get()+"value: "+value.toString()+" temp[0] "+temp[0]+" temp[1] "+temp[1]);
					context.write(new IntWritable(Integer.parseInt(temp[0])), new Text(temp[1]));
				}
			}
		}
	}
}

