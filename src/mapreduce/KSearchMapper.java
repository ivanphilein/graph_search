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

public class KSearchMapper extends Mapper<LongWritable, Text, IntWritable, Text> {
	private final static String LKNFile = "L_KN";
	private final static String LPNFile = "L_PN";

	/**
	 * based on query string, return query list
	 * @return
	 */
	private HashSet<Integer> getQueryList(String queryStr){
		if(queryStr.isEmpty()){
			return null;
		}
		HashSet<Integer> queryList = new HashSet<Integer>();
		String[] temp;
		String delimiter = ":";
		temp=queryStr.split(delimiter);
		for(int i=0;i<temp.length;i++){
			if(!queryList.contains(temp[i])){
				queryList.add(Integer.parseInt(temp[i]));
			}
		}
		return queryList;
	}
	
	public void map(LongWritable key, Text value, Context context) throws IOException,  InterruptedException {
		Configuration conf = context.getConfiguration();
		String query = conf.get("QUERY");
		HashSet<Integer> queryList = getQueryList(query);
		////////////////////////////////////////////////
		InputSplit inputSplit = context.getInputSplit();
		String fileName = ((FileSplit) inputSplit).getPath().getName();
		
		////System.out.println("input "+value);
		////////////////////////////////////////////////
		if(fileName.toString().startsWith(LKNFile)){
			String strLine = value.toString();
			//
			if(!strLine.startsWith("#")&&!strLine.isEmpty()){
				int bidInt = Integer.parseInt(strLine.substring(0, strLine.indexOf(" ")));
	  			//remove bid
	  			strLine = strLine.substring(strLine.indexOf(" ")+1);
	  			
	  			int kid = Integer.parseInt(strLine.substring(0, strLine.indexOf(" ")));
	  			if(queryList.contains(kid)){
					//
					IntWritable bid = new IntWritable(bidInt);
					Text word = new Text(LKNFile+" "+strLine);
					//System.out.println("LKN bid "+bid+" word "+word);
					context.write(bid,word); 
	  			}
				
			}
		}
		else if(fileName.toString().startsWith(LPNFile)){
			String strLine = value.toString();
			//
			if(!strLine.startsWith("#")&&!strLine.isEmpty()){
				int bidInt = Integer.parseInt(strLine.substring(0, strLine.indexOf(" ")));
	  			//remove bid
	  			strLine = strLine.substring(strLine.indexOf(" ")+1);
	  			IntWritable bid = new IntWritable(bidInt);
				Text word = new Text(LPNFile+" "+strLine);
				//System.out.println("LPN bid "+bid+" word "+word);
				context.write(bid,word);
			}
		}
		
	}
}

