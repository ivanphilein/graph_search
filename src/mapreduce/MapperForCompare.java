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

public class MapperForCompare extends Mapper<LongWritable, Text, IntWritable, Text> {
	private static String LKNFile = "L_KN";
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
	
	//private static String SOLUTION = "Solution:";
	public void map(LongWritable key, Text value, Context context) throws IOException,  InterruptedException {
		Configuration conf = context.getConfiguration();
		String query = conf.get("QUERY");
		HashSet<Integer> queryList = getQueryList(query);
		InputSplit inputSplit = context.getInputSplit();
		String fileName = ((FileSplit) inputSplit).getPath().getName();
		
		if(fileName.toString().startsWith(LKNFile)){
			String strLine = value.toString();
			if(!strLine.startsWith("#")&&!strLine.isEmpty()){
	  			//remove bid
	  			strLine = strLine.substring(strLine.indexOf(" ")+1);
	  			int kid = Integer.parseInt(strLine.substring(0, strLine.indexOf(" ")));
	  			if(queryList.contains(kid)){
					//System.out.println("|||||||||"+strLine);
					IntWritable bid = new IntWritable(0);
					String[] indexA = strLine.split(" ");
					for(int i=1;i<indexA.length;i++){
						//if(indexA[i].substring(0,strLine.indexOf(",")).equals("1.0")){
							Text word = new Text(LKNFile+" "+indexA[i]);
							context.write(bid,word);
						//}
					}
	  			}
			}
		}
		else{
			//String valueStr = value.toString();
			//if(valueStr.startsWith(SOLUTION) || valueStr.startsWith(SOLUNUM)){
				System.out.println(value.toString());
				String[] temp;
				String delimiter = "	";
				temp=value.toString().split(delimiter);
				if(temp.length>1){
					context.write(new IntWritable(0), new Text(temp[1]));
				}
			//}
		}
	}

}
