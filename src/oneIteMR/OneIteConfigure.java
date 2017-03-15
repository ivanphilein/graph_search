package oneIteMR;
/**
 * Class keywordSearch used to do keyword search based on a directed or undirected graph
 * Write by Yifan Hao
 * 
 */
 

import java.util.Date;
import java.util.HashSet;


import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.conf.Configured;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;
import org.apache.hadoop.util.Tool;
import org.kohsuke.args4j.CmdLineException;
import org.kohsuke.args4j.CmdLineParser;


/**
 * This is a class based on Hadoop Map/Reduce application.
 * It reads the graph input files, the map part can divide the huge graph to smaller part and partition that, each reducer will get the corresponding input file and do the search on sub-graph, the last step is merge all the sub-answers then output it.
 *
 * To run: 
 */
public class OneIteConfigure extends Configured implements Tool { 
	final private int partition = 50;
	
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
	
	
    @Override
    public int run(String[] args) throws Exception {
    	long lDateTimeStart=0;
    	long lDateTimeFinish=0;
    	
    	CmdOneIteMR option = new CmdOneIteMR();
    	CmdLineParser parser = new CmdLineParser(option);
		//1. get command line parameters 
		try {
			parser.parseArgument(args);
		} catch (CmdLineException e) {
		}
    	Configuration conf = new Configuration();
    	String query = option.query;
    	conf.set("QUERY",query);
    	int total = getQueryList(query).size();
    	conf.set("TOTAL",total+"");//total is the number of query keywords
    	conf.set("PARTITION", partition+"");
    	conf.set("TOPK",option.topK+"");
    	conf.set("mapred.map.child.java.opts", "-Xmx1024m");
    	conf.set("mapred.reduce.child.java.opts", "-Xmx2548m");
    	if(option.Dir){
    		conf.setInt("DIR",1);
    	}
    	else{
    		conf.setInt("DIR",0);
    	}
		//conf.set("mapred.map.tasks","10");
		//conf.set("mapred.reduce.tasks","2");
		FileSystem fs = FileSystem.get(conf);
		Path inputPath = new Path(option.input);
        Path outputPath = new Path(option.output+"/iter1/");
        if(fs.exists(outputPath)){
			fs.delete(outputPath, true);
	        //fs.mkdirs(outputPath);
		}
        Job job = new Job(conf);
        job.setJobName("Keyword Search");
		job.setJarByClass(OneIteConfigure.class);
		job.setMapperClass(OneIteMapper.class);
		job.setReducerClass(OneIteReducer.class);

    	FileInputFormat.addInputPath(job, inputPath);
        FileOutputFormat.setOutputPath(job, outputPath);

        job.setOutputKeyClass(IntWritable.class);
        job.setOutputValueClass(DouArrayWritable.class);
        
    	lDateTimeStart = new Date().getTime();
    	boolean success = job.waitForCompletion(true); 
        lDateTimeFinish = new Date().getTime();
        double time = lDateTimeFinish-lDateTimeStart;
        System.out.println("First MapReduce Job Running Time:	"+time+"ms	"+time/1000+"s");
        
        conf = new Configuration();
    	conf.set("TOPK",option.topK+"");
    	conf.set("QUERY",query);

    	conf.set("mapred.map.child.java.opts", "-Xmx2048m");
    	conf.set("mapred.reduce.child.java.opts", "-Xmx2048m");
    	job = new Job(conf);
        job.setJobName("Final search");


        inputPath = new Path(option.output+"/iter1/");
        outputPath = new Path(option.output+"/iter2/");
        if (fs.exists(outputPath))
          fs.delete(outputPath, true);
      
        
        job.setMapperClass(FinalMapper.class);
        job.setReducerClass(FinalReducer.class);
        job.setJarByClass(OneIteConfigure.class);
        
        FileInputFormat.setInputPaths(job, inputPath);
        FileOutputFormat.setOutputPath(job, outputPath);
        job.setOutputKeyClass(IntWritable.class);
        job.setOutputValueClass(Text.class);
        
    	lDateTimeStart = new Date().getTime();
    	success = job.waitForCompletion(true); 
        lDateTimeFinish = new Date().getTime();
        time = lDateTimeFinish-lDateTimeStart;
        System.out.println("SecondMapReduce Job Running Time:	"+time+"ms	"+time/1000+"s");
        
        return success ? 0 : 1;
    }	  
 
}
