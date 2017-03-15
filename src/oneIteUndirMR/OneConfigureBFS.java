package oneIteUndirMR;
/**
 * Class keywordSearch used to do keyword search based on a directed or undirected graph
 * Write by Yifan Hao
 * 
 */
 

import java.util.Date;
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

import shared.CmdOption;


/**
 * This is a class based on Hadoop Map/Reduce application.
 * It reads the graph input files, the map part can divide the huge graph to smaller part and partition that, each reducer will get the corresponding input file and do the search on sub-graph, the last step is merge all the sub-answers then output it.
 *
 * To run: 
 */
public class OneConfigureBFS extends Configured implements Tool { 
	
	
    @Override
    public int run(String[] args) throws Exception {
    	long lDateTimeStart=0;
    	long lDateTimeFinish=0;
    	
    	CmdOption option = new CmdOption();
    	CmdLineParser parser = new CmdLineParser(option);
		//1. get command line parameters 
		try {
			parser.parseArgument(args);
		} catch (CmdLineException e) {
		}
    	Configuration conf = new Configuration();
    	conf.set("TOPK",option.topK+"");
    	conf.set("QUERY", option.query);
    	
    	conf.set("mapred.map.child.java.opts", "-Xmx1024m");
    	conf.set("mapred.reduce.child.java.opts", "-Xmx2548m");
		FileSystem fs = FileSystem.get(conf);
		Path inputPath = new Path(option.input);
        Path outputPath = new Path(option.output+"/iter1/");
        if(fs.exists(outputPath)){
			fs.delete(outputPath, true);
	        //fs.mkdirs(outputPath);
		}
        Job job = new Job(conf);
        job.setJobName("BFS Keyword Search");
		job.setJarByClass(OneConfigureBFS.class);
		job.setMapperClass(OneMapperBFS.class);
		job.setReducerClass(OneReducerBFS.class);

    	FileInputFormat.addInputPath(job, inputPath);
        FileOutputFormat.setOutputPath(job, outputPath);

        job.setOutputKeyClass(IntWritable.class);
        job.setOutputValueClass(Text.class);
        
    	lDateTimeStart = new Date().getTime();
    	boolean success = job.waitForCompletion(true); 
        lDateTimeFinish = new Date().getTime();
        double time = lDateTimeFinish-lDateTimeStart;
        System.out.println("First MapReduce Job Running Time:	"+time+"ms	"+time/1000+"s");
        
        conf = new Configuration();
    	conf.set("TOPK",option.topK+"");
    	conf.set("QUERY", option.query);

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
        job.setJarByClass(OneConfigureBFS.class);
        
        FileInputFormat.setInputPaths(job, inputPath);
        FileOutputFormat.setOutputPath(job, outputPath);
        job.setOutputKeyClass(IntWritable.class);
        job.setOutputValueClass(Text.class);
        
    	lDateTimeStart = new Date().getTime();
    	success = job.waitForCompletion(true); 
        lDateTimeFinish = new Date().getTime();
        time = lDateTimeFinish-lDateTimeStart;
        System.out.println("Second MapReduce Job Running Time:	"+time+"ms	"+time/1000+"s");
        
        return success ? 0 : 1;
    }	  
 
}
