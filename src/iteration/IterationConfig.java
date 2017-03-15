package iteration;

import java.util.Date;

import oneIteUndirMR.OneMapperBFS;

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

public class IterationConfig extends Configured implements Tool { 
	
	
    @Override
    public int run(String[] args) throws Exception {
    	long lDateTimeStart=0;
    	long lDateTimeFinish=0;
    	long sumTime = 0;
    	CmdOption option = new CmdOption();
    	CmdLineParser parser = new CmdLineParser(option);
		//1. get command line parameters 
		try {
			parser.parseArgument(args);
		} catch (CmdLineException e) {
		}
        int iteration = 0;
    	Configuration conf = new Configuration();
    	conf.set("TOPK",option.topK+"");
    	conf.set("QUERY", option.query);
    	
    	conf.set("mapred.map.child.java.opts", "-Xmx512m");
    	conf.set("mapred.reduce.child.java.opts", "-Xmx3048m");
		FileSystem fs = FileSystem.get(conf);
		Path inputPath = new Path(option.input);
        Path outputPath = new Path(option.output+"/iter"+iteration+"/");
        if(fs.exists(outputPath)){
			fs.delete(outputPath, true);
	        //fs.mkdirs(outputPath);
		}
        Job job = new Job(conf);
        job.setJobName("BFS Keyword Search");
		job.setJarByClass(IterationConfig.class);
		job.setMapperClass(OneMapperBFS.class);
		job.setReducerClass(FirstReducer.class);

    	FileInputFormat.addInputPath(job, inputPath);
        FileOutputFormat.setOutputPath(job, outputPath);

        job.setOutputKeyClass(IntWritable.class);
        job.setOutputValueClass(Text.class);
        
    	lDateTimeStart = new Date().getTime();
    	boolean success = job.waitForCompletion(true); 
        lDateTimeFinish = new Date().getTime();
        double time = lDateTimeFinish-lDateTimeStart;
        System.out.println("First MapReduce Job Running Time:	"+time+"ms	"+time/1000+"s");
        sumTime += time;
        long updates = -1;//job.getCounters().findCounter(IterReducer.State.UPDATED).getValue();
        //System.out.println("value of update from initial:"+updates);
        iteration++;
        long topKSum = -1;
        while (updates < 0) {
        	conf = new Configuration();
        	conf.set("mapred.map.child.java.opts", "-Xmx1024m");
        	conf.set("mapred.reduce.child.java.opts", "-Xmx2548m");
        	conf.set("TOPK",option.topK+"");
        	conf.set("QUERY", option.query);
        	conf.set("TOPKSUM", topKSum+"");
        	job = new Job(conf);
        	job.setJobName("iteration search " + iteration);

        	inputPath = new Path(option.output+"/iter" + (iteration - 1) + "/");
        	outputPath = new Path(option.output+"/iter" + (iteration) + "/");
        	fs = FileSystem.get(conf);
        	if (fs.exists(outputPath))
        		fs.delete(outputPath, true);

        	job.setMapperClass(IterMapper.class);
        	job.setReducerClass(IterReducer.class);
        	job.setJarByClass(IterationConfig.class);
        	
        	job.setOutputKeyClass(IntWritable.class);
        	job.setOutputValueClass(Text.class);
        	
        	FileInputFormat.addInputPath(job, inputPath);
        	FileOutputFormat.setOutputPath(job, outputPath);
	      
  		  	//JobClient.runJob(job);
        	lDateTimeStart = new Date().getTime();
        	job.waitForCompletion(true);
        	lDateTimeFinish = new Date().getTime();
        	time = lDateTimeFinish-lDateTimeStart;
        	System.out.println("iteration "+iteration+" MapReduce Job Running Time:	"+time+"ms	"+time/1000+"s");
        	sumTime += time;
        	iteration++;
        	updates = job.getCounters().findCounter(IterReducer.State.UPDATED).getValue();
        	topKSum = job.getCounters().findCounter(IterReducer.Sum.TOPKSUM).getValue();
          
        }//end of while
        
        /*job = new Job(conf);
        job.setJobName("Keyword search " + iteration);


        inputPath = new Path(option.output+"/depth_" + (iteration - 1) + "/");
        Path LKNinput = new Path(option.input);
        outputPath = new Path(option.output+"/depth_" + (iteration) + "/");
        if (fs.exists(outputPath))
        	fs.delete(outputPath, true);
      
        
        job.setMapperClass(MapperForCompare.class);
        job.setReducerClass(ReducerForCompare.class);
        job.setJarByClass(KeywordSearch.class);
        
        job.setOutputKeyClass(DoubleWritable.class);
        job.setOutputValueClass(Text.class);
        FileInputFormat.addInputPaths(job, inputPath+","+LKNinput);
        FileOutputFormat.setOutputPath(job, outputPath);
        job.setOutputKeyClass(IntWritable.class);
        job.setOutputValueClass(Text.class);
      
        System.out.println("Final Iteration Job Running Time:	"+time+"ms	"+time/1000+"s");
        sumTime += time;*/
        System.out.println("TOTAL Running time:	"+sumTime);
        
        return success ? 0 : 1;
    }	  
 
}

