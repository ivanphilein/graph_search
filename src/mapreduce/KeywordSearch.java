package mapreduce;
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
import org.apache.hadoop.io.DoubleWritable;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.input.SequenceFileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;
import org.apache.hadoop.mapreduce.lib.output.SequenceFileOutputFormat;
import org.apache.hadoop.util.Tool;
import org.kohsuke.args4j.CmdLineException;
import org.kohsuke.args4j.CmdLineParser;



/**
 * This is a class based on Hadoop Map/Reduce application.
 * It reads the graph input files, the map part can divide the huge graph to smaller part and partition that, each reducer will get the corresponding input file and do the search on sub-graph, the last step is merge all the sub-answers then output it.
 *
 * To run: 
 */
public class KeywordSearch extends Configured implements Tool { 
	final private int TEXTFILE = 1;
	final private int BINARYFILE = 2;
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
    	long SumTime = 0;
    	long lDateTimeStart=0;
    	long lDateTimeFinish=0;
    	
    	CmdOptionMapReduce option = new CmdOptionMapReduce();
    	CmdLineParser parser = new CmdLineParser(option);
		//1. get command line parameters 
		try {
			parser.parseArgument(args);
		} catch (CmdLineException e) {
		}
    	int iteration = 0;
    	Configuration conf = new Configuration();
    	String query = option.query;
    	conf.set("QUERY",query);
    	int total = getQueryList(query).size();
    	conf.set("TOTAL",total+"");//total is the number of query keywords
    	conf.set("PARTITION", partition+"");
    	conf.set("SOLUTION", "");
    	conf.setInt("TOPK",option.topK);
    	conf.setBoolean("STOP",false);

    	conf.set("mapred.map.child.java.opts", "-Xmx1024m");
    	conf.set("mapred.reduce.child.java.opts", "-Xmx2548m");
		//conf.set("mapred.map.tasks","10");
		//conf.set("mapred.reduce.tasks","2");
		FileSystem fs = FileSystem.get(conf);
		Path inputPath = new Path(option.input);
        Path outputPath = new Path(option.output+"/depth_" + (iteration) + "/");
        if(fs.exists(outputPath)){
			fs.delete(outputPath, true);
	        //fs.mkdirs(outputPath);
		}
        
        Job job = new Job(conf);
        job.setJobName("Keyword Search");
		job.setJarByClass(KeywordSearch.class);
		job.setMapperClass(KSearchMapper.class);
		job.setReducerClass(KSearchReducerWithBID.class);
		

    	switch(option.filetype){
    	case TEXTFILE:
    		FileInputFormat.addInputPath(job, inputPath);
            FileOutputFormat.setOutputPath(job, outputPath);

            job.setOutputKeyClass(IntWritable.class);
            job.setOutputValueClass(Text.class);
    		break;
    	case BINARYFILE:
    		FileInputFormat.addInputPath(job, inputPath);
    		//job.setInputFormatClass(SequenceFileInputFormat.class);
    	    job.setOutputFormatClass(SequenceFileOutputFormat.class);
    		SequenceFileOutputFormat.setOutputPath(job, outputPath);

            job.setOutputKeyClass(IntWritable.class);
            job.setOutputValueClass(Text.class);
    		break;
    	}
    	lDateTimeStart = new Date().getTime();
        //System.out.println("Date() - Time in milliseconds: " + lDateTimeStart);
        job.waitForCompletion(true);
        lDateTimeFinish = new Date().getTime();
        //System.out.println("Date() - Time in milliseconds: " + lDateTimeFinish);
        //System.out.println("Query:	"+query);
        double time = lDateTimeFinish-lDateTimeStart;
        System.out.println("initial MapReduce Job Running Time:	"+time+"ms	"+time/1000+"s");
        SumTime += time;
			
        long updates = job.getCounters().findCounter(KSearchReducerWithBID.State.UPDATED).getValue();
        //System.out.println("value of update from initial:"+updates);
        iteration++;
        while (updates > 0) {
        	
          conf = new Configuration();
          conf.set("num.iteration", iteration + "");
      	  conf.set("TOTAL", total+"");

      	conf.set("mapred.map.child.java.opts", "-Xmx1024m");
      	conf.set("mapred.reduce.child.java.opts", "-Xmx2548m");
          
          job = new Job(conf);
          job.setJobName("Keyword search " + iteration);


          inputPath = new Path(option.output+"/depth_" + (iteration - 1) + "/");
          outputPath = new Path(option.output+"/depth_" + (iteration) + "/");
          fs = FileSystem.get(conf);
          if (fs.exists(outputPath))
            fs.delete(outputPath, true);
          
          switch(option.filetype){
	      	case TEXTFILE:
	      		//System.out.println("case 1");          
	            job.setMapperClass(LoopMapper1.class);
	            job.setReducerClass(LoopReducerWithBID.class);
	            job.setJarByClass(KeywordSearch.class);
	            
	            job.setOutputKeyClass(IntWritable.class);
	            job.setOutputValueClass(Text.class);
	      		FileInputFormat.addInputPath(job, inputPath);
	            FileOutputFormat.setOutputPath(job, outputPath);
	            job.setOutputKeyClass(IntWritable.class);
	            job.setOutputValueClass(Text.class);
	      		break;
	      	case BINARYFILE:
	      		//System.out.println("case 2");          
	            job.setMapperClass(LoopMapper1.class);
	            job.setReducerClass(LoopReducerWithBID.class);
	            job.setJarByClass(KeywordSearch.class);
	            
	      		job.setInputFormatClass(SequenceFileInputFormat.class);
	            job.setOutputFormatClass(SequenceFileOutputFormat.class);
	      		SequenceFileInputFormat.setInputPaths(job, inputPath);
	            SequenceFileOutputFormat.setOutputPath(job, outputPath);
	            job.setOutputKeyClass(IntWritable.class);
	            job.setOutputValueClass(Text.class);
	      		break;
	      }

  		  //JobClient.runJob(job);
          lDateTimeStart = new Date().getTime();
          //System.out.println("Date() - Time in milliseconds: " + lDateTimeStart);
	      job.waitForCompletion(true);
	      lDateTimeFinish = new Date().getTime();

	        time = lDateTimeFinish-lDateTimeStart;
	        System.out.println("iteration "+iteration+" MapReduce Job Running Time:	"+time+"ms	"+time/1000+"s");
	        SumTime += time;
          iteration++;
          updates = job.getCounters().findCounter(LoopReducerWithBID.State.UPDATED).getValue();
          //System.out.println("value of update  from iterator:"+updates);
        }//end of while
        
////////////////////////////////////////////////////////////////////////////
        /*conf = new Configuration();
        conf.set("QUERY",option.query);
        conf.set("TOTAL",total+"");
    	conf.set("TOPK",option.topK+"");
        if(option.filetype == BINARYFILE){
        	job = new Job(conf);
            job.setJobName("Keyword search " + iteration);

            inputPath = new Path(option.output+"/depth_" + (iteration - 1) + "/");
            Path LKNinput = new Path(option.input);
            outputPath = new Path(option.output+"/depth_" + (iteration) + "/");
            fs = FileSystem.get(conf);
            if (fs.exists(outputPath))
              fs.delete(outputPath, true);
          
	        job.setMapperClass(FinalMapper.class);
	        job.setReducerClass(FinalReducer.class);
	        job.setJarByClass(KeywordSearch.class);
	            
	        job.setInputFormatClass(SequenceFileInputFormat.class);
	      	//SequenceFileInputFormat.setInputPaths(job, inputPath);
	        SequenceFileInputFormat.addInputPaths(job, inputPath+","+LKNinput);
	        FileOutputFormat.setOutputPath(job, outputPath);
	        job.setOutputKeyClass(DoubleWritable.class);
	        job.setOutputValueClass(Text.class);
	        job.waitForCompletion(true);
        }
        else if(option.filetype == TEXTFILE){
        	job = new Job(conf);
            job.setJobName("Keyword search " + iteration);


            inputPath = new Path(option.output+"/depth_" + (iteration - 1) + "/");
            Path LKNinput = new Path(option.input);
            outputPath = new Path(option.output+"/depth_" + (iteration) + "/");
            //fs = FileSystem.get(conf);
            if (fs.exists(outputPath))
              fs.delete(outputPath, true);
          
            
            job.setMapperClass(FinalMapper.class);
            job.setReducerClass(FinalReducer.class);
            job.setJarByClass(KeywordSearch.class);
            
            job.setOutputKeyClass(DoubleWritable.class);
            job.setOutputValueClass(Text.class);
      		//FileInputFormat.addInputPath(job, inputPath);
      		//FileInputFormat.addInputPath(job, LKNinput);
            FileInputFormat.addInputPaths(job, inputPath+","+LKNinput);
            FileOutputFormat.setOutputPath(job, outputPath);
            job.setOutputKeyClass(IntWritable.class);
            job.setOutputValueClass(Text.class);
            
	        job.waitForCompletion(true);
        }*/
        ///////////////////////////////////////////////////////////////////////////////////
        //part just commond by test, need to change it back
        conf = new Configuration();
        conf.set("QUERY",option.query);
        conf.set("TOTAL",total+"");
    	conf.set("TOPK",option.topK+"");
    	conf.set("RUNNINGTIME", SumTime+"");

      	conf.set("mapred.map.child.java.opts", "-Xmx1024m");
      	conf.set("mapred.reduce.child.java.opts", "-Xmx2548m");
        if(option.filetype == BINARYFILE){
        	job = new Job(conf);
            job.setJobName("Keyword search " + iteration);

            inputPath = new Path(option.output+"/depth_" + (iteration - 1) + "/");
            Path LKNinput = new Path(option.input);
            outputPath = new Path(option.output+"/depth_" + (iteration) + "/");
            fs = FileSystem.get(conf);
            if (fs.exists(outputPath))
              fs.delete(outputPath, true);
          
	        job.setMapperClass(MapperForCompare.class);
	        job.setReducerClass(ReducerForCompare.class);
	        job.setJarByClass(KeywordSearch.class);
	        
	        
	            
	        job.setInputFormatClass(SequenceFileInputFormat.class);
	      	//SequenceFileInputFormat.setInputPaths(job, inputPath);
	        SequenceFileInputFormat.addInputPaths(job, inputPath+","+LKNinput);
	        FileOutputFormat.setOutputPath(job, outputPath);
	        job.setOutputKeyClass(DoubleWritable.class);
	        job.setOutputValueClass(Text.class);

        }
        else if(option.filetype == TEXTFILE){
        	job = new Job(conf);
            job.setJobName("Keyword search " + iteration);


            inputPath = new Path(option.output+"/depth_" + (iteration - 1) + "/");
            Path LKNinput = new Path(option.input);
            outputPath = new Path(option.output+"/depth_" + (iteration) + "/");
            //fs = FileSystem.get(conf);
            if (fs.exists(outputPath))
              fs.delete(outputPath, true);
          
            
            job.setMapperClass(MapperForCompare.class);
            job.setReducerClass(ReducerForCompare.class);
            job.setJarByClass(KeywordSearch.class);
            
            job.setOutputKeyClass(DoubleWritable.class);
            job.setOutputValueClass(Text.class);
      		//FileInputFormat.addInputPath(job, inputPath);
      		//FileInputFormat.addInputPath(job, LKNinput);
            FileInputFormat.addInputPaths(job, inputPath+","+LKNinput);
            FileOutputFormat.setOutputPath(job, outputPath);
            job.setOutputKeyClass(IntWritable.class);
            job.setOutputValueClass(Text.class);
        }

        //end of part just commond by test, need to change it back
        ///////////////////////////////////////////////////////////////////////////////////
 
        lDateTimeStart = new Date().getTime();
        boolean success = job.waitForCompletion(true); 
        lDateTimeFinish = new Date().getTime();
        //System.out.println("Date() - Time in milliseconds: " + lDateTimeFinish);

        time = lDateTimeFinish-lDateTimeStart;
        System.out.println("Final MapReduce Job Running Time:	"+time+"ms	"+time/1000+"s");
        SumTime += time;
        System.out.println("TOTAL Running time:	"+SumTime);
        System.out.println("");
        return success ? 0 : 1;
    }	  
    
    /*private Job getPrototypeJob(int iteration,Configuration conf) throws IOException {
        Job job = new Job(conf, "Iteration " + iteration);
		job.setJarByClass(KeywordSearch.class);
        //job.setInputFormatClass(SequenceFileInputFormat.class);
        //job.setOutputFormatClass(SequenceFileOutputFormat.class);
        job.setMapOutputKeyClass(IntWritable.class);
        job.setMapOutputValueClass(Text.class);
        job.setOutputKeyClass(IntWritable.class);
        job.setOutputValueClass(Text.class);
        return job;
        
    }*/
 
}
