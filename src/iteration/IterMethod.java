package iteration;
import java.io.BufferedReader;
import java.io.InputStreamReader;
import java.util.HashMap;
import java.util.Iterator;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.conf.Configured;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.input.TextInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;
import org.apache.hadoop.mapreduce.lib.output.TextOutputFormat;
import org.apache.hadoop.util.Tool;
import org.apache.hadoop.util.ToolRunner;


public class IterMethod extends Configured implements Tool 
{
	public static String OUT = "outfile";
	public static String IN = "OmarInput";
	
	//Almost exactly from http://hadoop.apache.org/mapreduce/docs/current/mapred_tutorial.html
    public int run(String[] args) throws Exception 
    {
        //http://code.google.com/p/joycrawler/source/browse/NetflixChallenge/src/org/niubility/learning/knn/KNNDriver.java?r=242
        getConf().set("mapred.textoutputformat.separator", " ");//make the key -> value space separated (for iterations)
 
        //set in and out to args.
        IN = args[0];
        OUT = args[1];
 
        String infile = IN;
        String outputfile = OUT + System.nanoTime();
 
        boolean isdone = false;
        boolean success = false;
 
        HashMap <Integer, Integer> _map = new HashMap<Integer, Integer>();
 
        while(isdone == false)
        {
 
            Job job = new Job(getConf());
            job.setJarByClass(IterMethod.class);
            job.setJobName("IterMethod");
            job.setOutputKeyClass(LongWritable.class);
            job.setOutputValueClass(Text.class);
            job.setMapperClass(IterMapper.class);
            job.setReducerClass(IterReducer.class);
            job.setInputFormatClass(TextInputFormat.class);
            job.setOutputFormatClass(TextOutputFormat.class);
 
            FileInputFormat.addInputPath(job, new Path(infile));
            FileOutputFormat.setOutputPath(job, new Path(outputfile));
 
            success = job.waitForCompletion(true);
 
            //remove the input file
            //http://eclipse.sys-con.com/node/1287801/mobile
            if(infile != IN){
                String indir = infile.replace("part-r-00000", "");
                Path ddir = new Path(indir);
                FileSystem dfs = FileSystem.get(getConf());
                dfs.delete(ddir, true);
            }
 
            infile = outputfile+"/part-r-00000";
            outputfile = OUT + System.nanoTime();
 
            //do we need to re-run the job with the new input file??
            //http://www.hadoop-blog.com/2010/11/how-to-read-file-from-hdfs-in-hadoop.html
            isdone = true;//set the job to NOT run again!
            Path ofile = new Path(infile);
            FileSystem fs = FileSystem.get(new Configuration());
            BufferedReader br=new BufferedReader(new InputStreamReader(fs.open(ofile)));
 
            HashMap<Integer, Integer> imap = new HashMap<Integer, Integer>();
            String line=br.readLine();
            while (line != null)
            {
                //each line looks like 0 1 2:3:
                //we need to verify node -> distance doesn't change
                String[] sp = line.split(" ");
                int node = Integer.parseInt(sp[0]);
                int distance = Integer.parseInt(sp[1]);
                imap.put(node, distance);
                line=br.readLine();
            }
            if(_map.isEmpty()){
                //first iteration... must do a second iteration regardless!
                isdone = false;
            }
            else
            {
                //http://www.java-examples.com/iterate-through-values-java-hashmap-example
                //http://www.javabeat.net/articles/33-generics-in-java-50-1.html
                Iterator<Integer> itr = imap.keySet().iterator();
                while(itr.hasNext())
                {
                    int key = itr.next();
                    int val = imap.get(key);
                    if(_map.get(key) != val)
                    {
                        //values aren't the same... we aren't at convergence yet
                        isdone = false;
                    }
                }
            }
            
            if(isdone == false)
            {
                _map.putAll(imap);//copy imap to _map for the next iteration (if required)
            }
        }
 
        return success ? 0 : 1;
    }
 
    public static void main(String[] args) throws Exception {
        System.exit(ToolRunner.run(new IterMethod(), args));
    }
}
