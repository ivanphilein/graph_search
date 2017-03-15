package iteration;


import org.apache.hadoop.util.ToolRunner;

public class Main {
	public static void main(String[] args) throws Exception {
	  	int ret = ToolRunner.run(new IterationConfig(), args);  
        System.exit(ret);
	}
}