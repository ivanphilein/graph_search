package oneIteMR;


import org.apache.hadoop.util.ToolRunner;

public class Main {
	public static void main(String[] args) throws Exception {
	  	int ret = ToolRunner.run(new OneIteConfigure(), args);  
        System.exit(ret);
	}
}