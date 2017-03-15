package mapreduce;



import org.apache.hadoop.util.ToolRunner;

public class Main {
	public static void main(String[] args) throws Exception {
	  	int ret = ToolRunner.run(new KeywordSearch(), args);  
        System.exit(ret);
	}
}
