package graphIndex;



import java.io.IOException;  
  
/*import org.apache.hadoop.conf.Configured;  
import org.apache.hadoop.fs.Path;  
import org.apache.hadoop.io.IntWritable;  
import org.apache.hadoop.io.LongWritable;  
import org.apache.hadoop.io.Text;  
import org.apache.hadoop.mapreduce.Job;  
import org.apache.hadoop.mapreduce.Mapper;  
import org.apache.hadoop.mapreduce.Reducer;  
import org.apache.hadoop.util.Tool;  
import org.apache.hadoop.util.ToolRunner;  
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;  
import org.apache.hadoop.mapreduce.lib.input.TextInputFormat;  
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;  
import org.apache.hadoop.mapreduce.lib.output.TextOutputFormat; */





public class Main {
	
	/**
	 * @param args
	 */
	public static void main(String[] args) throws IOException {


		//test
		//HashMap<Integer, Integer> intToVertex = new HashMap<Integer, Integer>();
		/*TreeSet<Object> testSet = new TreeSet<Object>(new CompareEdgeCount());
		CompareElement tempElement = new CompareElement();
		tempElement.setVertexId(1);
		tempElement.setCountByNum(1);
		testSet.add(tempElement);
		CompareElement tempElement1 = new CompareElement();
		tempElement.setVertexId(2);
		tempElement1.setCountByNum(2);
		testSet.add(tempElement1);
		
		tempElement.setCountByNum(3);
		testSet.add(tempElement);
		
		Iterator<Object> iter = testSet.iterator();
		while(iter.hasNext()){
			System.out.println(((CompareElement)iter.next()).getCount());
		}
		System.exit(0);*/
		//end of test
			/*CmdOption option = new CmdOption();
			CmdLineParser parser = new CmdLineParser(option);
			ReadAndWrite RWClass = new ReadAndWrite();
			
			//1. get command line parameters 
			try {
				parser.parseArgument(args);
			} catch (CmdLineException e) {
			}
			KSearchGraph storeGraph = new KSearchGraph();
			Partition pClass = new Partition();
			IndexClass indexC = new IndexClass();
			RWClass.Read(RWClass.NID2KEYWORD,option.folder+option.nid2keyword,storeGraph,pClass);
			RWClass.Read(RWClass.EDGEFILE,option.folder+option.edgefile,storeGraph,pClass);
			//RWClass.Read(RWClass.KID2KEYWORD,option.folder+option.kid2keyword,storeGraph);
			pClass.readPortalFile(option.folder+option.partition,storeGraph, indexC);
			pClass.generateLKN(storeGraph, indexC);
			pClass.generatePortalNode(storeGraph, indexC);
			//pClass.showPortalNode(indexC);
			/////
			
			pClass.generateOutPortalAnce(storeGraph, indexC);
			//pClass.writeIndexBToK(storeGraph, indexC);
			RWClass.Write(RWClass.L_PN, option.folder+option.L_PN, storeGraph, indexC,pClass);
			//RWClass.Write(RWClass.L_KN, option.folder+option.L_KN, storeGraph, indexC);
			System.out.println("DONE");*/
			//indexC.showIndexBlockToKeyword();
			//storeGraph.showVertex();
	}

}
