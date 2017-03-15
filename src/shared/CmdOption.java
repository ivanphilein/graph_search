package shared;
import org.kohsuke.args4j.Option;

public class CmdOption {
	
	@Option(name="-folder", usage="folder for input and output")
	//public String folder = "data/DBLPHugeNoRoot/";
	//public String folder = "data/compare/";
	//public String folder = "data/testgraph/";
	public String folder = "data/DBLPPaperWithAuthor/";
	
	@Option(name="-bestFolder", usage="folder for best first search output")
	public String bestFolder = "bestSearch/top10/first/";
	
	@Option(name="-bestFolderMR", usage="folder for best first search output with MapReduce")
	public String bestFolderMR = "bestSearch/top5/";
	
	@Option(name="-ranQueryFolder", usage="output folder for all generated query")
	public String ranQueryFolder = "level/";
	
		
	@Option(name="-nodefile", usage="vertex id to keywords file name")
	public String nodefile = "subnodes.txt"; 
	
	@Option(name="-numQ", usage="number of query")
	public int numQ = 5; 

	@Option(name="-edgefile", usage="Input edge file name")
	public String edgefile = "subedges.txt"; 
	
	@Option(name="-partitionfile", usage="partition file of graph")
	public String partitionfile = "metisGraphYifan.txt.part.50";
	
	@Option(name="-portalF", usage="File with portal nodes")
	public String portalF = "portalfile.txt";
	
	@Option(name="-finalP", usage="final partition file with portal-block")
	public String finalP = "finalPart";
	
	@Option(name="-kid2keyword", usage="keyword id to keyword file name")
	public String kid2keyword = "keywordID.txt"; 
	
	@Option(name="-topK", usage="Top-K results")
	public int topK = 5;
	
	@Option(name="-level", usage="level used for generating query")
	public double level = 6;
	
	@Option(name="-numQSizeStr", usage="numbers of query size")
	//public String numQSizeStr = "2 3 4 6 8";
	public String numQSizeStr = "2 ";
	
	@Option(name="-numB", usage="number of partitions")
	public int numB = 50; 
	
	@Option(name="-numK", usage="number of keyword")
	public int numK = 2; 
	
	//////////////////////////////////////////////////////////////////////////////////////////////////////////
	//used to generate running command
	@Option(name="-packageStr", usage="running package")
	//public String packageStr = "oneIteUndirMR";
	public String packageStr = "iteration";
	//////////////////////////////////////////////////////////////////////////////////////////////////////////

	
	//////////////////////////////////////////////////////////////////////////////////////////////////////////
	//cmd part for MapReduce
	@Option(name="-queryfolder", usage="input folder for MapReduce")
	public String queryfolder = "randomQuery/bestQuery/";
	
	@Option(name="-queryFile", usage="input folder for MapReduce")
	public String queryfile = "bestQuery_";//need to add the element from numQSizeStr 
	
	@Option(name="-input", usage="input folder for MapReduce")
	public String input = "input"; 
	
	@Option(name="-output", usage="output folder form MapReduce")
	public String output = "output";
	
	@Option(name="-query", usage="search query")
	public String query = "1:6";
	
	//End of MapReduce part
	//////////////////////////////////////////////////////////////////////////////////////////////////////////
	
	
}
