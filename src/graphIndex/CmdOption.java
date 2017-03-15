package graphIndex;
import org.kohsuke.args4j.Option;

public class CmdOption {
	
	@Option(name="-folder", usage="folder for input and output")
	//public String folder = "data/DBLPHugeNoRoot/";
	//public String folder = "data/compare/";
	public String folder = "data/testgraph/";
	//public String folder = "data/DBLPPaperWithAuthor/";
		
	@Option(name="-nid2keyword", usage="vertex id to keywords file name")
	public String nid2keyword = "subnodes.txt"; 
	
	@Option(name="numK", usage="number of keywords")
	public String numK = "2"; 

	@Option(name="-edge", usage="Input edge file name")
	public String edgefile = "subedges.txt"; 
	
	@Option(name="-portalF", usage="File with portal nodes")
	public String portalF = "portalfile.txt"; 
	
	
	@Option(name="-kid2keyword", usage="keyword id to keyword file name")
	public String kid2keyword = "keywordID.txt"; 
	
	@Option(name="-metisinput", usage="metisinput file of graph")
	public String metisinput = "metisGraphYifan.txt"; 
	
	@Option(name="-partition", usage="partition file of graph")
	public String partition = "metisGraphYifan.txt.part."; 
	
	@Option(name="-searchkey", usage="Input graph file name")
	public String searchkey = "2010"; 	
	
	//output files
	@Option(name="-L_PN", usage="portal node file")
	public String L_PN = "L_PNUpdateBlockNum.txt"; 
	//public String L_PN = "L_PNNotUpdateBlockNum.txt"; 
	
	@Option(name="-L_KNDericted", usage="index L_KN file")
	public String L_KNDericted = "L_KNDericted.txt"; 
	
	@Option(name="-L_KNUNDericted", usage="index L_KN file")
	public String L_KNUNDericted = "L_KNUNDericted.txt"; 
	
	@Option(name="-K", usage="Top-K results")
	public int K = 10; 	
	@Option(name="-Dericted", usage="Dericted or not")
	public boolean Dericted = false; 
	//public boolean Dericted = false; 
	
	//this part is used for generating the portal-node-blocks, used for second method
	@Option(name="-finalP", usage="final partition file with portal-block")
	public String finalP = "finalPart1"; 
	
	@Option(name="-numB", usage="number of partitions")
	public int numB = 50; 
	
	@Option(name="-level", usage="the level need to grow from partol nodes")
	//public int level = 19; 
	public int level = 6; 
	//public int level = 19; 
	
	@Option(name="-seperate", usage="seperate files for store or not")
	public boolean seperate=false;
}
