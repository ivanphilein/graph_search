package xmlGraph;
import org.kohsuke.args4j.Option;

public class XMLCmdOption {
	
	@Option(name="-xmlinput", usage="XML input graph")
	public String xmlinput = "data/xmlGraph/";
		
	@Option(name="-nid2keyword", usage="vertex id to keywords file name")
	public String nid2keyword = "nkeyword.txt"; 
	
	@Option(name="-edge", usage="Input edge file name")
	public String edgefile = "edges.txt"; 
	
	@Option(name="-kid2keyword", usage="keyword id to keyword file name")
	public String kid2keyword = "keywordid.txt"; 
	
	@Option(name="-partition", usage="partition file of graph")
	public String partition = "inputForMetis.txt.part.3"; 
	
	@Option(name="-searchkey", usage="Input graph file name")
	public String searchkey = ""; 	
	
	@Option(name="-nodefile", usage="portal node file")
	public String nodefile = "nodefile.txt"; 
	
	@Option(name="-L_KN", usage="index L_KN file")
	public String L_KN = "L_KN.txt"; 
	
	@Option(name="-K", usage="Top-K results")
	public int K = 10; 	
}

