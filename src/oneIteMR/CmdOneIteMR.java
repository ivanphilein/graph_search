package oneIteMR;

import org.kohsuke.args4j.Option;

public class CmdOneIteMR {
	@Option(name="-input", usage="folder for input")
	public String input = "input";
	
	@Option(name="-folder", usage="folder for input and output")
	//public String folder = "data/DBLPHugeNoRoot/";
	//public String folder = "data/compare/";
	//public String folder = "data/testgraph/";
	public String folder = "data/DBLPPaperWithAuthor/";
	
	@Option(name="-node", usage="node file")
	public String node = "subnodes.txt";
	
	@Option(name="-edge", usage="edge file")
	public String edge = "subedges.txt";
	
	
		
	@Option(name="-output", usage="folder for output")
	public String output = "output"; 
	
	@Option(name="-query", usage="search query")
	public String query = "1:2";
	//public String query = "1:3";
	
	@Option(name="-filetype", usage="option of binary file or text file, 1: text file 2: binary file")
	public int filetype = 2; 
	
	@Option(name="-topK", usage="Top-K results")
	public int topK = 10; 
	
	@Option(name="-Directeded", usage="Directeded or not")
	public boolean Dir = true; 
}
