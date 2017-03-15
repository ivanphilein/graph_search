package mapreduce;
import org.kohsuke.args4j.Option;

public class CmdOptionMapReduce {
	
	@Option(name="-input", usage="folder for input")
	public String input = "input";
		
	@Option(name="-output", usage="folder for output")
	public String output = "output"; 
	
	@Option(name="-query", usage="search query")
	public String query = "3594474:3594475";
	//public String query = "1:3";
	
	@Option(name="-nodefile", usage="portal node file")
	public String nodefile = "nodefile.txt"; 
	
	@Option(name="-L_KN", usage="index L_KN file")
	public String L_KN = "L_KN.txt"; 
	
	@Option(name="-filetype", usage="option of binary file or text file, 1: text file 2: binary file")
	public int filetype = 2; 
	
	@Option(name="-topK", usage="Top-K results")
	public int topK = 10; 	
}
