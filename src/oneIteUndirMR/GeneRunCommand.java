package oneIteUndirMR;

import java.io.BufferedReader;
import java.io.BufferedWriter;
import java.io.DataInputStream;
import java.io.FileInputStream;
import java.io.FileWriter;
import java.io.IOException;
import java.io.InputStreamReader;

import org.kohsuke.args4j.CmdLineException;
import org.kohsuke.args4j.CmdLineParser;

import shared.CmdOption;

public class GeneRunCommand {
	public void writeToCommand(String queryfolder, String queryfile, int topK, int numK, String packageStr){
		try{
			String logfile = "log_"+numK+"_top"+topK;
			String commandfile = "runBestQueryCom/"+"run"+queryfile+"_top"+topK+".sh";
			String outFolder = "output_"
						+numK+"_top"+topK;
			
			System.out.println("Write to command file:"+commandfile);
			FileWriter outfstream = new FileWriter(commandfile);
 			BufferedWriter out = new BufferedWriter(outfstream);
 			out.write("rm "+logfile+ "\n");
			
 			System.out.println("Read query file:"+queryfolder+queryfile);
			FileInputStream fstream = new FileInputStream(queryfolder+queryfile);
			DataInputStream in = new DataInputStream(fstream);
			BufferedReader br = new BufferedReader(new InputStreamReader(in));
			String strLine;
			String[] temp;
			String delimiter = " ";
			int line=0;
	  	    while ((strLine = br.readLine()) != null)   {
	  			if(strLine.startsWith("#"))
	  				continue;
	  			temp = strLine.split(delimiter);
	  			String commandStr = "../hadoop-1.0.3/bin/hadoop jar keywordsearch.jar "+packageStr+".Main -input /user/hadoop/input -output output/"
	  					+outFolder+"/query_"+line+" -topK " + topK + " -query ";
	  			line++;
	  			commandStr += temp[0];
	  			for(int i=1;i<temp.length; i++){
	  				commandStr += ":"+temp[i];
	  			}
	  			commandStr += " >> "+logfile;
	  			out.write(commandStr+"\n");
	  	    }
	  	    out.write("rm -rf output/"+outFolder+"\n");
	  	    out.write("../hadoop-1.0.3/bin/hadoop fs -get output/"+outFolder + " output/");
	  	    out.close();
			//Close the input stream
			in.close();
		}catch (Exception e){//Catch exception if any
			  System.err.println("Error: " + e.getMessage());
		}	
	}
	
	public static void main(String[] args) throws IOException {
    	CmdOption option = new CmdOption();
		CmdLineParser parser = new CmdLineParser(option);
		
		//1. get command line parameters 
		try {
			parser.parseArgument(args);
		} catch (CmdLineException e) {
		}
		String delimiter = " ";
		GeneRunCommand genCom = new GeneRunCommand();
		String queryQSize = option.numQSizeStr;
		String[] temp = queryQSize.split(delimiter);
		for(int i=0; i<temp.length; i++){
			int numK = Integer.parseInt(temp[i]);
			genCom.writeToCommand(option.queryfolder, option.queryfile+numK, option.topK, numK, option.packageStr);
		}
    }
}
