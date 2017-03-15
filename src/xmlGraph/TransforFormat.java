/**
 * This Class is used for transform the format of graph, from Blinks to Mapreduce or from Mapreduce to Blinks
 */
package xmlGraph;

import java.io.BufferedReader;
import java.io.BufferedWriter;
import java.io.DataInputStream;
import java.io.FileInputStream;
import java.io.FileWriter;
import java.io.IOException;
import java.io.InputStreamReader;

import shared.Debugger;

//class used to transform from graph format to node and edge format
public class TransforFormat {

	public static String folder = "";
	//private static HashMap<Integer, Integer> keyMap = null;
	/**
	 * Transform from graph file to node file, edge file and nodenum file
	 * @param inputfile
	 * @param nodefile
	 * @param edgefile
	 * @param nodeNum
	 */
	@SuppressWarnings("resource")
	public static void transformGraphToNodeEdge(String inputfile, String nodefile, String edgefile, String nodeNum){
		if(folder!=""){
			inputfile = folder+"/"+inputfile;
			nodefile = folder+"/"+nodefile;
			edgefile = folder+"/"+edgefile;
			nodeNum = folder+"/"+nodeNum;
		}
		System.out.println(Debugger.getCallerPosition()+"Read file: "+inputfile +"...");
  		try{
  			  // Open the file that is the first 
  			  // command line parameter
  			  FileInputStream fstream = new FileInputStream(inputfile);
  			  // Get the object of DataInputStream
  			  DataInputStream in = new DataInputStream(fstream);
  			  BufferedReader br = new BufferedReader(new InputStreamReader(in));
  			  
  			  FileWriter writefile = new FileWriter(nodeNum);
  			  BufferedWriter out = new BufferedWriter(writefile);
  			  
  			  String strLine;
  			  strLine = br.readLine();
	  	      int numNode = Integer.parseInt(strLine);
	  	      System.out.println(numNode);
	  	      out.write(strLine);
	  	      out.close();

  			  writefile = new FileWriter(nodefile);
  			  out = new BufferedWriter(writefile);
	  	      for(int i=0;i<numNode; i++){
	  	    	  strLine = br.readLine();
	  	    	  out.write(strLine);
	  	    	  out.write("\r\n");
	  	      }
	  	      out.close();
	  	      writefile = new FileWriter(edgefile);
			  out = new BufferedWriter(writefile);
			  int edgenum = 0;
	  	      while ((strLine = br.readLine()) != null)   {
	  	    	  if(strLine.startsWith("#"))
	  	    		  continue;
	  	    	  edgenum++;
	  	    	  out.write((edgenum)+" "+strLine);
	  	    	  out.write("\r\n");	
	  	      }
	  	      out.close();
  		}catch (Exception e){//Catch exception if any
			  System.err.println("Error: " + e.getMessage());
		}
	}
	
	/**
	 * Read node , edge, and nodenum files, write to the graph file
	 * @param outputfile
	 * @param nodefile
	 * @param edgefile
	 * @param nodeNum
	 */
	public static void transformNodeEdgeToGraph(String outputfile, String nodefile, String edgefile, String nodeNum){
		if(folder!=""){
			outputfile = folder+"/"+outputfile;
			nodefile = folder+"/"+nodefile;
			edgefile = folder+"/"+edgefile;
			nodeNum = folder+"/"+nodeNum;
		}
		System.out.println(Debugger.getCallerPosition()+"Read file: "+nodefile +"...");
  		try{
			  
			  FileWriter writefile = new FileWriter(outputfile);
			  BufferedWriter out = new BufferedWriter(writefile);
  			  // Open the file that is the first 
  			  // command line parameter
  			  FileInputStream fstream = new FileInputStream(nodeNum);
  			  // Get the object of DataInputStream
  			  DataInputStream in = new DataInputStream(fstream);
  			  BufferedReader br = new BufferedReader(new InputStreamReader(in));
  			  
  			  out.write(br.readLine());
  			  out.write("\r\n");
  			  in.close();
  			  br.close();
  			  String strLine;
  			  
  			  fstream = new FileInputStream(nodefile);
			  // Get the object of DataInputStream
			  in = new DataInputStream(fstream);
			  br = new BufferedReader(new InputStreamReader(in));
			  
	  	      while ((strLine = br.readLine()) != null)   {
	  	    	  if(strLine.startsWith("#"))
	  	    		  continue;
	  	    	  out.write(strLine+" ");
	  	    	  out.write("\r\n");	
	  	      }
  			  in.close();
  			  br.close();
	  	      System.out.println(Debugger.getCallerPosition()+"Read file: "+edgefile +"...");
	  	      fstream = new FileInputStream(edgefile);
			  // Get the object of DataInputStream
			  in = new DataInputStream(fstream);
			  br = new BufferedReader(new InputStreamReader(in));
			  //int n=0;
	  	      while ((strLine = br.readLine()) != null)   {
		  	     // System.out.println(strLine);
	  	    	  out.write(strLine.substring(strLine.indexOf(" ")+1)+" ");
	  	    	  out.write("\r\n");	
	  	      }
  			  in.close();
  			  br.close();
	  	      out.close();
	  	      
  		}catch (Exception e){//Catch exception if any
			  System.err.println("Error: " + e.getMessage());
		}
	}
	
	public static void main(String[] args) throws IOException {
		if(args.length<3){
			System.out.println("wrong!");
		}
		if(args.length>4){
			folder = args[4];
		}
		transformNodeEdgeToGraph(args[0], args[1], args[2], args[3]);
		//transformGraphToNodeEdge(args[0], args[1], args[2], args[3]);
	}
}
