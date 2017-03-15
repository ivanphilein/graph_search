package shared;

import excel.OperationExcel;

import java.io.BufferedReader;
import java.io.BufferedWriter;
import java.io.DataInputStream;
import java.io.File;
import java.io.FileInputStream;
import java.io.FileWriter;
import java.io.InputStreamReader;
import java.util.Random;
import java.util.TreeMap;
import java.util.TreeSet;

import org.kohsuke.args4j.CmdLineException;
import org.kohsuke.args4j.CmdLineParser;

public class CollectResult {
	OperationExcel excel = new OperationExcel();
	
	
	/**
	 * function for reading results for best first search
	 * @param folderStr
	 * @param outputExcel
	 */
	public void readResult(String folderStr, String outputExcel){
		File folder = new File(folderStr);
		//System.out.println("folder:"+folderStr);
		File[] listOfFiles = folder.listFiles();
		String startStr = "Time:";
		TreeMap<String, String> resultMap = new TreeMap<String, String>(new CompareString());
		TreeSet<String> fileSet = new TreeSet<String>(new CompareString());
		for (File file : listOfFiles) {
		    if (file.isFile()) {
		    	String filename = file.getName();
		    	String runTime;
		    	if(filename.contains(".xls")){
		    		continue;
		    	}
		    	fileSet.add(filename);

	    	try{
	  			  FileInputStream fstream = new FileInputStream(folderStr+filename);
	  			  // Get the object of DataInputStream
	  			  DataInputStream in = new DataInputStream(fstream);
	  			  BufferedReader br = new BufferedReader(new InputStreamReader(in));
	  			  String strLine;
		  	      while ((strLine = br.readLine()) != null)   {
		  	    	  if(strLine.startsWith("#"))
		  	    		  continue;
		  	    	  if(strLine.startsWith(startStr)){
		  	    		  strLine = strLine.substring(strLine.indexOf(":")+1);
		  	    		  runTime = strLine.substring(0, strLine.indexOf(" "));
		  	    		  resultMap.put(filename, runTime);
		  	    		//System.out.println(resultMap.size()+" !!!! "+filename);
		  	    	  }
		  	      }
	  			  //Close the input stream
	  			  in.close();
	  			  
	  		}catch (Exception e){//Catch exception if any
	  			  System.err.println("Error: " + e.getMessage());
	  		}	
		    }
	    	
		}

		OperationExcel excel = new OperationExcel();
		excel.writeDataToExcelFile(outputExcel, resultMap);
	}
	
	public void readResultBFS_OneMR(String basicFolder, int numK, int topK){
		String commandfile = "runBestQueryCom/runbestQuery_"+numK+"_top"+topK+".sh";
		String outputfolder = "output/output_"+numK+"_top"+topK+"/";
		String resultfolder = "results/slave/top"+topK+"/";
		String logfile = "log_"+numK+"_top"+topK;
		String containStr = "-query";
		try{
			  System.out.println("file:"+basicFolder+commandfile);
			  FileInputStream fstream = new FileInputStream(basicFolder+commandfile);
			  // Get the object of DataInputStream
			  DataInputStream in = new DataInputStream(fstream);
			  BufferedReader br = new BufferedReader(new InputStreamReader(in));
			  String strLine;
			  
			  FileInputStream fstreamLog = new FileInputStream(basicFolder+logfile);
			  // Get the object of DataInputStream
			  DataInputStream inLog = new DataInputStream(fstreamLog);
			  BufferedReader brLog = new BufferedReader(new InputStreamReader(inLog));
			  String strLineLog;
			  
			  int read = 0;
			  //read command file
	  	      while ((strLine = br.readLine()) != null)   {
	  	    	  int index = strLine.indexOf(containStr);
	  	    	  //System.out.println("command line:"+strLine+" index:"+index);
	  	    	  if(index != -1){
	  	    		  strLine = strLine.substring(index+7);
	  	    		  strLine = strLine.substring(0, strLine.indexOf(">>"));
	  	    		  String mr_resultfolder = basicFolder+outputfolder+"query_"+read+"/iter2/";
	  	    		  strLine = strLine.replace(":", "_").trim();
	  	    		  
	  	    		  String resultfile = resultfolder+"query_"+numK+"_"+strLine+"_BFSOneMR_top"+topK;
	  	    		  FileWriter outfstream = new FileWriter(resultfile);
	  	    		  BufferedWriter out = new BufferedWriter(outfstream);
	  	    		  out.write("query:"+strLine+"	topK:"+topK+"\n");
	  	    		  
	  	    		  File folder = new File(mr_resultfolder);
	  	    		  System.out.println("folder:"+mr_resultfolder);
	  	    		  File[] listOfFiles = folder.listFiles();
	  	    		  //for loop find the file with results
	  	    		  for (File file : listOfFiles) {
	  	    			  if (file.isFile()) {
	  	    				  String filename = file.getName();
			  	    		  //System.out.println("Filename:"+mr_resultfolder+filename);
	  	    				  try{
	  	    					  //read result file to get query result
	  	    					  FileInputStream finalfstream = new FileInputStream(mr_resultfolder+filename);
					  			  // Get the object of DataInputStream
					  			  DataInputStream inFinal = new DataInputStream(finalfstream);
					  			  BufferedReader brFinal = new BufferedReader(new InputStreamReader(inFinal));
					  			  String strLineFinal;
						  	      while ((strLineFinal = brFinal.readLine()) != null)   {
						  	    	  strLineFinal = strLineFinal.trim();
						  	    	  if(!strLineFinal.isEmpty()){
						  	    		  //System.out.println("strLineFinal:"+strLineFinal);
							  	    	  if(strLineFinal.contains("query:null")){
							  	    		  continue;
							  	    	  }
							  	    	  strLineFinal = strLineFinal.substring(strLineFinal.indexOf("	")+1);
						  	    		  out.write(strLineFinal+"\n");
						  	    	  }
						  	      }
						  	      inFinal.close();
						  	      brFinal.close();
						  	      //end reading result file to get query result
					  			  
	  	    				  }catch (Exception e){//Catch exception if any
	  	    					  System.err.println("Error: " + e.getMessage());
	  	    				  }	
		  	  		    	}//end  of if
	  	    		  	}//end of for loop find the file with results

			  	      	//read log file to get running time
			  	      	int readlog = 0;
			  	      	while((strLineLog = brLog.readLine()) != null){
			  	      		//System.out.println("Log:"+strLineLog);
				  	      	if(strLineLog.startsWith("attempt")){
				  	    		continue;
				  	    	}
			  	      		out.write(strLineLog+"\n");
			  	      		readlog++;
			  	      		if(readlog>1){
			  	      			break;
			  	      		}
			  	      	}
			  	      	//end reading log file to get running time
			  	      	out.close();
	    	    		read++;
	  	          }//end of if that is running command
	  	      }//end of while reading command file
			  in.close();	  
			  brLog.close();
		}catch (Exception e){//Catch exception if any
			  System.err.println("Error: " + e.getMessage());
		}	
	}
	
	
	public void readResultBFS_IterMR(String basicFolder, int numK, int topK){
		String commandfile = "runBestQueryCom/runbestQuery_"+numK+"_top"+topK+".sh";
		String outputfolder = "output/output_"+numK+"_top"+topK+"/";
		String resultfolder = "results/slave/top"+topK+"Iter/";
		String logfile = "log_"+numK+"_top"+topK;
		String containStr = "-query";
		try{
			  //System.out.println("file:"+basicFolder+commandfile);
			  FileInputStream fstream = new FileInputStream(basicFolder+commandfile);
			  // Get the object of DataInputStream
			  DataInputStream in = new DataInputStream(fstream);
			  BufferedReader br = new BufferedReader(new InputStreamReader(in));
			  String strLine;
			  
			  FileInputStream fstreamLog = new FileInputStream(basicFolder+logfile);
			  // Get the object of DataInputStream
			  DataInputStream inLog = new DataInputStream(fstreamLog);
			  BufferedReader brLog = new BufferedReader(new InputStreamReader(inLog));
			  String strLineLog;
			  
			  int read = 0;
			  //read command file
	  	      while ((strLine = br.readLine()) != null)   {
	  	    	  int index = strLine.indexOf(containStr);
	  	    	  //System.out.println("command line:"+strLine+" index:"+index);
	  	    	  if(index != -1){
	  	    		  strLine = strLine.substring(index+7);
	  	    		  strLine = strLine.substring(0, strLine.indexOf(">>"));
	  	    		  String mr_resultfolder = basicFolder+outputfolder+"query_"+read;
	  	    		  
	  	    		  File queryfolder = new File(mr_resultfolder);
	  	    		  File[] listOfFolder = queryfolder.listFiles();
	  	    		  String targetfolder = "";
	  	    		  int biggest = 0;
	  	    		  for(File file : listOfFolder){
	  	    			  if(file.isDirectory()){
	  	    				  String filename = file.getName();
	  	    				  int num = Integer.parseInt(filename.substring(filename.indexOf("r")+1));
	  	    				  if(num>biggest){
	  	    					  biggest = num;
	  	    					  targetfolder = filename;
	  	    				  }
	  	    			  }
	  	    		  }
	  	    		  mr_resultfolder += "/"+targetfolder+"/";
	  	    		  
	  	    		  
	  	    		  strLine = strLine.replace(":", "_").trim();
	  	    		  
	  	    		  String resultfile = resultfolder+"query_"+numK+"_"+strLine+"_BFSOneMR_top"+topK;
	  	    		  FileWriter outfstream = new FileWriter(resultfile);
	  	    		  BufferedWriter out = new BufferedWriter(outfstream);
	  	    		  out.write("query:"+strLine+"	topK:"+topK+"\n");
	  	    		  
	  	    		  /*File folder = new File(mr_resultfolder);
	  	    		  //System.out.println("folder:"+mr_resultfolder);
	  	    		  File[] listOfFiles = folder.listFiles();
	  	    		  //for loop find the file with results
	  	    		  for (File file : listOfFiles) {
	  	    			  if (file.isFile()) {
	  	    				  String filename = file.getName();
			  	    		  //System.out.println("Filename:"+mr_resultfolder+filename);
	  	    				  try{
	  	    					  //read result file to get query result
	  	    					  FileInputStream finalfstream = new FileInputStream(mr_resultfolder+filename);
					  			  // Get the object of DataInputStream
					  			  DataInputStream inFinal = new DataInputStream(finalfstream);
					  			  BufferedReader brFinal = new BufferedReader(new InputStreamReader(inFinal));
					  			  String strLineFinal;
						  	      while ((strLineFinal = brFinal.readLine()) != null)   {
						  	    	  strLineFinal = strLineFinal.trim();
						  	    	  if(!strLineFinal.isEmpty()){
						  	    		  if(!strLineFinal.startsWith("-2")){
							  	    		  continue;
							  	    	  }
							  	    	  strLineFinal = strLineFinal.substring(strLineFinal.indexOf("	")+1);
						  	    		  out.write(strLineFinal+"\n");
						  	    	  }
						  	      }
						  	      inFinal.close();
						  	      brFinal.close();
						  	      //end reading result file to get query result
					  			  
	  	    				  }catch (Exception e){//Catch exception if any
	  	    					  System.err.println("Error: " + e.getMessage());
	  	    				  }	
	  	    			  }//end  of if
	  	    		  }//end of for loop find the file with results
						*/
	  	    		  String filename = "part-r-00000";
	  	    		  //System.out.println("Filename:"+mr_resultfolder+filename);
    				  try{
    					  //read result file to get query result
    					  FileInputStream finalfstream = new FileInputStream(mr_resultfolder+filename);
			  			  // Get the object of DataInputStream
			  			  DataInputStream inFinal = new DataInputStream(finalfstream);
			  			  BufferedReader brFinal = new BufferedReader(new InputStreamReader(inFinal));
			  			  String strLineFinal;
				  	      while ((strLineFinal = brFinal.readLine()) != null)   {
				  	    	  strLineFinal = strLineFinal.trim();
				  	    	  if(!strLineFinal.startsWith("0")){
				  	    		  continue;
				  	    	  }
				  	    	  if(!strLineFinal.isEmpty()){
				  	    		  //System.out.println("strLineFinal:"+strLineFinal);
					  	    	  if(strLineFinal.contains("query:null")){
					  	    		  continue;
					  	    	  }
					  	    	  strLineFinal = strLineFinal.substring(strLineFinal.indexOf("	")+1);
				  	    		  out.write(strLineFinal+"\n");
				  	    	  }
				  	      }
				  	      inFinal.close();
				  	      brFinal.close();
				  	      //end reading result file to get query result
			  			  
    				  }catch (Exception e){//Catch exception if any
    					  System.err.println("Error: " + e.getMessage());
    				  }	
			  	      	//read log file to get running time
			  	      	int readlog = 0;
			  	      	while((strLineLog = brLog.readLine()) != null){
			  	      		System.out.println("Log:"+strLineLog);
				  	      	if(strLineLog.startsWith("attempt") || strLineLog.startsWith("Final")){
				  	    		continue;
				  	    	}
			  	      		out.write(strLineLog+"\n");
			  	      		readlog++;
			  	      		//if(readlog>biggest){
			  	      		if(strLineLog.contains("TOTAL")){
			  	      			//out.write("Total jobs:"+biggest);
			  	      			break;
			  	      		}
			  	      	}
			  	      	//end reading log file to get running time
			  	      	out.close();
	    	    		read++;
	  	          }//end of if that is running command
	  	      }//end of while reading command file
			  in.close();	  
			  brLog.close();
		}catch (Exception e){//Catch exception if any
			  System.err.println("Error: " + e.getMessage());
		}	
	}
	

    public void collecctOneBFSMRResult(String folderStr, String outfile, int topK){
	    try{
	    	System.out.println("Start writing excel file:"+outfile);
	    	File folder = new File(folderStr);
			//System.out.println("folder:"+folderStr);
			File[] listOfFiles = folder.listFiles();
			int startRow = 8;
			int startColumn = 0;
			/*int time = topK/5;
			startColumn = (time-1)*20;*/
			
			
			TreeMap<Integer, TreeMap<TreeSet<Integer>, ResultFileInfo>> storeMap = new TreeMap<Integer, TreeMap<TreeSet<Integer>, ResultFileInfo>>();
			
			for (File file : listOfFiles) {
			    if (file.isFile()) {
			    	String filename = file.getName();
			    	if(filename.contains(".xls") || filename.contains(".swp")){
			    		continue;
			    	}
			    	FileInputStream finalfstream = new FileInputStream(folderStr+filename);
			    	//System.out.println("file:"+filename);
		  			// Get the object of DataInputStream
		  			DataInputStream inFinal = new DataInputStream(finalfstream);
		  			BufferedReader brFinal = new BufferedReader(new InputStreamReader(inFinal));
		  			String strLineFinal;
		  			
		  			TreeSet<Integer> querySet = new TreeSet<Integer>();
		  			String[] temp = filename.split("_");
		  			int numK = Integer.parseInt(temp[1]);
		  			for(int i=0; i<numK; i++){
		  				int key = Integer.parseInt(temp[i+2]);
		  				querySet.add(key);
		  			}
		  			
		  			ResultFileInfo fileInfo = new ResultFileInfo();
		  			//read one file
			  	    while ((strLineFinal = brFinal.readLine()) != null)   {
			  	    	strLineFinal = strLineFinal.trim();
			  	    	/*if(strLineFinal.startsWith("attempt")){
			  	    		continue;
			  	    	}*/
			  	    	if(!strLineFinal.isEmpty()){
			  	    		if(strLineFinal.contains("query")){
			  	    			fileInfo.queryStr = strLineFinal;
			  	    		}
			  	    		else if(strLineFinal.contains("MapReduce")){
			  	    			String[] mrTemp = strLineFinal.split("	");
			  	    			System.out.println(filename+" "+strLineFinal);
			  	    			String timeStr = mrTemp[1].substring(0, mrTemp[1].indexOf("ms"));
			  	    			fileInfo.runTimeList.add(Double.parseDouble(timeStr));
			  	    		}
			  	    		else{
			  	    			double sum = Double.parseDouble(strLineFinal.substring(0, strLineFinal.indexOf(" ")));
			  	    			fileInfo.sumList.add(sum);
			  	    		}
				  	    }
			  	    }
			  	    inFinal.close();
			  	    brFinal.close();

		  			
			  	    TreeMap<TreeSet<Integer>, ResultFileInfo> infoMap = storeMap.get(numK);
		  			if(infoMap==null){
		  				infoMap = new TreeMap<TreeSet<Integer>, ResultFileInfo>(new CompareQuerySet());
		  				storeMap.put(numK, infoMap);
		  			}
			  	    infoMap.put(querySet, fileInfo);
			    }
			}
			excel.writeOneBFSMRToExcelFile(outfile, storeMap, startRow, startColumn);
			
	    	System.out.println("Finish writing excel file:"+outfile);
    	} catch (Exception e) {
	        e.printStackTrace();
	    }
    }
    
    public void collecctIterBFSMRResult(String folderStr, String outfile, int topK){
	    try{
	    	System.out.println("Start writing excel file:"+outfile);
	    	File folder = new File(folderStr);
			//System.out.println("folder:"+folderStr);
			File[] listOfFiles = folder.listFiles();
			int startRow = 8;
			int startColumn = 0;
			
			
			TreeMap<Integer, TreeMap<TreeSet<Integer>, ResultFileInfo>> storeMap = new TreeMap<Integer, TreeMap<TreeSet<Integer>, ResultFileInfo>>();
			
			for (File file : listOfFiles) {
			    if (file.isFile()) {
			    	String filename = file.getName();
			    	if(filename.contains(".xls") || filename.contains(".swp")){
			    		continue;
			    	}
			    	FileInputStream finalfstream = new FileInputStream(folderStr+filename);
			    	//System.out.println("file:"+filename);
		  			// Get the object of DataInputStream
		  			DataInputStream inFinal = new DataInputStream(finalfstream);
		  			BufferedReader brFinal = new BufferedReader(new InputStreamReader(inFinal));
		  			String strLineFinal;
		  			
		  			TreeSet<Integer> querySet = new TreeSet<Integer>();
		  			String[] temp = filename.split("_");
		  			int numK = Integer.parseInt(temp[1]);
		  			for(int i=0; i<numK; i++){
		  				int key = Integer.parseInt(temp[i+2]);
		  				querySet.add(key);
		  			}
		  			
		  			ResultFileInfo fileInfo = new ResultFileInfo();
		  			//read one file
			  	    while ((strLineFinal = brFinal.readLine()) != null)   {
			  	    	strLineFinal = strLineFinal.trim();
			  	    	/*if(strLineFinal.startsWith("attempt")){
			  	    		continue;
			  	    	}*/
			  	    	if(!strLineFinal.isEmpty()){
			  	    		if(strLineFinal.contains("query")||strLineFinal.contains("Query")){
			  	    			fileInfo.queryStr = strLineFinal;
			  	    		}
			  	    		else if(strLineFinal.contains("MapReduce")){
			  	    			String[] mrTemp = strLineFinal.split("	");
			  	    			String timeStr = mrTemp[1].substring(0, mrTemp[1].indexOf("ms"));
			  	    			fileInfo.runTimeList.add(Double.parseDouble(timeStr));
			  	    		}
			  	    		else if(strLineFinal.contains("TOTAL")){
			  	    			continue;
			  	    		}
			  	    		else{
			  	    			double sum = Double.parseDouble(strLineFinal.substring(0, strLineFinal.indexOf(" ")));
			  	    			fileInfo.sumList.add(sum);
			  	    		}
				  	    }
			  	    }
			  	    inFinal.close();
			  	    brFinal.close();

		  			
			  	    TreeMap<TreeSet<Integer>, ResultFileInfo> infoMap = storeMap.get(numK);
		  			if(infoMap==null){
		  				infoMap = new TreeMap<TreeSet<Integer>, ResultFileInfo>(new CompareQuerySet());
		  				storeMap.put(numK, infoMap);
		  			}
			  	    infoMap.put(querySet, fileInfo);
			    }
			}
			excel.writeOneBFSMRToExcelFile(outfile, storeMap, startRow, startColumn);
			
	    	System.out.println("Finish writing excel file:"+outfile);
    	} catch (Exception e) {
	        e.printStackTrace();
	    }
    }
	
	public static void main(String[] args) {
		CmdOption option = new CmdOption();
		CmdLineParser parser = new CmdLineParser(option);
		
		//1. get command line parameters 
		try {
			parser.parseArgument(args);
		} catch (CmdLineException e) {
		}
		CollectResult collect = new CollectResult();
		int type = 1;
		if(type==0){//
			String basicFolder = "../oneBFSSearch/slave/";
			String[] temp = option.numQSizeStr.split(" ");
			for(int i=0; i<temp.length; i++){
				int numK = Integer.parseInt(temp[i]);
				collect.readResultBFS_OneMR(basicFolder, numK, option.topK);
			}
			String outFolder = "results/slave/";
			String folderStr = outFolder+"top"+option.topK+"/";
			String outfile = "resultOneBFSMR_top"+option.topK+".xls";
			collect.collecctOneBFSMRResult(folderStr, folderStr+outfile, option.topK);
		}
		else if(type==1){
			String basicFolder = "../iterBFSSearch/slave/";
			String[] temp = option.numQSizeStr.split(" ");
			for(int i=0; i<temp.length; i++){
				int numK = Integer.parseInt(temp[i]);
				collect.readResultBFS_IterMR(basicFolder, numK, option.topK);
			}
			String outFolder = "results/slave/";
			String folderStr = outFolder+"top"+option.topK+"Iter/";
			String outfile = "resultBFSIterMR_top"+option.topK+".xls";
			collect.collecctIterBFSMRResult(folderStr, folderStr+outfile, option.topK);
		
		}
    }
}
