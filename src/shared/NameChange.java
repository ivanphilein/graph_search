package shared;

import java.io.BufferedReader;
import java.io.BufferedWriter;
import java.io.DataInputStream;
import java.io.File;
import java.io.FileInputStream;
import java.io.FileNotFoundException;
import java.io.FileWriter;
import java.io.IOException;
import java.io.InputStreamReader;
import java.util.ArrayList;
import java.util.Iterator;
import java.util.List;
import java.util.Map.Entry;
import java.util.TreeMap;

public class NameChange {
	private static final int BIDIRMR = 0;
	private static final int ITEMR = 1;
	private static TreeMap<Double, TreeMap<Integer, List<String>>> resultMap = new TreeMap<Double, TreeMap<Integer, List<String>>>();
	
	public static void addResult(double sum, String str){
		TreeMap<Integer, List<String>> map = resultMap.get(sum);
		if(map==null){
			map = new TreeMap<Integer, List<String>> ();
			resultMap.put(sum, map);
		}
		int vid = Integer.parseInt(str.substring(0, str.indexOf(",(")));
		List<String> strList = map.get(vid);
		if(strList==null){
			strList = new ArrayList<String>();
			map.put(vid, strList);
		}
		strList.add(str+" Sum:"+sum);
	}
	
	public static String changeBiDirMR(String folderFrom, String folderTo, String filename, int filetype, int numKey) throws IOException, InterruptedException{
		String outName="";
		resultMap = new TreeMap<Double, TreeMap<Integer, List<String>>>(); 
		FileInputStream fstream = new FileInputStream(folderFrom+filename);
		// Get the object of DataInputStream
		DataInputStream in = new DataInputStream(fstream);
		BufferedReader br = new BufferedReader(new InputStreamReader(in));
		String strLine;
	  	String[] temp;
	    String delimiter = "	";
		switch(filetype){
			 case BIDIRMR: //read file with nodeid:keyword lists information
			  //Read File Line By Line
				 String QueryStr = "";
				 while ((strLine = br.readLine()) != null)   {
					 strLine = strLine.substring(strLine.indexOf(delimiter)+1);
					 if(strLine.startsWith("query")){
						 QueryStr = strLine;
						 strLine = strLine.substring(strLine.indexOf("query:")+6);
						 outName = "query_"+numKey+"_"+strLine.replace(":", "_")+"_BiDirMR.txt";
						 continue;
					 }
					 temp = strLine.split("Sum:");
					 addResult(Double.parseDouble(temp[1]), temp[0]);
				 }
				 in.close(); 
				 if(outName != ""){
					 File theDir = new File(folderTo);
					  // if the directory does not exist, create it
					  if (!theDir.exists())
					  {
					    boolean result = theDir.mkdir();  
					    if(result){    
					       System.out.println("DIR created");  
					     }

					  }
					 FileWriter outfstream = new FileWriter(folderTo+outName); //true tells to append data.
					 BufferedWriter out = new BufferedWriter(outfstream);
					 out.write(QueryStr);
					 out.write("\n");
					 if(!resultMap.isEmpty()){
						 Iterator<Entry<Double, TreeMap<Integer, List<String>>>> iter = resultMap.entrySet().iterator();
						 while(iter.hasNext()){
							 Entry<Double, TreeMap<Integer, List<String>>> entry = iter.next();
							 Iterator<Entry<Integer, List<String>>> iterSec = entry.getValue().entrySet().iterator();
							 while(iterSec.hasNext()){
								 Entry<Integer, List<String>> secEntry = iterSec.next();
								 List<String> list = secEntry.getValue();
								 Iterator<String> iterList = list.iterator();
								 while(iterList.hasNext()){
									 out.write(iterList.next());
									 out.write("\n");
								 }
							 }
						 }
					 }
					 out.close();
				 }
				 /*
				 Runtime run = Runtime.getRuntime(); 
				 Process p = null;  
				 String cmd = "cp "+folderFrom+filename+" "+folderTo+outName;  
				 try {  
				    p = run.exec(cmd);  

				    p.getErrorStream();  
				    p.waitFor();

				 }  
				 catch (IOException e) {  
				    e.printStackTrace();  
				    System.out.println("ERROR.RUNNING.CMD");  

				 }finally{
				    p.destroy();
				 }  */
		}
		if(resultMap.isEmpty())
			return "";
		return outName;
	}
	
	public static void addIterResult(double sum, String str){
		TreeMap<Integer, List<String>> map = resultMap.get(sum);
		if(map==null){
			map = new TreeMap<Integer, List<String>> ();
			resultMap.put(sum, map);
		}
		int vid = Integer.parseInt(str.substring(0, str.indexOf(" ")));
		List<String> strList = map.get(vid);
		if(strList==null){
			strList = new ArrayList<String>();
			map.put(vid, strList);
		}
		strList.add(str+" Sum:"+sum);
	}
	
	public static String changeIterMR(String folderFrom, String folderTo, String filename, int filetype, int numKey) throws IOException, InterruptedException{
		String outName="";
		resultMap = new TreeMap<Double, TreeMap<Integer, List<String>>>();
		FileInputStream fstream = new FileInputStream(folderFrom+filename);
		// Get the object of DataInputStream
		DataInputStream in = new DataInputStream(fstream);
		BufferedReader br = new BufferedReader(new InputStreamReader(in));
		String strLine;
	  	String[] temp;
	    String delimiter = "	";
		switch(filetype){
			 case BIDIRMR: //read file with nodeid:keyword lists information
			  //Read File Line By Line
				 String queryStr = "";
				 while ((strLine = br.readLine()) != null)   {
					 strLine = strLine.substring(strLine.indexOf(delimiter)+1);
					 if(strLine.startsWith("LPN")){
						 return "";
					 }
					 if(strLine.startsWith("query")){
						 queryStr = strLine;
						 strLine = strLine.substring(strLine.indexOf("query:")+6);
						 outName = "query_"+numKey+"_"+strLine.replace(":", "_")+"_IterMR.txt";
						 continue;
					 }
					 if(strLine.startsWith("Total"))
						 continue;
					 temp = strLine.split("SUM:");
					 addIterResult(Double.parseDouble(temp[1]), temp[0]);
				 }
				 in.close(); 
				 if(outName != ""){
					 File theDir = new File(folderTo);
					  // if the directory does not exist, create it
					  if (!theDir.exists())
					  {
					    boolean result = theDir.mkdir();  
					    if(result){    
					       System.out.println("DIR created");  
					     }

					  }
					 FileWriter outfstream = new FileWriter(folderTo+outName); //true tells to append data.
					 BufferedWriter out = new BufferedWriter(outfstream);
					 out.write(queryStr);
					 out.write("\n");
					 if(!resultMap.isEmpty()){
						 Iterator<Entry<Double, TreeMap<Integer, List<String>>>> iter = resultMap.entrySet().iterator();
						 while(iter.hasNext()){
							 Entry<Double, TreeMap<Integer, List<String>>> entry = iter.next();
							 Iterator<Entry<Integer, List<String>>> iterSec = entry.getValue().entrySet().iterator();
							 while(iterSec.hasNext()){
								 Entry<Integer, List<String>> secEntry = iterSec.next();
								 List<String> list = secEntry.getValue();
								 Iterator<String> iterList = list.iterator();
								 while(iterList.hasNext()){
									 out.write(iterList.next());
									 out.write("\n");
								 }
							 }
						 }
					 }
					 out.close();
				 }
				 /*
				 Runtime run = Runtime.getRuntime(); 
				 Process p = null;  
				 String cmd = "cp "+folderFrom+filename+" "+folderTo+outName;  
				 try {  
				    p = run.exec(cmd);  

				    p.getErrorStream();  
				    p.waitFor();

				 }  
				 catch (IOException e) {  
				    e.printStackTrace();  
				    System.out.println("ERROR.RUNNING.CMD");  

				 }finally{
				    p.destroy();
				 }  */
		}
		if(resultMap.isEmpty())
			return "";
		return outName;
	}
	
	public static void readLogToFile(String folder, String folderTo, String filename, int numK, int iter) throws IOException{

		//FileInputStream fstream = new FileInputStream(folder+"log3.txt_top"+numK);
		FileInputStream fstream = new FileInputStream(folder+"log3.txt_top"+numK);
		System.out.println("Read "+folder+"log"+numK+".txt"+" write:"+folderTo+filename);
		// Get the object of DataInputStream
		DataInputStream in = new DataInputStream(fstream);
		BufferedReader br = new BufferedReader(new InputStreamReader(in));
		String strLine;
		FileWriter outfstream = new FileWriter(folderTo+filename, true); //true tells to append data.
	    BufferedWriter out = new BufferedWriter(outfstream);
	    int i=0;
		while((strLine = br.readLine()) != null){
			i++;
			if(i>iter*2+2){
				break;
			}
			if(i>iter*2){
				out.write(strLine);
			    out.write("\n");
			}
	    }
		 in.close();
		 out.close();
	}
	

	
	public static void readIterLogToFile(String folder, String folderTo, String filename, int numK, int iter, int step) throws IOException{

		FileInputStream fstream = new FileInputStream(folder+"log3.txt_top"+numK);
		System.out.println("Read "+folder+"log3.txt_top"+numK);
		// Get the object of DataInputStream
		DataInputStream in = new DataInputStream(fstream);
		BufferedReader br = new BufferedReader(new InputStreamReader(in));
		String strLine;
		FileWriter outfstream = new FileWriter(folderTo+filename, true); //true tells to append data.
	    BufferedWriter out = new BufferedWriter(outfstream);
	    boolean write = false;
		while((strLine = br.readLine()) != null)   {
			if(strLine.startsWith("initial")){
				if(iter==0){
					write = true;
				}
				else if(iter<0)
					break;
				iter--;
			}
			if(write){
				out.write(strLine);
			    out.write("\n");
			}
	    }
		 in.close();
		 out.close();
	}
	
	public static void main(String[] args) throws IOException, InterruptedException {
		String folder = args[0];
		String storeFolder = args[0];
		String folderTo = args[1];//"results/20130305_results/biDirMRResults/";
		String storeFolderTo = args[1];
		String filename = args[2];//"part-r-00000";
		String keyStr = args[3];
		System.out.println(keyStr);
		String[] keyStrArray = keyStr.split(":");
		int leng = keyStrArray.length;
		int[] keyArray = new int[leng];
		for(int i=0;i<leng;i++){
			keyArray[i] = Integer.parseInt(keyStrArray[i]);
		}
		//int[] keyArray = {2};
		int numQ = Integer.parseInt(args[4]);
		
		int type = Integer.parseInt(args[5]);
		String logfolder = args[6];
		if(type == 0){
			for(int i=0;i<keyArray.length;i++){
				//folder += "output"+keyArray[i]+"/";
				folder += keyArray[i]+"/";
				String store = folder;
				folderTo += keyArray[i]+"/";
				for(int j=0;j<numQ;j++){
					folder += "output"+j+"/iter2/";
					String outName = changeBiDirMR(folder, folderTo, filename, 0, keyArray[i]);
					folder = store;
					if(outName!="")
						readLogToFile(logfolder, folderTo, outName, keyArray[i], j);
				}
				folder = storeFolder;
				folderTo = storeFolderTo;
			}
		}
		else if(type==1){
			for(int i=0;i<keyArray.length;i++){
				System.out.println("i:"+i+":"+keyArray[i]);
				//folder += "output"+keyArray[i]+"/";
				String store = folder;
				for(int j=0;j<numQ;j++){
					System.out.println("J:"+j);
					folder += "output"+j;
					int numF=3;
					File f = new File(folder+"/depth_"+numF+"/"+filename);
					//System.out.println("file:"+ folder+"/depth_"+numF+"/"+filename);
					while(!f.exists()&&numF>=0) { 
						numF--;
						f = new File(folder+"/depth_"+numF+"/"+filename);
						//System.out.println("file:"+ folder+"/depth_"+numF+"/"+filename);
					}
					if(numF>=0){
						System.out.println("!!!!"+folder+"/depty_"+numF+"/"+filename);
						String outName = changeIterMR(folder+"/depth_"+numF+"/", folderTo, filename, 0, keyArray[i]);
						folder = store;
						if(outName!="")
							readIterLogToFile(logfolder, folderTo, outName, keyArray[i], j, 4);
					}
				}
				folder = storeFolder;
			}
		}
	}
}
