package xmlGraph;

import java.io.BufferedReader;

import java.io.BufferedWriter;
import java.io.DataInputStream;
import java.io.FileInputStream;
import java.io.FileWriter;
import java.io.InputStreamReader;
import java.text.SimpleDateFormat;
import java.util.Calendar;
import java.util.Map;
import java.util.TreeMap;
import java.util.ArrayList;

import shared.Debugger;


public class MetisGraph {
	private static String folder = "data/DBLP/";
	//private static String folder = "data/DBLPOnlyAuthor/";
	//private static String folder = "data/compare/";
	//private static String folder = "data/testgraph/";
	//private static String folder = "data/DBLPPaperWithAuthor/";
	private static String NODENUMFILE = folder+"subnodenum.txt";
	private static String EDGEFILE = folder+"subedges.txt";
	private static String METISGRAPH = folder+"metisGraphYifan.txt";
	private static TreeMap<Integer, ArrayList<Integer>> nodeAdjacent = null;
	private static TreeMap<Integer, Object> nodeAdjacentString = null;
	private static int nodeNum = 0;
	private static int edgeNum = 0;
	/**
	 * Insert into node id to its adjacent list map
	 * @param first
	 * @param second
	 * @return
	 */
	public static void insertNodeAdjacent2Map(int first, int second){
		ArrayList<Integer> list = nodeAdjacent.get(first);
		if(list==null){
			list = new ArrayList<Integer>();
			nodeAdjacent.put(first, list);
		}
		list.add(second);
		
		ArrayList<Integer> list2 = nodeAdjacent.get(second);
		if(list2==null){
			list2 = new ArrayList<Integer>();
			nodeAdjacent.put(second, list2);
		}
		list2.add(first);
		
		edgeNum++;
	}
	
	/**
	 * Insert into node id to its adjacent list map
	 * @param first
	 * @param second
	 * @return
	 */
	public static void insertNodeAdjacent2String(int first, int second){
		String list = (String)nodeAdjacentString.get(first);
		if(list==null){
			list = second+"";
		}
		else{
			list += " "+second;
		}
		nodeAdjacentString.put(first, list);
		String list2 = (String)nodeAdjacentString.get(second);
		if(list2==null){
			list2 = first+"";
		}
		else{
			list2 += " "+first;
		}
		nodeAdjacentString.put(second, list2);
		
		edgeNum++;
	}
	
	public static void creatMetisGraph(){
		String DATE_FORMAT_NOW = "yyyy-MM-dd HH:mm:ss";
		Calendar cal = Calendar.getInstance();
		SimpleDateFormat sdf = new SimpleDateFormat(DATE_FORMAT_NOW);
		System.out.println(Debugger.getCallerPosition()
				+"Read file: "+NODENUMFILE +"..."+sdf.format(cal.getTime()));
  		try{
  			  //read node file to get node number
  			  String strLine;
			  FileInputStream infstream = new FileInputStream(NODENUMFILE);
			  DataInputStream in = new DataInputStream(infstream);
			  BufferedReader br = new BufferedReader(new InputStreamReader(in));
			  while ((strLine = br.readLine()) != null)   {
				  	if(strLine.startsWith("#"))
				  		continue;
				  nodeNum = Integer.parseInt(strLine);
				  break;
			  }
			  in.close();
			  cal = Calendar.getInstance();
			  System.out.println(Debugger.getCallerPosition()+"Finish reading"+NODENUMFILE+",nodeNum ="+nodeNum+" "+sdf.format(cal.getTime()));
			  cal = Calendar.getInstance();
			  System.out.println(Debugger.getCallerPosition()+"Read file: "+EDGEFILE +"..."+sdf.format(cal.getTime()));
			  infstream = new FileInputStream(EDGEFILE);
			  in = new DataInputStream(infstream);
  			  br = new BufferedReader(new InputStreamReader(in));
  			  int cnt=0;
  			  if(nodeAdjacent == null) nodeAdjacent = new TreeMap<Integer, ArrayList<Integer>>();
  			  if(nodeAdjacentString == null) nodeAdjacentString = new TreeMap<Integer, Object>();
			  while ((strLine = br.readLine()) != null)   {
				  	if(strLine.startsWith("#")) continue;
				  	
				  	//remove edge id
		  			strLine = strLine.substring(strLine.indexOf(" ")+1);
				  	
				  	//get source node ID
				  	int srcid = Integer.parseInt(strLine.substring(0, strLine.indexOf(" ")));
				  	//get rid of srcid
				  	strLine = strLine.substring( strLine.indexOf(" ") + 1);
				  	int tgtid = Integer.parseInt(strLine.substring( 0, strLine.indexOf(" ")));
				  	
				  	insertNodeAdjacent2Map(srcid,tgtid);
				  	
				  	if((cnt++) % 1000000 ==0){
				  		cal = Calendar.getInstance();
				  		System.out.println(Debugger.getCallerPosition()+"Processed cnt="+cnt+" "+sdf.format(cal.getTime()));
					}
			  }
			  System.out.println("total edges "+cnt);
			  in.close();
			  
			  cal = Calendar.getInstance();
			  System.out.println(Debugger.getCallerPosition()+"Finish reading"+EDGEFILE+", edge number="+edgeNum+" "+sdf.format(cal.getTime()));
			  
			  
  			  FileWriter outfstream = new FileWriter(METISGRAPH);
  	  		  BufferedWriter out = new BufferedWriter(outfstream);
  			  out.write(nodeNum+" "+edgeNum);
  			  out.write("\r\n");
  			  
  			 //Set<Map.Entry<Integer,ArrayList<Integer>>> set= nodeAdjacent.entrySet();
  			 int nodeId = 1;
  			 boolean first = true;
  			 for(Map.Entry<Integer,ArrayList<Integer>> e: nodeAdjacent.entrySet()){
  				  //out.write(e.getKey()+": ");
  				  if(nodeId != e.getKey() && first){
  					  first = false;
  					  System.out.println("ERROR "+ e.getKey());
  				  }
  				  nodeId++;
  				  ArrayList<Integer> list = e.getValue();
  				  for(int i=0;i<list.size();i++){
  					  if(i==list.size()-1)
  						  out.write(list.get(i)+"\n");
  					  else
  						  out.write(list.get(i)+" ");
  				  }
  			  }
  			  /*for(Map.Entry<Integer,Object> e: nodeAdjacentString.entrySet()){
 				 
				  String list = (String)e.getValue();
				  out.write(list+"\n");
			  }*/
			  out.close();
			  cal = Calendar.getInstance();
			  System.out.println(Debugger.getCallerPosition()+"DONE "+sdf.format(cal.getTime()));
  		}catch (Exception e){//Catch exception if any
  			e.printStackTrace();
  		}
	}
	
	public static void main(String argv[]) {
		   try{
			   creatMetisGraph();
			   System.out.println(Debugger.getCallerPosition()+"output to "+METISGRAPH);
				
			}catch(Exception e){
				System.out.println( e.getMessage());
				e.printStackTrace();
			}
	 
	   }
}
