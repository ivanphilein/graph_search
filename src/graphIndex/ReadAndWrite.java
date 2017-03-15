package graphIndex;

import java.io.BufferedReader;
import java.io.BufferedWriter;
import java.io.DataInputStream;
import java.io.FileInputStream;
import java.io.FileWriter;
import java.io.InputStreamReader;
import java.util.HashMap;
import java.util.HashSet;
import java.util.Iterator;
import java.util.Map;
import java.util.Map.Entry;
import java.util.TreeSet;

import shared.Debugger;
import shared.IndexClass;
import shared.IndexElement;

public class ReadAndWrite {

	public final int NID2KEYWORD = 1;
	public final int EDGEFILE = 2;
	public final int KID2KEYWORD = 3;
	public final int PARTITION = 4;
	
	public final int L_KN = 6;
	public final int L_PN = 5;
	public final int METISINPUT = 8;
	public boolean READKEY = true;
	
	
	/**
	 * Read function
	 * @param filetype
	 * @param filename
	 * @param graph store graph
	 * @param sQuery store query
	 */
  	public void ReadNodeEdge(int filetype, String filename, KSearchGraph graph, Partition pClass, boolean store){
  		System.out.println(Debugger.getCallerPosition()+"Read file: "+filename +"...");
  		try{
  			  // Open the file that is the first 
  			  // command line parameter
  			  FileInputStream fstream = new FileInputStream(filename);
  			  // Get the object of DataInputStream
  			  DataInputStream in = new DataInputStream(fstream);
  			  BufferedReader br = new BufferedReader(new InputStreamReader(in));
  			  String strLine;
		  	  String[] temp;
	  	      String delimiter = " ";
	  	      int read = 0;
			  switch(filetype){
				  	case NID2KEYWORD: //read file with nodeid:keyword lists information
				  		//Read File Line By Line
				  		while ((strLine = br.readLine()) != null)   {
				  			if(strLine.startsWith("#"))
				  				continue;
				  	        if(READKEY){
							  	//get source node ID
							  	int vid = Integer.parseInt(strLine.substring(0, strLine.indexOf(" ")));
				  	        	
							  	
							  	//without adding keyword
							  	if(store){
							  		strLine = strLine.substring(strLine.indexOf(" ")+1);
						  			graph.writeKeywordList(vid, strLine);
							  	}
							  	else {
							  		graph.writeKeywordList(vid);
							  	}
					  			/* //Do not delete until remember what that mean?
					  			if(!strLine.trim().contains(" ")){
					  				graph.writeKeywordList(vid);
					  			}
					  			else{
					  				graph.writeKeywordList(vid, strLine);
					  			}*/
				  	        	
				  	        }
				  	        else{
				  	        	int vid = Integer.parseInt(strLine.substring(0, strLine.indexOf(" ")));
				  	        	graph.addVertex(vid);
				  	        }
				  	        read++;
				  	        if(read%100000==0)
				  	        	System.out.println("read node num:"+read);
				  		}
				  		break;
				  	case EDGEFILE: //read edge file
				  		//Read File Line By Line
				  		while ((strLine = br.readLine()) != null)   {
				  			if(strLine.startsWith("#"))
				  				continue;
				  	        temp=strLine.split(delimiter);
				  	        if(temp.length>=4){
				  	        	graph.writeUnDirectededEdgeInfo(read, Integer.parseInt(temp[1]), Integer.parseInt(temp[2]),Double.parseDouble(temp[3]));
				  	        	
					  	      	//pClass.generateCompareElementMap(Integer.parseInt(temp[1]), Integer.parseInt(temp[2]), graph);
				  	        }
				  	        read++;
				  	        if(read%100000==0)
				  	        	System.out.println("read edge num:"+read);
				  		}
				  		break;
				  	case KID2KEYWORD: //read file with keywordid:keyword information
				  		//Read File Line By Line
				  		while ((strLine = br.readLine()) != null)   {
				  			if(strLine.startsWith("#"))
				  				continue;
				  	        /*temp=strLine.split(delimiter);
				  	        if(temp.length>=2){
				  	        	graph.writeKeywordToInt(temp[1], Integer.parseInt(temp[0]));
				  	        }*/
				  	        read++;
				  	        if(read%10000==0)
				  	        	System.out.println("read keyword num:"+read);
				  		}
				  		graph.setKeywordNum(read);
				  		break;
				  		
			  }
  			  //Close the input stream
  			  in.close();
  		}catch (Exception e){//Catch exception if any
  			  System.err.println("Error: " + e.getMessage());
  		}	
  		
  		System.out.println(Debugger.getCallerPosition()+"Finish reading file: "+filename +"...");
  	}//end of Read functon
	
  	public void ReadEdge(int filetype, String filename, KSearchGraph graph, Partition pClass, boolean dericted){
  		System.out.println(Debugger.getCallerPosition()+"Read file: "+filename +"...");
  		try{
  			  // Open the file that is the first 
  			  // command line parameter
  			  FileInputStream fstream = new FileInputStream(filename);
  			  // Get the object of DataInputStream
  			  DataInputStream in = new DataInputStream(fstream);
  			  BufferedReader br = new BufferedReader(new InputStreamReader(in));
  			  String strLine;
		  	  String[] temp;
	  	      String delimiter = " ";
	  	      int read = 0;
			  switch(filetype){
				  	case EDGEFILE: //read edge file
				  		//Read File Line By Line
				  		while ((strLine = br.readLine()) != null)   {
				  			if(strLine.startsWith("#"))
				  				continue;
				  	        temp=strLine.split(delimiter);
				  	        int vfrom = Integer.parseInt(temp[1]);
				  	        int vto = Integer.parseInt(temp[2]);
				  	        //if(graph.getVertex(vfrom).getBlockList().iterator().next()!=graph.getVertex(vto).getBlockList().iterator().next()){
					  	        if(temp.length>=4){
					  	        	if(dericted)
					  	        		graph.writeDirectedEdgeInfo(Integer.parseInt(temp[0]),Integer.parseInt(temp[1]), Integer.parseInt(temp[2]),Double.parseDouble(temp[3]));
					  	        	else{
					  	        		graph.writeUnDirectededEdgeInfo(read, Integer.parseInt(temp[1]), Integer.parseInt(temp[2]),Double.parseDouble(temp[3]));
					  	        	}
						  	      	//pClass.generateCompareElementMap(Integer.parseInt(temp[1]), Integer.parseInt(temp[2]), graph);
					  	        }
				  	        //}
				  	        read++;
				  	        if(read%100000==0)
				  	        	System.out.println("read edge num:"+read);
				  		}
				  		break;
				  		
			  }
  			  //Close the input stream
  			  in.close();
  		}catch (Exception e){//Catch exception if any
  			  System.err.println("Error: " + e.getMessage());
  		}	
  		
  		System.out.println(Debugger.getCallerPosition()+"Finish reading file: "+filename +"...");
  	}//end of Read functon
  	
  	public void ReadPortalEdge(int filetype, String filename, KSearchGraph graph, Partition pClass, boolean dericted){
  		System.out.println(Debugger.getCallerPosition()+"Read file: "+filename +"...");
  		try{
  			  // Open the file that is the first 
  			  // command line parameter
  			  FileInputStream fstream = new FileInputStream(filename);
  			  // Get the object of DataInputStream
  			  DataInputStream in = new DataInputStream(fstream);
  			  BufferedReader br = new BufferedReader(new InputStreamReader(in));
  			  String strLine;
		  	  String[] temp;
	  	      String delimiter = " ";
	  	      int read = 0;
			  switch(filetype){
				  	case EDGEFILE: //read edge file
				  		//Read File Line By Line
				  		while ((strLine = br.readLine()) != null)   {
				  			if(strLine.startsWith("#"))
				  				continue;
				  	        temp=strLine.split(delimiter);
				  	        int vfrom = Integer.parseInt(temp[1]);
				  	        int vto = Integer.parseInt(temp[2]);
				  	        if(graph.getVertex(vfrom).getBlockList().iterator().next()!=graph.getVertex(vto).getBlockList().iterator().next()){
					  	        if(temp.length>=4){
					  	        	if(dericted)
					  	        		graph.writeDirectedEdgeInfo(Integer.parseInt(temp[0]),Integer.parseInt(temp[1]), Integer.parseInt(temp[2]),Double.parseDouble(temp[3]));
					  	        	else
					  	        		graph.writeUnDirectededEdgeInfo(read, Integer.parseInt(temp[1]), Integer.parseInt(temp[2]),Double.parseDouble(temp[3]));
					  	        	
						  	      	//pClass.generateCompareElementMap(Integer.parseInt(temp[1]), Integer.parseInt(temp[2]), graph);
					  	        }
				  	        }
				  	        read++;
				  	        if(read%100000==0)
				  	        	System.out.println("read edge num:"+read);
				  		}
				  		break;
				  		
			  }
  			  //Close the input stream
  			  in.close();
  		}catch (Exception e){//Catch exception if any
  			  System.err.println("Error: " + e.getMessage());
  		}	
  		
  		System.out.println(Debugger.getCallerPosition()+"Finish reading file: "+filename +"...");
  	}//end of Read functon
  	
  	
	/**
	 * Read function
	 * @param filetype
	 * @param filename
	 * @param graph store graph
	 * @param sQuery store query
	 */
  	public void Read(int filetype, String filename, KSearchGraph graph, Partition pClass, IndexClass indexC){
  		System.out.println(Debugger.getCallerPosition()+"Read file: "+filename +"...");
  		try{
  			  // Open the file that is the first 
  			  // command line parameter
  			  FileInputStream fstream = new FileInputStream(filename);
  			  // Get the object of DataInputStream
  			  DataInputStream in = new DataInputStream(fstream);
  			  BufferedReader br = new BufferedReader(new InputStreamReader(in));
  			  String strLine;
		  	  String[] temp;
	  	      String delimiter = " ";
	  	      int read = 0;
			  switch(filetype){
				  	case L_PN:
				  		//Read File Line By Line
				  		while ((strLine = br.readLine()) != null)   {
				  			if(strLine.startsWith("#"))
				  				continue;
				  	        
				  	        //System.out.println(strLine);
				  	        temp=strLine.split(delimiter);
				  	        int bid = Integer.parseInt(temp[0]);
				  	        pClass.addBlock2Num(bid);
				  	        int vid = Integer.parseInt(temp[1]);
				  	        //System.out.println("bid1:"+bid+" vid:"+vid+" "+graph.getVertexMap().size());
				  	        VertexClass tempVertex = graph.getVertex(vid);
				  	        //System.out.println("bid2:"+bid+" vid:"+vid);
				  	        tempVertex.addToBlockList(bid);
				  	        //System.out.println("bid3:"+bid+" vid:"+vid);
				  	        indexC.addBlockWithNodeID(bid, vid);
				  	       // System.out.println("bid4:"+bid+" vid:"+vid);
				  	        /*if(temp.length>2){
				  	        	TreeSet<IndexElement> anceSet = new TreeSet<IndexElement>(new CompareIndexElement());
				  	        	for(int i=2;i<temp.length;i++){
				  	        		IndexElement tempElement = new IndexElement(temp[i]);
				  	        		anceSet.add(tempElement);
				  	        	}
				  	        	tempVertex.addToOutportalMap(bid, anceSet);
				  	        }*/
				  	        read++;
				  	        if(read%10000==0)
				  	        	System.out.println("read L_PN num:"+read);
				  		}
				  		break;
				  		
			  }
  			  //Close the input stream
  			  in.close();
  		}catch (Exception e){//Catch exception if any
  			  System.err.println("Error: " + e.getMessage());
  		}	
  		
  		System.out.println(Debugger.getCallerPosition()+"Finish reading file: "+filename +"...");
  	}//end of Read functon
  	
  	
  	public void Write(int filetype, String filename, KSearchGraph graph, IndexClass indexC, Partition pClass){
  		System.out.println(Debugger.getCallerPosition()+"Write file: "+filename +"...");
  		try{
  			 FileWriter fstream = new FileWriter(filename);
  			 BufferedWriter out = new BufferedWriter(fstream);
  			 switch(filetype){
  			 case L_PN:
  				out.write("#blockID VertexID L_PN\r\n");
  				HashMap<Integer, VertexClass> allVertexMap = graph.getVertexMap();
  				Iterator<Entry<Integer, VertexClass>> iter = allVertexMap.entrySet().iterator();
  				while(iter.hasNext()){
  					@SuppressWarnings("rawtypes")
  					Map.Entry entry = (Map.Entry) iter.next(); 
  					int vid  = (Integer)entry.getKey(); 
  					//out.write(vid+"");
  					VertexClass tempVertex = graph.getVertex(vid);
  					HashSet<Integer> bList = tempVertex.getBlockList();//indexC.getNodeWithBlockList(vid);
  					Iterator<Integer> bListIter = bList.iterator();
  					HashSet<Integer> blockList = tempVertex.getBlockList();
  					if(blockList!=null){
	  					while(bListIter.hasNext()){
	  						int bid = bListIter.next();
	  	  					String outportal = graph.getVertex(vid).getOutPortalListElement(bid);
	  						out.write(bid+" "+vid);
	  						String outStr = tempVertex.generateOutPortalBlock();
	  						if(outStr != null){
	  							out.write(" "+outStr);
	  						}
	  						
	  	  					if(outportal != null && blockList.contains(bid)){
	  	  	  					out.write(" ");
	  	  	  					out.write(outportal);
	  	  					}
	  	  					out.write("\r\n");
	  	  					
	  					}
  					}
  					else{
  						int bid = bListIter.next();
  	  					String outportal = graph.getVertex(vid).getOutPortalListElement(bid);
  						out.write(bid+" "+vid);
  	  					if(outportal != null){
  	  	  					out.write(" ");
  	  	  					out.write(outportal);
  	  					}
  	  					out.write("\r\n");
  					}
  				}
  				break;
  			 case L_KN:
  				 out.write("#blockID keywordID L_KN\r\n");
  				 HashMap<Integer, Integer> blockMap = pClass.getBlockNumMap();
  				 //HashMap<String, Integer> keywordToInt = graph.getKeywordToInt();
  				 Iterator<Entry<Integer, Integer>> bidIter = blockMap.entrySet().iterator();
  				 int keyNum = graph.getKeywordNum();
  				 System.out.println("OKOK "+blockMap.size()+" keynum "+keyNum);
  				 while(bidIter.hasNext()){
  					 @SuppressWarnings("rawtypes")
  					 Map.Entry entryBMap = (Map.Entry) bidIter.next(); 
  					 int bid = (Integer)entryBMap.getKey();
  					 for(int kid=1;kid<=keyNum;kid++){
  						TreeSet<IndexElement> LKNSet = indexC.getIndexBToK(bid, kid);
  						if(bid==25 && kid==2)
  							System.out.println("Find bid:"+bid+" kid:"+kid);
  			    	    if(LKNSet!=null){
  	  						System.out.println("bid:"+bid+" kid:"+kid);
	  			    	    out.write(bid+" "+kid);
	  			    	    Iterator<IndexElement> itLKN = LKNSet.iterator();
	  			    	    //boolean first = true;
	  			    	    while(itLKN.hasNext()){
	  			    	    	out.write(" ");
	  			    	    	IndexElement tempElement = itLKN.next();
	  			    	    	out.write(tempElement.getElement());
	  			    	    }
	  			    	    out.write("\r\n");
  			    	    }
  					 }
  	  				 /*Iterator<Entry<String, Integer>> iterKMap =  keywordToInt.entrySet().iterator();
  					 while(iterKMap.hasNext()){
  						@SuppressWarnings("rawtypes")
  						Map.Entry entry = (Map.Entry) iterKMap.next(); 
  			    	    int kid = (Integer) entry.getValue();
  			    	    TreeSet<IndexElement> LKNSet = indexC.getIndexBToK(bid, kid);
  			    	    if(LKNSet!=null){
	  			    	    out.write(bid+" "+kid);
	  			    	    Iterator<IndexElement> itLKN = LKNSet.iterator();
	  			    	    //boolean first = true;
	  			    	    while(itLKN.hasNext()){
	  			    	    	out.write(" ");
	  			    	    	IndexElement tempElement = itLKN.next();
	  			    	    	out.write(tempElement.getElement());
	  			    	    }
	  			    	    out.write("\r\n");
  			    	    }
  					 }*/
  				 }
  				 break;
  			}
  			 //Close the output stream
  			 out.close();
	  	}catch (Exception e){//Catch exception if any
			  System.err.println("Error: " + e.getMessage());
		}	
  		System.out.println(Debugger.getCallerPosition()+"Finish writing file: "+filename +"...");
  	}
}
