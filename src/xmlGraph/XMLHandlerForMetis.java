package xmlGraph;

/*import java.io.BufferedReader;
import java.io.BufferedWriter;
import java.io.DataInputStream;
import java.io.FileInputStream;
import java.io.FileWriter;
import java.io.IOException;
import java.io.InputStreamReader;
/*import java.util.ArrayList;
import java.util.HashMap;
import java.util.Iterator;
import java.util.List;
import java.util.Map;
import java.util.Map.Entry;*/
import java.io.BufferedReader;
import java.io.BufferedWriter;
import java.util.TreeMap;

import graphIndex.KSearchGraph;

import org.xml.sax.helpers.DefaultHandler;
import org.xml.sax.*;

/*public class XMLHandlerForMetis extends DefaultHandler{

	private String INPUTNODEFILE = "data/DBLP/nodes.txt";
	
	private String NODEADJACENT = "data/DBLP/metisGraphYifanCopy.txt";
	
	
	private BufferedReader inNodeF = null;
	private BufferedReader inEdgeF = null;
	private BufferedWriter outNodeAdjacentF = null;
	
	private static TreeMap<Integer, String> nodeAdjacent = null;
	
	public static int nodeNum = 0;
	public static int edgeNum = 0;
	
	public static String[] nodeAdj = null;
	
	public static void insertNodeAdjacent(int first, int second){
		if(nodeAdjacent == null)
			nodeAdjacent = new TreeMap<Integer, String>();
		if(nodeAdjacent.containsKey(first)){
			String list = nodeAdjacent.get(first);
			nodeAdjacent.remove(first);
			nodeAdjacent.put(first, list+" "+second);
		}
		else{
			nodeAdjacent.put(first, second+"");

			if(nodeAdjacent.size() % 100000 ==0){
				System.out.println(nodeAdjacent.size());
			}
		}
		edgeNum++;
	}
	
	public static void insertNodeAdjArray(int first, int second){
		if(nodeAdj == null)
			nodeAdj = new String[nodeNum];
		if(nodeAdj[first]==null){
			nodeAdj[first] = second+"";
		}
		else
			nodeAdj[first] += " "+second;
		if(nodeAdj[second]==null)
			nodeAdj[second]=first+"";
		else
			nodeAdj[second] += " "+first;
		edgeNum++;
		if(edgeNum % 100000 ==0){
			System.out.println(edgeNum);
		}
	}
	
	public void startDocument() throws SAXException{
		graph = new KSearchGraph();
		try {
			inNodeF = readFile(INPUTNODEFILE);
			//inEdgeF = readFile(INPUTEDGEFILE);
			outNodeAdjacentF = openFile(NODEADJACENT);
			nodeAdj = new String[nodeNum+1];
			System.out.println("nodeNum "+nodeNum);
			nodeAdjacent = new TreeMap<Integer, String>(); 
		} catch (IOException e) {
			// TODO Auto-generated catch block
			e.printStackTrace();
		}
	}
	
	
	XMLHandlerForMetis() {
		super();
	}

	private KSearchGraph graph = null;
	
	public KSearchGraph getGraph(){
		return graph;
	}
	
	private boolean boolGraph = false;
	private boolean boolEdge = false;
	public void startElement(String uri, String localName, String qName, Attributes attributes)
	throws SAXException {
		
		if(qName.equalsIgnoreCase("graph")){
			boolGraph = true;
		}
		if(qName.equalsIgnoreCase("edge")){
			boolEdge = true;
		}
	}

	public void endElement(String uri, String localName, String qName)
	throws SAXException {
		//System.out.println("End Element :" + qName);
	}

	public void characters(char[] ch, int start, int length)
	throws SAXException {
		if(boolGraph){
			boolGraph = false;
		}
		if(boolEdge){
			String strLine = new String(ch, start, length);
			//remove the edge ID
		  	//strLine = strLine.substring(strLine.indexOf(" ")+1);
		  	//get source node ID
		  	int srcid = Integer.parseInt(strLine.substring(0, strLine.indexOf(" ")));
		  	//get rid of srcid
		  	strLine = strLine.substring( strLine.indexOf(" ") + 1);
		  	int tgtid = Integer.parseInt(strLine.substring( 0, strLine.indexOf(" ")));
		  	//System.out.println(srcid+" "+tgtid);
		  	//insertNodeAdjacent(srcid,tgtid);
		  	//insertNodeAdjacent(tgtid,srcid);
		  	insertNodeAdjArray(srcid,tgtid);
		  	insertNodeAdjArray(tgtid,srcid);
		  	
			boolEdge = false;
		}
		
		
	}

	public void endDocument() throws SAXException {
		try {
			closeFile(inNodeF);
			closeFile(inEdgeF);
			closeFile(outNodeAdjacentF);
			
			writeFile(outNodeAdjacentF, nodeNum+" "+edgeNum);
			for(int i=1;i<=nodeNum;i++){
				writeFile(outNodeAdjacentF, nodeAdj[i]);
			}
			//closeFile(outNodeAdjacentF);
			//showFindNodeIDMap();
			
		} catch (IOException e) {
			// TODO Auto-generated catch block
			e.printStackTrace();
		}
		System.out.println("END Document");
	}
	
	
	public BufferedReader readFile(String filename) throws IOException{
		FileInputStream infstream = new FileInputStream(filename);
		DataInputStream in = new DataInputStream(infstream);
		BufferedReader br = new BufferedReader(new InputStreamReader(in));
  		return br;
  	}
	
	public BufferedWriter openFile(String filename) throws IOException{
		FileWriter fstream = new FileWriter(filename);
  		BufferedWriter out = new BufferedWriter(fstream);
  		return out;
  	}
	public void writeFile(BufferedWriter out,String inputStr) throws IOException{
		out.write(inputStr);
		out.write("\r\n");
	}
	public void closeFile(BufferedReader in) throws IOException{
		in.close();
	}
	public void closeFile(BufferedWriter out) throws IOException{
		out.close();
	}
	
}*/