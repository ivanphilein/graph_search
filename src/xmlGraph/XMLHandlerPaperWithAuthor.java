package xmlGraph;

import java.io.BufferedReader;
import java.io.BufferedWriter;
import java.io.DataInputStream;
import java.io.FileInputStream;
import java.io.FileWriter;
import java.io.IOException;
import java.io.InputStreamReader;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.Iterator;
import java.util.List;
import java.util.Map;
import java.util.Map.Entry;

import graphIndex.KSearchGraph;

import org.xml.sax.helpers.DefaultHandler;
import org.xml.sax.*;


class edgeLabelPaperWithAuthor{
	int labelID;
	double weight;
	String labelName;
	String labelValue;
	boolean labelHave;
	edgeLabelPaperWithAuthor(){
		labelHave = false;
		weight = 1.0;
	}
};
public class XMLHandlerPaperWithAuthor extends DefaultHandler{

	private String COMMONFILE = "data/common_words.txt";
	private String DERICTION = "data/DBLPPaperWithAuthor/";
	private String NODEFILE = DERICTION+"nodes.txt";
	private String EDGEFILE = DERICTION+"edges.txt";
	private String KEYWORDFILE = DERICTION+"keywordID.txt";
	private String EDGELABEL = DERICTION+"edgelabel.txt";
	private String EDGEINFO = DERICTION+"edgeinfo.txt";
	private String NODECLEANFILE = DERICTION+"nodeclear.txt";
	private String NODEFORCHECK = DERICTION+"nodefrocheck.txt";
	private String ERRORF = DERICTION+"error.txt";
	private String NODENUM = DERICTION+"nodenum.txt";
	
	private BufferedReader inCommonF = null;
	
	private BufferedWriter outError = null;
	
	private BufferedWriter outNodeF = null;
	private BufferedWriter outEdgeF = null;
	private BufferedWriter outKeyIDF = null;
	private BufferedWriter outEdgeLableF = null;
	private BufferedWriter outEdgeInfoF = null;
	private BufferedWriter outNodeCleanF = null;
	private BufferedWriter outNodeNum = null;
	//check part
	private BufferedWriter outNodeForCheck = null;
	//end of check part
	
	private List<Object> qNameList = null;
	
	private HashMap<Object, Integer> keywordIDMap = null;
	private HashMap<Object, edgeLabelPaperWithAuthor> attributeMap = null;
	private HashMap<String, Integer> edgeLabelMap = null;
	private HashMap<Object, HashMap<Object, Integer>> findNodeIDMap = null;
	private List<Object> commonList = null;
	
	private List<String> stringListOneNode = new ArrayList<String>();
	private List<Integer> keyListInOneNode = new ArrayList<Integer>();
	private List<Integer> cleanKeyListInOneNode = new ArrayList<Integer>();
	private HashMap<String, Integer> authorMap = new HashMap<String, Integer>();
	private List<String> authorList = new ArrayList<String>();
	
	//private HashMap<Integer, List<Integer>> authorWithPaper = null;//from author keyword id to papers list
	
	public int nodeID = 0;//Start from 1
	public int edgeID = 0;
	private int keywordID = 1;
	private int edgeLableID = 1;
	
	private final int SPLIT=1;
	private final int NOTSPLIT=0;
	
	public void startDocument() throws SAXException{
		graph = new KSearchGraph();
		try {
			outNodeF = openFile(NODEFILE);
			outEdgeF = openFile(EDGEFILE);
			outKeyIDF = openFile(KEYWORDFILE);
			outEdgeLableF = openFile(EDGELABEL);
			outEdgeInfoF = openFile(EDGEINFO);
			outNodeCleanF = openFile(NODECLEANFILE);
			outNodeForCheck = openFile(NODEFORCHECK);
			outNodeNum = openFile(NODENUM);
			outError = openFile(ERRORF);
			readCommonList();
			
			qNameList = new ArrayList<Object>();

			//test
			String keyword[] = {"article","inproceedings","proceedings","book","incollection","phdthesis","mastersthesis","www"};
			for(int i=0;i<keyword.length;i++)
				qNameList.add(keyword[i].toLowerCase());
			//end of test
			
			//authorWithPaper = new HashMap<Integer, List<Integer>>();
			keywordIDMap = new HashMap<Object, Integer>();
			attributeMap = new HashMap<Object, edgeLabelPaperWithAuthor>();
			edgeLabelMap = new HashMap<String, Integer>();
			//initial edgeLabelMap
			String labelArray[] = {"crossref","author", "editor"};
			for(int i=0; i<labelArray.length;i++){
				edgeLabelMap.put(labelArray[i].toLowerCase(), i+1);
				writeFile(outEdgeLableF, (i+1)+" "+labelArray[i].toLowerCase());
			}
			//end of initial edgeLabelMap
		} catch (IOException e) {
			// TODO Auto-generated catch block
			e.printStackTrace();
		}
	}
	/**
	 * Read common file to generate common list
	 * @throws IOException
	 */
	private void readCommonList() throws IOException{
		if(commonList == null)
			commonList = new ArrayList<Object>();
		inCommonF = readFile(COMMONFILE);
		String strLine;
		while ((strLine = inCommonF.readLine()) != null)   {
		  	if(strLine.startsWith("#"))
		  		continue;
		  	commonList.add(strLine);
		}
		closeFile(inCommonF);
	}
	
	/**
	 * From keyword, remove useless symbols
	 * @param inputStr
	 * @return
	 */
	private String replaceSymbol(String inputStr){
		return inputStr.replace(",", "").replace(".","").replace("'", "").replace("\"", "");
	}

	
	/**
	 * Insert keyword to map
	 * @param inputStr
	 * @param split
	 * @return
	 */
	public boolean insertKeywordMap(String inputStr, int split){
		if(keywordIDMap==null)
			keywordIDMap = new HashMap<Object, Integer>();
		if(!inputStr.isEmpty()){
			if(split==SPLIT){
				String insertStr = replaceSymbol(inputStr);
				String[] temp;
				String delimiter = " ";
				temp = insertStr.split(delimiter);
				for(int i=0;i<temp.length;i++){
					String tempStr = temp[i];
					if(!keywordIDMap.containsKey(tempStr)){
						keywordIDMap.put(tempStr, keywordID);
						try {
							writeFile(outKeyIDF,keywordID+" "+tempStr);
						} catch (IOException e1) {
							// TODO Auto-generated catch block
							e1.printStackTrace();
						}
						keywordID++;
					}
				}
			}
			else if(split==NOTSPLIT){
				String insertStr = inputStr;
				String tempStr = insertStr;
				if(!keywordIDMap.containsKey(tempStr)){
					keywordIDMap.put(tempStr, keywordID);
					try {
						writeFile(outKeyIDF,keywordID+" "+tempStr);
					} catch (IOException e1) {
						// TODO Auto-generated catch block
						e1.printStackTrace();
					}
					keywordID++;
				}
			}
			return true;
		}
		return false;
	}

	void showFindNodeIDMap(){
		if(findNodeIDMap == null)
			System.out.println("empty!!!");
		else{
			Iterator<Entry<Object,HashMap<Object, Integer>>> iter = findNodeIDMap.entrySet().iterator();
			while(iter.hasNext()){
				  @SuppressWarnings("rawtypes")
				  Map.Entry entryBMap = (Map.Entry) iter.next(); 
				  String srcMap = (String) entryBMap.getKey();
				  HashMap<Object, Integer> listMap = findNodeIDMap.get(srcMap);
				  Iterator<Entry<Object,Integer>> iterSecond = listMap.entrySet().iterator();
				  System.out.print(srcMap+":");
				  while(iterSecond.hasNext()){
					  @SuppressWarnings("rawtypes")
					  Map.Entry entryBMapSecond = (Map.Entry) iterSecond.next();
					  String keyStr = (String) entryBMapSecond.getKey();
					  int id = listMap.get(keyStr);
					  System.out.print("("+keyStr+","+id+")");
				  }
				  System.out.println("");
			}
		}
	}
	/**
	 * Insert element to findNodeIDMap
	 * @param mapStr this is label name: author or crossref
	 * @param inputStr this is the new key of this label
	 * @return false if this sting in the corresponding map already, else true
	 * @throws IOException
	 */
	boolean insertFindNodeIDMap(String mapStr, String inputStr) throws IOException{
		if(findNodeIDMap == null)
			findNodeIDMap = new HashMap<Object,HashMap<Object, Integer>>();
		HashMap<Object,Integer> nodeIDMap = findNodeIDMap.get(mapStr);
		if(nodeIDMap != null){
			if(nodeIDMap.containsKey(inputStr)){
				return false;
			}
			else{
				nodeIDMap.put(inputStr, nodeID);
			}
		}
		else{
			nodeIDMap = new HashMap<Object, Integer>();
			nodeIDMap.put(inputStr, nodeID);
			findNodeIDMap.put(mapStr, nodeIDMap);
		}
		//writeFile(outAuthorIDF, nodeID+" "+insertStr);
		//outAuthorIDF.write(nodeID+" "+insertStr);
		return true;
	}
	
	/**
	 * Check map contains element or not
	 * @param mapStr
	 * @param inputStr
	 * @return
	 */
	boolean findNodeMapContain(String mapStr, String inputStr){
		if(findNodeIDMap == null)
			return false;
		//String insertStr = replaceSymbol(inputStr);
		HashMap<Object,Integer> nodeIDMap = findNodeIDMap.get(mapStr);
		if(nodeIDMap != null){
			if(nodeIDMap.containsKey(inputStr)){
				return true;
			}
		}
		return false;	
	}
	
	/**
	 * Get node id from findNodeMap
	 * @param mapStr
	 * @param inputStr
	 * @return -1 if can not find
	 */
	int getIDFormFindNodeMap(String mapStr, String inputStr){
		if(findNodeMapContain(mapStr,inputStr))
			return findNodeIDMap.get(mapStr).get(inputStr);
		return -1;	
	}
	
	XMLHandlerPaperWithAuthor() {
		super();
	}

	private KSearchGraph graph = null;
	
	public KSearchGraph getGraph(){
		return graph;
	}
	
	
	public void startElement(String uri, String localName, String qName, Attributes attributes)
	throws SAXException {
		
		if(!qName.equalsIgnoreCase("dblp")){
			if(!attributeMap.containsKey(qName.toLowerCase())){
				edgeLabelPaperWithAuthor newLabel = new edgeLabelPaperWithAuthor();
				newLabel.labelID = edgeLableID;
				newLabel.labelHave = true;
				newLabel.labelName = qName;
				attributeMap.put(qName, newLabel);
			}
			else{
				attributeMap.get(qName).labelHave = true;
			}
			if(qNameList.contains(qName.toLowerCase())){
				authorList.clear();
				stringListOneNode.clear();
			    keyListInOneNode.clear();
			    cleanKeyListInOneNode.clear();
				nodeID++;
				String crossrefKey = attributes.getValue("key");
				if(!crossrefKey.isEmpty()){
					try {
						insertFindNodeIDMap("crossref",crossrefKey);
					} catch (IOException e) {
						// TODO Auto-generated catch block
						e.printStackTrace();
					}
				}
				insertKeywordMap(attributes.getValue(0),NOTSPLIT);
				for(int i=0;i<attributes.getLength();i++){
					String value = attributes.getValue(i);
					stringListOneNode.add(value);
					insertKeywordMap(value,NOTSPLIT);
					int keyid = keywordIDMap.get(value);
					keyListInOneNode.add(keyid);
					if(!commonList.contains(replaceSymbol(value))){
						cleanKeyListInOneNode.add(keyid);
					}
				}
			}
		}
	}

	public void endElement(String uri, String localName, String qName)
	throws SAXException {
		if(qNameList.contains(qName.toLowerCase())){
			try {
				if(keyListInOneNode.size()!=0 && cleanKeyListInOneNode.size()!=0){
					writeFile(outNodeF,nodeID,keyListInOneNode);
					writeFile(outNodeCleanF, nodeID, cleanKeyListInOneNode);
					
					//for check part
					writeCheck(outNodeForCheck,nodeID , stringListOneNode);
					//end of check part
				}
				else{
					writeCheck(outError,nodeID , stringListOneNode);
				}
				if(authorList!=null){
					Iterator<String> iterAu = authorList.iterator();
					int article = nodeID;
					while(iterAu.hasNext()){
						String haveAuthor = iterAu.next();
						int authorid = nodeID+1;
						edgeID++;
						if(authorMap.containsKey(haveAuthor)){
							authorid = authorMap.get(haveAuthor);
							//System.out.println(authorid);
							writeFile(outEdgeF,edgeID+" "+article+" "+authorid+" 1.0");
							writeFile(outEdgeInfoF, edgeID+" "+edgeLabelMap.get("author"));
						}
						else{
							nodeID++;
							writeFile(outNodeF,nodeID+" 1.0 "+keywordIDMap.get(haveAuthor));
							writeFile(outNodeCleanF,nodeID+" 1.0 "+keywordIDMap.get(haveAuthor));
							writeFile(outNodeForCheck,nodeID+" 1.0 "+haveAuthor);
							authorMap.put(haveAuthor, authorid);
							writeFile(outEdgeF,edgeID+" "+article+" "+authorid+" 1.0");
							writeFile(outEdgeInfoF, edgeID+" "+edgeLabelMap.get("author"));
							//
						}
					}
				}
			} catch (IOException e) {
				// TODO Auto-generated catch block
				e.printStackTrace();
			}
		}
		//System.out.println("End Element :" + qName);
	}

	public void characters(char[] ch, int start, int length)
	throws SAXException {
		Iterator<Entry<Object, edgeLabelPaperWithAuthor>> iter = attributeMap.entrySet().iterator();
		while(iter.hasNext()){
			@SuppressWarnings("rawtypes")
			Map.Entry entryBMap = (Map.Entry) iter.next(); 
			String label = (String)entryBMap.getKey(); 
			edgeLabelPaperWithAuthor labelC = attributeMap.get(label);
			if(qNameList.contains(label.toLowerCase())){
				attributeMap.get(label).labelHave = false;
			}
			else{
				//int type=SPLIT;
				String keyStr = new String(ch, start, length);
				if(labelC.labelHave){
					int type = SPLIT;
					String labelStr = labelC.labelName.toLowerCase();
					//if(edgeLabelMap.containsKey(labelStr)){

					if((labelC.labelName.equalsIgnoreCase("author")||labelC.labelName.equalsIgnoreCase("editor"))){
						
						type = NOTSPLIT;
						insertKeywordMap(keyStr,type);
						authorList.add(keyStr);
						
					}
					else if(labelC.labelName.equalsIgnoreCase("crossref")){
						int retInt = getIDFormFindNodeMap("crossref", keyStr);
						if(retInt != -1){
							try {
								edgeID++;
								int labelid = edgeLabelMap.get(labelStr);
								writeFile(outEdgeF,edgeID+" "+nodeID+" "+retInt+" "+labelC.weight);
								writeFile(outEdgeInfoF, edgeID+" "+labelid);
							} catch (IOException e) {
								// TODO Auto-generated catch block
								e.printStackTrace();
							}
							continue;
						}
					}
					/*else{
						if(!keyStr.trim().isEmpty()){
							
							insertKeywordMap(keyStr,type);
							//System.out.println(labelC.labelName+" "+keyStr);
							if(type == SPLIT){
								keyStr = replaceSymbol(keyStr);
								String[] temp;
								String delimiter = " ";
								temp = keyStr.split(delimiter);
								for(int i=0; i<temp.length;i++){
									stringListOneNode.add(temp[i]);
									int keyid = keywordIDMap.get(temp[i]);
									keyListInOneNode.add(keyid);
									if(!commonList.contains(replaceSymbol(temp[i]))){
										cleanKeyListInOneNode.add(keyid);
									}
								}
								
							}
							else{
								stringListOneNode.add(keyStr);
								//System.out.println("after "+keyStr);
								int keyid = keywordIDMap.get(keyStr);
								keyListInOneNode.add(keyid);
								if(!commonList.contains(replaceSymbol(keyStr))){
									cleanKeyListInOneNode.add(keyid);
								}
							}
						}
					}*///end of else	
						
				}
				attributeMap.get(label).labelHave = false;
			}
		}
	}

	public void endDocument() throws SAXException {
		try {
			writeFile(outNodeNum, nodeID+"");
			closeFile(outNodeF);
			closeFile(outEdgeF);
			closeFile(outEdgeInfoF);
			closeFile(outKeyIDF);
			closeFile(outEdgeLableF);
			closeFile(outNodeNum);
			closeFile(outNodeCleanF);
			closeFile(outNodeForCheck); 
			closeFile(outError);
			
			
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
	
	public void writeFile(BufferedWriter out, int nodeid, List<Integer> inputList) throws IOException{
		if(inputList!=null){
			Iterator<Integer> iter = inputList.iterator();
			out.write(nodeid+" 1.0");
			if(iter.hasNext()){
				out.write(" "+iter.next());
			}
			while(iter.hasNext()){
				out.write(","+iter.next());
			}
			out.write("\r\n");
		}
	}
	
	public void writeCheck(BufferedWriter out, int nodeid, List<String> inputList) throws IOException{
		if(inputList!=null){
			Iterator<String> iter = inputList.iterator();
			out.write(nodeid+" 1.0");
			if(iter.hasNext()){
				out.write(" "+iter.next());
			}
			while(iter.hasNext()){
				out.write(","+iter.next());
			}
			out.write("\r\n");
		}
	}
	
	public void closeFile(BufferedReader in) throws IOException{
		in.close();
	}
	public void closeFile(BufferedWriter out) throws IOException{
		out.close();
	}
	
}