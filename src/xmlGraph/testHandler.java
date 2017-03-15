//package xmlGraph;
//
//import java.io.BufferedReader;
//import java.io.BufferedWriter;
//import java.io.DataInputStream;
//import java.io.FileInputStream;
//import java.io.FileWriter;
//import java.io.IOException;
//import java.io.InputStreamReader;
//import java.util.ArrayList;
//import java.util.HashMap;
//import java.util.Iterator;
//import java.util.List;
//import java.util.Map;
//import java.util.Map.Entry;
//
//import graphIndex.KSearchGraph;
//
//import org.xml.sax.helpers.DefaultHandler;
//import org.xml.sax.*;
//
//
//class onlyTest{
//	int labelID;
//	double weight;
//	String labelName;
//	String labelValue;
//	boolean labelHave;
//	onlyTest(){
//		labelHave = false;
//		weight = 1.0;
//	}
//};
//public class testHandler extends DefaultHandler{
//
//	//private StringBuffer buffer = new StringBuffer();
//	private String COMMONFILE = "data/common_words.txt";
//	private String DERICTION = "data/DBLPAuthorWithEditer/";
//	//private String DERICTION = "data/DBLPOnlyAuthor/";
//	private String NODEFILE = DERICTION+"nodes.txt";
//	private String EDGEFILE = DERICTION+"edges.txt";
//	private String NODEFORCHECK = DERICTION+"nodefrocheck.txt";
//	private String ERRORF = DERICTION+"error.txt";
//	private String NODENUM = DERICTION+"nodenum.txt";
//	private String KEYWORDFILE = DERICTION+"keywordID.txt";
//	
//	private BufferedReader inCommonF = null;
//	
//	private BufferedWriter outError = null;
//	
//	private BufferedWriter outNodeF = null;
//	private BufferedWriter outEdgeF = null;
//	private BufferedWriter outNodeNum = null;
//	private BufferedWriter outKeyIDF = null;
//	//check part
//	private BufferedWriter outNodeForCheck = null;
//	//end of check part
//	
//	private List<Object> qNameList = null;
//	private List<Object> commonList = null;
//	
//	private boolean author = false;
//	private boolean haveYear = false;
//	
//	private boolean test = false;
//	private int totalNum = 0;
//	
//	private HashMap<Object, Integer> keywordIDMap = null;
//	private String year = "";
//	
//	private List<StringBuffer> authorList = new ArrayList<StringBuffer>();
//	
//	private HashMap<Integer, List<Integer>> edgeMap = new HashMap<Integer, List<Integer>> ();
//	
//	public int nodeID = 0;//Start from 1
//	public int authorID = 0;
//	public int edgeID = 0;
//	private int keywordID = 1;
//	
//	private final int SPLIT=1;
//	private final int NOTSPLIT=0;
//	
//	
//	private StringBuffer str = null;  
//    public void characters (char ch[], int start, int length) {  
//        str.append(ch, start, length);  
//    }  
//    public void startElement(String uri, String localName, String qName,  
//            Attributes attr) throws SAXException {  
//        str = new StringBuffer();  
//    }  
//    public void endElement(String uri, String localName, String qName) throws SAXException { 
//    	String key = str.toString();
//        System.out.println(key);  
//    }
//	
//}