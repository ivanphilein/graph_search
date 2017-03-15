package xmlGraph;

import java.io.FileInputStream;


import org.xml.sax.InputSource;
import org.xml.sax.XMLReader;
import org.xml.sax.helpers.XMLReaderFactory;

 
public class ReadXMLFile {
 
	private static String input = "data/DBLP/dblp_2012_0907.xml"; 
	//private static String input = "data/DBLP/first.xml"; 
	
	private final static int SMALL = 0;
	private final static int BIGROOT = 1;
	private final static int BIGNOROOT = 2;
	private final static int SMALLWithAuthor = 3;
	private final static int OnlyAuthor = 4;
	private final static int PAPERAUTHOR = 5;

    public static void main(String argv[]) {
 
	   try{
		   int type = PAPERAUTHOR;
		   //int type = BIGNOROOT;
		   //int type = SMALL;
		   //int type = SMALLWithAuthor; 
		   //int type = OnlyAuthor; 
		   String xmlfile = input;
		    if(argv.length>=1)
		    	type = Integer.parseInt(argv[0]);
		    else if(argv.length>=2)
		    	xmlfile = argv[1].toString();
		    System.out.println("running file "+xmlfile+"...");
			XMLReader xr = XMLReaderFactory.createXMLReader();
		    switch(type){
		    case BIGROOT:
				XMLHandler handlerRoot = new XMLHandler();
				xr.setContentHandler( handlerRoot );
				xr.setErrorHandler( handlerRoot );
				xr.parse( new InputSource(new FileInputStream(xmlfile) ));
				break;
		    case BIGNOROOT:
				XMLHandlerNoRoot handlerNORoot = new XMLHandlerNoRoot();
				xr.setContentHandler( handlerNORoot );
				xr.setErrorHandler( handlerNORoot );
				xr.parse( new InputSource(new FileInputStream(xmlfile) ));
				break;
		    case SMALL:
				XMLSmallHandler handlerSmall = new XMLSmallHandler();
				xr.setContentHandler( handlerSmall );
				xr.setErrorHandler( handlerSmall );
				xr.parse( new InputSource(new FileInputStream(xmlfile) ));
				break;
		    case SMALLWithAuthor:
		    	XMLSmallHandlerWithAuthor handlerSmallAuthor = new XMLSmallHandlerWithAuthor();
				xr.setContentHandler( handlerSmallAuthor );
				xr.setErrorHandler( handlerSmallAuthor );
				xr.parse( new InputSource(new FileInputStream(xmlfile) ));
				break;
		    case OnlyAuthor:
		    	XMLHandlerOnlyAuthor handlerOnlyAuthor = new XMLHandlerOnlyAuthor();
		    	//testHandler handlerOnlyAuthor = new testHandler();
		    	xr.setContentHandler( handlerOnlyAuthor );
				xr.setErrorHandler( handlerOnlyAuthor );
				xr.parse( new InputSource(new FileInputStream(xmlfile) ));
				break;
		    case PAPERAUTHOR:
		    	XMLHandlerPaperWithAuthor handlerpaperAuthor = new XMLHandlerPaperWithAuthor();
		    	//testHandler handlerOnlyAuthor = new testHandler();
		    	xr.setContentHandler( handlerpaperAuthor );
				xr.setErrorHandler( handlerpaperAuthor );
				xr.parse( new InputSource(new FileInputStream(xmlfile) ));
				break;
		    }
			System.out.println("finish running file "+xmlfile+"! Generate input file for metis...");
			System.out.println("ALL DONE");
			
		}catch(Exception e){
			System.out.println( e.getMessage());
			e.printStackTrace();
		}
 
   }
 
}
