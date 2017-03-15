package graphIndex;

import java.io.IOException;
import java.util.Date;

import org.kohsuke.args4j.CmdLineException;
import org.kohsuke.args4j.CmdLineParser;

import shared.IndexClass;

public class GenePortalBlock {
	
	/**
	 * @param args
	 */
	public static void main(String[] args) throws IOException {
			CmdOption option = new CmdOption();
			CmdLineParser parser = new CmdLineParser(option);
			ReadAndWrite RWClass = new ReadAndWrite();
			try {
				parser.parseArgument(args);
			} catch (CmdLineException e) {
			}
			if(args.length==2){
				option.numK = args[0];
				option.level = Integer.parseInt(args[1]);
			}
			
			long lDateTimeStart=0;
	    	long lDateTimeFinish=0;
			System.out.println(option.numK+" "+option.level);
			KSearchGraph storeGraph = new KSearchGraph();
			Partition pClass = new Partition();
			IndexClass indexC = new IndexClass();
			RWClass.ReadNodeEdge(RWClass.NID2KEYWORD,option.folder+option.nid2keyword,storeGraph,pClass, false);
			pClass.readPartitionFile(option.folder+option.partition,storeGraph, indexC);
			RWClass.ReadEdge(RWClass.EDGEFILE,option.folder+option.edgefile,storeGraph,pClass,option.Dericted);
			//pClass.generatePortalNode(storeGraph, indexC);
			boolean seperate = option.seperate;
			double time = 0;
			if(!seperate){
				lDateTimeStart = new Date().getTime();
				//pClass.updatePartInfoForIterMR(storeGraph, indexC, option.numB, option.folder+option.finalP+"_"+option.numK+".txt", option.folder+option.portalF);
				pClass.updatePartitionInfo(storeGraph, indexC, option.level, option.folder+option.finalP+"_"+option.numK+".txt", option.folder+option.portalF, option.seperate);
				lDateTimeFinish = new Date().getTime();
				time += lDateTimeFinish-lDateTimeStart;
			}
			else{
				pClass.updatePartitionInfo(storeGraph, indexC, option.level, option.folder+"blocks/"+option.finalP+"_"+option.numK+".txt", option.folder+option.portalF, option.seperate);
			}
			storeGraph = null;
			storeGraph = new KSearchGraph();
			RWClass.ReadNodeEdge(RWClass.NID2KEYWORD,option.folder+option.nid2keyword,storeGraph,pClass, true);
			lDateTimeStart = new Date().getTime();
			pClass.readVertexKeywordUpPortalBlock(option.folder+option.finalP+"_"+option.numK+".txt", option.folder+option.finalP+"_"+option.numK+"_block"+option.numB, storeGraph);
			lDateTimeFinish = new Date().getTime();
	        System.out.println("Running time:	"+ (time+(lDateTimeFinish-lDateTimeStart))+"ms");
			System.out.println("DONE");
	}
	
}
