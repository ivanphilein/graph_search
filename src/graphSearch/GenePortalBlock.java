package graphSearch;


import java.io.IOException;
import java.util.Date;

import org.kohsuke.args4j.CmdLineException;
import org.kohsuke.args4j.CmdLineParser;

import shared.CmdOption;


public class GenePortalBlock {
	/**
	 * @param args
	 */
	public static void main(String[] args) throws IOException {
		String method = "iteration";
		CmdOption option = new CmdOption();
		CmdLineParser parser = new CmdLineParser(option);
		
		try {
			parser.parseArgument(args);
		} catch (CmdLineException e) {
		}
		
		long lDateTimeStart=0;
    	long lDateTimeFinish=0;
		
		String folder = option.folder;
		System.out.println(option.numK+" "+option.level);
		GraphClass graphClass = new GraphClass();
		graphClass.readUnDirectedGraph(folder+option.nodefile, folder+option.edgefile, false);
		graphClass.readPartitionFile(folder+option.partitionfile);
		Partition pClass = new Partition();
		System.out.println("graph:"+graphClass.getVidToVMap().size());
		
		double time = 0;
		if(option.packageStr.equals(method)){
			lDateTimeStart = new Date().getTime();
			//pClass.updatePartInfoForIterMR(graphClass, option.numB, folder+option.finalP+"_numK"+option.numK+"_top"+option.topK+"_iteration", folder+option.portalF);
			pClass.updatePartInfoForIterMR(graphClass, option.numB, folder+option.finalP+"_numK"+option.numK+"_top"+option.topK+"_iteration", folder+option.portalF);
	        lDateTimeFinish = new Date().getTime();
	        time = lDateTimeFinish-lDateTimeStart;
	        graphClass = null;
			graphClass = new GraphClass(folder+option.nodefile, null, true);
			lDateTimeStart = new Date().getTime();
			pClass.readVertexKeywordUpPortalBlock(folder+option.finalP+"_numK"+option.numK+"_top"+option.topK+"_iteration", folder+option.finalP+"_numK"+option.numK+"_top"+option.topK+"_iteration_block"+option.numB, graphClass);
			lDateTimeFinish = new Date().getTime();
	        System.out.println("Running time:	"+ (time+(lDateTimeFinish-lDateTimeStart))+"ms");
			System.out.println("DONE");
		}
		else{
			lDateTimeStart = new Date().getTime();
			pClass.updatePartitionInfo(graphClass, option.numB, option.level,folder+option.finalP+"_numK"+option.numK+"_top"+option.topK, folder+option.portalF, false);
	        lDateTimeFinish = new Date().getTime();
	        time = lDateTimeFinish-lDateTimeStart;
	        graphClass = null;
			graphClass = new GraphClass(folder+option.nodefile, null, true);
			lDateTimeStart = new Date().getTime();
			pClass.readVertexKeywordUpPortalBlock(folder+option.finalP+"_numK"+option.numK+"_top"+option.topK, folder+option.finalP+"_numK"+option.numK+"_top"+option.topK+"_block"+option.numB, graphClass);
			lDateTimeFinish = new Date().getTime();
	        System.out.println("Running time:	"+ (time+(lDateTimeFinish-lDateTimeStart))+"ms");
			System.out.println("DONE");
			
		}
		
		
	}
	
}
