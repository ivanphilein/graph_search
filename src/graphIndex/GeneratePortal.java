package graphIndex;

import java.io.IOException;
import java.util.Date;

import org.kohsuke.args4j.CmdLineException;
import org.kohsuke.args4j.CmdLineParser;

import shared.IndexClass;

public class GeneratePortal {
	
	/**
	 * @param args
	 */
	public static void main(String[] args) throws IOException {
			CmdOption option = new CmdOption();
			CmdLineParser parser = new CmdLineParser(option);
			ReadAndWrite RWClass = new ReadAndWrite();
			RWClass.READKEY = option.Dericted;
			
			try {
				parser.parseArgument(args);
			} catch (CmdLineException e) {
			}
			KSearchGraph storeGraph = new KSearchGraph();
			Partition pClass = new Partition();
			IndexClass indexC = new IndexClass();
			RWClass.ReadNodeEdge(RWClass.NID2KEYWORD,option.folder+option.nid2keyword,storeGraph,pClass, true);
			pClass.readPartitionFile(option.folder+option.partition+option.numB,storeGraph, indexC);
			RWClass.ReadPortalEdge(RWClass.EDGEFILE,option.folder+option.edgefile,storeGraph,pClass,option.Dericted);
			long start = new Date().getTime();
			pClass.generatePortalNode(storeGraph, indexC, option.folder+option.portalF+"_block"+option.numB);
			long end = new Date().getTime();
			//pClass.generateOutPortalFile(storeGraph, indexC, option.folder+option.portalF);
			System.out.println("Total Running Time:	"+(end-start)+"ms");
	}

}
