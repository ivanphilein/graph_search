package graphIndex;

import java.io.IOException;

import org.kohsuke.args4j.CmdLineException;
import org.kohsuke.args4j.CmdLineParser;

import shared.IndexClass;

public class GenerateLPN {
	
	/**
	 * @param args
	 */
	public static void main(String[] args) throws IOException {
			CmdOption option = new CmdOption();
			CmdLineParser parser = new CmdLineParser(option);
			ReadAndWrite RWClass = new ReadAndWrite();
			RWClass.READKEY = false;
			
			try {
				parser.parseArgument(args);
			} catch (CmdLineException e) {
			}
			KSearchGraph storeGraph = new KSearchGraph();
			Partition pClass = new Partition();
			IndexClass indexC = new IndexClass();
			RWClass.ReadEdge(RWClass.NID2KEYWORD,option.folder+option.nid2keyword,storeGraph,pClass, true);
			pClass.readPartitionFile(option.folder+option.partition,storeGraph, indexC);
			//RWClass.Read(RWClass.METISINPUT,option.folder+option.metisinput,storeGraph,pClass);
			RWClass.ReadEdge(RWClass.EDGEFILE,option.folder+option.edgefile,storeGraph,pClass,option.Dericted);
			//RWClass.Read(RWClass.KID2KEYWORD,option.folder+option.kid2keyword,storeGraph,pClass);
			//pClass.generatePortalNodeNoReSort(storeGraph, indexC);
			pClass.generatePortalNode(storeGraph, indexC);
			
			pClass.generateOutPortalAnce(storeGraph, indexC);
			//pClass.writeIndexBToK(storeGraph, indexC);
			RWClass.Write(RWClass.L_PN, option.folder+option.L_PN, storeGraph, indexC,pClass);
			//RWClass.Write(RWClass.L_KN, option.folder+option.L_KN, storeGraph, indexC,pClass);
			System.out.println("DONE");
	}

}
