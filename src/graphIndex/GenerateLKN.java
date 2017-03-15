package graphIndex;

import java.io.IOException;

import org.kohsuke.args4j.CmdLineException;
import org.kohsuke.args4j.CmdLineParser;

import shared.IndexClass;

public class GenerateLKN {
	
	/**
	 * @param args
	 */
	public static void main(String[] args) throws IOException {

			CmdOption option = new CmdOption();
			CmdLineParser parser = new CmdLineParser(option);
			ReadAndWrite RWClass = new ReadAndWrite();
			
			//1. get command line parameters 
			try {
				parser.parseArgument(args);
			} catch (CmdLineException e) {
			}
			KSearchGraph storeGraph = new KSearchGraph();
			Partition pClass = new Partition();
			IndexClass indexC = new IndexClass();
			RWClass.READKEY = true;
			boolean dericted = option.Dericted;
			RWClass.ReadEdge(RWClass.NID2KEYWORD,option.folder+option.nid2keyword,storeGraph,pClass, true);
			//pClass.readPortalFile(option.folder+option.partition,storeGraph, indexC);
			RWClass.Read(RWClass.L_PN,option.folder+option.L_PN,storeGraph,pClass, indexC);
			
			RWClass.ReadEdge(RWClass.EDGEFILE,option.folder+option.edgefile,storeGraph,pClass,dericted);
			RWClass.ReadEdge(RWClass.KID2KEYWORD,option.folder+option.kid2keyword,storeGraph,pClass, true);
			pClass.generateLKN(storeGraph, indexC, dericted);
			//RWClass.Write(RWClass.L_PN, option.folder+option.L_PN, storeGraph, indexC,pClass);
			if(dericted)
				RWClass.Write(RWClass.L_KN, option.folder+option.L_KNDericted, storeGraph, indexC,pClass);
			else
				RWClass.Write(RWClass.L_KN, option.folder+option.L_KNUNDericted, storeGraph, indexC,pClass);
			System.out.println("DONE");
			//indexC.showIndexBlockToKeyword();
			//storeGraph.showVertex();
	}
}
