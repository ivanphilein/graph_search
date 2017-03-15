package graphSearch;

import graphIndex.CompareElement;
import graphIndex.EdgeClass;

import java.io.BufferedReader;
import java.io.BufferedWriter;
import java.io.DataInputStream;
import java.io.FileInputStream;
import java.io.FileWriter;
import java.io.IOException;
import java.io.InputStreamReader;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.HashSet;
import java.util.Iterator;
import java.util.List;
import java.util.Set;
import java.util.Map.Entry;
import java.util.TreeMap;
import java.util.TreeSet;

import org.jgrapht.graph.AsWeightedGraph;
import org.jgrapht.graph.DefaultWeightedEdge;

public class Partition {
	public int getBiggestBlockSize(GraphClass graphClass){
		int biggest = 0;
		HashMap<Integer, HashSet<Integer>> blockMap = graphClass.getBidToVMap();
		Iterator<Entry<Integer, HashSet<Integer>>> iter = blockMap.entrySet().iterator();
		while(iter.hasNext()){
			Entry<Integer, HashSet<Integer>> entry = iter.next();
			int size = entry.getValue().size();
			if(biggest<size){
				biggest = size;
			}
		}
		return biggest;
	}
	
	
	
	public HashSet<Integer> genePortalNodes(GraphClass graphClass, String edgefile){
		List<DefaultWeightedEdge> candEdgeSet = this.geneCandPortalNodes(graphClass, edgefile);
		HashSet<Integer> portalSet = new HashSet<Integer>();
		return portalSet;
	}
	
	public List<DefaultWeightedEdge> geneCandPortalNodes(GraphClass graphClass, String edgefile){
		List<DefaultWeightedEdge> portalEdge = new ArrayList<DefaultWeightedEdge>();
		//TreeMap<Integer, Integer> storeMap = new HashMap<Integer, Integer>();
		try{
			  FileInputStream fstream = new FileInputStream(edgefile);
			  // Get the object of DataInputStream
			  DataInputStream in = new DataInputStream(fstream);
			  BufferedReader br = new BufferedReader(new InputStreamReader(in));
			  String strLine;
	  	      int read = 0;
	  	      while ((strLine = br.readLine()) != null)   {
	  	    	  if(strLine.startsWith("#"))
	  	    		  continue;
	  	    	  //DefaultWeightedEdge edge = graph.addEdge(v1, v2); 
	  	    	  read++;
	  	    	  if(read%100000==0)
	  	    		  System.out.println("read edge num:"+read);
	  	    	  //this.updateCandSet(graphClass, storeMap,portalEdge, strLine);
	  	    	  
	  	      }
			  //Close the input stream
			  in.close();
		}catch (Exception e){//Catch exception if any
			  System.err.println("Error: " + e.getMessage());
		}
		return portalEdge;
	}
	
	public void updateCandSet(GraphClass graphClass, HashMap<Integer, CompareElement> storeMap, List<EdgeClass> portalEdge, String edgeStr){
		String[] temp = edgeStr.split(" ");
		int vfrom = Integer.parseInt(temp[1]);
		int vto = Integer.parseInt(temp[2]);
		if(graphClass.getVertexFromVid(vfrom).getBlock()!=graphClass.getVertexFromVid(vto).getBlock()){
			CompareElement element = new CompareElement();
			//candSet.add(vfrom);
			//candSet.add(vto);
		}
	}
	
	/**
	 * Create new portal blocks, start from portal nodes, then grow to be one separate block
	 * @param graphClass
	 * @param bNum
	 * @param level
	 * @param outputFile
	 * @param portalFile
	 * @throws IOException
	 */
	public void updatePartitionInfo(GraphClass graphClass,int bNum, double level, String outputFile, String portalFile) throws IOException{
		AsWeightedGraph<Integer, DefaultWeightedEdge> graph = graphClass.getGraph();
		TreeMap<Integer, HashSet<Integer>> portalBlockMap = graphClass.getPortalMap(portalFile, false);
		double storeLevel = level;
		//bNum is the number of blocks
		//bigSize is the biggest block size
		//Iterator<Entry<Integer, HashSet<Integer>>> iter = portalBlockMap.entrySet().iterator();
		//write original block to file
		FileWriter fstream;
		fstream = new FileWriter(outputFile);
		BufferedWriter out = new BufferedWriter(fstream);
		for(int i=0;i<bNum;i++){
			HashSet<Integer> nodeSet = graphClass.getVertexSetFromBid(i);
			HashSet<DefaultWeightedEdge> edgeSet = new HashSet<DefaultWeightedEdge>();
			Iterator<Integer> iterNode = nodeSet.iterator();
			while(iterNode.hasNext()){
				int vid = iterNode.next();
				Vertex vertex = graphClass.getVertexFromVid(vid);
				//System.out.println("vid weight "+vertex.getWeight()+"|"+vertex.getKeyWord());
				int portalId = vertex.getPortalBlock();
				if(portalId == -1){
					out.write(bNum+" vertex "+vid+" "+vertex.getWeight()+" "+vertex.getKeySet().toString().replace("[", "").replace("]", "").replace(" ", "").replace(",", " "));
					out.write("\n");
				}
				else{
					out.write(bNum+" portal "+ portalId +" "+vid+" "+vertex.getWeight()+" "+vertex.getKeySet().toString().replace("[", "").replace("]", "").replace(" ", "").replace(",", " "));
					out.write("\n");
				}
				Set<DefaultWeightedEdge> adjEdgeSet = graph.edgesOf(vid);
				Iterator<DefaultWeightedEdge> iterEdge = adjEdgeSet.iterator();
				while(iterEdge.hasNext()){
					DefaultWeightedEdge edge = iterEdge.next();
					int vfrom = graph.getEdgeSource(edge);
					int vto = graph.getEdgeTarget(edge);
					if(vfrom == vid){
						if(nodeSet.contains(vto)){
							//Vertex secVer = graphClass.getVertexFromVid(vto);
						//if(secVer.getBlockList().contains(i)){
							edgeSet.add(edge);
						//}
						}
					}
					else if(vto == vid){
						if(nodeSet.contains(vfrom)){
							//Vertex secVer = graphClass.getVertexFromVid(vfrom);
						//if(secVer.getBlockList().contains(i)){
							edgeSet.add(edge);
						//}
						}
					}
				}
			}//end of this while, all nodes are written to file and all realted edges are stored in edgeSet
			
			Iterator<DefaultWeightedEdge> iterEdge = edgeSet.iterator();
			while(iterEdge.hasNext()){
				DefaultWeightedEdge tempEdge = iterEdge.next();
				out.write(i+" edge "+graph.getEdgeSource(tempEdge)+" "+graph.getEdgeSource(tempEdge)+" "+graph.getEdgeWeight(tempEdge));
				out.write("\n");
			}
		}
		graphClass.getBidToVMap().clear();
		
		
		//while(iter.hasNext()){
		while(!portalBlockMap.isEmpty()){
			HashSet<DefaultWeightedEdge> visitedSet = new HashSet<DefaultWeightedEdge>();
			Entry<Integer, HashSet<Integer>> entry = portalBlockMap.pollFirstEntry();
			HashSet<Integer> nodeSet = entry.getValue();
			//Based on nodeSet, add all the edges related with the nodes in that set, the value is a edge list with same distance value
			TreeMap<Double, List<DefaultWeightedEdge>> edgeMap = new TreeMap<Double, List<DefaultWeightedEdge>>();
			Iterator<Integer> iterNode = nodeSet.iterator();
			//this while is used to initial added all the edges directly related with all portal-nodes in this new block
			while(iterNode.hasNext()){
				int nodeid = iterNode.next();
				Set<DefaultWeightedEdge> adjEdgeSet = graph.edgesOf(nodeid);
				Iterator<DefaultWeightedEdge> iterEdge = adjEdgeSet.iterator();
				while(iterEdge.hasNext()){
					DefaultWeightedEdge edge = iterEdge.next();
					double dis = graph.getEdgeWeight(edge);
					List<DefaultWeightedEdge> edgeList = edgeMap.get(dis);
					if(edgeList==null){
						edgeList = new ArrayList<DefaultWeightedEdge>();
						edgeMap.put(dis, edgeList);
					}
					edgeList.add(edge);
				}
			}//after this while loop, all the adj edges based on original portal nodes in this new block are added to edgeMap
			double dis=0;
			//System.out.println("dis:"+dis+" edgeMap:"+edgeMap.size()+" nodeset:"+nodeSet.size());
			
			boolean run = true;
			//this while is used to add all related nodes to this block
			while(run && edgeMap.size()!=0){
				Entry<Double, List<DefaultWeightedEdge>> listEntry = edgeMap.pollFirstEntry();
				dis = (double)listEntry.getKey();
				level = level-(int)dis;
				if(level<0){
					System.out.println("level break:"+bNum);
					break;
				}
				List<DefaultWeightedEdge> listEdge = listEntry.getValue();
				Iterator<DefaultWeightedEdge> iterEdge = listEdge.iterator();
				while(iterEdge.hasNext()){
					DefaultWeightedEdge tempEdge = iterEdge.next();
					visitedSet.add(tempEdge);
					int vfrom = graph.getEdgeSource(tempEdge);
					int vto = graph.getEdgeTarget(tempEdge);
					if(nodeSet.add(vfrom)){
						//indexC.addBlockWithNodeID(bNum, vfrom);
						//Vertex vertexF = graphClass.getVertexFromVid(vfrom);
						//vertexF.addToBlockList(bNum);
						/*
						size++;
						if(size>=bigSize){
							run=false;
							System.out.println("size break:"+bigSize+" "+bNum);
							break;
						}*/
						Set<DefaultWeightedEdge> adjList = graph.edgesOf(vfrom);
						//System.out.println("vid:"+vfrom+"adj edge:"+adjList.size());
						if(adjList!=null){
							Iterator<DefaultWeightedEdge> iterFrom = adjList.iterator();
							while(iterFrom.hasNext()){
								DefaultWeightedEdge edge = iterFrom.next();
								//System.out.println("edge:"+edge.showEdge());
								if(!visitedSet.contains(edge)){
									double disFrom = graph.getEdgeWeight(edge);
									List<DefaultWeightedEdge> edgeList = edgeMap.get(disFrom);
									if(edgeList==null){
										edgeList = new ArrayList<DefaultWeightedEdge>();
										edgeMap.put(disFrom, edgeList);
									}
									edgeList.add(edge);
								}
							}
						}
					}
					if(nodeSet.add(vto)){
						//indexC.addBlockWithNodeID(bNum, vfrom);
						//Vertex vertexF = graphClass.getVertexFromVid(vto);
						//vertexF.addToBlockList(bNum);
						/*
						size++;
						if(size>=bigSize){
							run=false;
							System.out.println("size break:"+bigSize+" "+bNum);
							break;
						}*/
						Set<DefaultWeightedEdge> adjList = graph.edgesOf(vto);
						//System.out.println("vid:"+vfrom+"adj edge:"+adjList.size());
						if(adjList!=null){
							Iterator<DefaultWeightedEdge> iterFrom = adjList.iterator();
							while(iterFrom.hasNext()){
								DefaultWeightedEdge edge = iterFrom.next();
								//System.out.println("edge:"+edge.showEdge());
								if(!visitedSet.contains(edge)){
									double disFrom = graph.getEdgeWeight(edge);
									List<DefaultWeightedEdge> edgeList = edgeMap.get(disFrom);
									if(edgeList==null){
										edgeList = new ArrayList<DefaultWeightedEdge>();
										edgeMap.put(disFrom, edgeList);
									}
									edgeList.add(edge);
								}
							}
						}
					}
				}//end of while(iterEdge.hasNext())
			}//After this while, for this new block with bid=bNum, it is done!
			
			//store this block
			//indexC.addBlockWithNodeSet(bNum, nodeSet);
			//
			
			//not sotre, write to file
			HashSet<DefaultWeightedEdge> edgeSet = new HashSet<DefaultWeightedEdge>();
			iterNode = nodeSet.iterator();
			while(iterNode.hasNext()){
				int vid = iterNode.next();
				Vertex vertex = graphClass.getVertexFromVid(vid);
				//System.out.println("vid weight "+vertex.getWeight()+"|"+vertex.getKeyWord());
				int portalId = vertex.getPortalBlock();
				if(portalId == -1){
					out.write(bNum+" vertex "+vid+" "+vertex.getWeight()+" "+vertex.getKeySet().toString().replace("[", "").replace("]", "").replace(" ", "").replace(",", " "));
					out.write("\n");
				}
				else{
					out.write(bNum+" portal "+ portalId +" "+vid+" "+vertex.getWeight()+" "+vertex.getKeySet().toString().replace("[", "").replace("]", "").replace(" ", "").replace(",", " "));
					out.write("\n");
				}
				Set<DefaultWeightedEdge> adjEdge = graph.edgesOf(vid);
				Iterator<DefaultWeightedEdge> iterEdge = adjEdge.iterator();
				while(iterEdge.hasNext()){
					DefaultWeightedEdge tempEdge = iterEdge.next();
					int vfrom = graph.getEdgeSource(tempEdge);
					int vto = graph.getEdgeTarget(tempEdge);
					if(vfrom == vid){
						if(nodeSet.contains(vto)){
							//Vertex secVer = graphClass.getVertexFromVid(vto);
						//if(secVer.getBlockList().contains(bNum)){
							edgeSet.add(tempEdge);
						//}
						}
					}
					else if(vto == vid){
						if(nodeSet.contains(vfrom)){
							//Vertex secVer = graphClass.getVertexFromVid(vfrom);
						//if(secVer.getBlockList().contains(bNum)){
							edgeSet.add(tempEdge);
						//}
						}
						
					}
				}
			}//end of this while, all nodes are written to file and all realted edges are stored in edgeSet
			Iterator<DefaultWeightedEdge> iterEdge = edgeSet.iterator();
			while(iterEdge.hasNext()){
				DefaultWeightedEdge tempEdge = iterEdge.next();
				out.write(bNum+" edge "+graph.getEdgeSource(tempEdge)+" "+graph.getEdgeSource(tempEdge)+" "+graph.getEdgeWeight(tempEdge));
				out.write("\n");
			}
			//
			nodeSet.clear();
			edgeSet.clear();
			
			bNum++;
			level = storeLevel;
		}
		out.close();
	}
	
	/**
	 * Generate final portal file with overlapping, it means that grow original block
	 * @param graphClass
	 * @param bNum
	 * @param level
	 * @param outputFile
	 * @param portalFile
	 * @param seperate
	 * @throws IOException
	 */
	public void updatePartitionInfo(GraphClass graphClass, int bNum, double level, String outputFile, String portalFile, boolean seperate) throws IOException{
		
		TreeMap<Integer, HashSet<Integer>> portalBlockMap = graphClass.getPortalMap(portalFile, false);
		int bigSize = this.getBiggestBlockSize(graphClass)*2;
		//Iterator<Entry<Integer, HashSet<Integer>>> iter = portalBlockMap.entrySet().iterator();
		boolean run = true;
		AsWeightedGraph<Integer, DefaultWeightedEdge> graph = graphClass.getGraph();
		//System.out.println("step:"+1+" "+portalBlockMap.size());
		System.out.println("write to:"+outputFile);
		FileWriter fstream;
		fstream = new FileWriter(outputFile);
		BufferedWriter out = new BufferedWriter(fstream);
		while(!portalBlockMap.isEmpty()){
		//while(iter.hasNext()){
			Entry<Integer, HashSet<Integer>> entry = portalBlockMap.pollFirstEntry();
			int bid = entry.getKey();
			HashSet<Integer> portalNodeSet = entry.getValue();//portal node set
			HashSet<Integer> nodeSet = graphClass.getVertexSetFromBid(bid);//original node set
			//System.out.println("bid:"+bid+" size:"+nodeSet.size());
			Iterator<Integer> iterNode = portalNodeSet.iterator();
			//this while is used to initial added all the edges directly related with all portal-nodes in this new block
			TreeMap<Double, List<DefaultWeightedEdge>> portalEdgeMap = new TreeMap<Double, List<DefaultWeightedEdge>>();
			while(iterNode.hasNext()){
				int popid = iterNode.next();
				//Vertex vertex = graphClass.getVertexFromVid(popid);
				Set<DefaultWeightedEdge> edgeSet = graph.edgesOf(popid);
				//System.out.println("bid:"+bid+" popid:"+popid+" size:"+edgeSet.size());
				Iterator<DefaultWeightedEdge> iterEdge = edgeSet.iterator();
				double weight = 0;
				while(iterEdge.hasNext()){
					DefaultWeightedEdge edge = iterEdge.next();
					weight = graph.getEdgeWeight(edge);
					List<DefaultWeightedEdge> edgeList = portalEdgeMap.get(weight);
					if(edgeList==null){
						edgeList = new ArrayList<DefaultWeightedEdge>();
						portalEdgeMap.put(weight, edgeList);
					}
					edgeList.add(edge);
				}
			}
			portalNodeSet.clear();
			portalNodeSet = null;
			//HashSet<DefaultWeightedEdge> visitedEdge = new HashSet<DefaultWeightedEdge>();
			run = true;
			while(!portalEdgeMap.isEmpty() && run){
				Entry<Double, List<DefaultWeightedEdge>> popEntry = portalEdgeMap.pollFirstEntry();
				List<DefaultWeightedEdge> edgeList = popEntry.getValue();
				double edgeWei = popEntry.getKey();
				//System.out.println("edge wei:"+edgeWei+" "+edgeList.toString());
				//System.out.print("wei:"+edgeWei+" egeList:"+edgeList.toString());
				//System.out.println("weightLevel:"+edgeWei+" edgewei:"+edgeWei);
				if(edgeWei>level){
					System.out.println("bid:"+bid+" level break");
					break;
				}
				if(nodeSet.size()>bigSize){
					run=false;
					System.out.println("bid:"+bid+" size break "+bigSize+" size:"+nodeSet.size()+" weightlevel:"+edgeWei);
					break;
				}
				//weightLevel += edgeWei;
				Iterator<DefaultWeightedEdge> iterList = edgeList.iterator();
				while(iterList.hasNext()){
					DefaultWeightedEdge popEdge = iterList.next();
					//visitedEdge.add(popEdge);
					int newid = graph.getEdgeTarget(popEdge);
					//System.out.println(" pop edge:"+weightLevel+" edge:"+graph.getEdgeSource(popEdge)+" "+graph.getEdgeTarget(popEdge)+" "+graph.getEdgeWeight(popEdge)+" newid:"+newid);
					if(nodeSet.add(newid)){
						//System.out.println("add vid:"+newid+" "+nodeSet.size());
						//Vertex newVer = graphClass.getVertexFromVid(newid);
						//newVer.addToBlockList(bid);
						/*if(nodeSet.size()>bigSize){
							run=false;
							System.out.println("size break "+bigSize+" "+nodeSet.size());
							break;
						}*/
						Set<DefaultWeightedEdge> newEdgeSet = graph.edgesOf(newid);
						Iterator<DefaultWeightedEdge> iterNewEdge = newEdgeSet.iterator();
						while(iterNewEdge.hasNext()){
							DefaultWeightedEdge newAddEdge = iterNewEdge.next();
							//if(!visitedEdge.contains(newAddEdge)){
								double newWeight = edgeWei +graph.getEdgeWeight(newAddEdge);
								List<DefaultWeightedEdge> newEdgeList = portalEdgeMap.get(newWeight);
								//System.out.println("add new edge:"+newWeight+" edge:"+graph.getEdgeSource(newAddEdge)+" "+graph.getEdgeTarget(newAddEdge)+" "+graph.getEdgeWeight(newAddEdge));
								if(newEdgeList==null){
									newEdgeList = new ArrayList<DefaultWeightedEdge>();
									portalEdgeMap.put(newWeight, newEdgeList);
								}
								newEdgeList.add(newAddEdge);
								//visitedEdge.add(newAddEdge);
							//}
						}
					}
					newid = graph.getEdgeSource(popEdge);
					//System.out.println(" pop edge:"+weightLevel+" edge:"+graph.getEdgeSource(popEdge)+" "+graph.getEdgeTarget(popEdge)+" "+graph.getEdgeWeight(popEdge)+" newid:"+newid);
					if(nodeSet.add(newid)){
						//System.out.println("add vid:"+newid+" "+nodeSet.size());
						//Vertex newVer = graphClass.getVertexFromVid(newid);
						//newVer.addToBlockList(bid);
						/*if(nodeSet.size()>bigSize){
							run=false;
							System.out.println("size break "+bigSize+" "+nodeSet.size());
							break;
						}*/
						Set<DefaultWeightedEdge> newEdgeSet = graph.edgesOf(newid);
						Iterator<DefaultWeightedEdge> iterNewEdge = newEdgeSet.iterator();
						while(iterNewEdge.hasNext()){
							DefaultWeightedEdge newAddEdge = iterNewEdge.next();
							//if(!visitedEdge.contains(newAddEdge)){
								double newWeight = edgeWei +graph.getEdgeWeight(newAddEdge);
								List<DefaultWeightedEdge> newEdgeList = portalEdgeMap.get(newWeight);
								//System.out.println("add new edge:"+newWeight+" edge:"+graph.getEdgeSource(newAddEdge)+" "+graph.getEdgeTarget(newAddEdge)+" "+graph.getEdgeWeight(newAddEdge));
								if(newEdgeList==null){
									newEdgeList = new ArrayList<DefaultWeightedEdge>();
									portalEdgeMap.put(newWeight, newEdgeList);
								}
								newEdgeList.add(newAddEdge);
								//visitedEdge.add(newAddEdge);
							//}
						}
					}
				}
				edgeList.clear();
				edgeList = null;
			}
			
				
			//write to file
			HashSet<DefaultWeightedEdge> edgeSet = new HashSet<DefaultWeightedEdge>();
			iterNode = nodeSet.iterator();
			while(iterNode.hasNext()){
				int vid = iterNode.next();
				Vertex vertex = graphClass.getVertexFromVid(vid);
				////System.out.println("vid weight "+vertex.getWeight()+"|"+vertex.getKeyWord());
				int portalId = vertex.getPortalBlock();
				if(portalId == -1){
					HashSet<Integer> keySet = vertex.getKeySet();
					if(keySet==null){
						out.write(bid+" vertex "+vid+" "+vertex.getWeight());
						out.write("\n");
					}
					else{
						out.write(bid+" vertex "+vid+" "+vertex.getWeight()+" "+vertex.getKeySet().toString().replace("[", "").replace("]", "").replace(" ", "").replace(",", " "));
						out.write("\n");
					}
				}
				else{
					HashSet<Integer> keySet = vertex.getKeySet();
					if(keySet==null){
						out.write(bid+" portal "+ portalId +" "+vid+" "+vertex.getWeight());
						out.write("\n");
					}
					else{
						out.write(bid+" portal "+ portalId +" "+vid+" "+vertex.getWeight()+" "+vertex.getKeySet().toString().replace("[", "").replace("]", "").replace(" ", "").replace(",", " "));
						out.write("\n");
					}
				}
				Set<DefaultWeightedEdge> adjEdge = graph.edgesOf(vid);
				Iterator<DefaultWeightedEdge> iterEdge = adjEdge.iterator();
				while(iterEdge.hasNext()){
					DefaultWeightedEdge tempEdge = iterEdge.next();
					int vfrom = graph.getEdgeSource(tempEdge);
					int vto = graph.getEdgeTarget(tempEdge);
					if(vfrom == vid){
						if(nodeSet.contains(vto)){
						//Vertex secVer = graphClass.getVertexFromVid(vto);
						//if(secVer.getBlockList().contains(bNum)){
							edgeSet.add(tempEdge);
						//}
						}
					}
					else if(vto == vid){
						if(nodeSet.contains(vfrom)){
						//Vertex secVer = graphClass.getVertexFromVid(vfrom);
						//if(secVer.getBlockList().contains(bNum)){
							edgeSet.add(tempEdge);
						//}
						}
					}
				}
			}//end of this while, all nodes are written to file and all realted edges are stored in edgeSet
			nodeSet.clear();
			nodeSet = null;
			Iterator<DefaultWeightedEdge> iterEdge = edgeSet.iterator();
			while(iterEdge.hasNext()){
				DefaultWeightedEdge tempEdge = iterEdge.next();
				int vfrom = graph.getEdgeSource(tempEdge);
				int vto = graph.getEdgeTarget(tempEdge);
				double edgewei = graph.getEdgeWeight(tempEdge);
				out.write(bid+" edge "+vfrom+" "+vto+" "+edgewei);
				out.write("\n");
			}
			edgeSet.clear();
			edgeSet = null;
		}
		out.close();
		/*if(seperate==false){
			try {
				System.out.println("write to:"+outputFile);
				FileWriter fstream;
				fstream = new FileWriter(outputFile);
				BufferedWriter out = new BufferedWriter(fstream);
				HashMap<Integer, HashSet<Integer>> blockMap = graphClass.getBidToVMap();
				////System.out.println("blockMap:"+blockMap.size());
				Iterator<Entry<Integer, HashSet<Integer>>> iterBlock = blockMap.entrySet().iterator();
				while(iterBlock.hasNext()){
				//for(int i=0;i<bNum;i++){
					Entry<Integer, HashSet<Integer>> entry = iterBlock.next();
					int bid = entry.getKey();
					HashSet<Integer> nodeSet = graphClass.getVertexSetFromBid(bid);
					////System.out.println("step:"+3+" size:"+nodeSet.size());
					HashSet<DefaultWeightedEdge> edgeSet = new HashSet<DefaultWeightedEdge>();
					Iterator<Integer> iterNode = nodeSet.iterator();
					while(iterNode.hasNext()){
						int vid = iterNode.next();
						Vertex vertex = graphClass.getVertexFromVid(vid);
						////System.out.println("vid weight "+vertex.getWeight()+"|"+vertex.getKeyWord());
						out.write(bid+" vertex "+vid+" "+vertex.getWeight()+" "+vertex.getKeySet().toString().replace("[", "").replace("]", "").replace(" ", "").replace(",", " "));
						out.write("\n");
						Set<DefaultWeightedEdge> adjEdge = graph.edgesOf(vid);
						Iterator<DefaultWeightedEdge> iterEdge = adjEdge.iterator();
						while(iterEdge.hasNext()){
							DefaultWeightedEdge tempEdge = iterEdge.next();
							int vfrom = graph.getEdgeSource(tempEdge);
							int vto = graph.getEdgeTarget(tempEdge);
							if(vfrom == vid){
								Vertex secVer = graphClass.getVertexFromVid(vto);
								if(secVer.getBlockList().contains(bid)){
									edgeSet.add(tempEdge);
									//out.write(bid+" edge "+vfrom+" "+vto+" "+edgewei);
									//out.write("\n");
								}
							}
							else if(vto == vid){
								Vertex secVer = graphClass.getVertexFromVid(vfrom);
								if(secVer.getBlockList().contains(bid)){
									edgeSet.add(tempEdge);
									//out.write(bid+" edge "+vfrom+" "+vto+" "+edgewei);
									//out.write("\n");
								}
							}
						}
					}//end of this while, all nodes are written to file and all realted edges are stored in edgeSet
					Iterator<DefaultWeightedEdge> iterEdge = edgeSet.iterator();
					while(iterEdge.hasNext()){
						DefaultWeightedEdge tempEdge = iterEdge.next();
						int vfrom = graph.getEdgeSource(tempEdge);
						int vto = graph.getEdgeTarget(tempEdge);
						double edgewei = graph.getEdgeWeight(tempEdge);
						out.write(bid+" edge "+vfrom+" "+vto+" "+edgewei);
						out.write("\n");
					}
				}
				out.close();
			} catch (IOException e) {
				// TODO Auto-generated catch block
				e.printStackTrace();
			}
		}
		else if(seperate){
			
		}*/
	}

	public void readVertexKeywordUpPortalBlock(String portlBlockFile, String readPortlBlockFile, GraphClass graphClass) throws IOException{
		FileWriter fstream;
		fstream = new FileWriter(readPortlBlockFile);
		BufferedWriter out = new BufferedWriter(fstream);
		
		FileInputStream readfstream = new FileInputStream(portlBlockFile);
		// Get the object of DataInputStream
		DataInputStream in = new DataInputStream(readfstream);
		BufferedReader br = new BufferedReader(new InputStreamReader(in));
		String strLine;
	  	String[] temp;
	    String delimiter = " ";
	    while ((strLine = br.readLine()) != null)   {
	    	if(strLine.contains("vertex")){
	    		temp = strLine.split(delimiter);
	    		int vid = Integer.parseInt(temp[2]);
	    		strLine += " "+graphClass.getVertexFromVid(vid).getKeySet().toString().replace("[", "").replace("]", "").replace(" ", "").replace(",", " ");
	    	}
	    	else if(strLine.contains("portal")){
	    		temp = strLine.split(delimiter);
	    		int vid = Integer.parseInt(temp[3]);
	    		strLine += " "+graphClass.getVertexFromVid(vid).getKeySet().toString().replace("[", "").replace("]", "").replace(" ", "").replace(",", " ");
	    	}
	    	out.write(strLine+"\n");
	    }
	    out.close();
	    br.close();
	}
	
	
	public void updatePartInfoForIterMR(GraphClass graphClass, int bNum, String outputfile, String portalFile) throws IOException{
		
		TreeMap<Integer, HashSet<Integer>> portalBlockMap = graphClass.getPortalMap(portalFile, false);
		AsWeightedGraph<Integer, DefaultWeightedEdge> graph = graphClass.getGraph();
		System.out.println("write to:"+outputfile);
		FileWriter fstream;
		fstream = new FileWriter(outputfile);
		BufferedWriter out = new BufferedWriter(fstream);
		while(!portalBlockMap.isEmpty()){
			Entry<Integer, HashSet<Integer>> entry = portalBlockMap.pollFirstEntry();
			int bid = entry.getKey();
			HashSet<Integer> portalNodeSet = entry.getValue();//portal node set
			Iterator<Integer> iterPortal = portalNodeSet.iterator();
			while(iterPortal.hasNext()){
				int portalId = iterPortal.next();
				Set<DefaultWeightedEdge> edgeSet = graph.edgesOf(portalId);
				Iterator<DefaultWeightedEdge> iterEdge = edgeSet.iterator();
				while(iterEdge.hasNext()){
					DefaultWeightedEdge edge = iterEdge.next();
					int from = graph.getEdgeSource(edge);
					int to = graph.getEdgeTarget(edge);
					//System.out.println("from:"+from+" to:"+to+" portalId:"+portalId+" bid:"+bid);
					if(from == portalId){
						Vertex toVer = graphClass.getVertexFromVid(to);
						int toBid = toVer.getBlock();
						//System.out.println("ver:"+to+" block:"+toBid+" portalBlock:"+toVer.getPortalBlock());
						if(toBid != bid){
							graphClass.getVertexSetFromBid(toBid).add(portalId);
						}
					}
					else if(to == portalId){
						Vertex fromVer = graphClass.getVertexFromVid(from);
						int fromBid = fromVer.getBlock();
						//System.out.println("ver:"+from+" block:"+fromBid+" portalBlock:"+fromVer.getPortalBlock());
						if(fromBid != bid){
							graphClass.getVertexSetFromBid(fromBid).add(portalId);
						}
					}
				}
			}
		}
		
		HashMap<Integer, HashSet<Integer>> bidToVMap = graphClass.getBidToVMap();
		
		Iterator<Entry<Integer, HashSet<Integer>>> iterBToV = bidToVMap.entrySet().iterator();
		while(iterBToV.hasNext()){
			Entry<Integer, HashSet<Integer>> entry = iterBToV.next();
			int bid = entry.getKey();
			HashSet<Integer> nodeSet = entry.getValue();
			//System.out.println("bid:"+bid+" nodes:"+nodeSet.toString());
			TreeSet<Integer> portalSet = new TreeSet<Integer>();
			HashSet<DefaultWeightedEdge> edgeSet = new HashSet<DefaultWeightedEdge>();
			
			//generate edge set
			Iterator<Integer> iterNode = nodeSet.iterator();
			while(iterNode.hasNext()){
				int vid = iterNode.next();
				Vertex vertex = graphClass.getVertexFromVid(vid);
				int portalId = vertex.getPortalBlock();
				if(portalId == -1){
					HashSet<Integer> keySet = vertex.getKeySet();
					if(keySet==null){
						out.write(bid+" vertex "+vid+" "+vertex.getWeight());
						out.write("\n");
					}
					else{
						out.write(bid+" vertex "+vid+" "+vertex.getWeight()+" "+vertex.getKeySet().toString().replace("[", "").replace("]", "").replace(" ", "").replace(",", " "));
						out.write("\n");
					}
				}
				else{
					HashSet<Integer> keySet = vertex.getKeySet();
					if(keySet==null){
						out.write(bid+" portal "+ portalId +" "+vid+" "+vertex.getWeight());
						out.write("\n");
					}
					else{
						out.write(bid+" portal "+ portalId +" "+vid+" "+vertex.getWeight()+" "+vertex.getKeySet().toString().replace("[", "").replace("]", "").replace(" ", "").replace(",", " "));
						out.write("\n");
					}
				}
				Set<DefaultWeightedEdge> adjEdge = graph.edgesOf(vid);
				Iterator<DefaultWeightedEdge> iterEdge = adjEdge.iterator();
				while(iterEdge.hasNext()){
					DefaultWeightedEdge tempEdge = iterEdge.next();
					int vfrom = graph.getEdgeSource(tempEdge);
					int vto = graph.getEdgeTarget(tempEdge);
					if(vfrom == vid){
						if(nodeSet.contains(vto)){
							edgeSet.add(tempEdge);
						}
					}
					else if(vto == vid){
						if(nodeSet.contains(vfrom)){
							edgeSet.add(tempEdge);
						}
					}
				}
			}//end of this while, all nodes are written to file and all realted edges are stored in edgeSet
			
			Iterator<DefaultWeightedEdge> iterEdge = edgeSet.iterator();
			while(iterEdge.hasNext()){
				DefaultWeightedEdge edge = iterEdge.next();
				int vfrom = graph.getEdgeSource(edge);
				int vto = graph.getEdgeTarget(edge);
				double edgewei = graph.getEdgeWeight(edge);
				out.write(bid+" edge "+vfrom+" "+vto+" "+edgewei);
				out.write("\n");
			}
			
			//this.writeFileForIterBFSMR(bid, nodeSet, edgeSet, portalSet,graphClass, out);
		}
		out.close();
	}

/**
 * In case to save memory space, do not read edge information before this function, check the edge file in this function
 * @param graphClass
 * @param bNum
 * @param outputfile
 * @param portalFile
 * @throws IOException
 */
public void updatePartInfoForIterMRWithEdge(GraphClass graphClass, int bNum, String outputfile, String edgefile, String portalFile) throws IOException{
	
	TreeMap<Integer, HashSet<Integer>> portalBlockMap = graphClass.getPortalMap(portalFile, false);
	AsWeightedGraph<Integer, DefaultWeightedEdge> graph = graphClass.getGraph();
	System.out.println("write to:"+outputfile);
	FileWriter fstream;
	fstream = new FileWriter(outputfile);
	BufferedWriter out = new BufferedWriter(fstream);
	while(!portalBlockMap.isEmpty()){
		Entry<Integer, HashSet<Integer>> entry = portalBlockMap.pollFirstEntry();
		int bid = entry.getKey();
		HashSet<Integer> portalNodeSet = entry.getValue();//portal node set
		Iterator<Integer> iterPortal = portalNodeSet.iterator();
		FileInputStream fstreamEdge = new FileInputStream(edgefile);
		while(iterPortal.hasNext()){
			int portalId = iterPortal.next();
			  // Get the object of DataInputStream
			  DataInputStream in = new DataInputStream(fstreamEdge);
			  BufferedReader br = new BufferedReader(new InputStreamReader(in));
			  String strLine;
		  	  String[] temp;
	  	      String delimiter = " ";
	  	      int read = 0;
	  	      while ((strLine = br.readLine()) != null)   {
	  	    	//System.out.println("read edge num:"+read);
	  	    	  if(strLine.startsWith("#"))
	  	    		  continue;
	  	    	  
	  	    	  temp = strLine.split(delimiter);
	  	    	  int from = Integer.parseInt(temp[1]);
	  	    	  int to = Integer.parseInt(temp[2]);
	  	    	  
	  	    	  if(from == portalId){
	  	    		  Vertex toVer = graphClass.getVertexFromVid(to);
	  	    		  int toBid = toVer.getBlock();
	  	    		  //System.out.println("ver:"+to+" block:"+toBid+" portalBlock:"+toVer.getPortalBlock());
	  	    		  if(toBid != bid){
	  	    			  graphClass.getVertexSetFromBid(toBid).add(portalId);
	  	    		  }
	  	    	  }
	  	    	  else if(to == portalId){
	  	    		  Vertex fromVer = graphClass.getVertexFromVid(from);
	  	    		  int fromBid = fromVer.getBlock();
	  	    		  //System.out.println("ver:"+from+" block:"+fromBid+" portalBlock:"+fromVer.getPortalBlock());
	  	    		  if(fromBid != bid){
	  	    			  graphClass.getVertexSetFromBid(fromBid).add(portalId);
	  	    		  }
				  }
	  	    	  
	  	    	  read++;
	  	    	  if(read%100000==0)
	  	    		  System.out.println("read edge num:"+read);
	  	      }
	  	      //Close the input stream
	  	      in.close();
		}
	}
	
	HashMap<Integer, HashSet<Integer>> bidToVMap = graphClass.getBidToVMap();
	
	Iterator<Entry<Integer, HashSet<Integer>>> iterBToV = bidToVMap.entrySet().iterator();
	while(iterBToV.hasNext()){
		Entry<Integer, HashSet<Integer>> entry = iterBToV.next();
		int bid = entry.getKey();
		HashSet<Integer> nodeSet = entry.getValue();
		//System.out.println("bid:"+bid+" nodes:"+nodeSet.toString());
		TreeSet<Integer> portalSet = new TreeSet<Integer>();
		HashSet<DefaultWeightedEdge> edgeSet = new HashSet<DefaultWeightedEdge>();
		
		//generate edge set
		Iterator<Integer> iterNode = nodeSet.iterator();
		while(iterNode.hasNext()){
			int vid = iterNode.next();
			Vertex vertex = graphClass.getVertexFromVid(vid);
			int portalId = vertex.getPortalBlock();
			if(portalId == -1){
				out.write(bid+" vertex "+vid+" "+vertex.getWeight()+" "+vertex.getKeySet().toString().replace("[", "").replace("]", "").replace(" ", "").replace(",", " "));
				out.write("\n");
			}
			else{
				portalSet.add(vid);
				out.write(bid+" portal "+ portalId +" "+vid+" "+vertex.getWeight()+" "+vertex.getKeySet().toString().replace("[", "").replace("]", "").replace(" ", "").replace(",", " "));
				out.write("\n");
			}
			Set<DefaultWeightedEdge> adjEdge = graph.edgesOf(vid);
			Iterator<DefaultWeightedEdge> iterEdge = adjEdge.iterator();
			while(iterEdge.hasNext()){
				DefaultWeightedEdge tempEdge = iterEdge.next();
				int vfrom = graph.getEdgeSource(tempEdge);
				int vto = graph.getEdgeTarget(tempEdge);
				if(vfrom == vid){
					if(nodeSet.contains(vto)){
						edgeSet.add(tempEdge);
					}
				}
				else if(vto == vid){
					if(nodeSet.contains(vfrom)){
						edgeSet.add(tempEdge);
					}
				}
			}
		}//end of this while, all nodes are written to file and all realted edges are stored in edgeSet
		
		Iterator<DefaultWeightedEdge> iterEdge = edgeSet.iterator();
		while(iterEdge.hasNext()){
			DefaultWeightedEdge edge = iterEdge.next();
			int vfrom = graph.getEdgeSource(edge);
			int vto = graph.getEdgeTarget(edge);
			double edgewei = graph.getEdgeWeight(edge);
			out.write(bid+" edge "+vfrom+" "+vto+" "+edgewei);
			out.write("\n");
		}
		
		//this.writeFileForIterBFSMR(bid, nodeSet, edgeSet, portalSet,graphClass, out);
	}
	out.close();
}
	

	public void writeFileForIterBFSMR(int bid, HashSet<Integer> nodeSet, HashSet<DefaultWeightedEdge> edgeSet, TreeSet<Integer> portalSet,GraphClass graphClass, BufferedWriter out){
		try {
			//System.out.println("bid:"+bid+" portal set:"+portalSet.toString());
			AsWeightedGraph<Integer, DefaultWeightedEdge> graph = graphClass.getGraph();

			HashMap<Integer, Set<DefaultWeightedEdge>> vToEdgeMap = new HashMap<Integer, Set<DefaultWeightedEdge>>();
			Iterator<Integer> iterNode = nodeSet.iterator();
			while(iterNode.hasNext()){
				int nid = iterNode.next();
				Iterator<DefaultWeightedEdge> iterEdge = edgeSet.iterator();
				while(iterEdge.hasNext()){
					DefaultWeightedEdge edge = iterEdge.next();
					int from = graph.getEdgeSource(edge);
					int to = graph.getEdgeTarget(edge);
					if(from == nid || to==nid){
						Set<DefaultWeightedEdge> nToESet = vToEdgeMap.get(nid);
						if(nToESet==null){
							nToESet = new HashSet<DefaultWeightedEdge>();
							vToEdgeMap.put(nid, nToESet);
						}
						nToESet.add(edge);
					}
					
				}
			}
			int portalSize = portalSet.size();
			while(!portalSet.isEmpty()){
				int portalId = portalSet.pollFirst();
				int size = 0;
				portalSize--;
				//HashMap<Double, HashSet<Integer>> disMap = new HashMap<Double, HashSet<Integer>>();//used to store the distance from this vertexes to other portal vertexes
				TreeMap<Double, HashSet<Integer>> toVisitMap = new TreeMap<Double, HashSet<Integer>>();//used to grow edges
				HashSet<Integer> visitedSet = new HashSet<Integer>();
				HashSet<Integer> verSet = new HashSet<Integer>();
				verSet.add(portalId);
				toVisitMap.put(0.0, verSet);
				while(!toVisitMap.isEmpty() && portalSize>size){
					Entry<Double, HashSet<Integer>> popEntry = toVisitMap.pollFirstEntry();
					double wei = popEntry.getKey();
					HashSet<Integer> toVisitSet = popEntry.getValue();
					Iterator<Integer> iterVisited = toVisitSet.iterator();
					while(iterVisited.hasNext()){
						int popid = iterVisited.next();
						visitedSet.add(popid);
						Set<DefaultWeightedEdge> adjEdge = vToEdgeMap.get(popid);
						Iterator<DefaultWeightedEdge> iterEdge = adjEdge.iterator();
						while(iterEdge.hasNext()){
							DefaultWeightedEdge edge = iterEdge.next();
							int fromVid = graph.getEdgeSource(edge);
							int toVid = graph.getEdgeTarget(edge);
							double edgeWei = wei + graph.getEdgeWeight(edge);
							//System.out.println(bid+" popvid:"+popid+" edge "+fromVid+" "+toVid+" "+edgeWei);
							//System.out.println("portal set:"+portalSet.toString());
							if(fromVid != popid && !visitedSet.contains(fromVid)){
								HashSet<Integer> vSet = toVisitMap.get(edgeWei);
								if(vSet == null){
									vSet = new HashSet<Integer>();
									toVisitMap.put(edgeWei, vSet);
								}
								if(vSet.add(fromVid)&& portalSet.contains(fromVid)){
									//Vertex fromVer = graphClass.getVertexFromVid(fromVid);
									//System.out.println("portal:"+portalId+" new vid:"+fromVid+" portal block:"+fromVer.getPortalBlock());
									//if(fromVer.getPortalBlock() != -1){
										/*HashSet<Integer> disSet = disMap.get(edgeWei);
										if(disSet == null){
											disSet = new HashSet<Integer>();
											disMap.put(edgeWei, disSet);
										}
										if(disSet.add(fromVid)){*/
											size++;
											out.write(bid+" connect "+portalId+" "+fromVid+" "+edgeWei+"\n");
										//}
									//}
								}
							}
							else if(toVid != popid && !visitedSet.contains(toVid)){
								HashSet<Integer> vSet = toVisitMap.get(edgeWei);
								if(vSet == null){
									vSet = new HashSet<Integer>();
									toVisitMap.put(edgeWei, vSet);
								}
								if(vSet.add(toVid) && portalSet.contains(toVid)){
									//Vertex toVer = graphClass.getVertexFromVid(toVid);
									//System.out.println("portal:"+portalId+" new vid:"+toVid+" portal block:"+toVer.getPortalBlock());
									//if(toVer.getPortalBlock() != -1){
										/*HashSet<Integer> disSet = disMap.get(edgeWei);
										if(disSet == null){
											disSet = new HashSet<Integer>();
											disMap.put(edgeWei, disSet);
										}
										if(disSet.add(toVid)){*/
											size++;
											out.write(bid+" connect "+portalId+" "+toVid+" "+edgeWei+"\n");
										//}
									//}
								}
							}
						}
					}
					toVisitSet = null;
				}
			}
			
		} catch (IOException e) {
			// TODO Auto-generated catch block
			e.printStackTrace();
		}
	}
	
	public Partition(){;}
}
