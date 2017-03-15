package shared;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.HashSet;
import java.util.Iterator;
import java.util.List;
import java.util.Map.Entry;
import java.util.TreeMap;
import java.util.TreeSet;

public class IndexClass {
	//key format is (bid,kid) (bid,keyword id) --> L_KN element
	private HashMap<Integer, TreeMap<Integer, TreeSet<IndexElement>>> BlockToKeyword = null;//L_KN
	//private TreeMap<Integer, TreeSet<IndexElement>> indexBlockToKeyword  = new TreeMap<Integer, TreeSet<IndexElement>>();

	
	//from block id to vertex list
	private HashMap<Integer, HashSet<Integer>> blockOfNode =  
		new HashMap<Integer, HashSet<Integer>>();
	
	
	//All the portal nodes
	private List<Integer> portalNList = new ArrayList<Integer>();
	
	////////BlockToKeyword(L_KN) part
	/**
	 * Based on block id and keyword id, get the corresponding Index set
	 * @param bid block id
	 * @param kid keyword id
	 * @return tree set, null if can not find
	 */
	public TreeSet<IndexElement> getIndexBToK(int bid, int kid){
		if(BlockToKeyword==null)
			return null;
		TreeMap<Integer, TreeSet<IndexElement>> keyMap = BlockToKeyword.get(bid);
		if(keyMap == null)
			return null;
		return keyMap.get(kid);
	}
	
	/**
	 * Add nre input element to the LKN
	 * @param bid corresponding block id
	 * @param kid corresponding index id
	 * @param putElement the new add element
	 */
	public void addIndexBToK(int bid, int kid, IndexElement putElement){
		if(BlockToKeyword==null)
			BlockToKeyword = new HashMap<Integer, TreeMap<Integer, TreeSet<IndexElement>>>();
		TreeMap<Integer, TreeSet<IndexElement>> keyMap = BlockToKeyword.get(bid);
		if(keyMap == null){
			keyMap = new TreeMap<Integer, TreeSet<IndexElement>>();
			BlockToKeyword.put(bid, keyMap);
		}
		TreeSet<IndexElement> newSet = keyMap.get(kid);
		if(newSet == null){
			newSet = new TreeSet<IndexElement>(new CompareIndexElement());
			keyMap.put(kid, newSet);
		}
		newSet.add(putElement);
		
	}
	////////End of BlockToKeyword(L_KN) part
	
	
	public void setPortalList(List<Integer> portalList){
		portalNList = portalList;
	}
	
	public List<Integer> getPortalList(){
		return portalNList;
	}
	
	/**
	 * get node list of block bid
	 * @param bid
	 * @return
	 */
	public HashSet<Integer> getNodeSetOfBlock(int bid){
		return blockOfNode.get(bid);
	}
	
	public HashMap<Integer, HashSet<Integer>> getBolckToNodeMap(){
		return blockOfNode;
	}
	
	/**
	 * Get number of block from blockOfNode
	 * @return number of blocks
	 */
	public int getBlockNum(){
		return blockOfNode.size();
	}
	/**
	 * Get the size of biggest block
	 * @return
	 */
	public int getBiggestBlockSise(){
		Iterator<Entry<Integer, HashSet<Integer>>> iter = blockOfNode.entrySet().iterator();
		int size=0;
		while(iter.hasNext()){
			Entry<Integer, HashSet<Integer>> entry = iter.next();
			int tempSize = entry.getValue().size();
			if(size<tempSize){
				size = tempSize;
			}
		}
		return size;
	}
	
	//get block list from vertex id
	/*public List<Integer> getNodeWithBlockList(int vid){
		return v2BlockMap.get(vid);
	}*/
		
	/**
	 * Add vertex vid to block bid, that is a map from bid to all vertex id list in that block
	 * @param bid
	 * @param vid
	 */
	public void addBlockWithNodeID(int bid, int vid){
		HashSet<Integer> vList = blockOfNode.get(bid);//
		if(vList == null){
			vList = new HashSet<Integer>();
			vList.add(vid);
			blockOfNode.put(bid, vList);
		}
		else{
			vList.add(vid);
		}
		
	}
	
	/**
	 * Put block id bid and the corresponding node set to the blockOfNode
	 * @param bid
	 * @param nodeSet
	 */
	public void addBlockWithNodeSet(int bid, HashSet<Integer> nodeSet){
		blockOfNode.put(bid, nodeSet);
	}
	/**
	 * Return the size of block bid
	 * @param bid
	 * @return
	 */
	/*public int getBlockSize(int bid){
		List<Integer> tempList = blockOfNode.get(bid);
		if(tempList==null)
			return -1;
		return tempList.size();
	}*/
	
	//add vertex id and block id list to blockOfNoe
	/*public void addNodeWithBlockID(int vid, int bid){
		List<Integer> bList = v2BlockMap.get(vid);
		if(bList == null){
			bList = new ArrayList<Integer>();
			bList.add(bid);
			v2BlockMap.put(vid, bList);
		}
		else if(!bList.contains(bid)){
			bList.add(bid);
			v2BlockMap.put(vid, bList);
		}
	}*/
	//////////////////////////////////////////////////////////////////
	//indexBlockToKeyword part
	//based on block id and keyword, return list of index
	/*public TreeSet<IndexElement> getIndexBToK(int keyword){
		return indexBlockToKeyword.get(keyword);
	}//end of generateIndexBToK
	
	public boolean indexBToKContain(int key){
		return indexBlockToKeyword.containsKey(key);
	}
	
	public TreeSet<IndexElement> getListBToK(int key){
		return indexBlockToKeyword.get(key);
	}*/
	
	
	/**
	 * just put one indexElement to the corresponding block
	 * @param key
	 * @param putElement
	 */
	/*public void putListBToK(int key, IndexElement putElement){
		TreeSet<IndexElement> newSet = indexBlockToKeyword.get(key);
		if(newSet == null){
			newSet = new TreeSet<IndexElement>(new CompareIndexElement());

			indexBlockToKeyword.put(key, newSet);
		}
		newSet.add(putElement);
		//System.out.println(indexBlockToKeyword.size());
	}
	
	public TreeSet<IndexElement> removeFromMapBToK(String key){
		TreeSet<IndexElement> returnSet = indexBlockToKeyword.get(key);
		indexBlockToKeyword.remove(key);
		return returnSet;
	}
	
	
	//show Index part
	public void showIndexBlockToKeyword(){
		Iterator<Entry<Integer, TreeSet<IndexElement>>> iter = indexBlockToKeyword.entrySet().iterator(); 
    	while (iter.hasNext()) { 
    	    @SuppressWarnings("rawtypes")
			Map.Entry entry = (Map.Entry) iter.next(); 
    	    Object key = entry.getKey(); 
    	   // System.out.println("Block "+key+":");
    	    Iterator<IndexElement> itSet = indexBlockToKeyword.get(key).iterator();
    	    while(itSet.hasNext()){
    	    	itSet.next().showElement();
    	    }
    	} 
	}*/
	//end of show index part
}
