package mapreduce;


import java.util.TreeMap;
import java.util.TreeSet;

import shared.CompareIndexElement;
import shared.IndexElement;


public class IndexMapReduce {
	private TreeMap<Integer, TreeSet<IndexElement>> indexBlockToKeyword = null; //L_KN
	
	public int getBlockID(String keyStr){
		String[] temp;
		String delimiter = " ";
		temp = keyStr.split(delimiter);
		return Integer.parseInt(temp[0]);
	}
	
	public int getKeywordID(String keyStr){
		String[] temp;
		String delimiter = " ";
		temp = keyStr.split(delimiter);
		return Integer.parseInt(temp[1]);
	}
	//indexBlockToKeyword part
	/**
	 * based on each input string, store vertex and its Ancestors information
	 * @param temp string array
	 */
	public void writeIndexBlockToKeyword(String[] temp){
		IndexElement putElement = null;
		if(temp.length>=3){
			int kid = Integer.parseInt(temp[1].toString());
			/*String[] elementSet;
			String delimiter = "-";
			String replace = "),(";
			elementSet = temp[3].toString().replace(replace, delimiter).split(delimiter);*/
			for(int i=2;i<temp.length;i++){
				putElement = new IndexElement(temp[i]);
				this.putListBToK(kid, putElement);
			}
		}//end of writing LKN to indexC class
	}
	/**
	 * based on each input string, store vertex and its Ancestors information
	 * @param vertexStr input string
	 */
	public void writeIndexBlockToKeyword(String indexB2KStr){
		String[] temp;
		String delimiter = " ";
		temp=indexB2KStr.split(delimiter);
		IndexElement putElement = null;
		if(temp.length>=2){
			int kid = Integer.parseInt(temp[0].toString());
			for(int i=1;i<temp.length;i++){
				putElement = new IndexElement(temp[i]);
				this.putListBToK(kid, putElement);
			}
		}//end of writing LKN to indexC class
	}
	
	public boolean indexBToKContain(String key){
		return indexBlockToKeyword.containsKey(key);
	}
	
	public TreeSet<IndexElement> getListBToK(String key){
		if(indexBlockToKeyword == null)
			return null;
		else
			return indexBlockToKeyword.get(key);
	}
	
	
	//just put one indexElement to the corresponding block
	public void putListBToK(int key, IndexElement putElement){
		if(indexBlockToKeyword == null){
			indexBlockToKeyword = new TreeMap<Integer, TreeSet<IndexElement>>();
		}
		if(indexBlockToKeyword.containsKey(key)){
			indexBlockToKeyword.get(key).add(putElement);
			System.out.println(key+"find!!!!!");
		}
		else{
			TreeSet<IndexElement> newSet = new TreeSet<IndexElement>(new CompareIndexElement());
			newSet.add(putElement);
			indexBlockToKeyword.put(key, newSet);
		}
	}
	
	public TreeSet<IndexElement> removeFromMapBToK(String key){
		if(indexBlockToKeyword == null|| !indexBlockToKeyword.containsKey(key)){
			return null;
		}
		TreeSet<IndexElement> returnSet = indexBlockToKeyword.get(key);
		indexBlockToKeyword.remove(key);
		return returnSet;
	}
	//end of indexBlockToKeyword part
}
