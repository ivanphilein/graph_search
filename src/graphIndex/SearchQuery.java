package graphIndex;


public class SearchQuery {
	private int[] searchKey;
	//private String indexPath;
	
	public void setSearchKey(String keywordList, KSearchGraph graph){
		String [] keyword;
  		String delimiter = ",";
  		keyword=keywordList.split(delimiter);
  		searchKey = new int[keyword.length];
  		for(int i=0;i<keyword.length;i++){
  			getSearchKey()[i] = graph.getIDOfKeyword(keyword[i]);
  		}
	}
   
	//return the array of search key
	public int[] getSearchKey(){
		return searchKey;
	}
	
	//show search key
	public void showSearchKey(){
		for(int i=0;i<searchKey.length;i++){
			System.out.print(searchKey[i]+" ");
		}
	}
	
}
