package iteration;

public class ResultElement {
	private int keyword;
	private double length;
	private int startVertex;
	private int endVertex;
	private int nextVertex;
	//private IndexClass nextIndex;
	
	public ResultElement(){
		keyword=9999;
		length = 0.0;
		startVertex = 0;
		endVertex = 0;
		nextVertex = 0;
		//nextIndex = null;
	}
	
	public ResultElement(int key,double dis, int s, int n, int e){
		keyword=key;
		length = dis;
		startVertex = s;
		endVertex = e;
		nextVertex = n;
		//nextIndex = null;
	}
	/**
	 * Giving a String as input, get the correct value and initial class 
	 * @param element String like 0.0,1,2,3
	 */
	public ResultElement(String element){
		//element = element.replace("(", "").replace(")", "");
		String[] temp;
		String delimiter = ",";
		//temp = element.replace("(","").replace(")","").split(delimiter);
		temp = element.split(delimiter);
		//System.out.println("element "+element);
		keyword = Integer.parseInt(temp[0]);
		length = Double.parseDouble(temp[1]);
		startVertex = Integer.parseInt(temp[2]);
		endVertex = Integer.parseInt(temp[4]);
		nextVertex = Integer.parseInt(temp[3]);
		//nextIndex = null;
	}
	
	public void setElement(int key, double len, int start, int next, int end){
		keyword = key;
		length = len;
		startVertex = start;
		endVertex = end;
		nextVertex = next;
	}
	
	public void setkeyword(int key){
		keyword = key;
	}
	
	public int getkeyword(){
		return keyword;
	}
	
	public void setLength(double len){
		length = len;
	}
	public double getLength(){
		return length;
	}
	public void setStartVertex(int sVertex){
		startVertex = sVertex;
	}
	public int getStartVertex(){
		return startVertex;
	}
	public void setEndVertex(int eVertex){
		endVertex = eVertex;
	}
	public int getEndVertex(){
		return endVertex;
	}
	public void setNextVertex(int nVertex){
		nextVertex = nVertex;
	}
	public int getNextVertex(){
		return nextVertex;
	}
	/*public void setIndexClass(IndexClass tempIndex){
		nextIndex = tempIndex;
	}
	public IndexClass getNextIndex(){
		return nextIndex;
	}*/
	
	public String getElement(){
		return keyword+","+length+","+startVertex+","+nextVertex+","+endVertex;
	}
	public void showElement(){
		System.out.println("("+keyword+","+length+","+startVertex+","+nextVertex+","+endVertex+")");
	}
}
