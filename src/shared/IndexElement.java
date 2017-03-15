package shared;

import java.text.DecimalFormat;


public class IndexElement {
	private double length;
	private int startVertex;
	private int endVertex;
	private int nextVertex;
	
	public int source = 0;//for iteration mapreduce method
	//private IndexClass nextIndex;
	
	public IndexElement(){
		length = 0.0;
		startVertex = 0;
		endVertex = 0;
		nextVertex = 0;
		//nextIndex = null;
	}
	
	public IndexElement(double dis, int s, int e){
		length = dis;
		startVertex = s;
		endVertex = e;
		nextVertex = 0;
		//nextIndex = null;
	}
	
	public IndexElement(double dis, int s, int n, int e){
		length = dis;
		startVertex = s;
		endVertex = e;
		nextVertex = n;
		//nextIndex = null;
	}
	
	public IndexElement(double dis, int s, int n, int e, int sou){
		length = dis;
		startVertex = s;
		endVertex = e;
		nextVertex = n;
		source = sou;
		//nextIndex = null;
	}
	
	/**
	 * Giving a String as input, get the correct value and initial class 
	 * @param element String like 0.0,1,2,3
	 */
	public IndexElement(String element){
		//element = element.replace("(", "").replace(")", "");
		String[] temp;
		String delimiter = ",";
		//temp = element.replace("(","").replace(")","").split(delimiter);
		temp = element.split(delimiter);
		//System.out.println("element "+element);
		length = Double.parseDouble(temp[0]);
		startVertex = Integer.parseInt(temp[1]);
		endVertex = Integer.parseInt(temp[3]);
		nextVertex = Integer.parseInt(temp[2]);
		//nextIndex = null;
	}
	
	public IndexElement(String element, int sou){
		//element = element.replace("(", "").replace(")", "");
		String[] temp;
		String delimiter = ",";
		//temp = element.replace("(","").replace(")","").split(delimiter);
		temp = element.split(delimiter);
		//System.out.println("element "+element);
		length = Double.parseDouble(temp[0]);
		startVertex = Integer.parseInt(temp[1]);
		endVertex = Integer.parseInt(temp[3]);
		nextVertex = Integer.parseInt(temp[2]);
		this.source = sou;
		//nextIndex = null;
	}
	public void setElement(double len, int start, int next, int end){
		length = len;
		startVertex = start;
		endVertex = end;
		nextVertex = next;
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
		return length+","+startVertex+","+nextVertex+","+endVertex;
	}
	
	public String getElementWithoutNext(){
		return length+","+startVertex+","+endVertex;
	}
	
	public void showElement(){
		System.out.println("("+length+","+startVertex+","+nextVertex+","+endVertex+")");
	}
}
