package graphSearch;

public class PQElement {
	private int vid;
	private int nextId;//next vertex id
	private Double priority=0.0;
	//private Double cost=0.0;
	private int source;
	private int key;//the keyword id which can be found from source
	
	public int getKeyword(){
		return key;
	}
	
	public void setKeyword(int k){
		key = k;
	}
	
	public int getSource(){
		return source;
	}
	
	public void setSource(int sou){
		source = sou;
	}
	
	public int getNextID(){
		return nextId;
	}
	
	public void setNextID(int v){
		nextId = v;
	}
	
	public int getVertexID(){
		return vid;
	}
	
	public void setVid(int v){
		vid = v;
	}
	
	public void setPriority(Double pri){
		priority = pri;
	}
	
	public Double getPriority(){
		return priority;
	}
	
	/*public void setCost(Double c){
		cost = c;
	}
	
	public Double getCost(){
		return cost;
	}*/
	
	public String returnElement(){
		return "kid:"+this.getKeyword()+" vid:"+this.getVertexID()+" p:"+this.getPriority()+" n:"+this.getNextID()+" s:"+this.getSource();
	}
}
