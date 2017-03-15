package graphIndex;

public class EdgeClass {
	private int edgeID;
	private int vFrom;
	private int vTo;
	private double weight;
	//private boolean visited;
	
	public EdgeClass(){
		//visited = false;
	}
	
	public void setEdgeID(int id){
		edgeID = id;
	}
	public int getEdgeID(){
		return edgeID;
	}
	public void setVFrom(int from){
		vFrom= from;
	}
	public int getVFrom(){
		return vFrom;
	}
	public void setVTo(int to){
		vTo = to;
	}
	public int getVTo(){
		return vTo;
	}
	public void setWeight(double w){
		weight = w;
	}
	public double getWeight(){
		return weight;
	}
	
	/*public void setVisit(boolean w){
		visited = w;
	}
	public boolean getVisit(){
		return visited;
	}*/
	
	public String showEdge(){
		return edgeID+" "+vFrom+" "+vTo+" "+weight;
	}
	
}
