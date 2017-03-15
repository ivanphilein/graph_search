package oneIteMR;

public class EdgeOne {
	private int edgeID;
	private int vFrom;
	private int vTo;
	private double weight;
	
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
	
	public String showEdge(){
		return vFrom+" "+vTo+" "+weight;
	}
}
