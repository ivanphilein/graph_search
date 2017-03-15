package graphSearch;

import java.text.DecimalFormat;
import java.util.Iterator;
import java.util.Map.Entry;
import java.util.TreeMap;

import shared.IndexElement;

public class SolutionClass {
	int vid;
	double sum;
	private TreeMap<Integer, IndexElement> solutionMap = new TreeMap<Integer, IndexElement>();
	
	public SolutionClass(){
		
	}
	
	public SolutionClass(String solution){
		String[] temp = solution.split(" ");
		sum = Double.parseDouble(temp[0]);
		for(int i=1; i<temp.length; i++){
			int index = temp[i].indexOf(":");
			int kid = Integer.parseInt(temp[i].substring(0, index));
			IndexElement element = new IndexElement(temp[i].substring(index+1));
			this.addSolution(kid, element);
		}
	}
	/////////////////////////////////////////////////////////////////////////////////
	//vid
	public int getVid(){
		return vid;
	}
	
	public void setVid(int v){
		vid = v;
	}
	//end of vid
	/////////////////////////////////////////////////////////////////////////////////
	/////////////////////////////////////////////////////////////////////////////////
	//sum
	public double getSum(){
		return sum;
	}
	
	public void setSum(double s){
		sum = s;
	}
	//end of sum
	/////////////////////////////////////////////////////////////////////////////////
	/////////////////////////////////////////////////////////////////////////////////
	//solutionMap part
	
	public TreeMap<Integer, IndexElement> getSolMap(){
		return solutionMap;
	}
	
	public int getPathEndV(int kid){
		return solutionMap.get(kid).getEndVertex();
	}
	public void addSolution(int kid, IndexElement index){
		solutionMap.put(kid, index);
	}
	
	public String getSolution(){
		DecimalFormat df = new DecimalFormat("#.##");
		String retStr = df.format(sum)+"";
		Iterator<Entry<Integer, IndexElement>> iter = solutionMap.entrySet().iterator();
		while(iter.hasNext()){
			Entry<Integer, IndexElement> entry = iter.next();
			retStr += " "+df.format(entry.getKey())+":"+entry.getValue().getElement();
		}
		return retStr;
	}
	
	public SolutionClass geneAndUpSolution(int kid, IndexElement index){
		SolutionClass newSol = new SolutionClass();
		newSol.sum = this.sum;
		Iterator<Entry<Integer, IndexElement>> iter = solutionMap.entrySet().iterator();
		while(iter.hasNext()){
			Entry<Integer, IndexElement> entry = iter.next();
			int otherid = entry.getKey();
			if(otherid==kid){
				newSol.addSolution(kid, index);
			}
			newSol.addSolution(otherid, entry.getValue());
		}
		return newSol;
	}
	//end of solutionsMap part
	/////////////////////////////////////////////////////////////////////////////////
}
