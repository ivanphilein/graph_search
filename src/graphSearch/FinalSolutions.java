package graphSearch;


import java.util.ArrayList;
import java.util.HashMap;
import java.util.Iterator;
import java.util.List;
import java.util.Map.Entry;
import java.util.TreeMap;

import shared.IndexElement;


public class FinalSolutions {
	private List<SolutionClass> solutionList = new ArrayList<SolutionClass>();
	private List<SolutionClass> topKSolList = null;//new ArrayList<SolutionClass>();
	private HashMap<Integer, HashMap<Integer, Double>> portalDisMap = null;//from this vertex, the distance to other portal nodes

	private String connLable = "connect";
	private HashMap<Integer, Integer> targetMap = null;
	
	private double topKSum = 0;
	
	public double getTopKSum(){
		return topKSum;
	}
	
	public void setTopKSum(double sum){
		topKSum = sum;
	}
	
	public void addToTargetMap(int vid, int blockid){
		if(targetMap==null){
			targetMap = new HashMap<Integer, Integer>();
		}
		targetMap.put(vid, blockid);
	}
	
	public void readTargetInfo(String inputStr){
		String[] temp = inputStr.split(" ");
		for(int i=0; i<temp.length; i=i+2){
			this.addToTargetMap(Integer.parseInt(temp[i]), Integer.parseInt(temp[i+1]));
		}
	}
	
	public int getTargetBlock(int vid){
		return targetMap.get(vid);
	}
	
	public String returnTargetMap(){
		if(targetMap==null)
			return null;
		String retStr = "target ";
		Iterator<Entry<Integer, Integer>> iter = targetMap.entrySet().iterator();
		if(iter.hasNext()){
			Entry<Integer, Integer> entry = iter.next();
			retStr += entry.getKey()+" "+entry.getValue();
		}
		while(iter.hasNext()){
			Entry<Integer, Integer> entry = iter.next();
			retStr += " "+entry.getKey()+" "+entry.getValue();
		}
		return retStr;
	}
	////////////////////////////////////////////////////////////////////////////////////////////////////////
	//portalDisMap part
	/**
	 * Add distance to one portal node, insert if there is no record of this portal node before
	 * @param portal
	 * @param dis
	 */
	public void addToPortalDisMap(int vid, int portal, Double dis, boolean create){
		if(portalDisMap==null){
			portalDisMap = new HashMap<Integer, HashMap<Integer, Double>>();
		}
		HashMap<Integer, Double> disMap = portalDisMap.get(vid);
		if(disMap==null){
			disMap = new HashMap<Integer, Double>();
			portalDisMap.put(vid, disMap);
		}
		Object value = disMap.get(portal);
		boolean update =false;
		if(value == null){
			disMap.put(portal, dis);
			update = true;
		}
		else{
			double v = (Double) value;
			if(v>dis){
				disMap.put(portal, dis);
				update = true;
			}
		}
		
		//update exsited distance
		if(create && update){
			Iterator<Entry<Integer, Double>> iterDis = disMap.entrySet().iterator();
			while(iterDis.hasNext()){
				Entry<Integer, Double> entry = iterDis.next();
				int portalid = entry.getKey();
				if(portalid==portal)
					continue;
				double portalDis = entry.getValue();
				if(portalid<portal)
					this.addToPortalDisMap(portalid, portal, portalDis+dis, create);
				else
					this.addToPortalDisMap(portal, portalid, portalDis+dis, create);
			}
		}
	}
	
	public void addToPortalDisMapForIterMR(int vid, int portal, Double dis){
		if(portalDisMap==null){
			portalDisMap = new HashMap<Integer, HashMap<Integer, Double>>();
		}
		HashMap<Integer, Double> disMap = portalDisMap.get(vid);
		if(disMap==null){
			disMap = new HashMap<Integer, Double>();
			portalDisMap.put(vid, disMap);
		}
		Object value = disMap.get(portal);
		//boolean update =false;
		if(value == null){
			disMap.put(portal, dis);
			//update = true;
		}
		else{
			double v = (Double) value;
			if(v>dis){
				disMap.put(portal, dis);
				//update = true;
			}
		}
		
		//update exsited distance
		/*if(update){
			Iterator<Entry<Integer, Double>> iterDis = disMap.entrySet().iterator();
			while(iterDis.hasNext()){
				Entry<Integer, Double> entry = iterDis.next();
				int portalid = entry.getKey();
				if(portalid==portal)
					continue;
				double portalDis = entry.getValue();
				this.addToPortalDisMapForIterMR(portalid, portal, portalDis+dis);
				this.addToPortalDisMapForIterMR(portal, portalid, portalDis+dis);
			}
		}*/
	}
	
	public HashMap<Integer, HashMap<Integer, Double>> getPortalDisMap(){
		return portalDisMap;
	}
	
	
	public HashMap<Integer, Double> getPortalDisMapFromVid(int vid){
		if(portalDisMap==null){
			return null;
		}
		return portalDisMap.get(vid);
	}
	/**
	 * That is generate one distance map to other portal nodes
	 * @param vid
	 * @return
	 */
	public String retPDisMapFromVidStr(int vid){
		if(portalDisMap==null){
			return null;
		}
		String retStr = connLable+" "+vid;
		HashMap<Integer, Double> disMap = portalDisMap.get(vid);
		Iterator<Entry<Integer, Double>> iter = disMap.entrySet().iterator();
		boolean add =false;
		while(iter.hasNext()){
			Entry<Integer, Double> entry = iter.next();
			int portal = entry.getKey();
			if(portal>vid){
				add = true;
				retStr += " "+portal+":"+entry.getValue();
			}
		}
		if(add)
			return retStr;
		else return null;
	}
	
	public String returnPortalDisMap(){
		String retStr = "portal_dis_map:";
		if(portalDisMap==null){
			return retStr+"NULL";
		}
		Iterator<Entry<Integer, HashMap<Integer, Double>>> iter = portalDisMap.entrySet().iterator();
		while(iter.hasNext()){
			Entry<Integer, HashMap<Integer, Double>> entry = iter.next();
			int vid = entry.getKey();
			retStr +="\n vid:"+vid;
			HashMap<Integer, Double> disMap = entry.getValue();
			Iterator<Entry<Integer, Double>> iterDis = disMap.entrySet().iterator();
			while(iterDis.hasNext()){
				Entry<Integer, Double> disEntry = iterDis.next();
				retStr += " pid:"+disEntry.getKey()+" dis:"+disEntry.getValue();
			}
		}
		return retStr;
	}
	
	//end of portalDisMap part
	////////////////////////////////////////////////////////////////////////////////////////////////////////
	
	
	///////////////////////////////////////////////////////////////////////////////////////////////////////
	//candidate solutions part
	//candidate solutions for portal nodes, first integer is portal node id, second integer is kid, last list is all the index from portal node id to that kid
	private HashMap<Integer, HashMap<Integer, List<IndexElement>>> candMap = null;

	public void addPathToCand(int kid, int vid, int nextid, int endid, double weight){
		IndexElement index = new IndexElement(weight, vid, nextid, endid);
		if(candMap==null){
			candMap = new HashMap<Integer, HashMap<Integer, List<IndexElement>>>();
		}
		HashMap<Integer, List<IndexElement>> pathMap = candMap.get(vid);
		if(pathMap==null){
			pathMap = new HashMap<Integer, List<IndexElement>>();
			List<IndexElement> list = new ArrayList<IndexElement>();
			list.add(index);
			pathMap.put(kid, list);
			candMap.put(vid, pathMap);
		}
		else{
			List<IndexElement> list = pathMap.get(kid);
			if(list == null){
				list = new ArrayList<IndexElement>();
				pathMap.put(kid, list);
			}
			list.add(index);
		}
	}
	
	public String returnCandMapStr(){
		if(candMap==null)
			return "NULL";
		String retStr = "Candidate:\n";
		Iterator<Entry<Integer, HashMap<Integer, List<IndexElement>>>> iter = candMap.entrySet().iterator();
		while(iter.hasNext()){
			Entry<Integer, HashMap<Integer, List<IndexElement>>> entry = iter.next();
			retStr += "vid:"+entry.getKey();
			Iterator<Entry<Integer, List<IndexElement>>> iterPath = entry.getValue().entrySet().iterator();
			while(iterPath.hasNext()){
				Entry<Integer, List<IndexElement>> entryPath = iterPath.next();
				retStr += " "+entryPath.getKey()+":";
				Iterator<IndexElement> iterIndex = entryPath.getValue().iterator();
				while(iterIndex.hasNext()){
					retStr += iterIndex.next().getElement()+"-";
				}
			}
			retStr += "\n";
		}
		return retStr;
	}
	
	public HashMap<Integer, HashMap<Integer, List<IndexElement>>> getCandMap(){
		return candMap;
	}
	//end of candidate solutions part
	///////////////////////////////////////////////////////////////////////////////////////////////////////
	public int getSize(){
		if(solutionList!=null)
			return solutionList.size();
		else
			return topKSolList.size();
	}
	
	public List<SolutionClass> getInterSolutions(){
		return solutionList;
	}
	public boolean addSolution(SolutionClass solution){
		Iterator<SolutionClass> iter = solutionList.iterator();
		SolutionClass remSol = null;
    	while(iter.hasNext()){
    		SolutionClass sol = iter.next();
    		if(sameSolution(sol, solution)){
    			if(sol.sum<=solution.sum)
    				return false;
    			remSol = sol;
    		}
    	}
    	solutionList.add(solution);
    	if(remSol!=null){
    		solutionList.remove(remSol);
    	}
    	return true;
	}
	
	public List<SolutionClass> getTopKSol(){
		return topKSolList;
	}
	
	public String returnSolutions(){
    	String retStr = "";
    	if(topKSolList!=null){
	    	Iterator<SolutionClass> iter = topKSolList.iterator();
	    	while(iter.hasNext()){
	    		SolutionClass solution = iter.next();
	    		retStr += solution.getSolution()+"\n";
	    	}
    	}
    	return retStr;
    }
	
    public String returnSolutions(boolean old){
    	String retStr = "";
    	if(solutionList!=null){
	    	Iterator<SolutionClass> iter = solutionList.iterator();
	    	while(iter.hasNext()){
	    		SolutionClass solution = iter.next();
	    		retStr += solution.getSolution()+"\n";
	    	}
    	}
    	return retStr;
    }
    
    public double returnSolutions(int topK){
    	if(solutionList == null){
    		return -1;
    	}
    	double biggestSum = -1;
    	TreeMap<Double, List<SolutionClass>> finalMap = new TreeMap<Double, List<SolutionClass>>();
    	Iterator<SolutionClass> iter = solutionList.iterator();
    	while(iter.hasNext()){
    		SolutionClass solution = iter.next();
    		double solSum = solution.getSum();
    		List<SolutionClass> solList = finalMap.get(solSum);
    		if(solList == null){
    			solList = new ArrayList<SolutionClass>();
    			finalMap.put(solSum, solList);
    		}
    		solList.add(solution);
    	}
		Iterator<Entry<Double, List<SolutionClass>>> iterMap = finalMap.entrySet().iterator();
		topKSolList = new ArrayList<SolutionClass>();
    	while(topK>0&&iterMap.hasNext()){
    		List<SolutionClass> tempList = iterMap.next().getValue();
    		Iterator<SolutionClass> tempIter = tempList.iterator();
    		while(tempIter.hasNext()){
    			SolutionClass sol = tempIter.next();
    			topKSolList.add(sol);
    			topK--;
    			if(topK<=0){
    				biggestSum = sol.getSum();
    				break;
    			}
    		}
    	}
    	solutionList = null;
    	return biggestSum;
    }
    

	/**
	 * Only check when this is one whole solution
	 * @param secSol
	 * @return
	 */
	public boolean sameSolution(SolutionClass fisSol, SolutionClass secSol){
		Iterator<Entry<Integer, IndexElement>> iter = fisSol.getSolMap().entrySet().iterator();
    	while(iter.hasNext()){
    		Entry<Integer, IndexElement> entry = iter.next();
    		int kid = entry.getKey();
    		if(secSol.getPathEndV(kid)!=fisSol.getPathEndV(kid)){
    			return false;
    		}
    	}
    	return true;
	}
}
