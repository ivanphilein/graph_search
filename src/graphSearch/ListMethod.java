package graphSearch;

import java.io.BufferedReader;
import java.io.BufferedWriter;
import java.io.DataInputStream;
import java.io.File;
import java.io.FileInputStream;
import java.io.FileWriter;
import java.io.IOException;
import java.io.InputStreamReader;
import java.util.*;


/**
 * This ListMehod contians math method like: sum, mean, median, sd, and one level function for 
 * @author yifanhao
 *
 */
public class ListMethod {
	/**
	 * Calculate sum of one double list
	 * @param a
	 * @return
	 */
    public double sum (List<Double> a){
        if (a.size() > 0) {
        	double sum = 0;

            for (double i : a) {
                sum += i;
            }
            return sum;
        }
        return 0;
    }
    /**
     * Calculate mean of one double list
     * @param a
     * @return
     */
    public double mean (List<Double> a){
    	double sum = sum(a);
        double mean = 0;
        mean = sum / (a.size() * 1.0);
        return mean;
    }
    
    /**
     * Calculate median of one double list
     * @param a
     * @return
     */
    public double median (List<Double> a){
        int middle = a.size()/2;

        if (a.size() % 2 == 1) {
            return a.get(middle);
        } else {
           return (a.get(middle-1) + a.get(middle)) / 2.0;
        }
    }
    
    /**
     * Calculate Standard deviation of one double list
     * @param a
     * @return
     */
    public double sd (List<Double> a){
        int sum = 0;
        double mean = mean(a);

        for (Double i : a)
            sum += Math.pow((i - mean), 2);
        return Math.sqrt( sum / ( a.size() - 1 ) ); // sample
    }
    /**
     * Generate the level based on mean+3*sd
     * @param a
     * @return
     */
    public int getlevel (List<Double> a){
        return (int)(mean(a)+3*sd(a)+0.5);
        
    }
    
    /**
     * Based on query result files, generate a list of longest pathes
     * @param folderStr
     * @param numK
     * @return
     * @throws IOException
     */
    public List<Double> getLongestPath(String folderStr, int numK, int topK) throws IOException{
    	String fileType = "query_"+numK;
    	List<Double> retList = new ArrayList<Double>();
    	File folder = new File(folderStr);
		System.out.println("folder:"+folderStr);
		File[] listOfFiles = folder.listFiles();
		int numF=0;
		for (File file : listOfFiles) {
		    if (file.isFile()) {
		    	String filename = file.getName();
		    	if(filename.contains(".xls")){
		    		continue;
		    	}
		    	numF++;
		    	if(filename.startsWith(fileType) && filename.contains("top"+topK)){
		    		System.out.println("filename:"+filename+" size:"+retList.size()+" file:"+numF);
		    		filename = folderStr+filename;
		    		FileInputStream fstreamN = new FileInputStream(filename);
		    		DataInputStream innode = new DataInputStream(fstreamN);
		    		BufferedReader br = new BufferedReader(new InputStreamReader(innode));
		    		String strLine;
		    		//Read File Line By Line
	    			double biggest = 0;
		    		while ((strLine = br.readLine()) != null)   {
		    			if(strLine.startsWith("#"))
		    				continue;
		    			if(strLine.contains("vid:")){
			    			String[] temp = strLine.split(":");
			    			for(int i=2; i<temp.length; i++){
			    				double cost = Double.parseDouble(temp[i].substring(0, temp[i].indexOf(",")));
			    				if(biggest<cost){
			    					biggest = cost;
			    				}
			    			}
		    			}
		    		}
	    			if(biggest>0)
	    				retList.add(biggest);
		    		br.close();
		    	}
		    }
		}
		return retList;
    }
    
    public void writeToFile(String writeFile, List<Double> c){
		//System.out.println(Debugger.getCallerPosition()+"Write file: "+writeFile +"...");
  		try{
  			
  			FileWriter fstream = new FileWriter(writeFile);
  			BufferedWriter out = new BufferedWriter(fstream);
  			out.write("median:"+this.median(c));
  			out.write("\n");
  			out.write("mean"+mean(c));
  			out.write("\n");
  			out.write("sd:"+sd(c));
  			out.write("\n");
  			out.write("level:"+getlevel(c));
  			//Close the output stream
  			out.close();
	  	}catch (Exception e){//Catch exception if any
			  System.err.println("Error: " + e.getMessage());
		}	
  		System.out.println("Finish writing file: "+writeFile +"...");
    }
    
    public static void main (String[]args) throws NumberFormatException, IOException {
    	String folder = args[0];
    	String outFoler = args[1];
    	int numK = Integer.parseInt(args[2]);
    	int topK = Integer.parseInt(args[3]);
        ListMethod m = new ListMethod();
        List<Double> c = m.getLongestPath(folder, numK, topK);//Arrays.asList(2,49,11,44,88,1,1,5,33,88,5,44,2,44,44,132,6,2,22,22,5,1,22,22);
        /*FileInputStream fstreamN = new FileInputStream(args[0]);
		// Get the object of DataInputStream
		DataInputStream innode = new DataInputStream(fstreamN);
		BufferedReader brnode = new BufferedReader(new InputStreamReader(innode));
		String strLine;
		//Read File Line By Line
		while ((strLine = brnode.readLine()) != null)   {
			if(strLine.startsWith("#"))
				continue;
			c.add(Double.parseDouble(strLine));
		}
		//Close the input stream
		innode.close();*/
        System.out.println("size:"+c.size());
        Collections.sort(c);
        m.writeToFile(outFoler+"query_"+numK+"_top"+topK+"_level.txt", c);
        System.out.println(m.median(c));
        System.out.println(m.mean(c));
        System.out.println(m.sd(c));
        System.out.println(m.getlevel(c));
    }
}
/*public class NormalDistrubition {
    
}*/
