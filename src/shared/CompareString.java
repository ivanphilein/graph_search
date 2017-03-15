package shared;

import java.util.Comparator;

public class CompareString implements Comparator<String>{
	
	public int compare(String e1, String e2) 
	{
	    String[] temp1 = e1.split("_");
	    int size1 = Integer.parseInt(temp1[1]);
	    String[] temp2 = e2.split("_");
	    int size2 = Integer.parseInt(temp2[1]);
	    if(size1<size2)
	    	return -1;
	    if(size1>size2)
	    	return 1;
	    int[] tempInt1 = new int[size1+1];
	    for(int i=0;i<size1;i++){
	    	tempInt1[i] = Integer.parseInt(temp1[i+2]);
	    }
	    
	    int[] tempInt2 = new int[size2+1];
	    for(int i=0;i<size2;i++){
	    	tempInt2[i] = Integer.parseInt(temp2[i+2]);
	    }
	    System.out.println("String:"+e1+" "+e2);
	    System.out.println("size:"+size1+" "+size2);
	    for(int i=0; i<size1+1; i++){
	    	System.out.println("temp:"+tempInt1[i]+" "+tempInt2[i]);
	    	if(tempInt1[i]<tempInt2[i]){
	    		return -1;
	    	}
	    	if(tempInt1[i]>tempInt2[i]){
	    		return 1;
	    	}
	    }
	    System.out.print("equal");
	    return 0;
	}
}
