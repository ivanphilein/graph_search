package oneIteMR;

import org.apache.hadoop.io.ArrayWritable;
import org.apache.hadoop.io.DoubleWritable;

public class DouArrayWritable extends ArrayWritable { 
	public DouArrayWritable() {
		super(DoubleWritable.class); 
	} 
	
}
