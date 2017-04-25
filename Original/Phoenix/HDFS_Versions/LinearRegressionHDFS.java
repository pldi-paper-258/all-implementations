package mold;

import java.io.BufferedReader;
import java.io.BufferedWriter;
import java.io.InputStreamReader;
import java.io.OutputStream;
import java.io.OutputStreamWriter;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileStatus;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;

public class LinearRegressionHDFS {
	public static void main (String [] args) throws Exception{
		// Open input file from HDFS
		Configuration conf = new Configuration();
    	conf.addResource(new Path("/etc/hadoop/conf/core-site.xml"));
    	conf.addResource(new Path("/etc/hadoop/conf/hdfs-site.xml"));
    	conf.set("fs.hdfs.impl", org.apache.hadoop.hdfs.DistributedFileSystem.class.getName());
	    conf.set("fs.file.impl", org.apache.hadoop.fs.LocalFileSystem.class.getName());
    	FileSystem hdfs = FileSystem.get(conf);
    	
    	// Get input files
    	FileStatus[] status = hdfs.listStatus(new Path(args[0]));
    	
    	// Open output file in HDFS
        OutputStream os = hdfs.create(new Path(args[1]), true);
        BufferedWriter bw = new BufferedWriter( new OutputStreamWriter(os) );
        
        // Compute regression
        double SX_ll = 0, SY_ll = 0, SXX_ll = 0, SYY_ll = 0, SXY_ll = 0;
        
        for (int i=0;i<status.length;i++){
            BufferedReader br = new BufferedReader(new InputStreamReader(hdfs.open(status[i].getPath())));
            String line;
            line=br.readLine();
	        while (line != null){
	        	String[] data = line.split("\\s");
				
	    		// ADD UP RESULTS
	    		for (int j = 0; j < data.length; j+=2) {
	    			// Compute SX, SY, SYY, SXX, SXY
	    			double x = Double.parseDouble(data[j]);
					double y = Double.parseDouble(data[j+1]);
					
	    			SX_ll += x;
	    			SY_ll += y;
	    			SXX_ll += x * x;
	    			SYY_ll += y * y;
	    			SXY_ll += x * y;
	    		}
	            line=br.readLine();
	        }
        }
        
        bw.write("x " + SX_ll + "\n");
        bw.write("y " + SY_ll + "\n");
        bw.write("xx " + SXX_ll + "\n");
        bw.write("yy " + SYY_ll + "\n");
        bw.write("xy " + SXY_ll + "\n");
        bw.close();
	}
}
