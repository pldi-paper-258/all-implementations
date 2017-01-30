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

public class PcaRowWiseMeanHDFS {
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
        double[] mean = new double[100000];
		int num_cols = 500000;        

        for (int i=0;i<status.length;i++){
            BufferedReader br = new BufferedReader(new InputStreamReader(hdfs.open(status[i].getPath())));
            String line;
            line=br.readLine();
	        while (line != null){
	        	String[] data = line.split("\\s");
				
				int row = Integer.parseInt(data[0]);
				mean[row] += Double.parseDouble(data[2]);
	    		
	            line=br.readLine();
	        }
        }

		for(int i=0; i<mean.length; i++){
			mean[i] = mean[i] / num_cols;
			bw.write(i + " " + mean[i] + "\n");
		}
        
        bw.close();
	}
}
