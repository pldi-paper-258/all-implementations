package mold;

import java.io.BufferedReader;
import java.io.BufferedWriter;
import java.io.InputStreamReader;
import java.io.OutputStream;
import java.io.OutputStreamWriter;
import java.util.Arrays;
import java.util.List;

import mold.Histogram.Pixel;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileStatus;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;

public class HistogramHDFS {
	public static void main (String [] args) throws Exception{
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
        
        // Compute histogram
        long[] r = new long[256];
        long[] g = new long[256];
        long[] b = new long[256];
    	
        for (int i=0;i<status.length;i++){
            BufferedReader br = new BufferedReader(new InputStreamReader(hdfs.open(status[i].getPath())));
            String line;
            line=br.readLine();
            while (line != null){
            	String[] data = line.split("\\s");
    			for (int j=0; j< data.length; j+=3) {
    				int rval = Integer.parseInt(data[j]);
    				int gval = Integer.parseInt(data[j+1]);
    				int bval = Integer.parseInt(data[j+2]);
    				
    				r[rval] = r[rval] + 1;
    				g[gval] = g[gval] + 1;
    				b[bval] = b[bval] + 1;
    			}
                line=br.readLine();
            }
        }
        
        bw.write("red " + Arrays.toString(r));
        bw.write("green " + Arrays.toString(g));
        bw.write("blue " + Arrays.toString(b));
        bw.close();
	}
}