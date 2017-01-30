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

public class StringMatchHDFS {
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
        
        // String search
        String key1 = "AAHS";
    	String key2 = "AALS";
    	String key3 = "SOMETHING";
    	
    	boolean foundKey1 = false;
		boolean foundKey2 = false;
		boolean foundKey3 = false;
    	
        for (int i=0;i<status.length;i++){
            BufferedReader br = new BufferedReader(new InputStreamReader(hdfs.open(status[i].getPath())));
            String line;
            line=br.readLine();
            while (line != null){
            	String[] data = line.split("\\s");
            	for(int j=0;j<data.length;j++) {
        			if(key1.equals(data[j]))
        				foundKey1 = true;
        			if(key2.equals(data[j]))
        				foundKey2 = true;
        			if(key3.equals(data[j]))
        				foundKey3 = true;
        		}
                line=br.readLine();
            }
        }
        
        bw.write("key1 " + foundKey1);
        bw.write("key2 " + foundKey2);
        bw.write("key3 " + foundKey3);
        bw.close();
	}
}
