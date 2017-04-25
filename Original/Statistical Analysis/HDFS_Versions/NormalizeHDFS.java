package magpie;

import java.io.BufferedReader;
import java.io.BufferedWriter;
import java.io.InputStreamReader;
import java.io.OutputStream;
import java.io.OutputStreamWriter;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileStatus;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;

public class NormalizeHDFS {
	public static void main (String [] args) throws Exception{
		System.out.println("Doing magpie.normalize");
		
		// Open input file from HDFS
		Configuration conf = new Configuration();
    	conf.addResource(new Path("/etc/hadoop/conf/core-site.xml"));
    	conf.addResource(new Path("/etc/hadoop/conf/hdfs-site.xml"));
    	conf.set("fs.hdfs.impl", org.apache.hadoop.hdfs.DistributedFileSystem.class.getName());
	    conf.set("fs.file.impl", org.apache.hadoop.fs.LocalFileSystem.class.getName());
    	FileSystem hdfs = FileSystem.get(conf);
    	
    	// Get input files vector 1
    	FileStatus[] status = hdfs.listStatus(new Path(args[0]));
    	
    	// Open output file in HDFS
        OutputStream os = hdfs.create(new Path(args[1]), true);
        BufferedWriter bw = new BufferedWriter( new OutputStreamWriter(os) );
        
        // mean
        double tempGaussian = 100;
        
        for (int i=0;i<status.length;i++){
        	BufferedReader br = new BufferedReader(new InputStreamReader(hdfs.open(status[i].getPath())));
            String line;
            line=br.readLine();
            
            while (line != null){
            	String[] data = line.split("\\s");
            	double[] a = new double[data.length];
            	for(int j=0;j<data.length;j++) {
        			a[j] = Double.parseDouble(data[j]);
        		}
                
            	double[] result = normalize(a,tempGaussian);
            	
            	for(double val : result){
            		bw.write(val + "\n");
            	}
            	
            	line=br.readLine();
            }
        }
        
        bw.close();
	}
	
	public static double[] normalize(double[] data, double tempGaussian){
		for (int i=0; i< data.length; i++){ //normalise
            data[i] /= tempGaussian;
        }
		return data;
    }
}
