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

public class VectorVectorMultiplyHDFS {
	public static void main (String [] args) throws Exception{
		System.out.println("Doing magpie.vec*vec");
		
		// Open input file from HDFS
		Configuration conf = new Configuration();
    	conf.addResource(new Path("/etc/hadoop/conf/core-site.xml"));
    	conf.addResource(new Path("/etc/hadoop/conf/hdfs-site.xml"));
    	conf.set("fs.hdfs.impl", org.apache.hadoop.hdfs.DistributedFileSystem.class.getName());
	    conf.set("fs.file.impl", org.apache.hadoop.fs.LocalFileSystem.class.getName());
    	FileSystem hdfs = FileSystem.get(conf);
    	
    	// Get input files vector 1
    	FileStatus[] status1 = hdfs.listStatus(new Path(args[0]));
    	
    	// Get input files vector 2
    	FileStatus[] status2 = hdfs.listStatus(new Path(args[1]));
    	
    	// Open output file in HDFS
        OutputStream os = hdfs.create(new Path(args[2]), true);
        BufferedWriter bw = new BufferedWriter( new OutputStreamWriter(os) );
        
        // Vmulmul
        for (int i=0;i<status1.length;i++){
        	BufferedReader br1 = new BufferedReader(new InputStreamReader(hdfs.open(status1[i].getPath())));
            String line1;
            line1=br1.readLine();
            
            BufferedReader br2 = new BufferedReader(new InputStreamReader(hdfs.open(status2[i].getPath())));
            String line2;
            line2=br2.readLine();
            
            while (line1 != null && line2 != null){
            	String[] data = line1.split("\\s");
            	double[] a = new double[data.length];
            	for(int j=0;j<data.length;j++) {
        			a[j] = Double.parseDouble(data[j]);
        		}
                
            	String[] data2 = line2.split("\\s");
            	double[] b = new double[data2.length];
            	for(int j=0;j<data2.length;j++) {
        			b[j] = Double.parseDouble(data2[j]);
        		}
            	
            	double[] result = multiply(a,b);

            	for(double val : result){
            		bw.write(val + "\n");
            	}
            	
            	line1=br1.readLine();
            	line2=br2.readLine();
            }
        }
        
        bw.close();
	}
	
	public static double[] multiply(double[] a, double[] b){
        double[] temp= new double[a.length];
        for(int i=0; i<a.length;i++) temp[i]= a[i] * b[i];
        return temp;
    }
}