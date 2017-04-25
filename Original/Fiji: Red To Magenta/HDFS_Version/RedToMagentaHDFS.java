package fiji.redtomagenta;

import ij.process.ColorProcessor;

import java.awt.Image;
import java.awt.image.BufferedImage;
import java.io.BufferedReader;
import java.io.BufferedWriter;
import java.io.File;
import java.io.InputStreamReader;
import java.io.OutputStream;
import java.io.OutputStreamWriter;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;

import javax.imageio.ImageIO;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileStatus;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;

public class RedToMagentaHDFS {
	/* Many small images */
	public static void manySmallImages() throws Exception {
		File pathToFolder = new File("/home/Maaz/Downloads/othermonkeys");
		File[] listOfImages = pathToFolder.listFiles();
		
		for(int i=0; i<listOfImages.length; i++){
			File imageFile = new File(listOfImages[i].getAbsolutePath());
			Image image = ImageIO.read(imageFile);
		    ColorProcessor cp = new ColorProcessor(image);
		    Convert_Red_To_Magenta.process(cp);
		    File outputfile = new File(listOfImages[i].getAbsolutePath().replace("othermonkeys", "othermonkeys_t"));
		    ImageIO.write((BufferedImage)cp.getBufferedImage(), "gif", outputfile);
		}
	}
	
	/* One large file */
	public static void oneBigImage(String inpFilename) throws Exception {
		Configuration conf = new Configuration();
    	conf.addResource(new Path("/etc/hadoop/conf/core-site.xml"));
    	conf.addResource(new Path("/etc/hadoop/conf/hdfs-site.xml"));
    	conf.set("fs.hdfs.impl", org.apache.hadoop.hdfs.DistributedFileSystem.class.getName());
	    conf.set("fs.file.impl", org.apache.hadoop.fs.LocalFileSystem.class.getName());
    	FileSystem hdfs = FileSystem.get(conf);
    	
    	// Get input files
    	FileStatus[] status = hdfs.listStatus(new Path(inpFilename));
        
        // Convert red to magenta 
        for (int i=0;i<status.length;i++){
        	// Open output file in HDFS
            OutputStream os = hdfs.create(new Path(inpFilename+"_t"), true);
        	BufferedWriter bw = new BufferedWriter( new OutputStreamWriter(os) );
        	BufferedReader br = new BufferedReader(new InputStreamReader(hdfs.open(status[i].getPath())));
            String line;
            line=br.readLine();
            while (line != null){
            	String[] data = line.split("\\s");
    			int x = Integer.parseInt(data[0]);
    			int y = Integer.parseInt(data[1]);
            	int value = Integer.parseInt(data[2]);
				int red = (value >> 16) & 0xff;
				int green = (value >> 8) & 0xff;
				int blue = value & 0xff;
				if (false && blue > 16)
					continue;
				bw.write(x+","+y+","+((red << 16) | (green << 8) | red)+"\n");
                line=br.readLine();
            }
            bw.close();
        }
	}
	
	/* One large file */
	public static void oneBigImage2(String inpFilename) throws Exception {
		Configuration conf = new Configuration();
    	conf.addResource(new Path("/etc/hadoop/conf/core-site.xml"));
    	conf.addResource(new Path("/etc/hadoop/conf/hdfs-site.xml"));
    	conf.set("fs.hdfs.impl", org.apache.hadoop.hdfs.DistributedFileSystem.class.getName());
	    conf.set("fs.file.impl", org.apache.hadoop.fs.LocalFileSystem.class.getName());
    	FileSystem hdfs = FileSystem.get(conf);
    	
		// Open output file in HDFS
        OutputStream os = hdfs.create(new Path(inpFilename+"_t"), true);
    	BufferedWriter bw = new BufferedWriter( new OutputStreamWriter(os) );
    	BufferedReader br = new BufferedReader(new InputStreamReader(hdfs.open(new Path(inpFilename))));
        String line;
        line=br.readLine();
        
        int w = 50000;
        int h = 50000;
        
        List<Integer> pixels = new ArrayList<Integer>();
        while (line != null){
        	String[] data = line.split("\\s");
			int value = Integer.parseInt(data[2]);
        	pixels.add(value);
        	line=br.readLine();
        }
        
        for (int j = 0; j < h; j++) {	
			for (int i = 0; i < w; i++) {
				int value = pixels.get(i + j * w);
				int red = (value >> 16) & 0xff;
				int green = (value >> 8) & 0xff;
				int blue = value & 0xff;
				if (false && blue > 16)
					continue;
				pixels.set(i + j * w, (red << 16) | (green << 8) | red);
			}
		}
        
	}
}