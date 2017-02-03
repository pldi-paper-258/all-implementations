package fiji;

import java.util.ArrayList;
import java.util.List;

import org.apache.spark.SparkConf;
import org.apache.spark.api.java.JavaPairRDD;
import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.api.java.JavaSparkContext;
import org.apache.spark.api.java.function.Function;

import scala.Tuple2;
import ij.ImagePlus;
import ij.plugin.filter.PlugInFilter;
import ij.process.ColorProcessor;
import ij.process.ImageProcessor;

/**
* Convert all reds to magentas (to help red-green blind viewers)
*/
public class Convert_Red_To_Magenta implements PlugInFilter {
	protected ImagePlus image;

	/**
	 * This method gets called by ImageJ / Fiji to determine
	 * whether the current image is of an appropriate type.
	 *
	 * @param arg can be specified in plugins.config
	 * @param image is the currently opened image
	 */
	public int setup(String arg, ImagePlus image) {
		this.image = image;
		return DOES_RGB;
	}

	/**
	 * This method is run when the current image was accepted.
	 *
	 * @param ip is the current slice (typically, plugins use
	 * the ImagePlus set above instead).
	 */
	public void run(ImageProcessor ip) {
		List<Integer> pixels = new ArrayList<Integer>();
		int[] data = (int[])((ColorProcessor)ip).getPixels();
		for (int casper_index = 0; casper_index < data.length; casper_index++){
		    pixels.add(data[casper_index]);
		}
		process(pixels,((ColorProcessor)ip).getWidth(),((ColorProcessor)ip).getHeight());
		image.updateAndDraw();
	}

	public static void process(List<Integer> pixels, int w, int h) {
		{
			int j = 0;
			j = 0;
			boolean loop$0 = false;
			loop$0 = false;
			SparkConf conf = new SparkConf().setAppName("spark");
			JavaSparkContext sc = new JavaSparkContext(conf);
			
			JavaRDD<java.lang.Integer> rdd_0_0 = sc.parallelize(pixels);
			final boolean loop0_final = loop$0;
			
			JavaRDD<Integer> mapEmits = rdd_0_0.map(new Function<java.lang.Integer,Integer>(){
				public Integer call(java.lang.Integer pixels_i) throws Exception {
					return (((pixels_i) & 0xffff00) | (pixels_i >> 16) & 0xff);
				}
			});
			
			List<Integer> output_rdd_0_0 = mapEmits.collect();
			pixels.clear();
			pixels.addAll(output_rdd_0_0);
		}
	}
}