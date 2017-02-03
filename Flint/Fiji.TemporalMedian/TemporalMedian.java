package fiji;

/*
 * Copyright (c) 2013, Graeme Ball
 * Micron Oxford, University of Oxford, Department of Biochemistry.
 *
 *  This program is free software: you can redistribute it and/or modify
 *  it under the terms of the GNU General Public License as published by
 *  the Free Software Foundation, either version 3 of the License, or
 *  (at your option) any later version.
 *
 *  This program is distributed in the hope that it will be useful,
 *  but WITHOUT ANY WARRANTY; without even the implied warranty of
 *  MERCHANTABILITY or FITNESS FOR A PARTICULAR PURPOSE.  See the
 *  GNU General Public License for more details.
 *
 *  You should have received a copy of the GNU General Public License
 *  along with this program.  If not, see http://www.gnu.org/licenses/ .
 */

import ij.IJ;
import ij.ImageJ;
import ij.ImagePlus;
import ij.ImageStack;
import ij.gui.GenericDialog;
import ij.plugin.PlugIn;
import ij.process.ColorProcessor;
import ij.process.ImageProcessor;
import ij.process.FloatProcessor;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.Iterator;
import java.util.List;
import java.util.Map;

import org.apache.spark.SparkConf;
import org.apache.spark.api.java.JavaPairRDD;
import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.api.java.JavaSparkContext;
import org.apache.spark.api.java.function.FlatMapFunction;
import org.apache.spark.api.java.function.Function;
import org.apache.spark.api.java.function.Function2;
import org.apache.spark.api.java.function.PairFlatMapFunction;

import scala.Tuple2;

/**
 * A "probabilistic" temporal median filter to extract a foreground
 * probability imp from a time sequence.
 *
 * @author graemeball@googlemail.com
 */
public class TemporalMedian implements PlugIn {

    // image properties
    int width;
    int height;
    int nc;
    int nz;
    int nt;

    // plugin parameters with defaults
    public int twh = 5;       // time window half-width for median calc
    public double nsd = 2.0;  // number of stdev's above median for foreground

    public void run(String arg) {
        ImagePlus imp = IJ.getImage();

        if (showDialog()) {
            if (imp.getNFrames() > (2*twh + 1)) {
                ImagePlus result = exec(imp);
                result.show();
            } else {
                IJ.showMessage("Insufficient time points, " + nt);
            }
        }
    }

    boolean showDialog() {
        GenericDialog gd = new GenericDialog("Temporal Median");
        gd.addNumericField("time_window half-width", twh, 0);
        gd.addNumericField("foreground_stdevs over median", nsd, 1);
        gd.showDialog();
        if (gd.wasCanceled()) {
            return false;
        } else {
            twh = (int)gd.getNextNumber();
            nsd = (float)gd.getNextNumber();
            return true;
        }
    }

    /**
     * Execute temporal median filter, returning new foreground ImagePlus.
     * Uses array of pixel arrays for sliding window of time frames.
     *
     * @param imp (multi-dimensional, i.e. multiple frames)
     */
    public ImagePlus exec(ImagePlus imp) {
        this.width = imp.getWidth();
        this.height = imp.getHeight();
        this.nc = imp.getNChannels();
        this.nz = imp.getNSlices();
        this.nt = imp.getNFrames();
        ImageStack inStack = imp.getStack();
        int size = inStack.getSize();
        ImageStack outStack = new ImageStack(width, height, size);
        // for all channels and slices, process sliding time window
        int progressCtr = 0;
        IJ.showStatus("Finding Foreground...");
        for (int c = 1; c <= nc; c++) {
            for (int z = 1; z <= nz; z++) {
                // build initial time window array of pixel arrays
                float[][] tWinPix = new float[2 * twh + 1][width * height];
                int wmin = 0;  // window min index
                int wcurr = 0;  // index within window of current frame
                int wmax = twh;  // window max index
                for (int t = 1; t <= wmax + 1; t++) {
                    int index = imp.getStackIndex(c, z, t);
                    tWinPix[t - 1] = vsPixels(inStack, index);
                }
                // process each t and update sliding time window
                for (int t = 1; t <= nt; t++) {
                    float[] fgPix = calcFg(tWinPix, wcurr, wmin, wmax);
                    FloatProcessor fp2 = new FloatProcessor(width, height, fgPix);
                    int index = imp.getStackIndex(c, z, t);
                    outStack.addSlice("" + index, (ImageProcessor)fp2, index);
                    outStack.deleteSlice(index);  // addSlice() *inserts*
                    // sliding window update for next t
                    if (t > twh) {
                        // remove old pixel array from start
                        tWinPix = rmFirst(tWinPix, wmax);
                    } else {
                        wcurr += 1;
                        wmax += 1;
                    }
                    if (t < nt - twh) {
                        // append new pixel array (frame t+twh) to end
                        int newPixIndex =
                                imp.getStackIndex(c, z, t + twh + 1);
                        tWinPix[wmax] = vsPixels(inStack, newPixIndex);
                    } else {
                        wmax -= 1;
                    }
                    IJ.showProgress(progressCtr++, nc * nz * nt);
                }
            }
        }
        ImagePlus result = new ImagePlus("FG_" + imp.getTitle(), outStack);
        result.setDimensions(nc, nz, nt);
        result.setOpenAsHyperStack(true);
        return result;
    }

    /**
     * Return a float array of variance-stabilized pixels for a given
     * stack slice - applies Anscombe transform.
     */
    final List<Float> vsPixels(ImageStack stack, int index) {
        ImageProcessor ip = stack.getProcessor(index);
        FloatProcessor fp = (FloatProcessor)ip.convertToFloat();
        float[] pix = (float[])fp.getPixels();
        List<Float> pixels = new ArrayList<Float>();
        for (int casper_index = 0; casper_index < pix.length; casper_index++){
		    pixels.add(pix[casper_index]);
		}
        {
			int i = 0;
			i = 0;
			boolean loop$0 = false;
			loop$0 = false;
			SparkConf conf = new SparkConf().setAppName("spark");
			JavaSparkContext sc = new JavaSparkContext(conf);
			
			JavaRDD<java.lang.Float> rdd_0_0 = sc.parallelize(pixels);
			final boolean loop0_final = loop$0;
			
			JavaRDD<Float> mapEmits = rdd_0_0.map(new Function<java.lang.Float,Float>(){
				public Float call(java.lang.Float pixels_i) throws Exception {
					return (2 * Math.sqrt(pixels_i + (3 / 8)));
				}
			});
			
			List<Float> output_rdd_0_0 = mapEmits.collect();
			pixels = output_rdd_0_0;
		}
        return pixels;
    }

    /** Calculate foreground pixel array using for tCurr using tWinPix. */
    final float[] calcFg(float[][] tWinPix, int wcurr, int wmin, int wmax) {
        float sd = estimStdev(tWinPix, wmin, wmax);
        int numPix = width * height;
        float[] fgPix = new float[numPix];
        for (int v = 0; v < numPix; v++) {
            float[] tvec = getTvec(tWinPix, v, wmin, wmax);
            float median = median(tvec);
            float currPix = tWinPix[wcurr][v];
            fgPix[v] = calcFgProb(currPix, median, sd);
        }
        return fgPix;
    }

    /** Build time vector for this pixel for  given window. */
    final float[] getTvec(List<float[]> tWinPix, int v, int wmin, int wmax) {
        float[] tvec = null;
        int flat$0 = wmin + 1;
        int flat$1 = wmax - flat$0;
        tvec = (new float[flat$1]);
        {
			int w = 0;
			w = wmin;
			boolean loop$1 = false;
			loop$1 = false;
			SparkConf conf = new SparkConf().setAppName("spark");
			JavaSparkContext sc = new JavaSparkContext(conf);
			
			JavaRDD<float[]> rdd_0_1_0 = sc.parallelize(tWinPix);
			JavaPairRDD<float[], Long> rdd_0_1_1 = rdd_0_1_0.zipWithIndex();
			final int wmax_final = wmax;
			final int wmin_final = wmin;
			final int v_final = v;
			final boolean loop1_final = loop$1;
			
			JavaRDD<Float> mapEmits = rdd_0_1_1.flatMap(new FlatMapFunction<Tuple2<float[], Long>,Float>(){
				public Iterator<Float> call(Tuple2<float[], Long> casper_data_set_i) throws Exception {
					List<Float> emits = new ArrayList<Float>();
					
					if(casper_data_set_i._2 < wmax_final && casper_data_set_i._2 > wmin_final) emits.add(casper_data_set_i._1[v_final]);
					
					
					return emits.iterator();
				}
			});
			
			List<Float> output_rdd_0_1 = mapEmits.collect();
			int casper_index=0;
			for(Float output_rdd_0_1_v : output_rdd_0_1){
				tvec[casper_index] = output_rdd_0_1_v;
				casper_index++;
			}
		}
        return tvec;
    }

    /** Calculate median of an array of floats. */
    final float median(float[] m) {
        Arrays.sort(m);
        int middle = m.length / 2;
        if (m.length % 2 == 1) {
            return m[middle];
        } else {
            return (m[middle - 1] + m[middle]) / 2.0f;
        }
    }

    /** Remove first array of pixels and shift the others to the left. */
    final List<float[]> rmFirst(List<float[]> tWinPix, int wmax) {
    	{
			int i = 0;
			i = 0;
			boolean loop$2 = false;
			loop$2 = false;
			SparkConf conf = new SparkConf().setAppName("spark");
			JavaSparkContext sc = new JavaSparkContext(conf);
			
			JavaRDD<float[]> rdd_0_2_0 = sc.parallelize(tWinPix);
			JavaPairRDD<float[], Long> rdd_0_2_1 = rdd_0_2_0.zipWithIndex();
			final int wmax_final = wmax;
			final boolean loop2_final = loop$2;
			
			JavaPairRDD<Integer,float[]> mapEmits = rdd_0_2_1.flatMapToPair(new PairFlatMapFunction<Tuple2<float[], Long>,Integer,float[]>(){
				public Iterator<Tuple2<Integer, float[]>> call(Tuple2<float[], Long> casper_data_set_i) throws Exception {
					List<Tuple2<Integer, float[]>> emits = new ArrayList<Tuple2<Integer, float[]>>();
					
					if(casper_data_set_i._2 < wmax_final) emits.add(new Tuple2(casper_data_set_i._2-1,casper_data_set_i._1));
					
					
					return emits.iterator();
				}
			});
			
			Map<Integer,float[]> output_rdd_0_2 = mapEmits.collectAsMap();
			for(Integer output_rdd_0_2_k : output_rdd_0_2.keySet()){
				tWinPix.set(output_rdd_0_2_k, output_rdd_0_2.get(output_rdd_0_2_k));
			}
		}
		return tWinPix;
    }

    /**
     * Estimate Stdev for this time window using random 0.1% of tvecs.
     * Returns the average (mean) stdev of a sample of random tvecs.
     */
    final float estimStdev(float[][] tWinPix, int wmin, int wmax) {
        float sd = 0;
        int pixArrayLen = tWinPix[0].length;
        int samples = tWinPix.length * pixArrayLen / 1000;
        for (int n = 0; n < samples; n++) {
            int randPix = (int)Math.floor(Math.random() * pixArrayLen);
            float[] tvec = getTvec(tWinPix, randPix, wmin, wmax);
            sd += calcSD(tvec) / samples;
        }
        return sd;
    }

    /** Standard deviation of a vector of float values. */
    final float calcSD(List<Float> vec) {
    	float sd;
        sd = 0;
        float mean;
        mean = 0;
        float variance;
        variance = 0;
		{
			SparkConf conf = new SparkConf().setAppName("spark");
			JavaSparkContext sc = new JavaSparkContext(conf);
			
			JavaRDD<java.lang.Float> rdd_0_3 = sc.parallelize(vec);
			
			JavaPairRDD<Integer, Float> mapEmits = rdd_0_3.flatMapToPair(new PairFlatMapFunction<java.lang.Float, Integer, Float>() {
				public Iterator<Tuple2<Integer, Float>> call(java.lang.Float vec_t) throws Exception {
					List<Tuple2<Integer, Float>> emits = new ArrayList<Tuple2<Integer, Float>>();
					
					emits.add(new Tuple2(1,new Tuple2(1,vec_t)));
					
					
					return emits.iterator();
				}
			});
			
			JavaPairRDD<Integer, Float> reduceEmits = mapEmits.reduceByKey(new Function2<Float,Float,Float>(){
				public Float call(Float val1, Float val2) throws Exception {
					return (val1+val2);
				}
			});
			
			Map<Integer, Float> output_rdd_0_3 = reduceEmits.collectAsMap();
			mean = output_rdd_0_3.get(1);
		}
		float flat$0 = (float)vec.size();
		float flat$1 = mean / flat$0;
		mean = flat$0;
		{
			SparkConf conf = new SparkConf().setAppName("spark");
			JavaSparkContext sc = new JavaSparkContext(conf);
			
			JavaRDD<java.lang.Float> rdd_0_4 = sc.parallelize(vec);
			final double mean_final = mean;
			
			JavaPairRDD<Integer,Float> mapEmits = rdd_0_4.flatMapToPair(new PairFlatMapFunction<java.lang.Float, Integer,Float>() {
				public Iterator<Tuple2<Integer,Float>> call(java.lang.Float vec_i) throws Exception {
					List<Tuple2<Integer,Float>> emits = new ArrayList<Tuple2<Integer,Float>>();
					
					emits.add(new Tuple2(1,(vec_i-mean_final)*(vec_i-mean_final)));
					
					
					return emits.iterator();
				}
			});
			
			JavaPairRDD<Integer,Float> reduceEmits = mapEmits.reduceByKey(new Function2<Float,Float,Float>(){
				public Float call(Float val1,Float val2) throws Exception {
					return (val1+val2);
				}
			});
			
			Map<Integer, Float> output_rdd_0_0 = reduceEmits.collectAsMap();
			variance = output_rdd_0_0.get(1);
		}
		float flat$2 = (float)vec.size();
		float flat$3 = variance / flat$2;
		variance = flat$3;
        sd = (float)Math.sqrt(variance);
        return sd;
    }

    /**
     * Calculate foreground probability for a pixel using tvec median & stdev.
     * foreground probability, P(x,y,z,t) = Q(-v), where:
     * v = [I(x,y,z,t) - Ibg(x,y,z,t) - k*sigma]/sigma ;
     * I and Ibg are Intensity and background intensity (i.e. temporal median);
     * sigma is standard deviation of intensity over time ;
     * Q is the Q-function (see calcQ).
     *
     */
    final float calcFgProb(float currPix, float median, float sd) {
        float fgProb;
        fgProb = (currPix - median - (float)nsd * sd) / sd;
        fgProb = calcQ(-fgProb);
        return fgProb;
    }

    /**
     * Calculate Q-function, Q(v) = 0.5*(1 - erf(v/sqrt(2))) ;
     * where erf is the error function ;
     * see: see http://en.wikipedia.org/wiki/Q-function
     */
    static final float calcQ(float v) {
        final float root2 = 1.4142135623730950488016887f; // magic ;-)
        float Q;
        Q = (0.5f * (1.0f - calcErf(v / root2)));
        return Q;
    }

    /**
     * Calculate the error function. See e.g.:
     * http://en.wikipedia.org/wiki/Error_function
     */
    static final float calcErf(float v) {
        // room for improvement - this approximation is fast & loose
        float erf;
        final float a1 = 0.278393f;
        final float a2 = 0.230389f;
        final float a3 = 0.000972f;
        final float a4 = 0.078108f;
        boolean neg = false;
        // to use approximation for -ve values of 'v', use: erf(v) = -erf(-v)
        if (v < 0) {
            v = -v;
            neg = true;
        }
        erf = 1.0f - (float)(1.0 / Math.pow((double)(1.0 +
                                        a1 * v +
                                        a2 * v * v +
                                        a3 * v * v * v +
                                        a4 * v * v * v * v), 4.0));
        if (neg) {
            erf = -erf;
        }
        return erf;
    }

    public void showAbout() {
        IJ.showMessage("TemporalMedian",
            "A probabilistic temporal median filter, as described in "
            + "Parton et al. (2011), JCB 194 (1): 121."
        );
    }

    /** Main method for debugging - loads a test imp from Fiji wiki. */
    public static void main(String[] args) {
        Class<?> clazz = TemporalMedian.class;
        // print calcErf and calcQ results in range -2->2 to check
        for (float i = -2; i < 2; i += 0.2) {
            System.out.println("erf(" + i + ") = " + calcErf(i));
            System.out.println("Q(" + i + ") = " + calcQ(i));
        }
        new ImageJ();
        // open TrackMate FakeTracks test data from the Fiji wiki
        ImagePlus image = IJ.openImage(
                "http://fiji.sc/tinevez/TrackMate/FakeTracks.tif");
        image.show();
        IJ.runPlugIn(clazz.getName(), "");  // run this plugin
    }
}