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

import java.util.ArrayList;
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
import ij.IJ;
import ij.ImageJ;
import ij.ImagePlus;
import ij.ImageStack;
import ij.gui.GenericDialog;
import ij.plugin.PlugIn;
import ij.process.ImageProcessor;
import ij.process.FloatProcessor;

/**
 * Trail/average intensities over a time window for an imp sequence.
 *
 * @author graemeball@googlemail.com
 */
public class Trails implements PlugIn {

    // ImagePlus and properties
    ImagePlus imp;
    int width;
    int height;
    int nc;
    int nz;
    int nt;

    // plugin parameters with defaults
    public int twh = 2;     // time window half-width for trails

    public void run(String arg) {
        ImagePlus imp = IJ.getImage();
        if (showDialog()) {
            if (imp.getNFrames() > (2 * twh + 1)) {
                ImagePlus imResult = exec(imp);
                imResult.show();
            } else {
                IJ.showMessage("Insufficient time points, " + nt);
            }
        }
    }

    boolean showDialog() {
        GenericDialog gd = new GenericDialog("Trails");
        gd.addNumericField("time_window half-width", twh, 0);
        gd.showDialog();
        if (gd.wasCanceled())
            return false;
        twh = (int)gd.getNextNumber();
        return true;
    }

    /**
     * Execute time-averaging, returning trailed ImagePlus.
     * Builds array of pixel arrays for sliding window of time frames.
     *
     * @param imp (multi-dimensional, i.e. multiple frames)
     */
    public ImagePlus exec(ImagePlus imp) {
        this.nt = imp.getNFrames();
        this.nz = imp.getNSlices();
        this.nc = imp.getNChannels();
        this.width = imp.getWidth();
        this.height = imp.getHeight();
        ImageStack inStack = imp.getStack();
        int size = inStack.getSize();
        ImageStack outStack = new ImageStack(width, height, size);

        // for all channels and slices, process sliding time window
        for (int c = 1; c <= nc; c++) {
            for (int z = 1; z <= nz; z++) {
                // build initial time window array of pixel arrays
                float[][] tWinPix = new float[2 * twh + 1][width * height];
                int wmin = 0;  // window min index
                int wcurr = 0;  // index within window of current frame
                int wmax = twh;  // window max index
                for (int t = 1; t <= wmax + 1; t++) {
                    int index = imp.getStackIndex(c, z, t);
                    tWinPix[t-1] = getfPixels(inStack, index);
                }
                // process each t and update sliding time window
                for (int t = 1; t <= nt; t++) {
                    float[] fgPix = trail(tWinPix, wcurr, wmin, wmax);
                    FloatProcessor fp2 =
                            new FloatProcessor(width, height, fgPix);
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
                        int newPixIndex = imp.getStackIndex(c, z, t + twh + 1);
                        tWinPix[wmax] = getfPixels(inStack, newPixIndex);
                    } else {
                        wmax -= 1;
                    }
                }
            }
        }
        ImagePlus result = new ImagePlus("Trail" + Integer.toString(2 * twh + 1)
                + "_" + imp.getTitle(), outStack);
        result.setDimensions(nc, nz, nt);
        result.setOpenAsHyperStack(true);
        return result;
    }

    /**
     * Return a float array of pixels for a given stack slice.
     */
    final float[] getfPixels(ImageStack stack, int index) {
        ImageProcessor ip = stack.getProcessor(index);
        FloatProcessor fp = (FloatProcessor)ip.convertToFloat();
        float[] pix = (float[])fp.getPixels();
        return pix;
    }

    /** Trail tCurr pixels using tWinPix time window. */
    final float[] trail(float[][] tWinPix, int wcurr, int wmin, int wmax) {
        int numPix = width*height;
        float[] tPix = new float[numPix];
        for (int v=0; v<numPix; v++) {
            float[] tvec = getTvec(tWinPix, v, wmin, wmax);
            tPix[v] = mean(tvec);
        }
        return tPix;
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
			boolean loop$0 = false;
			loop$0 = false;
			SparkConf conf = new SparkConf().setAppName("spark");
			JavaSparkContext sc = new JavaSparkContext(conf);
			
			JavaRDD<float[]> rdd_0_0_0 = sc.parallelize(tWinPix);
			JavaPairRDD<float[], Long> rdd_0_0_1 = rdd_0_0_0.zipWithIndex();
			final int wmax_final = wmax;
			final int wmin_final = wmin;
			final int v_final = v;
			final boolean loop2_final = loop$0;
			
			JavaRDD<Float> mapEmits = rdd_0_0_1.flatMap(new FlatMapFunction<Tuple2<float[], Long>,Float>(){
				public Iterator<Float> call(Tuple2<float[], Long> casper_data_set_i) throws Exception {
					List<Float> emits = new ArrayList<Float>();
					
					if(casper_data_set_i._2 < wmax_final && casper_data_set_i._2 > wmin_final) emits.add(casper_data_set_i._1[v_final]);
					
					
					return emits.iterator();
				}
			});
			
			List<Float> output_rdd_0_0 = mapEmits.collect();
			int casper_index=0;
			for(Float output_rdd_0_0_v : output_rdd_0_0){
				tvec[casper_index] = output_rdd_0_0_v;
				casper_index++;
			}
		}
        return tvec;
    }

    /** Calculate mean of array of floats. */
    final float mean(List<Float> tvec) {
    	float mean = 0;
		mean = 0;
		{
			int t = 0;
			t = 0;
			boolean loop$3 = false;
			loop$3 = false;
			SparkConf conf = new SparkConf().setAppName("spark");
			JavaSparkContext sc = new JavaSparkContext(conf);
			
			JavaRDD<java.lang.Float> rdd_0_1 = sc.parallelize(tvec);
			final boolean loop0_final = loop$3;
			
			JavaPairRDD<Integer, Float> mapEmits = rdd_0_1.flatMapToPair(new PairFlatMapFunction<java.lang.Float, Integer, Float>() {
				public Iterator<Tuple2<Integer, Float>> call(java.lang.Float tvec_t) throws Exception {
					List<Tuple2<Integer, Float>> emits = new ArrayList<Tuple2<Integer, Float>>();
					
					emits.add(new Tuple2(1,new Tuple2(1,tvec_t)));
					
					
					return emits.iterator();
				}
			});
			
			JavaPairRDD<Integer, Float> reduceEmits = mapEmits.reduceByKey(new Function2<Float,Float,Float>(){
				public Float call(Float val1, Float val2) throws Exception {
					return (val1+val2);
				}
			});
			
			Map<Integer, Float> output_rdd_0_1 = reduceEmits.collectAsMap();
			mean = output_rdd_0_1.get(1);
		}
		int flat$0 = tvec.size();
		float flat$1 = (float) flat$0;
		float flat$2 = mean / flat$1;
        return flat$2;
    }

    /** Remove first array of pixels and shift the others to the left. */
    final List<float[]> rmFirst(List<float[]> tWinPix, int wmax) {
    	{
			int i = 0;
			i = 0;
			boolean loop$4 = false;
			loop$4 = false;
			SparkConf conf = new SparkConf().setAppName("spark");
			JavaSparkContext sc = new JavaSparkContext(conf);
			
			JavaRDD<float[]> rdd_0_2_0 = sc.parallelize(tWinPix);
			JavaPairRDD<float[], Long> rdd_0_2_1 = rdd_0_2_0.zipWithIndex();
			final int wmax_final = wmax;
			final boolean loop4_final = loop$4;
			
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

    public void showAbout() {
        IJ.showMessage("Trails",
            "Trail/average intensities over a given time window."
        );
    }

    /** Main method for testing. */
    public static void main(String[] args) {
        Class<?> clazz = Trails.class;
        new ImageJ();
        // open TrackMate FakeTracks test data from the Fiji wiki
        ImagePlus image = IJ.openImage(
                "http://fiji.sc/tinevez/TrackMate/FakeTracks.tif");
        image.show();
        IJ.runPlugIn(clazz.getName(), "");
    }
}