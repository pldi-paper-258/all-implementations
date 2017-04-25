package statistics;

import java.util.ArrayList;
import java.util.Iterator;
import java.util.List;
import java.util.Map;

import org.apache.spark.SparkConf;
import org.apache.spark.api.java.JavaPairRDD;
import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.api.java.JavaSparkContext;
import org.apache.spark.api.java.function.Function;
import org.apache.spark.api.java.function.Function2;
import org.apache.spark.api.java.function.PairFlatMapFunction;

import scala.Tuple2;

/**
 * StatsUtil.java
 * ==============
 *
 * This file is a part of a program which serves as a utility for data analysis
 * of experimental data
 *
 * Copyright (C) 2012-2014  Magdalen Berns <m.berns@sms.ed.ac.uk>
 *
 * This program is free software: you can redistribute it and/or modify
 * it under the terms of the GNU General Public License as published by
 * the Free Software Foundation, either version 3 of the License, or
 * (at your option) any later version.
 * This program is distributed in the hope that it will be useful,
 * but WITHOUT ANY WARRANTY; without even the implied warranty of
 * MERCHANTABILITY or FITNESS FOR A PARTICULAR PURPOSE.  See the
 * GNU General Public License for more details.
 * You should have received a copy of the GNU General Public License
 * along with this program.  If not, see <http://www.gnu.org/licenses/>.
 */
public class StatsUtil{

    private static final double period = 2 * Math.PI;
    /**
     * mean
     *                   Works out the fit of the data
     * @param data
     *                   Array of doubles holding the x and y values in [i][0] and [j][1] respectively
     *
     * @return
     *                  The covariance as a double giving the fit difference of least squares
     */
    public static double mean(List<Double> data){
    	double sum = 0;
		sum = 0.0;
		{
			int i = 0;
			i = 0;
			boolean loop$0 = false;
			loop$0 = false;
			SparkConf conf = new SparkConf().setAppName("spark");
			JavaSparkContext sc = new JavaSparkContext(conf);
			
			JavaRDD<java.lang.Double> rdd_0_0 = sc.parallelize(data);
			final boolean loop0_final = loop$0;
			
			JavaPairRDD<Integer,Double> mapEmits = rdd_0_0.flatMapToPair(new PairFlatMapFunction<java.lang.Double, Integer,Double>() {
				public Iterator<Tuple2<Integer,Double>> call(java.lang.Double data_i) throws Exception {
					List<Tuple2<Integer,Double>> emits = new ArrayList<Tuple2<Integer,Double>>();
					
					emits.add(new Tuple2(1,data_i));
					
					
					return emits.iterator();
				}
			});
			
			JavaPairRDD<Integer,Double> reduceEmits = mapEmits.reduceByKey(new Function2<Double,Double,Double>(){
				public Double call(Double val1,Double val2) throws Exception {
					return (val1+val2);
				}
			});
			
			Map<Integer,Double> output_rdd_0_0 = reduceEmits.collectAsMap();
			sum = output_rdd_0_0.get(1);
		}
		int flat$5 = data.size();
		double flat$6 = (double) flat$5;
		double flat$7 = sum / flat$6;
		double flat$8 = flat$7 - 1;
		return flat$8;
    }

    /**
     * covariance
     *                   Works out the fit of the data
     * @param xVariance
     *                   Array of doubles holding the x variance values
     * @param yVariance
     *                   Array of doubles holding the y variance values
     * @param data
     *                   Array of doubles holding the x and y values in [i][0] and [j][1] respectively
     *
     * @return
     *                  The covariance as a double giving the fit difference of least squares
     */
    public static double covariance(double xVariance, double yVariance, List<double[]> data){
    	double covariance = 0;
		covariance = 0.0;
		{
			int i = 0;
			i = 0;
			boolean loop$0 = false;
			loop$0 = false;
			SparkConf conf = new SparkConf().setAppName("spark");
			JavaSparkContext sc = new JavaSparkContext(conf);
			
			JavaRDD<double[]> rdd_0_0 = sc.parallelize(data);
			final double yVariance_final = yVariance;
			final double xVariance_final = xVariance;
			final boolean loop0_final = loop$0;
			
			JavaPairRDD<Integer,Double> mapEmits = rdd_0_0.flatMapToPair(new PairFlatMapFunction<double[], Integer, Double>() {
				public Iterator<Tuple2<Integer,Double>> call(double[] data_i) throws Exception {
					List<Tuple2<Integer,Double>> emits = new ArrayList<Tuple2<Integer,Double>>();
					
					emits.add(new Tuple2(1,(data_i[0]-xVariance_final)*(data_i[1]-yVariance_final)));
					
					
					return emits.iterator();
				}
			});
			
			JavaPairRDD<Integer,Double> reduceEmits = mapEmits.reduceByKey(new Function2<Double,Double,Double>(){
				public Double call(Double val1,Double val2) throws Exception {
					return (val1+val2);
				}
			});
			
			Map<Integer, Double> output_rdd_0_0 = reduceEmits.collectAsMap();
			covariance = output_rdd_0_0.get(1);
		}
		return covariance;
    }

    /**
     * covariance
     *                   Works out the fit of the data
     * @param xVariance
     *                   Array of doubles holding the x variance values
     * @param yVariance
     *                   Array of doubles holding the y variance values
     * @param x
     *                  Array of doubles holding the x values
     * @param y
     *                  Array of doubles holding the y values
     *
     * @return
     *                  The covariance as a double giving the fit difference of least squares
     */
    public static double covariance(double xVariance, double yVariance, List<Double> x, List<Double> y){
    	double covariance = 0;
		covariance = 0.0;
		{
			int i = 0;
			i = 0;
			boolean loop$0 = false;
			loop$0 = false;
			SparkConf conf = new SparkConf().setAppName("spark");
			JavaSparkContext sc = new JavaSparkContext(conf);
			
			JavaRDD<Double> rdd_0_0_1 = sc.parallelize(x);
			JavaRDD<Double> rdd_0_0_2 = sc.parallelize(y);
			JavaPairRDD<Double,Double> rdd_0_0_3 = rdd_0_0_1.zip(rdd_0_0_2);
			final double yVariance_final = yVariance;
			final double xVariance_final = xVariance;
			final boolean loop0_final = loop$0;
			
			JavaPairRDD<Integer,Double> mapEmits = rdd_0_0_3.flatMapToPair(new PairFlatMapFunction<Tuple2<Double,Double>, Integer, Double>() {
				public Iterator<Tuple2<Integer,Double>> call(Tuple2<Double,Double> casper_data_set_i) throws Exception {
					List<Tuple2<Integer,Double>> emits = new ArrayList<Tuple2<Integer,Double>>();
					
					emits.add(new Tuple2(1,(casper_data_set_i._1-xVariance_final)*(casper_data_set_i._2-yVariance_final)));
					
					
					return emits.iterator();
				}
			});
			
			JavaPairRDD<Integer,Double> reduceEmits = mapEmits.reduceByKey(new Function2<Double,Double,Double>(){
				public Double call(Double val1,Double val2) throws Exception {
					return (val1+val2);
				}
			});
			
			Map<Integer, Double> output_rdd_0_0 = reduceEmits.collectAsMap();
			covariance = output_rdd_0_0.get(1);
		}
		return covariance;
    }

    /**
     * variance
     *          Works out the difference of least squares fit
     * @param data
     *          The data being analysed
     * @param mean
     *          The mean of the data
     *
     * @return
     *          The sum of all the variances
     */
    public static double variance(List<Double> data, double mean){
    	double variance = 0;
		variance = 0.0;
		{
			int i = 0;
			i = 0;
			boolean loop$0 = false;
			loop$0 = false;
			SparkConf conf = new SparkConf().setAppName("spark");
			JavaSparkContext sc = new JavaSparkContext(conf);
			
			JavaRDD<Double> rdd_0_0 = sc.parallelize(data);
			final double mean_final = mean;
			final boolean loop0_final = loop$0;
			
			JavaPairRDD<Integer,Double> mapEmits = rdd_0_0.flatMapToPair(new PairFlatMapFunction<java.lang.Double, Integer,Double>() {
				public Iterator<Tuple2<Integer,Double>> call(java.lang.Double data_i) throws Exception {
					List<Tuple2<Integer,Double>> emits = new ArrayList<Tuple2<Integer,Double>>();
					
					emits.add(new Tuple2(1,Math.pow(data_i-mean_final,2)));
					
					
					return emits.iterator();
				}
			});
			
			JavaPairRDD<Integer,Double> reduceEmits = mapEmits.reduceByKey(new Function2<Double,Double,Double>(){
				public Double call(Double val1,Double val2) throws Exception {
					return (val1+val2);
				}
			});
			
			Map<Integer, Double> output_rdd_0_0 = reduceEmits.collectAsMap();
			variance = output_rdd_0_0.get(1);
		}
		int flat$7 = data.size();
		double flat$8 = variance / flat$7;
		double flat$9 = flat$8 - 1;
		return flat$9;
    }

    /**
     * standardDeviation
     *                   Works out the standard deviation of least squares fit
     * @param variance
     *                   The variance of the data being analysed
     * @param n
     *                   The integer length of the data array
     *
     * @return
     *                   The the standard deviation of least squares fit as a double
     */
    public static double standardDeviation(double variance, int n){
        double stdDev= 0.0;
        if(n > 0) stdDev = Math.sqrt(variance / n);
        return stdDev;
    }

    /**
     * gradient
     *                   Works out the standard deviation of least squares fit
     * @param covariance
     *                   The covariance of the data being analysed
     * @param xVariance
     *                   The integer length of the data array
     * @return
     *          The the standard deviation of least squares fit as a double
     */
    public static double gradient(double covariance, double xVariance){
        return covariance / xVariance;
    }

    /**
     * yIntercept
     *                   Works out the offset of the data (i.e the constant value by which y is offset)
     * @xMean
     *                   The mean value of the x coordinate of the data being analysed
     * @yMean
     *                   The mean value of the y coordinate of the data being analysed
     * @gradient
     *                  The gradient
     * @return
     *          The the standard deviation of least squares fit as a double
     */
    public static double yIntercept(double xMean, double yMean, double gradient){
        return yMean - gradient * xMean;
    }

    /**
     * fit
     *                   Works out the fit of the data
     * @param data
     *                   Array of doubles holding the x and y values in [i][0] and [j][1] respectively
     * @param gradient
     *                  The gradient
     * @param offset
     *                  The offset constant value on the y axis
     * @return
     *                  The the least squares fit as an array of doubles
     */
    public static double[] fit(double gradient, double offset, List<double[]> data){
    	int flat$1 = data.size();
 		double[] fit = null;
 		fit = (new double[flat$1]);
 		{
 			int i = 0;
 			i = 0;
 			boolean loop$0 = false;
 			loop$0 = false;
 			SparkConf conf = new SparkConf().setAppName("spark");
 			JavaSparkContext sc = new JavaSparkContext(conf);
 			
 			JavaRDD<double[]> rdd_0_0 = sc.parallelize(data);
 			final double offset_final = offset;
 			final double gradient_final = gradient;
 			final boolean loop0_final = loop$0;
 			
 			JavaRDD<Double> mapEmits = rdd_0_0.map(new Function<double[], Double>() {
 				public Double call(double[] data_i) throws Exception {
 					return (gradient_final*data_i[0])+offset_final;
 				}
 			});
 			
 			List<Double> output_rdd_0_0 = mapEmits.collect();
 			int casper_index=0;
 			for(Double output_rdd_0_0_v : output_rdd_0_0){
 				fit[casper_index] = output_rdd_0_0_v;
 				casper_index++;
 			}
 		}
 		return fit;
    }

    /**
     * fit
     *                   Works out the fit of the data
     * @param x
     *                   Array of doubles holding the x values
     * @param gradient
     *                  The gradient
     * @param offset
     *                  The offset constant value on the y axis
     * @return
     *                  The the least squares fit as an array of doubles
     */
    public static double[] fit(List<Double> x, double gradient, double offset){
        int flat$1 = x.size();
		double[] fit = null;
		fit = (new double[flat$1]);
		{
			int i = 0;
			i = 0;
			boolean loop$0 = false;
			loop$0 = false;
			SparkConf conf = new SparkConf().setAppName("spark");
			JavaSparkContext sc = new JavaSparkContext(conf);
			
			JavaRDD<Double> rdd_0_0 = sc.parallelize(x);
			final double offset_final = offset;
			final double gradient_final = gradient;
			final boolean loop0_final = loop$0;
			
			JavaRDD<Double> mapEmits = rdd_0_0.map(new Function<Double, Double>() {
				public Double call(Double x_i) throws Exception {
					return (gradient_final*x_i)+offset_final;
				}
			});
			
			List<Double> output_rdd_0_0 = mapEmits.collect();
			int casper_index=0;
			for(Double output_rdd_0_0_v : output_rdd_0_0){
				fit[casper_index] = output_rdd_0_0_v;
				casper_index++;
			}
		}
		return fit;
    }

    /**
     * standardError
     *                  Gives the residual sum of squares.
     * @param data
     *                  Array of doubles holding the x and y values in [i][0] and [j][1] respectively
     * @param fit
     *                  The the least squares fit as an array of doubles
     * @return
     *                  Standard error in mean of y as a double value
     */
    public static double standardError(List<double[]> data, double[] fit){
    	double rss = 0;
		rss = 0.0;
		{
			int i = 0;
			i = 0;
			boolean loop$0 = false;
			loop$0 = false;
			SparkConf conf = new SparkConf().setAppName("spark");
			JavaSparkContext sc = new JavaSparkContext(conf);
			
			JavaRDD<double[]> rdd_0_0 = sc.parallelize(data);
			final boolean loop0_final = loop$0;
			
			JavaPairRDD<Integer,Double> mapEmits = rdd_0_0.flatMapToPair(new PairFlatMapFunction<double[], Integer, Double>() {
				public Iterator<Tuple2<Integer,Double>> call(double[] data_i) throws Exception {
					List<Tuple2<Integer,Double>> emits = new ArrayList<Tuple2<Integer,Double>>();
					
					emits.add(new Tuple2(1,(data_i[0]-data_i[1])*(data_i[0]-data_i[1])));
					
					
					return emits.iterator();
				}
			});
			
			JavaPairRDD<Integer,Double> reduceEmits = mapEmits.reduceByKey(new Function2<Double,Double,Double>(){
				public Double call(Double val1, Double val2) throws Exception {
					return (val2+val1);
				}
			});
			
			Map<Integer,Double> output_rdd_0_0 = reduceEmits.collectAsMap();
			rss = output_rdd_0_0.get(1);
		}
		return rss;
    }

    /**
     * standardError
     *                  Gives the residual sum of squares.
     * @param y
     *                  Array of doubles holding the y values
     *
     * @param fit
     *                  The the least squares fit as an array of doubles
     * @return
     *                  Standard error in mean i.e. residual sum of squares
     *                  as a double
     */
    public static double standardError(List<Double> y, List<Double> fit){
    	double rss = 0;
		rss = 0.0;
		{
			int i = 0;
			i = 0;
			boolean loop$0 = false;
			loop$0 = false;
			SparkConf conf = new SparkConf().setAppName("spark");
			JavaSparkContext sc = new JavaSparkContext(conf);
			
			JavaRDD<Double> rdd_0_0_1 = sc.parallelize(y);
			JavaRDD<Double> rdd_0_0_2 = sc.parallelize(fit);
			JavaPairRDD<Double,Double> rdd_0_0_3 = rdd_0_0_1.zip(rdd_0_0_2);
			final boolean loop0_final = loop$0;
			
			JavaPairRDD<Integer,Double> mapEmits = rdd_0_0_3.flatMapToPair(new PairFlatMapFunction<Tuple2<Double,Double>, Integer,Double>() {
				public Iterator<Tuple2<Integer,Double>> call(Tuple2<Double,Double> casper_data_set_i) throws Exception {
					List<Tuple2<Integer,Double>> emits = new ArrayList<Tuple2<Integer,Double>>();
					
					emits.add(new Tuple2(1,(casper_data_set_i._1-casper_data_set_i._2)*(casper_data_set_i._1-casper_data_set_i._2)));
					
					
					return emits.iterator();
				}
			});
			
			JavaPairRDD<Integer,Double> reduceEmits = mapEmits.reduceByKey(new Function2<Double,Double,Double>(){
				public Double call(Double val1, Double val2) throws Exception {
					return (val2+val1);
				}
			});
			
			Map<Integer,Double> output_rdd_0_0 = reduceEmits.collectAsMap();
			rss = output_rdd_0_0.get(1);
		}
		return rss;
    }

    /**
     * regressionSOS
     *                  Regression sum of squares
     * @param fit
     *                  The the least squares fit as an array of doubles
     * @param yMean
     *                  Array of doubles holding the mean y values
     * @return
     *                  The regression sum of squares as a double
     */
    public static double regressionSumOfSquares(List<Double> fit, double yMean){
        double ssr = 0.0;
		ssr = 0.0;
		{
			int i = 0;
			i = 0;
			boolean loop$0 = false;
			loop$0 = false;
			SparkConf conf = new SparkConf().setAppName("spark");
			JavaSparkContext sc = new JavaSparkContext(conf);
			
			JavaRDD<Double> rdd_0_0 = sc.parallelize(fit);
			final double yMean_final = yMean;
			final boolean loop0_final = loop$0;
			
			JavaPairRDD<Integer,Double> mapEmits = rdd_0_0.flatMapToPair(new PairFlatMapFunction<java.lang.Double, Integer,Double>() {
				public Iterator<Tuple2<Integer,Double>> call(java.lang.Double fit_i) throws Exception {
					List<Tuple2<Integer,Double>> emits = new ArrayList<Tuple2<Integer,Double>>();
					
					emits.add(new Tuple2(1,(fit_i - yMean_final) * (fit_i - yMean_final)));
					
					
					return emits.iterator();
				}
			});
			
			JavaPairRDD<Integer,Double> reduceEmits = mapEmits.reduceByKey(new Function2<Double,Double,Double>(){
				public Double call(Double val1,Double val2) throws Exception {
					return (val1+val2);
				}
			});
			
			Map<Integer, Double> output_rdd_0_0 = reduceEmits.collectAsMap();
			ssr = output_rdd_0_0.get(1);
		}
		return ssr;
    }

    /**
     * residuals
     *                  Difference between y from the least squares fit and the actual data
     * @param y
     *                  Array of doubles holding the y values
     * @param fit
     *                  The the least squares fit as an array of doubles
     * @return
     *                  Array of doubles holding the data's residual points
     */
    public static double[] residuals(List<Double> y, List<Double> fit){
    	int flat$1 = y.size();
		double[] residuals = null;
		residuals = (new double[flat$1]);
		{
			int i = 0;
			i = 0;
			boolean loop$0 = false;
			loop$0 = false;
			SparkConf conf = new SparkConf().setAppName("spark");
			JavaSparkContext sc = new JavaSparkContext(conf);
			
			JavaRDD<Double> rdd_0_0_1 = sc.parallelize(y);
			JavaRDD<Double> rdd_0_0_2 = sc.parallelize(fit);
			JavaPairRDD<Double,Double> rdd_0_0_3 = rdd_0_0_1.zip(rdd_0_0_2);
			
			JavaRDD<Double> mapEmits = rdd_0_0_3.map(new Function<Tuple2<Double,Double>, Double>() {
				public Double call(Tuple2<Double,Double> casper_dataset_i) throws Exception {
					return casper_dataset_i._1-casper_dataset_i._2;
				}
			});
			
			List<Double> output_rdd_0_0 = mapEmits.collect();
			int casper_index=0;
			for(Double output_rdd_0_0_v : output_rdd_0_0){
				residuals[casper_index] = output_rdd_0_0_v;
				casper_index++;
			}
		}
		return residuals;
    }

    /**
     * linearCorrelationCoefficient
     *                              Linear correlation coefficient found when we calculate the regression sum of
     *                              squares over the variance given in y
     * @param regressionSumOfSquares
     *                              Standard error of mean from having calculated the regression sum of squares
     * @param yVariance
     *                              Variance in y as an array of double values

     * @return
     *                              Linear correlation coefficient of data as a double
     */

    public static double linearCorrelationCoefficient(double regressionSumOfSquares, double yVariance){
        return regressionSumOfSquares / yVariance;
    }

    /**
     * errorOffset
     *              Gives the error in the offset
     * @param n
     *              Integer length of the array of data doubles
     * @param xVariance
     *              Variance in x as an array of double values
     * @param xMean
     *              Array of doubles holding the mean x values
     * @param rss
     *              The regression sum of squares as a double
     * @return
     *              Error in the y offset constant value of the line of best fit as a double
     */
    public static double errorOffset(double n, double xVariance, double xMean, double rss) {
        double degreesFreedom = n - 2;  //Assumes that data has only 2 degrees of freedom.
        double sigma = rss / degreesFreedom;
        double sVariance = (sigma / xVariance);
        return Math.sqrt( sigma / n + Math.pow(xMean,2) * sVariance);
    }

    /**
     * errorGradient
     *                  Gives the error in the gradient
     * @param xVariance
     *                  Double holding the value of the variance in x
     * @param rss
     *                  Standard error in the mean of x
     * @return
     *                  Error in the line of best fit's gradient calculation as a double
     */
    public static double errorGradient(double xVariance, double rss, int n){
        double degreesFreedom = n - 2;
        double stdVariance = rss / degreesFreedom;
        return Math.sqrt(stdVariance/ xVariance);
    }

    public static double errorFit(double stdFit){ // TODO Check: forgotten what this is about!?!
        return Math.sqrt(stdFit);
    }

   /**
    * gaussian
    *                         a normalised gaussian to reflect the distribution of the data 
    * @param numberOfSamples
    *                         sample number appropriate to size of original data array
    * @param variance                    
    *                         variance of data
    * @param mean
    *                         mean of data
    * @return
    *                         normalised gaussian in the form of 1D array of doubles for y axis
    */
    public static double[] gaussian(int numberOfSamples, double variance, double mean){
        double[] gaussian = new double[numberOfSamples];
        double tempGaussian= 0.0;

        for (int i=0; i<numberOfSamples; i++){
            gaussian[i] = Math.sqrt(1/(period)* variance)*(Math.exp(-(i-mean)*(i-mean)/(2 * variance)));
            tempGaussian += gaussian[i];
        }
        
        for (int i=0; i< numberOfSamples; i++){ //normalise
            gaussian[i] /= tempGaussian;
        }
        return gaussian;
    }

    /**
     * convolve
     *                        convolve data with a normalised gaussian in order to smooth the output after
     *                        reducing sample rate
     * @param data
     *                         2D data array to be smoothed
     * @param gaussian
     *                         normalised gaussian in the form of 1D array of doubles for y axis
     * @param numberOfSamples
     *                         sample number appropriate to size of original data array
     * @return
     *                        Smoothed data as array of doubles 
     */ 
    public static double[] convolve(int[] data, double[] gaussian, int numberOfSamples){
        double convolved[] = new double[data.length - (numberOfSamples + 1)];
        for (int i=0; i<convolved.length; i++){
            convolved[i] = 0.0;  // Set all doubles to 0.
            for (int j=i, k=0; j<i + numberOfSamples; j++, k++){
                convolved[i] +=  data[j] * gaussian[k];
            }
        }
        return convolved;
    }
}