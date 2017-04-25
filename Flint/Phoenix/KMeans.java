package phoenix;

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
 * 
 * Translation of Phoenix k-means implementation
 * 
 */

public class KMeans {

	private static class Result{
		public Double[][] means;
		public int[] clusters;
		boolean modified;

		Result(Double[][] m, int[] c, boolean mod){
			this.means = m;
			this.clusters = c;
			this.modified = mod;
		}
	}

	final int GRID_SIZE = 1000;

	public static void main(String[] args) {

		int numPoints = 1000, numMeans = 10, dim = 3;

		Double[][] points = generatePoints(numPoints, dim);
		Double[][] means = generatePoints(numMeans, dim);
		int[] clusters = new int[numPoints];

		boolean modified = false;

		while (!modified) {
			modified = findClustersAndCalcMeans(points,means,
					clusters).modified;
		}

		System.out.println("\n\nFinal Means:\n");
		dumpMatrix(means);
	}

	private static void dumpMatrix(Double[][] a) {
		for (int i = 0; i < a.length; i++) {
			for (int j = 0; j < a[i].length; j++)
				System.out.print(" " + a[i][j]);
			System.out.println();
		}
	}

	private static Result findClustersAndCalcMeans(Double[][] points,
			Double[][] means, int[] clusters) {
		int i, j;
		Double minDist, curDist;
		int minIdx;
		int dim = points[0].length;
		boolean modified = false;
		for (i = 0; i < points.length; i++) {
			minDist = getSqDist(points[i], means[0]);
			minIdx = 0;
			for (j = 1; j < means.length; j++) {
				curDist = getSqDist(points[i], means[j]);
				if (curDist < minDist) {
					minDist = curDist;
					minIdx = j;
				}
			}

			if (clusters[i] != minIdx) {
				clusters[i] = minIdx;
				modified = true;
			}
		}

		for (int ii = 0; ii < means.length; ii++) {
			Double[] sum = new Double[dim];
			int groupSize = 0;
			for (int jj = 0; jj < points.length; jj++) {
				if (clusters[jj] == ii) {
					sum = add(sum, points[jj]);
					groupSize++;
				}
			}
			dim = points[0].length;
			Double[] meansi = means[ii];
			for (int kk = 0; kk < dim; kk++) {
				if (groupSize != 0) {
					meansi[kk] = sum[kk] / groupSize;
				}
			}
			means[ii] = meansi;
		}
		return new Result(means, clusters, modified);
	}

	private static Double[] add(List<Double> v1, List<Double> v2) {
		int flat$1 = v1.size();
		Double[] sum = null;
		sum = (new Double[flat$1]);
		{
			int i = 0;
			i = 0;
			boolean loop$0 = false;
			loop$0 = false;
			SparkConf conf = new SparkConf().setAppName("spark");
			JavaSparkContext sc = new JavaSparkContext(conf);
			
			JavaRDD<Double> rdd_0_0_1 = sc.parallelize(v1);
			JavaRDD<Double> rdd_0_0_2 = sc.parallelize(v2);
			JavaPairRDD<Double, Double> rdd_0_1_3 = rdd_0_0_1.zip(rdd_0_0_2);
			final boolean loop0_final = loop$0;
			
			JavaRDD<Double> mapEmits = rdd_0_1_3.map(new Function<Tuple2<Double,Double>,Double>(){
				public Double call(Tuple2<Double, Double> casper_data_set_i) throws Exception {
					return casper_data_set_i._1+casper_data_set_i._2;
				}
			});
			
			List<Double> output_rdd_0_0 = mapEmits.collect();
			int casper_index=0;
			for(Double output_rdd_0_0_v : output_rdd_0_0){
				sum[casper_index] = output_rdd_0_0_v;
				casper_index++;
			}
		}
		return sum;
	}

	private static Double getSqDist(List<Double> v1, List<Double> v2) {
		Double dist = null;
		dist = 0.0;
		{
			int i = 0;
			i = 0;
			boolean loop$1 = false;
			loop$1 = false;
			SparkConf conf = new SparkConf().setAppName("spark");
			JavaSparkContext sc = new JavaSparkContext(conf);
			
			JavaRDD<Double> rdd_0_1_1 = sc.parallelize(v1);
			JavaRDD<Double> rdd_0_1_2 = sc.parallelize(v2);
			JavaPairRDD<Double, Double> rdd_0_1_3 = rdd_0_1_1.zip(rdd_0_1_2);
			final boolean loop1_final = loop$1;
			
			JavaPairRDD<Integer, java.lang.Double> mapEmits = rdd_0_1_3.flatMapToPair(new PairFlatMapFunction<Tuple2<Double, Double>, Integer, java.lang.Double>() {
				public Iterator<Tuple2<Integer, java.lang.Double>> call(Tuple2<Double, Double> casper_data_set_i) throws Exception {
					List<Tuple2<Integer, java.lang.Double>> emits = new ArrayList<Tuple2<Integer, java.lang.Double>>();
					
					emits.add(new Tuple2(1,(casper_data_set_i._1-casper_data_set_i._2)*(casper_data_set_i._1-casper_data_set_i._2)));
					
					return emits.iterator();
				}
			});
			
			JavaPairRDD<Integer, java.lang.Double> reduceEmits = mapEmits.reduceByKey(new Function2<java.lang.Double,java.lang.Double,java.lang.Double>(){
				public java.lang.Double call(java.lang.Double val1, java.lang.Double val2) throws Exception {
					return (val1+val2);
				}
			});
			
			Map<Integer, java.lang.Double> output_rdd_0_0 = reduceEmits.collectAsMap();
			dist = output_rdd_0_0.get(1);
		}
		return dist;
	}

	private static Double[][] generatePoints(int numPoints, int dim) {
		Double[][] p = new Double[numPoints][dim];
		for (int i = 0; i < numPoints; i++) {
			p[i] = new Double[dim];
			for (int j = 0; j < dim; j++)
				p[i][j] = Math.random();
		}
		return p;
	}

}