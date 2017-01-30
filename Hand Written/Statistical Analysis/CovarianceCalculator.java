package magpiemanual;

import org.apache.spark.api.java.JavaDoubleRDD;
import org.apache.spark.api.java.JavaPairRDD;
import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.api.java.JavaSparkContext;
import org.apache.spark.api.java.function.Function;
import org.apache.spark.api.java.function.Function2;
import org.apache.spark.broadcast.Broadcast;

import scala.Tuple2;

public class CovarianceCalculator {
	public static void calcCoVarience(Double xVarience,Double yVarience,JavaDoubleRDD x,JavaDoubleRDD y) {
		//get spark context from spark factory
		JavaSparkContext ctx = SparkFactory.getContext();
		//broadcast x variance
		final Broadcast<Double> bxVarience = ctx.broadcast(xVarience);
		//broad cast y variance
		final Broadcast<Double> byVarience = ctx.broadcast(yVarience);
		//calculating difference Xi-xvariance
		JavaRDD<Double> xdiff = x.map(new Function<Double, Double>() {
			private static final long serialVersionUID = 1L;
			@Override
			public Double call(Double x) {
				//substract from x the broadcasted x-variance
				return x - bxVarience.getValue();
			}
		});
		//calculate ydiff as Yi-yvariance
		JavaRDD<Double> ydiff = y.map(new Function<Double, Double>() {
			private static final long serialVersionUID = 1L;
			@Override
			public Double call(Double y) {
				//substract from y the broadcasted y-variance
				return y - byVarience.getValue();
			}
		});
		//zip both the RDD's
		//zip will make pair RDD (X1,Y1),(X2,Y2) and so on
		JavaPairRDD<Double, Double> pairRdd = xdiff.zip(ydiff);
		// multiply both elements Xi*Yi to complete formula of co variance
		JavaRDD<Double> result = pairRdd.map(new Function<Tuple2<Double, Double>, Double>() {
			private static final long serialVersionUID = 1L;
			@Override
			public Double call(Tuple2<Double, Double> x) {
				return x._1 * x._2;
			}
		});
		//using reduce method to calculate some of previous elements 
		Double coVarience = result.reduce(new Function2<Double, Double, Double>() {
			private static final long serialVersionUID = 1L;
			@Override
			public Double call(Double a, Double b) throws Exception {
				return a + b;
			}
		});
		
		//return the final covariance
	}
}
