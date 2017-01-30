package magpiemanual;

import org.apache.spark.api.java.JavaDoubleRDD;
import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.api.java.JavaSparkContext;
import org.apache.spark.api.java.function.Function;
import org.apache.spark.api.java.function.Function2;
import org.apache.spark.broadcast.Broadcast;

public class RegressionSumOfSquares {

	public static Double regressionSumOfSquares(JavaDoubleRDD x, Double yMean) {
		// get the spark context from factory
		JavaSparkContext ctx = SparkFactory.getContext();
		// broadcast yMean
		final Broadcast<Double> bYMean = ctx.broadcast(yMean);
		// calculate square of error
		JavaRDD<Double> rdd = x.map(new Function<Double, Double>() {
			private static final long serialVersionUID = 1L;

			@Override
			public Double call(Double x) {
				// (Xi-mean)^2
				Double result = (x - bYMean.getValue()) * (x - bYMean.getValue());
				// return result
				return result;
			}
		});
		//calculate sum of square of error
		Double sse = rdd.reduce(new Function2<Double, Double, Double>() {
			private static final long serialVersionUID = 1L;
			@Override
			public Double call(Double a, Double b) throws Exception {
				//add and return
				return a + b;
			}
		});
		//return final sum of square of error
		return sse;
	}
}
