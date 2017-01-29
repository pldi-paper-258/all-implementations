package magpiemanual;

import org.apache.spark.api.java.JavaDoubleRDD;
import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.api.java.function.Function;
import org.apache.spark.api.java.function.Function2;
import org.apache.spark.broadcast.Broadcast;

public class VarienceCalculator {
	//takes java double rdd and returns variance
	public static Double calcVariance(JavaDoubleRDD rdd, Double mean) {
		//This mean value need to be broadcasted
		//the broadcasted value is available on all the nodes of apache spark
		final Broadcast<Double> bMean = SparkFactory.getContext().broadcast(mean);
		//next wee need to apply formula of variance
		//calculating square of difference of value and mean i.e. (Xi-mean)^2
		JavaRDD<Double> varianceRDD = rdd.map(new Function<Double, Double>() {
			private static final long serialVersionUID = 1L;
			@Override
			public Double call(Double x) {
				//calculate difference
				Double diff = (x - bMean.getValue());
				//take the power
				return Math.pow(diff, 2);
			}
		});
		//now need to sum of square of difference sum((Xi-mean)^2)
		//using reduce method
		Double variance = varianceRDD.reduce(new Function2<Double, Double, Double>() {
			private static final long serialVersionUID = 1L;
			@Override
			public Double call(Double a, Double b) throws Exception {
				//return sum of two elements
				return a + b;
			}
		});
		//will return actual variance
		return variance;
	}
}
