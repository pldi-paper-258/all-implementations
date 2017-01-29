package magpiemanual;

import org.apache.spark.api.java.JavaDoubleRDD;
import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.api.java.JavaSparkContext;
import org.apache.spark.api.java.function.Function;
import org.apache.spark.broadcast.Broadcast;

public class Fit {
	//fit the gradient over the RDD
	public static void fit(JavaDoubleRDD x,double gradient, double offset) {
		//get spark context
		JavaSparkContext ctx = SparkFactory.getContext();
		//broadcast gradient
		final Broadcast<Double> bGradient = ctx.broadcast(gradient);
		//broadcast offset
		final Broadcast<Double> bOffset = ctx.broadcast(offset);
		//map this rdd to the formula of fit
		JavaRDD<Double> fitRdd = x.map(new Function<Double, Double>() {
			private static final long serialVersionUID = 1L;
			@Override
			public Double call(Double x) {
				//multiple each element with gradient and add offset in it
				Double result = x * bGradient.getValue() + bOffset.getValue();
				//return the result
				return result;
			}
		});
		//return the fitted rdd
		fitRdd.saveAsTextFile("fit");
	}

}
