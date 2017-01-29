package magpiemanual;

import org.apache.spark.api.java.JavaDoubleRDD;
import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.api.java.JavaSparkContext;
import org.apache.spark.api.java.function.Function;
import org.apache.spark.broadcast.Broadcast;

public class Normalizer {

	public static void normalize(JavaDoubleRDD x, Double tempGaussian) {
		//get spark context
		JavaSparkContext ctx = SparkFactory.getContext();
		//broadcast the temp gaussian
		final Broadcast<Double> bgaussian = ctx.broadcast(tempGaussian);
		//divide each element by gaussian temp
		JavaRDD<Double> rdd = x.map(new Function<Double, Double>() {
			private static final long serialVersionUID = 1L;
			@Override
			public Double call(Double x) {
				//divide and return
				return x / bgaussian.getValue();
			}
		});
		//return the normalized RDD
		rdd.saveAsTextFile("norm");
	}
}
