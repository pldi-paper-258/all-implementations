package magpiemanual;

import org.apache.spark.api.java.JavaDoubleRDD;
import org.apache.spark.api.java.JavaPairRDD;
import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.api.java.JavaSparkContext;
import org.apache.spark.api.java.function.Function;
import org.apache.spark.broadcast.Broadcast;

import scala.Tuple2;

public class Multiply {

	/*
	 * multiply each element with a constant b
	 * @param a rdd of type double
	 * @param b a double value
	 * @return rdd of double
	 */
	public static void multiply(JavaDoubleRDD x, Double b) {
		//get spark context
		JavaSparkContext ctx = SparkFactory.getContext();
		//broad cast the b value a constant
		final Broadcast<Double> bB = ctx.broadcast(b);
		//multiple each element of a with b 
		JavaRDD<Double> rdd = x.map(new Function<Double, Double>() {
			private static final long serialVersionUID = 1L;

			@Override
			public Double call(Double x) {
				//multiple and return
				return x * bB.getValue();
			}
		});
		//return this rdd
		rdd.saveAsTextFile("mulsca");
	}

	/*
	 * multiply each element with a element of b
	 * @param a rdd of type double
	 * @param b rdd of type double
	 * @return rdd of double
	 */
	public static void multiply(JavaDoubleRDD a, JavaDoubleRDD b) {
		//zip and make pair rdd from rdd a and rdd b
		JavaPairRDD<Double, Double> pairRdd = a.zip(b);
		//multiply elements of a pair with each other 
		//make a new rdd by a*b
		JavaRDD<Double> result = pairRdd.map(new Function<Tuple2<Double, Double>, Double>() {
			private static final long serialVersionUID = 1L;
			@Override
			public Double call(Tuple2<Double, Double> x) {
				//multiple 
				return x._1 * x._2;
			}
		});
		//return the result
		result.saveAsTextFile("mulvec");
	}

}
