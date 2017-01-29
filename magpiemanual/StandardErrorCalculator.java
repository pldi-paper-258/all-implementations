package magpiemanual;

import org.apache.spark.api.java.JavaDoubleRDD;
import org.apache.spark.api.java.JavaPairRDD;
import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.api.java.function.Function;
import org.apache.spark.api.java.function.Function2;

import scala.Tuple2;

public class StandardErrorCalculator {

	public static Double calculateError(JavaDoubleRDD y, JavaDoubleRDD y2) {
		//zip the y rdd and fit rdd making it pair rdd
		JavaPairRDD<Double, Double> pairRdd = y.zip(y2);
		//from this pair rdd calculate the standard error using given formula
		JavaRDD<Double> result = pairRdd.map(new Function<Tuple2<Double, Double>, Double>() {
			private static final long serialVersionUID = 1L;

			@Override
			public Double call(Tuple2<Double, Double> x) {
				//calculate difference and take square
				Double res = (x._1 - x._2) * (x._1 - x._2);
				//return the result
				return res;
			}
		});
		//for error, we need to sum all the values
		//so using reduce method
		Double error = result.reduce(new Function2<Double, Double, Double>() {
			private static final long serialVersionUID = 1L;

			@Override
			public Double call(Double a, Double b) throws Exception {
				//add and return
				return a + b;
			}
		});
		//return total error
		return error;
	}

}
