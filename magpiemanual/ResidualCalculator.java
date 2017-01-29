package magpiemanual;

import org.apache.spark.api.java.JavaDoubleRDD;
import org.apache.spark.api.java.JavaPairRDD;
import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.api.java.function.Function;

import scala.Tuple2;

public class ResidualCalculator {

	public static void calculateResidual(JavaDoubleRDD y, JavaDoubleRDD fit) {
		//zip both the rdd's
		//one will be used as returned from fit method
		//the other will be used as read from the file
		//zip them in the shape of pairs
		JavaPairRDD<Double, Double> pairRdd = y.zip(fit);
		//calculate the difference between the elements of each pair
		JavaRDD<Double> result = pairRdd.map(new Function<Tuple2<Double, Double>, Double>() {
			private static final long serialVersionUID = 1L;
			@Override
			public Double call(Tuple2<Double, Double> x) {
				//difference between first and second element of each pair
				Double res = (x._1 - x._2);
				//return the result
				return res;
			}
		});
		//return the residual rdd
		result.saveAsTextFile("res");
	}

}
