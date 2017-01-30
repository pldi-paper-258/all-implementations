package com.simple.sparkjobs;

import org.apache.spark.api.java.JavaDoubleRDD;
import org.apache.spark.api.java.JavaPairRDD;
import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.api.java.function.Function;
import org.apache.spark.api.java.function.Function2;

import scala.Tuple2;

public class GetSqDist {

	public static void main(String[] args) {
		String file1 = "input1/";
		String file2 = "input1/";
		JavaDoubleRDD rdd1 = SparkFactory.readFile(file1);
		JavaDoubleRDD rdd2 = SparkFactory.readFile(file2);
		getSqDist(rdd1, rdd2);
	}

	public static Double getSqDist(JavaDoubleRDD rdd1, JavaDoubleRDD rdd2) {
		// zip two rdd's
		JavaPairRDD<Double, Double> pairRdd = rdd1.zip(rdd2);
		// calculate square distance
		JavaRDD<Double> result = pairRdd.map(new Function<Tuple2<Double, Double>, Double>() {
			private static final long serialVersionUID = 1L;

			@Override
			public Double call(Tuple2<Double, Double> p) throws Exception {
				// add elements of pair
				return (p._1 - p._2) * (p._1 - p._2);
			}
		});
		//sum all the square distances
		Double distance = result.reduce(new Function2<Double, Double, Double>() {
			private static final long serialVersionUID = 1L;
			@Override
			public Double call(Double a, Double b) throws Exception {
				//add both elements
				return a + b;
			}
		});
		return distance;
	}
}
