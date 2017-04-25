package com.simple.sparkjobs;

import org.apache.spark.api.java.JavaDoubleRDD;
import org.apache.spark.api.java.JavaPairRDD;
import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.api.java.function.Function;

import scala.Tuple2;

public class AddVector {

	public static void main(String[] args) {
		String file1 = "input1/";
		String file2 = "input1/";
		JavaDoubleRDD rdd1 = SparkFactory.readFile(file1);
		JavaDoubleRDD rdd2 = SparkFactory.readFile(file2);
		addVectors(rdd1, rdd2);
	}

	public static JavaRDD<Double> addVectors(JavaDoubleRDD rdd1, JavaDoubleRDD rdd2) {
		//zip two rdd's
		JavaPairRDD<Double, Double> pairRdd = rdd1.zip(rdd2);
		//calculate sum 
		JavaRDD<Double> result=pairRdd.map(new Function<Tuple2<Double, Double>, Double>() {
			private static final long serialVersionUID = 1L;
			@Override
			public Double call(Tuple2<Double, Double> p) throws Exception {
				//add elements of pair
				return p._1 + p._2;
			}
		});
		return result;
	}
}
