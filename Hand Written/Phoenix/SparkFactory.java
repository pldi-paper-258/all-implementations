package com.simple.sparkjobs;

import java.util.Arrays;

import org.apache.spark.SparkConf;
import org.apache.spark.api.java.JavaDoubleRDD;
import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.api.java.JavaSparkContext;
import org.apache.spark.api.java.function.DoubleFunction;
import org.apache.spark.api.java.function.FlatMapFunction;

public class SparkFactory {
	public static SparkConf sparkConf = null;
	public static JavaSparkContext ctx = null;

	public static JavaSparkContext getContext() {
		// if context is not initialized already then initialize it
		if (ctx == null) {
			sparkConf = new SparkConf().setAppName("JavaWordCount");
			ctx = new JavaSparkContext(sparkConf);
		}
		return ctx;
	}
	// read file and return Java Double RDD
	// assumes each file contain only number separated by space
	public static JavaDoubleRDD readFile(String file) {
		// get Spark context
		JavaSparkContext ctx = SparkFactory.getContext();
		// read the file
		JavaRDD<String> lines = ctx.textFile(file);
		// using flat map converting lines into numbers
		JavaRDD<String> numbers = lines.flatMap(new FlatMapFunction<String, String>() {
			private static final long serialVersionUID = 1L;
			@Override
			public Iterable<String> call(String s) {
				// split by space and return the list
				return Arrays.asList(s.split(" "));
			}
		});

		// converting each element into double type
		JavaDoubleRDD rdd = numbers.mapToDouble(new DoubleFunction<String>() {
			private static final long serialVersionUID = 1L;
			@Override
			public double call(String s) throws Exception {
				// parse and return
				return Double.parseDouble(s);
			}
		});
		// cache this rdd
		rdd.cache();
		return rdd;
	}

	// close spark context
	public static void close() {
		if (ctx != null) {
			ctx.close();
			ctx = null;
			sparkConf = null;
		}
	}
}
