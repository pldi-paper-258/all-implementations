package com.simple.sparkjobs;

import java.util.Arrays;
import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.api.java.JavaSparkContext;
import org.apache.spark.api.java.function.FlatMapFunction;
import org.apache.spark.api.java.function.Function;

public class MatchWords {

	public static void main(String[] args) {
		//testing
		boolean[] result = matchWords("E:/up/words.txt");
		//found key1
		System.out.println("Is key1 found ? " + result[0]);
		System.out.println("Is key2 found ? " + result[1]);
	}

	public static boolean[] matchWords(String file) {
		final String key1 = "Facebook";
		final String key2 = "networking";
		JavaSparkContext ctx = SparkFactory.getContext();
		JavaRDD<String> lines = ctx.textFile(file);
		// using flat map converting lines into numbers
		JavaRDD<String> words = lines.flatMap(new FlatMapFunction<String, String>() {
			private static final long serialVersionUID = 1L;

			@Override
			public Iterable<String> call(String s) {
				// split by space and return the list
				return Arrays.asList(s.split(" "));
			}
		});
		// cache word rdd
		words.cache();
		// filter for key1
		JavaRDD<String> filterKey1 = words.filter(new Function<String, Boolean>() {
			private static final long serialVersionUID = 1L;

			@Override
			public Boolean call(String w) throws Exception {
				return w.equals(key1);
			}
		});
		// filter for key2
		JavaRDD<String> filterKey2 = words.filter(new Function<String, Boolean>() {
			private static final long serialVersionUID = 1L;

			@Override
			public Boolean call(String w) throws Exception {
				return w.equals(key2);
			}
		});
		boolean foundKey1 = false;
		boolean foundKey2 = false;
		// check if key1 found
		if (filterKey1.count() > 0)
			foundKey1 = true;
		// check if key2 found
		if (filterKey2.count() > 0)
			foundKey2 = true;
		boolean[] res = { foundKey1, foundKey2 };
		return res;
	}
}
