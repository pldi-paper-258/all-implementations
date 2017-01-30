package com.simple.sparkjobs;

import java.util.Arrays;
import java.util.Map;

import org.apache.spark.api.java.JavaPairRDD;
import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.api.java.JavaSparkContext;
import org.apache.spark.api.java.function.FlatMapFunction;
import org.apache.spark.api.java.function.Function2;
import org.apache.spark.api.java.function.PairFunction;

import scala.Tuple2;

public class WordCounter {

	public static void main(String[] args) {
		//testing this word count example
		Map<String, Integer> counts=countWords("E:/up/x.txt");
		//displaying the whole map
		for(String key:counts.keySet()){
			System.out.println(key+" -> "+counts.get(key));
		}
	}
	public static Map<String, Integer> countWords(String file) {
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
		// mapping these words into pairs
		JavaPairRDD<String, Integer> wordToPair = words.mapToPair(new PairFunction<String, String, Integer>() {
			private static final long serialVersionUID = 1L;

			public Tuple2<String, Integer> call(String x) {
				// map each word to word,1 pair
				return new Tuple2<String, Integer>(x, 1);
			}
		});
		//count words using reduce by key
		JavaPairRDD<String, Integer> counts=wordToPair.reduceByKey(new Function2<Integer, Integer, Integer>() {
			private static final long serialVersionUID = 1L;
			public Integer call(Integer x, Integer y) {
				//adding counts
				return x + y;
			}
		});
		//convert into map
		return counts.collectAsMap();
	}
}
