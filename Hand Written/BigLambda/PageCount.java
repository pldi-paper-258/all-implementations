package com.simple.sparkjobs;

import java.util.Arrays;
import java.util.Map;

import org.apache.spark.api.java.JavaPairRDD;
import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.api.java.JavaSparkContext;
import org.apache.spark.api.java.function.FlatMapFunction;
import org.apache.spark.api.java.function.Function;
import org.apache.spark.api.java.function.Function2;
import org.apache.spark.api.java.function.PairFunction;

import scala.Tuple2;

public class PageCount {

	public static void main(String[] args) {

	}

	public static Map<String, Integer> pageCount(String file) {
		JavaSparkContext ctx = SparkFactory.getContext();
		JavaRDD<String> lines = ctx.textFile(file);
		// using flat map converting lines into numbers
		JavaRDD<String> pages = lines.flatMap(new FlatMapFunction<String, String>() {
			private static final long serialVersionUID = 1L;

			@Override
			public Iterable<String> call(String s) {
				// split by space and return the list
				return Arrays.asList(s.split(" "));
			}
		});
		// convert into points
		JavaRDD<Record> recordRDD = pages.map(new Function<String, Record>() {
			private static final long serialVersionUID = 1L;

			@Override
			public Record call(String point) throws Exception {
				String[] tokens = point.split(",");
				String name = tokens[0].replaceAll("\'","");
				int views = Integer.parseInt(tokens[1]);
				int something = Integer.parseInt(tokens[1]);
				Record record = new Record();
				record.name = name;
				record.views = views;
				record.something = something;
				return record;
			}
		});
		JavaPairRDD<String, Integer> pairs = recordRDD.mapToPair(new PairFunction<Record, String, Integer>() {
			private static final long serialVersionUID = 1L;
			@Override
			public Tuple2<String, Integer> call(Record r) throws Exception {
				return new Tuple2<String, Integer>(r.name, r.views);
			}
		});
		JavaPairRDD<String, Integer> pageCoundRDD = pairs.reduceByKey(new Function2<Integer, Integer, Integer>() {
			private static final long serialVersionUID = 1L;

			@Override
			public Integer call(Integer a, Integer b) throws Exception {
				return a + b;
			}
		});
		return pageCoundRDD.collectAsMap();
	}
}
