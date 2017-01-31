package com.simple.sparkjobs;

import java.util.Map;

import org.apache.spark.api.java.JavaPairRDD;
import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.api.java.JavaSparkContext;
import org.apache.spark.api.java.function.Function;
import org.apache.spark.api.java.function.Function2;
import org.apache.spark.api.java.function.PairFunction;
import org.apache.spark.broadcast.Broadcast;

import scala.Tuple2;

public class ReviewCounter {

	public static void main(String[] args) {
		Map<String, Integer> map = reviewCount("E:/up/reviews.txt");
		for (String key : map.keySet()) {
			System.out.println(key + " => " + map.get(key));
		}
	}

	public static Map<String, Integer> reviewCount(String file) {
		String key = "Good for Kids";
		// get spark context
		JavaSparkContext ctx = SparkFactory.getContext();
		final Broadcast<String> bKey = ctx.broadcast(key);
		// read the file
		JavaRDD<String> lines = ctx.textFile(file);
		// convert into yelp record objects
		JavaRDD<YelpRecord> recordRDD = lines.map(new Function<String, YelpRecord>() {
			private static final long serialVersionUID = 1L;

			@Override
			public YelpRecord call(String point) throws Exception {
				// initialize
				YelpRecord record = new YelpRecord();
				String[] tokens = point.split(" ");
				record.state = tokens[0];
				record.city = tokens[1];
				record.comment = tokens[2];
				record.score = Integer.parseInt(tokens[3]);
				// get map string
				// replace brackets
				String map = tokens[4].replaceAll("{", "");
				map = map.replaceAll("}", "");
				// split by comma
				tokens = map.split(",");
				// iterate over tokens
				for (int i = 0; i < tokens.length; i++) {
					// split by :
					String[] splits = tokens[i].split(":");
					String key = splits[0];
					boolean value = Boolean.parseBoolean(splits[1]);
					record.flags.put(key, value);
				}
				return record;
			}
		});
		// filter records that have the given key
		JavaRDD<YelpRecord> filteredRDD = recordRDD.filter(new Function<YelpRecord, Boolean>() {
			private static final long serialVersionUID = 1L;

			@Override
			public Boolean call(YelpRecord record) throws Exception {
				// check if key exist or not
				return record.flags.containsKey(bKey.getValue());
			}
		});
		// convert into pair RDD
		JavaPairRDD<String, Integer> pairRDD = filteredRDD.mapToPair(new PairFunction<YelpRecord, String, Integer>() {
			private static final long serialVersionUID = 1L;

			@Override
			public Tuple2<String, Integer> call(YelpRecord r) throws Exception {
				// convert to pair as city,1
				return new Tuple2<String, Integer>(r.city, 1);
			}
		});
		// reduce by key
		JavaPairRDD<String, Integer> reducedRDD = pairRDD.reduceByKey(new Function2<Integer, Integer, Integer>() {
			private static final long serialVersionUID = 1L;

			@Override
			public Integer call(Integer a, Integer b) throws Exception {
				// add the pair elements
				return a + b;
			}
		});
		// return map
		return reducedRDD.collectAsMap();

	}
}
