package com.simple.sparkjobs;

import java.util.Arrays;
import java.util.Iterator;

import org.apache.spark.api.java.JavaPairRDD;
import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.api.java.JavaSparkContext;
import org.apache.spark.api.java.function.FlatMapFunction;
import org.apache.spark.api.java.function.Function;
import org.apache.spark.api.java.function.PairFunction;

import scala.Tuple2;

public class RowWiseMean {
	public static JavaRDD<Double> countWords(String file) {
		JavaSparkContext ctx = SparkFactory.getContext();
		JavaRDD<String> lines = ctx.textFile(file);
		// using flat map converting lines into numbers
		JavaRDD<String> record = lines.flatMap(new FlatMapFunction<String, String>() {
			private static final long serialVersionUID = 1L;

			@Override
			public Iterable<String> call(String s) {
				// split by space and return the list
				return Arrays.asList(s.split(" "));
			}
		});
		// mapping these words into pairs
		JavaPairRDD<Integer, Double> tuples = record.mapToPair(new PairFunction<String, Integer, Double>() {
			private static final long serialVersionUID = 1L;

			public Tuple2<Integer, Double> call(String x) {
				// split each record by comma
				String[] tokens = x.split(",");
				Integer row = Integer.parseInt(tokens[0]);
				Double value = Double.parseDouble(tokens[2]);
				return new Tuple2<Integer, Double>(row, value);
			}
		});
		//aggregate by key
		JavaPairRDD<Integer, Iterable<Double>> keysums=tuples.groupByKey();
		//calculate mean by key and sort
		JavaPairRDD<Integer, Double> sorted=keysums.mapToPair(new PairFunction<Tuple2<Integer,Iterable<Double>>, Integer, Double>() {
			private static final long serialVersionUID = 1L;
			@Override
			public Tuple2<Integer, Double> call(Tuple2<Integer, Iterable<Double>> values) throws Exception {
				Double sum=0.0;
				int count=0;
				Iterator<Double> itr=values._2.iterator();
				while(itr.hasNext()){
					sum+=itr.next();
					count+=1;
				}
				Double mean=sum/count;
				return new Tuple2<Integer, Double>(values._1, mean);
			}
		}).sortByKey();
		JavaRDD<Double> result=sorted.map(new Function<Tuple2<Integer,Double>, Double>() {
			private static final long serialVersionUID = 1L;

			@Override
			public Double call(Tuple2<Integer, Double> pair) throws Exception {
				return pair._2;
			}
		});
		return result;
	}
}
