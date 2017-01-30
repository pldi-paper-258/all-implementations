package com.simple.sparkjobs;

import java.util.Arrays;

import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.api.java.JavaSparkContext;
import org.apache.spark.api.java.function.FlatMapFunction;
import org.apache.spark.api.java.function.Function;
import org.apache.spark.api.java.function.Function2;

public class RegressCalculator {

	public static void main(String[] args) {

	}

	public static int[] regress(String file) {
		//get spark context
		JavaSparkContext ctx = SparkFactory.getContext();
		//read the file
		JavaRDD<String> lines = ctx.textFile(file);
		// using flat map converting lines into x,y
		JavaRDD<String> points = lines.flatMap(new FlatMapFunction<String, String>() {
			private static final long serialVersionUID = 1L;

			@Override
			public Iterable<String> call(String s) {
				// split by space and return the list
				return Arrays.asList(s.split(" "));
			}
		});
		//convert into points
		JavaRDD<Point> pointRDD = points.map(new Function<String, Point>() {
			private static final long serialVersionUID = 1L;

			@Override
			public Point call(String point) throws Exception {
				String[] tokens = point.split(",");
				int x = Integer.parseInt(tokens[0]);
				int y = Integer.parseInt(tokens[1]);
				return new Point(x, y);
			}
		});
		
		//get sum of x-axes
		int SX_ll = pointRDD.map(new Function<Point, Integer>() {
			private static final long serialVersionUID = 1L;

			@Override
			public Integer call(Point p) throws Exception {
				return p.x;
			}
		}).reduce(new Function2<Integer, Integer, Integer>() {
			private static final long serialVersionUID = 1L;

			@Override
			public Integer call(Integer a, Integer b) throws Exception {
				return a + b;
			}
		});
		//get sum of y-axes
		int SY_ll = pointRDD.map(new Function<Point, Integer>() {
			private static final long serialVersionUID = 1L;

			@Override
			public Integer call(Point p) throws Exception {
				return p.y;
			}
		}).reduce(new Function2<Integer, Integer, Integer>() {
			private static final long serialVersionUID = 1L;

			@Override
			public Integer call(Integer a, Integer b) throws Exception {
				return a + b;
			}
		});
		//get sum of squares of x-axes
		int SXX_ll = pointRDD.map(new Function<Point, Integer>() {
			private static final long serialVersionUID = 1L;

			@Override
			public Integer call(Point p) throws Exception {
				return p.x * p.x;
			}
		}).reduce(new Function2<Integer, Integer, Integer>() {
			private static final long serialVersionUID = 1L;
			@Override
			public Integer call(Integer a, Integer b) throws Exception {
				return a + b;
			}
		});
		//get sum of squares of y-axes
		int SYY_ll = pointRDD.map(new Function<Point, Integer>() {
			private static final long serialVersionUID = 1L;
			@Override
			public Integer call(Point p) throws Exception {
				return p.y * p.y;
			}
		}).reduce(new Function2<Integer, Integer, Integer>() {
			private static final long serialVersionUID = 1L;

			@Override
			public Integer call(Integer a, Integer b) throws Exception {
				return a + b;
			}
		});
		//get sum of XY
		int SXY_ll = pointRDD.map(new Function<Point, Integer>() {
			private static final long serialVersionUID = 1L;
			@Override
			public Integer call(Point p) throws Exception {
				return p.x * p.y;
			}
		}).reduce(new Function2<Integer, Integer, Integer>() {
			private static final long serialVersionUID = 1L;
			@Override
			public Integer call(Integer a, Integer b) throws Exception {
				return a + b;
			}
		});

		//make an array and return the result
		int[] result = new int[5];
		result[0] = SX_ll;
		result[1] = SXX_ll;
		result[2] = SY_ll;
		result[3] = SYY_ll;
		result[4] = SXY_ll;
		return result;

	}
}
