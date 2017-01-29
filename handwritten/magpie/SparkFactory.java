package magpiemanual;

import java.util.Arrays;
import java.util.Iterator;

import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapred.TextInputFormat;
import org.apache.spark.SparkConf;
import org.apache.spark.api.java.JavaDoubleRDD;
import org.apache.spark.api.java.JavaPairRDD;
import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.api.java.JavaSparkContext;
import org.apache.spark.api.java.function.DoubleFunction;
import org.apache.spark.api.java.function.FlatMapFunction;

import scala.Tuple2;

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
		JavaPairRDD<LongWritable, Text> lines = ctx.hadoopFile(file, TextInputFormat.class, LongWritable.class, Text.class);
		// using flat map converting lines into numbers
		JavaRDD<String> numbers = lines.flatMap(new FlatMapFunction<Tuple2<LongWritable, Text>, String>() {
			private static final long serialVersionUID = 1L;
			@Override
			public Iterator<String> call(Tuple2<LongWritable, Text> s) throws Exception {
				// split by space and return the list
				return Arrays.asList(s._2.toString().split(" ")).iterator();
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
