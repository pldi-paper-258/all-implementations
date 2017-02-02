package biglambda;

import org.apache.spark.SparkConf;
import org.apache.spark.api.java.JavaSparkContext;
import org.apache.spark.api.java.JavaPairRDD;
import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.api.java.function.Function2;
import org.apache.spark.api.java.function.PairFlatMapFunction;

import scala.Tuple2;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;
import java.util.Map;
import java.util.Iterator;
import java.util.HashMap;
import java.util.Map;

public class ShakespearSentiment {
	
	public Map<String,Integer> sentiment(String text) {
		Map<String,Integer> result = null;
		result = new HashMap<String,Integer>();
		String keyword1 = null;
		keyword1 = "love";
		String keyword2 = null;
		keyword2 = "hate";
		result.put(keyword1, 0);
		result.put(keyword2, 0);
		String[] words = null;
		words = text.split(" ");
		SparkConf conf = new SparkConf().setAppName("spark");
		JavaSparkContext sc = new JavaSparkContext(conf);
		
		List<String> pre_rdd_0_0 = new ArrayList<String>(Arrays.asList(words));
		JavaRDD<java.lang.String> rdd_0_0 = sc.parallelize(pre_rdd_0_0);
		final java.lang.String keyword1_final = keyword1;
		final java.lang.String keyword2_final = keyword2;
		
		JavaPairRDD<String, java.lang.Integer> mapEmits = rdd_0_0.flatMapToPair(new PairFlatMapFunction<java.lang.String, String, java.lang.Integer>() {
			public Iterator<Tuple2<String, java.lang.Integer>> call(java.lang.String words_casper_index) throws Exception {
				List<Tuple2<String, java.lang.Integer>> emits = new ArrayList<Tuple2<String, java.lang.Integer>>();
				
				if(keyword2_final.equals(words_casper_index)) emits.add(new Tuple2(keyword2_final, 1));
				if(keyword1_final.equals(words_casper_index)) emits.add(new Tuple2(keyword1_final, 1));
				
				
				return emits.iterator();
			}
		});
		
		JavaPairRDD<String, java.lang.Integer> reduceEmits = mapEmits.reduceByKey(new Function2<java.lang.Integer,java.lang.Integer,java.lang.Integer>(){
			public java.lang.Integer call(java.lang.Integer val1, java.lang.Integer val2) throws Exception {
				return (val2+val1);
			}
		});
		
		Map<String, java.lang.Integer> output_rdd_0_0 = reduceEmits.collectAsMap();
		result = output_rdd_0_0;
		return result;
	}
	
	public ShakespearSentiment() { super(); }
}