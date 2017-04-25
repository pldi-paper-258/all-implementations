package biglambda;

import org.apache.spark.SparkConf;
import org.apache.spark.api.java.JavaSparkContext;
import org.apache.spark.api.java.JavaPairRDD;
import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.api.java.function.Function2;
import org.apache.spark.api.java.function.PairFlatMapFunction;
import scala.Tuple2;
import java.util.ArrayList;
import java.util.Map;
import java.util.Iterator;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

public class YelpKids {
	class Record {
		public String state;
		public String city;
		public String comment;
		public int score;
		public boolean goodForKids;
		
		public Record() { super(); }
	}
	
	
	public Map<String,Integer> reviewCount(List<Record> data) {
		Map<String,Integer> result = null;
		result = new HashMap<String,Integer>();
		SparkConf conf = new SparkConf().setAppName("spark");
		JavaSparkContext sc = new JavaSparkContext(conf);
		
		JavaRDD<biglambda.YelpKids.Record> rdd_0_0 = sc.parallelize(data);
		
		JavaPairRDD<String, java.lang.Integer> mapEmits = rdd_0_0.flatMapToPair(new PairFlatMapFunction<biglambda.YelpKids.Record, String, java.lang.Integer>() {
			public Iterator<Tuple2<String, java.lang.Integer>> call(biglambda.YelpKids.Record data_casper_index) throws Exception {
				List<Tuple2<String, java.lang.Integer>> emits = new ArrayList<Tuple2<String, java.lang.Integer>>();
				
				if(data_casper_index.goodForKids) emits.add(new Tuple2(data_casper_index.city, 1));
				
				
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
	
	public YelpKids() { super(); }
}