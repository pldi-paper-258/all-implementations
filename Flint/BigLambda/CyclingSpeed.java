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

public class CyclingSpeed {
	class Record {
		public int fst;
		public int snd;
		public int emit;
		public double speed;
		
		public Record() { super(); }
	}
	
	
	public Map<Integer,Integer> cyclingSpeed(List<Record> data) {
		Map<Integer,Integer> result = null;
		result = new HashMap<Integer,Integer>();
		SparkConf conf = new SparkConf().setAppName("spark");
		JavaSparkContext sc = new JavaSparkContext(conf);
		
		JavaRDD<biglambda.CyclingSpeed.Record> rdd_0_0 = sc.parallelize(data);
		
		JavaPairRDD<Integer, java.lang.Integer> mapEmits = rdd_0_0.flatMapToPair(new PairFlatMapFunction<biglambda.CyclingSpeed.Record, Integer, java.lang.Integer>() {
			public Iterator<Tuple2<Integer, java.lang.Integer>> call(biglambda.CyclingSpeed.Record data_casper_index) throws Exception {
				List<Tuple2<Integer, java.lang.Integer>> emits = new ArrayList<Tuple2<Integer, java.lang.Integer>>();
				
				emits.add(new Tuple2(Math.ceil(data_casper_index.speed), 1));
				
				
				return emits.iterator();
			}
		});
		
		JavaPairRDD<Integer, java.lang.Integer> reduceEmits = mapEmits.reduceByKey(new Function2<java.lang.Integer,java.lang.Integer,java.lang.Integer>(){
			public java.lang.Integer call(java.lang.Integer val1, java.lang.Integer val2) throws Exception {
				return (val2+val1);
			}
		});
		
		Map<Integer, java.lang.Integer> output_rdd_0_0 = reduceEmits.collectAsMap();
		result = output_rdd_0_0;
		return result;
	}
	
	public CyclingSpeed() { super(); }
}