package phoenix;

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
import java.util.List;
import java.util.Arrays;
import java.lang.Integer;

public class Equal {
	
	public static void main(String[] args) {
		List<Integer> numbers = null;
		numbers = Arrays.asList(10, 10, 5, 10, 10);
		equal(numbers);
	}
	
	public static boolean equal(List<Integer> data) {
		boolean equal = false;
		equal = true;
		int val = 0;
		val = data.get(0);
		{
			int i = 0;
			i = 0;
			boolean loop$0 = false;
			loop$0 = false;
			SparkConf conf = new SparkConf().setAppName("spark");
			JavaSparkContext sc = new JavaSparkContext(conf);
			
			JavaRDD<java.lang.Integer> rdd_0_0 = sc.parallelize(data);
			final int val_final = val;
			final boolean loop0_final = loop$0;
			
			JavaPairRDD<Integer, Tuple2<Integer,Boolean>> mapEmits = rdd_0_0.flatMapToPair(new PairFlatMapFunction<java.lang.Integer, Integer, Tuple2<Integer,Boolean>>() {
				public Iterator<Tuple2<Integer, Tuple2<Integer,Boolean>>> call(java.lang.Integer data_i) throws Exception {
					List<Tuple2<Integer, Tuple2<Integer,Boolean>>> emits = new ArrayList<Tuple2<Integer, Tuple2<Integer,Boolean>>>();
					
					if(val_final!=data_i) emits.add(new Tuple2(1,new Tuple2(1,loop0_final)));
					
					
					return emits.iterator();
				}
			});
			
			JavaPairRDD<Integer, Tuple2<Integer,Boolean>> reduceEmits = mapEmits.reduceByKey(new Function2<Tuple2<Integer,Boolean>,Tuple2<Integer,Boolean>,Tuple2<Integer,Boolean>>(){
				public Tuple2<Integer,Boolean> call(Tuple2<Integer,Boolean> val1, Tuple2<Integer,Boolean> val2) throws Exception {
					if(val1._1 == 1){
						return new Tuple2(val1._1,(loop0_final));
					}
					
					return null;
				}
			});
			
			Map<Integer, Tuple2<Integer,Boolean>> output_rdd_0_0 = reduceEmits.collectAsMap();
			if(output_rdd_0_0.containsKey(1)) equal = output_rdd_0_0.get(1)._2;
			else equal = true;
		}
		return equal;
	}
	
	public Equal() { super(); }
}