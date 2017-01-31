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

public class Count {
	
	public static void main(String[] args) {
		List<Integer> numbers = null;
		numbers = Arrays.asList(10, 5, 7, 12, 3);
		countList(numbers);
	}
	
	public static int countList(List<Integer> data) {
		int count = 0;
		count = 0;
		{
			int i = 0;
			i = 0;
			boolean loop$0 = false;
			loop$0 = false;
			SparkConf conf = new SparkConf().setAppName("spark");
			JavaSparkContext sc = new JavaSparkContext(conf);
			
			JavaRDD<java.lang.Integer> rdd_0_0 = sc.parallelize(data);
			final boolean loop0_final = loop$0;
			
			JavaPairRDD<Integer, Tuple2<Integer,Integer>> mapEmits = rdd_0_0.flatMapToPair(new PairFlatMapFunction<java.lang.Integer, Integer, Tuple2<Integer,Integer>>() {
				public Iterator<Tuple2<Integer, Tuple2<Integer,Integer>>> call(java.lang.Integer data_i) throws Exception {
					List<Tuple2<Integer, Tuple2<Integer,Integer>>> emits = new ArrayList<Tuple2<Integer, Tuple2<Integer,Integer>>>();
					
					emits.add(new Tuple2(1,new Tuple2(1,1)));
					
					
					return emits.iterator();
				}
			});
			
			JavaPairRDD<Integer, Tuple2<Integer,Integer>> reduceEmits = mapEmits.reduceByKey(new Function2<Tuple2<Integer,Integer>,Tuple2<Integer,Integer>,Tuple2<Integer,Integer>>(){
				public Tuple2<Integer,Integer> call(Tuple2<Integer,Integer> val1, Tuple2<Integer,Integer> val2) throws Exception {
					if(val1._1 == 1){
						return new Tuple2(val1._1,(val1._2+val2._2));
					}
					
					return null;
				}
			});
			
			Map<Integer, Tuple2<Integer,Integer>> output_rdd_0_0 = reduceEmits.collectAsMap();
			count = output_rdd_0_0.get(1)._2;;
		}
		return count;
	}
	
	public Count() { super(); }
}