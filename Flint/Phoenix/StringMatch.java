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
import java.lang.String;
import java.util.Arrays;
import java.util.List;

public class StringMatch {
	
	public static void main(String[] args) {
		List<String> words = null;
		words = Arrays.asList("foo", "key1", "cat", "bar", "dog");
		matchWords(words);
	}
	
	public static boolean[] matchWords(List<String> words) {
		String key1 = null;
		key1 = "key1";
		String key2 = null;
		key2 = "key2";
		boolean foundKey1 = false;
		foundKey1 = false;
		boolean foundKey2 = false;
		foundKey2 = false;
		{
			int i = 0;
			i = 0;
			boolean loop$0 = false;
			loop$0 = false;
			SparkConf conf = new SparkConf().setAppName("spark");
			JavaSparkContext sc = new JavaSparkContext(conf);
			
			JavaRDD<java.lang.String> rdd_0_0 = sc.parallelize(words);
			final java.lang.String key1_final = key1;
			final java.lang.String key2_final = key2;
			final boolean loop0_final = loop$0;
			
			JavaPairRDD<Integer, Boolean> mapEmits = rdd_0_0.flatMapToPair(new PairFlatMapFunction<java.lang.String, Integer, Boolean>() {
				public Iterator<Tuple2<Integer, Boolean>> call(java.lang.String words_i) throws Exception {
					List<Tuple2<Integer, Boolean>> emits = new ArrayList<Tuple2<Integer, Boolean>>();
					
					if(words_i.equals(key2_final)) emits.add(new Tuple2(2, true));
					if(words_i.equals(key1_final)) emits.add(new Tuple2(1, true));				
					
					return emits.iterator();
				}
			});

			Map<Integer, Boolean> output_rdd_0_0 = mapEmits.collectAsMap();
			
			foundKey2 = output_rdd_0_0.get(2);
			foundKey1 = output_rdd_0_0.get(1);
		}
		boolean[] res = null;
		res = (new boolean[] { foundKey1, foundKey2 });
		return res;
	}
	
	public StringMatch() { super(); }
}