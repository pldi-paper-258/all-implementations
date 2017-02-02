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

class HistogramJava {
	public static class Pixel {
		public int r;
		public int g;
		public int b;
		
		public Pixel(int r, int g, int b) {
			super();
			this.r = r;
			this.g = g;
			this.b = b;
		}
	}
	
	
	public static void main(String[] args) {
		Pixel flat$1 = new Pixel(10, 10, 10);
		Pixel flat$2 = new Pixel(120, 120, 120);
		Pixel flat$3 = new Pixel(210, 210, 210);
		Pixel flat$4 = new Pixel(10, 120, 210);
		List<Pixel> pixels = null;
		pixels = Arrays.asList(flat$1, flat$2, flat$3, flat$4);
		int[] hR = null;
		hR = (new int[256]);
		int[] hG = null;
		hG = (new int[256]);
		int[] hB = null;
		hB = (new int[256]);
		histogram(pixels, hR, hG, hB);
	}
	
	@SuppressWarnings("unchecked")
	public static int[][] histogram(List<Pixel> image, int[] hR, int[] hG, int[] hB) {
		{
			int i = 0;
			i = 0;
			boolean loop$0 = false;
			loop$0 = false;
			SparkConf conf = new SparkConf().setAppName("spark");
			JavaSparkContext sc = new JavaSparkContext(conf);
			
			JavaRDD<HistogramJava.Pixel> rdd_0_0 = sc.parallelize(image);
			final boolean loop0_final = loop$0;
			
			JavaPairRDD<Tuple2<Integer,Integer>, Tuple2<Integer,Integer>> mapEmits = rdd_0_0.flatMapToPair(new PairFlatMapFunction<HistogramJava.Pixel, Tuple2<Integer,Integer>, Tuple2<Integer,Integer>>() {
				public Iterator<Tuple2<Tuple2<Integer,Integer>, Tuple2<Integer,Integer>>> call(HistogramJava.Pixel image_i) throws Exception {
					List<Tuple2<Tuple2<Integer,Integer>, Tuple2<Integer,Integer>>> emits = new ArrayList<Tuple2<Tuple2<Integer,Integer>, Tuple2<Integer,Integer>>>();
					
					emits.add(new Tuple2(new Tuple2(2,image_i.g), new Tuple2(2,1)));
					emits.add(new Tuple2(new Tuple2(1,image_i.r), new Tuple2(1,1)));
					emits.add(new Tuple2(new Tuple2(3,image_i.b), new Tuple2(3,1)));
					
					
					return emits.iterator();
				}
			});
			
			JavaPairRDD<Tuple2<Integer,Integer>, Tuple2<Integer,Integer>> reduceEmits = mapEmits.reduceByKey(new Function2<Tuple2<Integer,Integer>,Tuple2<Integer,Integer>,Tuple2<Integer,Integer>>(){
				public Tuple2<Integer,Integer> call(Tuple2<Integer,Integer> val1, Tuple2<Integer,Integer> val2) throws Exception {
					if(val1._1 == 1){
						return new Tuple2(val1._1,(val1._2+val2._2));
					}
					if(val1._1 == 2){
						return new Tuple2(val1._1,(val2._2+val1._2));
					}
					if(val1._1 == 3){
						return new Tuple2(val1._1,(val2._2+val1._2));
					}
					
					return null;
				}
			});
			
			Map<Tuple2<Integer,Integer>, Tuple2<Integer,Integer>> output_rdd_0_0 = reduceEmits.collectAsMap();
			for(Tuple2<Integer,Integer> output_rdd_0_0_k : output_rdd_0_0.keySet()){
				if(output_rdd_0_0_k._1 == 1) hR[output_rdd_0_0_k._2] = output_rdd_0_0.get(output_rdd_0_0_k)._2;
				else if(output_rdd_0_0_k._1 == 2) hG[output_rdd_0_0_k._2] = output_rdd_0_0.get(output_rdd_0_0_k)._2;
				else if(output_rdd_0_0_k._1 == 3) hB[output_rdd_0_0_k._2] = output_rdd_0_0.get(output_rdd_0_0_k)._2;
			}
		}
		int[][] result = null;
		result = (new int[3][]);
		result[0] = hR;
		result[1] = hG;
		result[2] = hB;
		return result;
	}
	
	public HistogramJava() { super(); }
}