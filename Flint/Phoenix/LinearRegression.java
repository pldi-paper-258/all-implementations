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

public class LinearRegression {
	public static class Point {
		public int x;
		public int y;
		
		public Point(int x, int y) {
			super();
			this.x = x;
			this.y = y;
		}
	}
	
	public static void main(String[] args) {
		Point flat$1 = new Point(100, 500);
		Point flat$2 = new Point(3, 1000);
		Point flat$3 = new Point(7, 600);
		Point flat$4 = new Point(300, 34);
		List<Point> points = null;
		points = Arrays.asList(flat$1, flat$2, flat$3, flat$4);
		regress(points);
	}
	
	public static int[] regress(List<Point> points) {
		int SX_ll = 0;
		SX_ll = 0;
		int SY_ll = 0;
		SY_ll = 0;
		int SXX_ll = 0;
		SXX_ll = 0;
		int SYY_ll = 0;
		SYY_ll = 0;
		int SXY_ll = 0;
		SXY_ll = 0;
		{
			int i = 0;
			i = 0;
			boolean loop$0 = false;
			loop$0 = false;
			SparkConf conf = new SparkConf().setAppName("spark");
			JavaSparkContext sc = new JavaSparkContext(conf);
			
			JavaRDD<LinearRegression.Point> rdd_0_0 = sc.parallelize(points);
			final boolean loop0_final = loop$0;
			
			JavaPairRDD<Integer, Tuple2<Integer,Integer>> mapEmits = rdd_0_0.flatMapToPair(new PairFlatMapFunction<LinearRegression.Point, Integer, Tuple2<Integer,Integer>>() {
				public Iterator<Tuple2<Integer, Tuple2<Integer,Integer>>> call(LinearRegression.Point points_i) throws Exception {
					List<Tuple2<Integer, Tuple2<Integer,Integer>>> emits = new ArrayList<Tuple2<Integer, Tuple2<Integer,Integer>>>();
					
					emits.add(new Tuple2(1,new Tuple2(1,points_i.x)));
					emits.add(new Tuple2(3,new Tuple2(3,points_i.y)));
					emits.add(new Tuple2(2,new Tuple2(2,points_i.x*points_i.x)));
					emits.add(new Tuple2(5,new Tuple2(5,points_i.x*points_i.y)));
					emits.add(new Tuple2(4,new Tuple2(4,points_i.y*points_i.y)));
					
					
					return emits.iterator();
				}
			});
			
			JavaPairRDD<Integer, Tuple2<Integer,Integer>> reduceEmits = mapEmits.reduceByKey(new Function2<Tuple2<Integer,Integer>,Tuple2<Integer,Integer>,Tuple2<Integer,Integer>>(){
				public Tuple2<Integer,Integer> call(Tuple2<Integer,Integer> val1, Tuple2<Integer,Integer> val2) throws Exception {
					if(val1._1 == 1){
						return new Tuple2(val1._1,(val1._2+val2._2));
					}
					if(val1._1 == 2){
						return new Tuple2(val1._1,(val1._2+val2._2));
					}
					if(val1._1 == 3){
						return new Tuple2(val1._1,(val1._2+val2._2));
					}
					if(val1._1 == 4){
						return new Tuple2(val1._1,(val1._2+val2._2));
					}
					if(val1._1 == 5){
						return new Tuple2(val1._1,(val2._2+val1._2));
					}
					
					return null;
				}
			});
			
			Map<Integer, Tuple2<Integer,Integer>> output_rdd_0_0 = reduceEmits.collectAsMap();
			SXY_ll = output_rdd_0_0.get(1)._2;
			SYY_ll = output_rdd_0_0.get(2)._2;
			SY_ll = output_rdd_0_0.get(3)._2;
			SXX_ll = output_rdd_0_0.get(4)._2;
			SX_ll = output_rdd_0_0.get(5)._2;;
		}
		int[] result = null;
		result = (new int[5]);
		result[0] = SX_ll;
		result[1] = SXX_ll;
		result[2] = SY_ll;
		result[3] = SYY_ll;
		result[4] = SXY_ll;
		return result;
	}
	
	public LinearRegression() { super(); }
}