package statistics;

import java.util.ArrayList;
import java.util.List;

import org.apache.spark.SparkConf;
import org.apache.spark.api.java.JavaPairRDD;
import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.api.java.JavaSparkContext;
import org.apache.spark.api.java.function.Function;

import scala.Tuple2;

public class Calculate {
	private double num1;
	private double num;
	
	public Calculate() {
		super();
		num1 = 0.0;
		num = 0.0;
	}
	
	public static double subtract(double a, double b) {
		double flat$2 = a - b;
		return flat$2;
	}
	
	public static double add(double a, double b) {
		double flat$3 = a + b;
		return flat$3;
	}
	
	public static double divide(double a, double b) {
		double flat$4 = a / b;
		return flat$4;
	}
	
	public static double multiply(double a, double b) {
		double flat$5 = a * b;
		return flat$5;
	}
	
	public double subtract(double num2) {
		double flat$6 = num1;
		double flat$7 = flat$6 - num2;
		return flat$7;
	}
	
	public double add(double num2) {
		double flat$8 = num1;
		double flat$9 = flat$8 + num2;
		return flat$9;
	}
	
	public double divide(double num2) {
		double flat$10 = num1;
		double flat$11 = flat$10 / num2;
		return flat$11;
	}
	
	public double multiply(double num2) {
		double flat$12 = num1;
		double flat$13 = flat$12 * num2;
		return flat$13;
	}
	
	public static double[] multiply(List<Double> a, double b) {
		int flat$1 = a.size();
		double[] temp = null;
		temp = (new double[flat$1]);
		{
			int i = 0;
			i = 0;
			boolean loop$0 = false;
			loop$0 = false;
			SparkConf conf = new SparkConf().setAppName("spark");
			JavaSparkContext sc = new JavaSparkContext(conf);
			
			JavaRDD<Double> rdd_0_0 = sc.parallelize(a);
			final double b_final = b;
			final boolean loop0_final = loop$0;
			
			JavaRDD<Double> mapEmits = rdd_0_0.map(new Function<Double,Double>(){
				public Double call(Double a_i) throws Exception {
					return a_i*b_final;
				}
			});
			
			List<Double> output_rdd_0_0 = mapEmits.collect();
			int casper_index=0;
			for(Double output_rdd_0_0_v : output_rdd_0_0){
				temp[casper_index] = output_rdd_0_0_v;
				casper_index++;
			}
		}
		return temp;
	}
	
	public static double[] multiply(List<Double> a, List<Double> b) {
		int flat$1 = a.size();
		double[] temp = null;
		temp = (new double[flat$1]);
		{
			int i = 0;
			i = 0;
			boolean loop$1 = false;
			loop$1 = false;
			SparkConf conf = new SparkConf().setAppName("spark");
			JavaSparkContext sc = new JavaSparkContext(conf);
			
			JavaRDD<Double> rdd_0_1_1 = sc.parallelize(a);
			JavaRDD<Double> rdd_0_1_2 = sc.parallelize(b);
			JavaPairRDD<Double, Double> rdd_0_1_3 = rdd_0_1_1.zip(rdd_0_1_2);
			final boolean loop1_final = loop$1;
			
			JavaRDD<Double> mapEmits = rdd_0_1_3.map(new Function<Tuple2<Double,Double>,Double>(){
				public Double call(Tuple2<Double, Double> casper_data_set_i) throws Exception {
					return casper_data_set_i._1*casper_data_set_i._2;
				}
			});
			
			List<Double> output_rdd_0_0 = mapEmits.collect();
			int casper_index=0;
			for(Double output_rdd_0_0_v : output_rdd_0_0){
				temp[casper_index] = output_rdd_0_0_v;
				casper_index++;
			}
		}
		return temp;
	}
}