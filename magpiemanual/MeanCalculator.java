package magpiemanual;

import org.apache.spark.api.java.JavaDoubleRDD;

public class MeanCalculator {
	//this method takes java double rdd and returns the mean
	public static Double calcMean(JavaDoubleRDD rdd){
		//call mean method over RDD and return it
	    return rdd.mean();
	}
}
