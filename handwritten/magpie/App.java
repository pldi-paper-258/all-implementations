package magpiemanual;

import org.apache.spark.api.java.JavaDoubleRDD;

/**
 * Hello world!
 *
 */
public class App 
{
    public static void main( String[] args )
    {
    	//path to file x
    	String xFile="input1/";
    	//path to file y
    	String yFile="input2/";
    	
    	//read file x
    	JavaDoubleRDD x=SparkFactory.readFile(xFile);
    	//read file y
    	JavaDoubleRDD y=SparkFactory.readFile(yFile);
    	
    	Double mean, variance_x, variance_y, gradient, offset;
    	
    	switch(args[0]){
    		case "mean":
    			System.err.println(">>>>>>>>>>>>>>>>>>>>>>>>>>> mean <<<<<<<<<<<<<<<<<<<<<<<<<");
    			MeanCalculator.calcMean(x);
    			break;
    		case "variance":
    			mean = 110.0;
    			VarienceCalculator.calcVariance(x,mean);
    			break;
    		case "covariance":
    			variance_x = 10.0;
    			variance_y = 15.0;
    			CovarianceCalculator.calcCoVarience(variance_x, variance_y, x, y);
    			break;
    		case "fit":
    			gradient=0.03;
    	    	offset=5.0;
    	    	Fit.fit(x, gradient, offset);
    	    	break;
    		case "normalize":
    			Normalizer.normalize(x, 20.0);
    			break;
    		case "rss":
    			mean = 110.0;
    			RegressionSumOfSquares.regressionSumOfSquares(x, mean);
    			break;
    		case "residual":
    			ResidualCalculator.calculateResidual(x, y);
    			break;
    		case "serror":
    			StandardErrorCalculator.calculateError(x, y);
    			break;
    		case "multiplysca":
    			Multiply.multiply(x, 2.0);
    			break;
    		case "multiplyvec":
    			Multiply.multiply(x, y);
    			break;
    		default:
    			System.err.println("unsupported");
    	}
    }
}
