package fiji.trails;

import ij.IJ;
import ij.ImageJ;
import ij.ImagePlus;
import ij.ImageStack;
import ij.gui.GenericDialog;
import ij.plugin.PlugIn;
import ij.process.ImageProcessor;
import ij.process.FloatProcessor;

public class TrailsSimple {
	
	int width;
	int height;
	
    /** Trail tCurr pixels using tWinPix time window. */
    final float[] trail(float[][] tWinPix, int wmin, int wmax) {
        int numPix = width*height;
        float[] tPix = new float[numPix];
        for (int v=0; v<numPix; v++) {
            float[] tvec = getTvec(tWinPix, v, wmin, wmax);
            tPix[v] = mean(tvec);
        }
        return tPix;
    }

    /** Build time vector for this pixel for  given window. */
    final float[] getTvec(float[][] tWinPix, int v, int wmin, int wmax) {
        float[] tvec = new float[wmax - wmin + 1];
        for (int w=wmin; w<=wmax; w++) {
            tvec[w] = tWinPix[w][v];  // time window vector for a pixel
        }
        return tvec;
    }

    /** Calculate mean of array of floats. */
    final float mean(float[] tvec) {
        float mean = 0;
        for (int t=0; t<tvec.length; t++) {
            mean += tvec[t];
        }
        return mean / tvec.length;
    }

    /** Main method for testing. */
    public static void main(String[] args) {
        Class<?> clazz = Trails.class;
        new ImageJ();
        // open TrackMate FakeTracks test data from the Fiji wiki
        ImagePlus image = IJ.openImage(
                "http://fiji.sc/tinevez/TrackMate/FakeTracks.tif");
        image.show();
        IJ.runPlugIn(clazz.getName(), "");
    }
}