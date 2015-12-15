package org.janelia.thickness;

import ij.ImageJ;
import ij.ImagePlus;
import net.imglib2.algorithm.gauss3.Gauss3;
import net.imglib2.exception.IncompatibleTypeException;
import net.imglib2.img.array.ArrayImg;
import net.imglib2.img.array.ArrayImgs;
import net.imglib2.img.basictypeaccess.array.FloatArray;
import net.imglib2.img.display.imagej.ImageJFunctions;
import net.imglib2.type.numeric.real.FloatType;
import net.imglib2.view.Views;
import org.janelia.thickness.plugin.ZPositionCorrection;

import java.util.Random;

/**
 * Created by hanslovskyp on 10/16/15.
 */
public class WhiteNoiseData {

    public static void main(String[] args) throws IncompatibleTypeException {
        int width = Integer.parseInt(args[0]);
        int height = Integer.parseInt(args[1]);
        int size = Integer.parseInt(args[2]);
        double sigma = Double.parseDouble(args[3]);
        long[] dim = new long[]{width, height, size};
        long longSigma = (long) sigma;
        long doubleLongSigma = 2*longSigma;

        long[] noSigmaDim = new long[]{width - 2*doubleLongSigma, height - 2*doubleLongSigma, size - 2*doubleLongSigma};

        Random rng = new Random(100);

        ArrayImg<FloatType, FloatArray> img = ArrayImgs.floats(dim);

        for( FloatType i : img )
        {
            i.set((float) rng.nextGaussian());
        }

        new ImageJ();
//        ImageJFunctions.show(img);
        ArrayImg<FloatType, FloatArray> smoothed = ArrayImgs.floats(dim);
        Gauss3.gauss( sigma, Views.extendMirrorDouble( img ), smoothed );

        ImagePlus imp = ImageJFunctions.wrap( Views.offsetInterval( smoothed, new long[] { doubleLongSigma, doubleLongSigma, doubleLongSigma }, noSigmaDim ), "smoothed");

        imp.setDimensions( 1, size, 1 );

        ImagePlus imp2 = imp.duplicate();
        imp2.show();

        new ZPositionCorrection().run("");

    }

}
