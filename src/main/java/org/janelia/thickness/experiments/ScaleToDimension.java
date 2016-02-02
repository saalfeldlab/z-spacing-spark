package org.janelia.thickness.experiments;

import ij.ImagePlus;
import ij.io.FileSaver;
import mpicbg.models.*;
import net.imglib2.FinalInterval;
import net.imglib2.Interval;
import net.imglib2.RealRandomAccessible;
import net.imglib2.img.array.ArrayImg;
import net.imglib2.img.array.ArrayImgs;
import net.imglib2.img.basictypeaccess.array.FloatArray;
import net.imglib2.img.display.imagej.ImageJFunctions;
import net.imglib2.interpolation.randomaccess.NLinearInterpolatorFactory;
import net.imglib2.realtransform.RealViews;
import net.imglib2.type.numeric.real.FloatType;
import net.imglib2.view.IntervalView;
import net.imglib2.view.Views;
import org.apache.spark.api.java.function.PairFunction;
import org.janelia.thickness.ScaleOptions;
import org.janelia.thickness.utility.FPTuple;
import org.janelia.utility.io.IO;
import org.janelia.utility.realtransform.ScaleAndShift;
import scala.Tuple2;

import java.io.FileNotFoundException;
import java.io.Serializable;
import java.util.ArrayList;
import java.util.concurrent.Callable;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;

/**
 * Created by hanslovskyp on 10/25/15.
 */
public class ScaleToDimension {

    public static class Stat implements Serializable
    {
//        public final double
    }

    public static class TransformAndCompare implements PairFunction<Tuple2<Integer,FPTuple[]>,Integer, Stat>
    {



        @Override
        public Tuple2<Integer, Stat> call(Tuple2<Integer, FPTuple[]> integerTuple2) throws Exception {
            return null;
        }
    }

    public static void scale(
            String base,
            ScaleOptions config,
            int startIndex,
            int lastIndex
    ) throws InterruptedException {

        double[] targetRadius = new double[]{config.radii[lastIndex][0], config.radii[lastIndex][1]};
        double[] targetStep   = new double[]{config.steps[lastIndex][0], config.radii[lastIndex][1]};

        ImagePlus targetSizeDummyImg =
                new ImagePlus(String.format(base + String.format("/out/%02d/forward/%04d.tif", lastIndex, config.start)));

        ExecutorService es = Executors.newFixedThreadPool(Runtime.getRuntime().availableProcessors());
        ArrayList<Callable< Void>> callables = new ArrayList< Callable<Void>>();

        for ( int n = startIndex; n < lastIndex; ++n ) {

            String currentBase = base + String.format("/out/%02d", n);
            String pattern = currentBase + "/forward/%04d.tif";
            String patternBackward = currentBase + "/backward/%04d.tif";

            String targetPattern = currentBase + String.format( "/forward-scaled-to-%02d", lastIndex ) + "/%04d.tif";
            String targetPatternBackward = currentBase  + String.format( "/backward-scaled-to-%02d", lastIndex ) + "/%04d.tif";

            int start = config.start;
            int stop  = config.stop;

            double[] step1 = new double[]{config.steps[n][0], config.radii[n][1]};

            double[] radius1 = new double[]{config.radii[n][0], config.radii[n][1]};

            double[] step = new double[2];
            double[] radius = new double[2];

            FinalInterval targetInterval = new FinalInterval( targetSizeDummyImg.getWidth(), targetSizeDummyImg.getHeight() );

            for (int i = 0; i < step.length; ++i) {
                step[i] = step1[i] / targetStep[i];
                radius[i] = (radius1[i] - targetRadius[i]) / targetStep[i];
            }

            ScaleAndShift transform = new ScaleAndShift(step, radius);



            scale( start, stop, pattern, transform, targetInterval, targetPattern, callables );
            scale( start, stop, patternBackward, transform, targetInterval, targetPatternBackward, callables );

            System.out.println( String.format( "Finished iteration % 4d.", n ) );


        }

        es.invokeAll( callables );
        es.shutdown();
    }

    public static void main(String[] args) throws NotEnoughDataPointsException, IllDefinedDataPointsException, FileNotFoundException, InterruptedException {

        String root = "/nobackup/saalfeld/hanslovskyp/CutOn4-15-2013_ImagedOn1-27-2014/aligned/substacks/1300-3449" +
                "/4000x2500+5172+1416/downscale-z-by-4/corrected/z=[25,499]/z=[0,99]/downscale-xy-by-2/deformation";
        String id = "01";
        String run = "20160202_102949";
//      String run = "z=[60,159]/20160122_112824";
        String base = String.format("%s/%s/%s", root, id, run);

        ScaleOptions config = ScaleOptions.createFromFile(base + "/config.json");

        int startIndex = 0;
        int lastIndex = 6;
        scale( base, config, startIndex, lastIndex );
    }

    public static void scale(
            final int start,
            final int stop,
            final String pattern,
            final ScaleAndShift transform,
            final Interval targetInterval,
            final String targetPattern,
            final ArrayList<Callable<Void>> callables )
    {
        for( int z = start; z < stop; ++z )
        {
            final int finalZ = z;
            callables.add(new Callable<Void>() {
                final int z = finalZ;
                @Override
                public Void call() throws Exception {
//                    System.out.println( "Scaling at z=" + z );

                    ImagePlus imp = new ImagePlus(String.format(pattern, z));

                    ArrayImg<FloatType, FloatArray> img =
                            ArrayImgs.floats((float[]) imp.getProcessor().convertToFloatProcessor().getPixels(), imp.getWidth(), imp.getHeight());

                    for ( FloatType i : img )
                    {
                        if ( i.get() == 0.0f )
                            i.set( Float.NaN );
                    }

                    RealRandomAccessible<FloatType> interpolatedAndExtended =
                            Views.interpolate(Views.extendBorder(img), new NLinearInterpolatorFactory<FloatType>());
                    IntervalView<FloatType> transformed = Views.interval(RealViews.transform(interpolatedAndExtended, transform), targetInterval);

                    IO.createDirectoryForFile( String.format( targetPattern, z ) );
                    new FileSaver(ImageJFunctions.wrapFloat( transformed, "" ) ).saveAsTiff( String.format( targetPattern, z ) );
                    return null;
                }
            });



        }

    }

}
