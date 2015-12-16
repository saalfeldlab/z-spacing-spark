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

    public static void main(String[] args) throws NotEnoughDataPointsException, IllDefinedDataPointsException, FileNotFoundException {

        String root = "/nobackup/saalfeld/hanslovskyp/CutOn4-15-2013_ImagedOn1-27-2014/aligned/substacks" +
                "/1300-3449/4000x2500+5172+1416/downscale-by-2/deformation";
        String id = "13";
        String run = "20151110_220042";
        String base = String.format("%s/%s/%s", root, id, run);

        ScaleOptions config = ScaleOptions.createFromFile(base + "/config.json");

        int startIndex = 0;
        int lastIndex = 6;

        double[] targetRadius = new double[]{config.radii[lastIndex][0], config.radii[lastIndex][1]};
        double[] targetStep   = new double[]{config.steps[lastIndex][0], config.radii[lastIndex][1]};

        for ( int n = startIndex; n < lastIndex; ++n ) {

            String currentBase = base + String.format("/out/%02d", n);
            String pattern = currentBase + "/forward/%04d.tif";

            String targetPattern = currentBase + "/forward-scaled-to-06/%04d.tif";

            int start = 0;
            int stop = 2070;

            double[] step1 = new double[]{config.steps[n][0], config.radii[n][1]};
//            double[] step2 = new double[]{7, 7};

            double[] radius1 = new double[]{config.radii[n][0], config.radii[n][1]};
//            double[] radius2 = new double[]{15, 15};

            double[] step = new double[2];
            double[] radius = new double[2];

            FinalInterval targetInterval = new FinalInterval(284, 177);

            for (int i = 0; i < step.length; ++i) {
                step[i] = step1[i] / targetStep[i];
                radius[i] = (radius1[i] - targetRadius[i]) / targetStep[i];
            }

            ScaleAndShift transform = new ScaleAndShift(step, radius);

            scale( start, stop, pattern, transform, targetInterval, targetPattern );

        }

    }

    public static void scale( int start, int stop, String pattern, ScaleAndShift transform, Interval targetInterval, String targetPattern )
    {
        for( int z = start; z < stop; ++z )
        {

            System.out.println( "Collecting matches at z=" + z );

            ImagePlus imp = new ImagePlus(String.format(pattern, z));

            ArrayImg<FloatType, FloatArray> img =
                    ArrayImgs.floats((float[]) imp.getProcessor().convertToFloatProcessor().getPixels(), imp.getWidth(), imp.getHeight());

            RealRandomAccessible<FloatType> interpolatedAndExtended =
                    Views.interpolate(Views.extendBorder(img), new NLinearInterpolatorFactory<FloatType>());
            IntervalView<FloatType> transformed = Views.interval(RealViews.transform(interpolatedAndExtended, transform), targetInterval);

            IO.createDirectoryForFile( String.format( targetPattern, z ) );
            new FileSaver(ImageJFunctions.wrapFloat( transformed, "" ) ).saveAsTiff( String.format( targetPattern, z ) );


        }

//        HashMap<Tuple2<Integer, Integer>, AffineModel1D> modelMap = new HashMap<Tuple2<Integer, Integer>, AffineModel1D>();
//        for( Map.Entry<Tuple2<Integer, Integer>, ArrayList<PointMatch>> entry : matchMap.entrySet() )
//        {
//            AffineModel1D m = new AffineModel1D();
//            m.fit( entry.getValue() );
//            modelMap.put( entry.getKey(), m );
//        }
//        AffineModel1D m = new AffineModel1D();
//        m.fit( matches );
//
//        for( int z = start; z < stop; ++z )
//        {
//
//            System.out.println( "Transforming section z=" + z );
//
//            ImagePlus imp1 = new ImagePlus(String.format(pattern1, z));
//            ImagePlus imp2 = new ImagePlus(String.format(pattern2, z));
//
//            ArrayImg<FloatType, FloatArray> img1 =
//                    ArrayImgs.floats((float[]) imp1.getProcessor().convertToFloatProcessor().getPixels(), imp1.getWidth(), imp1.getHeight());
//            ArrayImg<FloatType, FloatArray> img2 =
//                    ArrayImgs.floats((float[]) imp2.getProcessor().convertToFloatProcessor().getPixels(), imp2.getWidth(), imp2.getHeight());
//            RealRandomAccessible<FloatType> interpolatedAndExtended =
//                    Views.interpolate(Views.extendBorder(img1), new NLinearInterpolatorFactory<FloatType>());
//            IntervalView<FloatType> transformed = Views.interval(RealViews.transform(interpolatedAndExtended, transform), img2);
//
//            FloatProcessor targetProcessor = new FloatProcessor(imp2.getWidth(), imp2.getHeight());
//            ArrayImg<FloatType, FloatArray> target = ArrayImgs.floats( (float[]) targetProcessor.getPixels(), imp2.getWidth(), imp2.getHeight() );
//
//            FloatProcessor diffProcessor = new FloatProcessor(imp2.getWidth(), imp2.getHeight());
//            ArrayImg<FloatType, FloatArray> diff = ArrayImgs.floats( (float[]) diffProcessor.getPixels(), imp2.getWidth(), imp2.getHeight() );
//
//            double[] dummy = new double[1];
//
//            for(Cursor<FloatType> c2 = img2.cursor(), c1 = Views.flatIterable( transformed ).cursor(), t = target.cursor(), d = diff.cursor();
//                c2.hasNext(); )
//            {
//                dummy[0] = c2.next().getRealDouble();
//                 double applied = m.apply(dummy)[0];
////                double applied = modelMap.get( Utility.tuple2( c2.getIntPosition( 0 ), c2.getIntPosition( 1 ) ) ).apply( dummy )[0];
//                t.next().setReal(applied);
//                d.next().setReal( applied - c1.next().getRealDouble() );
//            }
//
//            String path = String.format(targetPattern, z);
//            IO.createDirectoryForFile(path);
//            new FileSaver( new ImagePlus("",targetProcessor) ).saveAsTiff( path );
//
//            String diffPath = String.format(diffPattern, z);
//            IO.createDirectoryForFile(diffPath);
//            new FileSaver( new ImagePlus("",diffProcessor) ).saveAsTiff( diffPath );
//
//
//        }
//
//        double[] matrix = new double[2];
//        System.out.println( Arrays.toString(m.getMatrix(matrix) ));
//
//        System.out.println("Done.");

    }

}
