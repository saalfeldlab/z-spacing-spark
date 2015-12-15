package org.janelia.thickness;

import ij.ImagePlus;
import ij.io.FileSaver;
import ij.plugin.FolderOpener;
import ij.process.FloatProcessor;
import mpicbg.models.*;
import net.imglib2.Cursor;
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
import net.imglib2.view.composite.CompositeIntervalView;
import net.imglib2.view.composite.RealComposite;
import org.apache.spark.api.java.function.PairFunction;
import org.janelia.utility.io.IO;
import org.janelia.utility.realtransform.ScaleAndShift;
import scala.Tuple2;

import java.io.Serializable;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.HashMap;
import java.util.Map;
import java.util.concurrent.Callable;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;

/**
 * Created by hanslovskyp on 10/25/15.
 */
public class CompareDeformations {

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

    public static void main(String[] args) throws NotEnoughDataPointsException, IllDefinedDataPointsException, InterruptedException {

        String root = "/nobackup/saalfeld/hanslovskyp/CutOn4-15-2013_ImagedOn1-27-2014/aligned/substacks" +
                "/1300-3449/4000x2500+5172+1416/downscale-by-2/deformation";
        String id = "13";
        String run = "test";
        String base = String.format("%s/%s/%s", root, id, run);

        final int start = 0;
        final int stop = 2070;

        ExecutorService es = Executors.newFixedThreadPool(2);
        ArrayList<Callable<Void>> jobs = new ArrayList<Callable<Void>>();

        final String pattern1 = String.format("%s/%s", root, id) + "/transform/%04d.tif";

        for ( int n = 6; n < 7; ++n ) {

            String currentBase = base + String.format( "/out/%02d", n );
            final String pattern2 = currentBase + "/forward-scaled-to-06/%04d.tif";

            final String targetPattern = currentBase + "/affine-transformed-scaled-to-06/%04d.tif";

            final String diffPattern = currentBase + "/affine-transformed-diff-scaled-to-06/%04d.tif";



            double[] step1 = new double[]{10, 10};
            double[] step2 = new double[]{7, 7};

            double[] radius1 = new double[]{10, 10};
            double[] radius2 = new double[]{15, 15};

            double[] step = new double[2];
            double[] radius = new double[2];

            for (int i = 0; i < step.length; ++i) {
                step[i] = step1[i] / step2[i];
                radius[i] = (radius1[i] - radius2[i]) / step2[i];
            }

            ImagePlus imp0 = new ImagePlus(String.format(pattern1, start));

            int width = imp0.getWidth();
            int height = imp0.getHeight();
            int size = stop - start;
            final int nPoints = width * height * size;


            final ScaleAndShift transform = new ScaleAndShift(step, radius);
            jobs.add(new Callable<Void>() {
                @Override
                public Void call() throws Exception {
                    compare( nPoints, start, stop, pattern1, pattern2, transform, targetPattern, diffPattern);
                    return null;
                }
            });
        }

        es.invokeAll( jobs );

        System.out.println( "Done with all of it!" );

    }

    public static void compare( int nPoints, int start, int stop, String pattern1, String pattern2, ScaleAndShift transform, String targetPattern, String diffPattern )
            throws NotEnoughDataPointsException, IllDefinedDataPointsException {
        ArrayList<PointMatch> matches = new ArrayList<PointMatch>( nPoints );

//        HashMap<Tuple2<Integer, Integer>, ArrayList<PointMatch>> matchMap = new HashMap<Tuple2<Integer, Integer>, ArrayList<PointMatch>>();
//        for( int y = 0; y < imp0.getHeight(); ++y )
//        {
//            for ( int x = 0; x < imp0.getWidth(); ++x )
//            {
//                matchMap.put( Utility.tuple2( x, y ), new ArrayList<PointMatch>());
//            }
//        }

        for( int z = start; z < stop; ++z )
        {

            System.out.println( "Collecting matches at z=" + z );

            ImagePlus imp1 = new ImagePlus(String.format(pattern1, z));
            ImagePlus imp2 = new ImagePlus(String.format(pattern2, z));

            ArrayImg<FloatType, FloatArray> img1 =
                    ArrayImgs.floats((float[]) imp1.getProcessor().convertToFloatProcessor().getPixels(), imp1.getWidth(), imp1.getHeight());
            ArrayImg<FloatType, FloatArray> img2 =
                    ArrayImgs.floats((float[]) imp2.getProcessor().convertToFloatProcessor().getPixels(), imp2.getWidth(), imp2.getHeight());

            RealRandomAccessible<FloatType> interpolatedAndExtended =
                    Views.interpolate(Views.extendBorder(img1), new NLinearInterpolatorFactory<FloatType>());
            IntervalView<FloatType> transformed = Views.interval(RealViews.transform(interpolatedAndExtended, transform), img2);
            for(Cursor<FloatType> c2 = img2.cursor(), c1 = Views.flatIterable( transformed ).cursor(); c1.hasNext(); )
            {
                double pos1 = c1.next().getRealDouble();
                double pos2 = c2.next().getRealDouble();
                PointMatch match =
                        new PointMatch(
                                new Point(new double[]{c2.getDoublePosition(0), c2.getDoublePosition(1), pos2} ),
                                new Point(new double[]{c1.getDoublePosition(0), c1.getDoublePosition(1), pos1} )
                                );
                matches.add( match );
//                matchMap.get( Utility.tuple2( c1.getIntPosition( 0 ), c1.getIntPosition( 1 ) ) ).add( match );
            }

        }

        System.out.flush();

//        HashMap<Tuple2<Integer, Integer>, AffineModel1D> modelMap = new HashMap<Tuple2<Integer, Integer>, AffineModel1D>();
//        for( Map.Entry<Tuple2<Integer, Integer>, ArrayList<PointMatch>> entry : matchMap.entrySet() )
//        {
//            AffineModel1D m = new AffineModel1D();
//            m.fit( entry.getValue() );
//            modelMap.put( entry.getKey(), m );
//        }
        AffineModel3D m = new AffineModel3D();
        m.fit( matches );

        for( int z = start; z < stop; ++z )
        {

            System.out.println( "Transforming section z=" + z );

            ImagePlus imp1 = new ImagePlus(String.format(pattern1, z));
            ImagePlus imp2 = new ImagePlus(String.format(pattern2, z));

            ArrayImg<FloatType, FloatArray> img1 =
                    ArrayImgs.floats((float[]) imp1.getProcessor().convertToFloatProcessor().getPixels(), imp1.getWidth(), imp1.getHeight());
            ArrayImg<FloatType, FloatArray> img2 =
                    ArrayImgs.floats((float[]) imp2.getProcessor().convertToFloatProcessor().getPixels(), imp2.getWidth(), imp2.getHeight());
            RealRandomAccessible<FloatType> interpolatedAndExtended =
                    Views.interpolate(Views.extendBorder(img1), new NLinearInterpolatorFactory<FloatType>());
            IntervalView<FloatType> transformed = Views.interval(RealViews.transform(interpolatedAndExtended, transform), img2);

            FloatProcessor targetProcessor = new FloatProcessor(imp2.getWidth(), imp2.getHeight());
            ArrayImg<FloatType, FloatArray> target = ArrayImgs.floats( (float[]) targetProcessor.getPixels(), imp2.getWidth(), imp2.getHeight() );

            FloatProcessor diffProcessor = new FloatProcessor(imp2.getWidth(), imp2.getHeight());
            ArrayImg<FloatType, FloatArray> diff = ArrayImgs.floats( (float[]) diffProcessor.getPixels(), imp2.getWidth(), imp2.getHeight() );

            double[] dummy = new double[3];

            for(Cursor<FloatType> c2 = img2.cursor(), c1 = Views.flatIterable( transformed ).cursor(), t = target.cursor(), d = diff.cursor();
                c2.hasNext(); )
            {
                dummy[2] = c2.next().getRealDouble();
                dummy[0] = c2.getDoublePosition(0);
                dummy[1] = c2.getDoublePosition(1);
                double applied = m.apply(dummy)[2];
//                double applied = modelMap.get( Utility.tuple2( c2.getIntPosition( 0 ), c2.getIntPosition( 1 ) ) ).apply( dummy )[0];
                t.next().setReal(applied);
                d.next().setReal( applied - c1.next().getRealDouble() );
            }

            String path = String.format(targetPattern, z);
            IO.createDirectoryForFile(path);
            new FileSaver( new ImagePlus("",targetProcessor) ).saveAsTiff( path );

            String diffPath = String.format(diffPattern, z);
            IO.createDirectoryForFile(diffPath);
            new FileSaver( new ImagePlus("",diffProcessor) ).saveAsTiff( diffPath );


        }

//        double[] matrix = new double[2];
//        System.out.println( Arrays.toString(m.getMatrix(matrix) ));

        System.out.println("Done.");

    }

}
