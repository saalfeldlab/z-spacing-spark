package org.janelia.thickness.experiments;

import ij.ImagePlus;
import ij.io.FileSaver;
import ij.process.FloatProcessor;
import mpicbg.models.*;
import net.imglib2.Cursor;
import net.imglib2.img.array.ArrayImg;
import net.imglib2.img.array.ArrayImgs;
import net.imglib2.img.basictypeaccess.array.FloatArray;
import net.imglib2.type.numeric.real.FloatType;
import org.apache.spark.api.java.function.PairFunction;
import org.janelia.thickness.ScaleOptions;
import scala.Tuple2;

import java.io.File;
import java.io.FileNotFoundException;
import java.io.Serializable;
import java.util.ArrayList;
import java.util.concurrent.Callable;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;

/**
 * @author Philipp Hanslovsky &lt;hanslovskyp@janelia.hhmi.org&gt;
 */
public class CompareDeformations {

    public static int nThreads = Runtime.getRuntime().availableProcessors();

    public static class Stat implements Serializable
    {
//        public final double
    }

    public static class TransformAndCompare implements PairFunction<Tuple2<Integer,FloatProcessor[]>,Integer, Stat>
    {



        @Override
        public Tuple2<Integer, Stat> call(Tuple2<Integer, FloatProcessor[]> integerTuple2) throws Exception {
            return null;
        }
    }

    public static void comparePositions (
            final String groundTruthPattern,
            final String base,
            final ScaleOptions config,
            final int off,
            final int windowRange
    ) throws InterruptedException {


        final int start = config.start + off;
        final int stop  = config.stop - off;

        final int refIndex = config.radii.length - 1;

        final String startStop = String.format( "start=%04d-stop=%04d-wr=%04d", start, stop, windowRange );

        ExecutorService es = Executors.newFixedThreadPool( CompareDeformations.nThreads );
        ArrayList<Callable<Void>> jobs = new ArrayList<Callable<Void>>();


        // start from -1?
        for ( int n = 0; n < config.radii.length; ++n ) {

            String currentBase = base + String.format( "/out/%02d", n );
            final String pattern2 = currentBase + String.format( "/forward-scaled-to-%02d", refIndex ) + "/%04d.tif";
            System.out.println( currentBase );
            System.out.println( pattern2 );
            System.out.println();

            final String targetPattern = currentBase + String.format( "/affine-transformed-scaled-to-%02d-%s", refIndex, startStop ) + "/%04d.tif";

            final String diffPattern = currentBase + String.format( "/affine-transformed-diff-scaled-to-%02d-%s", refIndex, startStop ) + "/%04d.tif";
            final String diffPatternGradient = currentBase + String.format( "/gradient-diff-scaled-to-%02d-%s", refIndex, startStop ) + "/%04d.tif";

            jobs.add(new Callable<Void>() {
                @Override
                public Void call() throws Exception {
                    compare( start, stop, groundTruthPattern, pattern2, targetPattern, diffPattern, windowRange);
                    return null;
                }
            });
        }

        es.invokeAll( jobs );
        es.shutdown();

        System.out.println( "Done with all of it (positions)!" );
    }

    public static void compareGradients (
            final String groundTruthPattern,
            final String base,
            final ScaleOptions config
    ) throws InterruptedException {


        final int start = config.start;
        final int stop  = config.stop;

        final int refIndex = config.radii.length - 1;

        final String startStop = String.format( "start=%04d-stop=%04d", start, stop );

        ExecutorService es = Executors.newFixedThreadPool( CompareDeformations.nThreads );
        ArrayList<Callable<Void>> jobs = new ArrayList<Callable<Void>>();


        // start from -1?
        for ( int n = 0; n < config.radii.length; ++n ) {

            String currentBase = base + String.format( "/out/%02d", n );
            final String patternGradientComp =
                    currentBase + String.format( "/forward-gradient-scaled-to-%02d", refIndex ) + "/%04d.tif";
            final String diffPatternGradient =
                    currentBase + String.format( "/gradient-diff-scaled-to-%02d-%s", refIndex, startStop ) + "/%04d.tif";

            jobs.add(new Callable<Void>() {
                @Override
                public Void call() throws Exception {
                    compare_no_affine( start, stop, groundTruthPattern, patternGradientComp, diffPatternGradient );
                    return null;
                }
            });
        }

        es.invokeAll( jobs );
        es.shutdown();

        System.out.println( "Done with all of it (gradients)!" );
    }


    public static void main(String[] args) throws NotEnoughDataPointsException, IllDefinedDataPointsException, InterruptedException, FileNotFoundException {

        String root = "/nobackup/saalfeld/hanslovskyp/CutOn4-15-2013_ImagedOn1-27-2014/aligned/substacks" +
                "/1300-3449/4000x2500+5172+1416/downscale-z-by-4/corrected/z=[25,499]/z=[0,99]/deformation";

        String id = "01";
        String run = "20160129_135000";

        String base = String.format("%s/%s/%s", root, id, run);

        String configPath = base + "/config.json";

        ScaleOptions config = ScaleOptions.createFromFile(configPath);
        int off = 0;
        final int windowRange = 99;//( stop - start );

        final String patternRef = String.format("%s/%s", root, id) + "/backward-scaled-to-07/%04d.tif";
        final String patternGradientRef = String.format("%s/%s", root, id) + "/backward-gradient-scaled-to-07/%04d.tif";


        comparePositions(
                patternRef,
                base,
                config,
                off,
                windowRange
        );


//        compareGradients(
//                patternGradientRef,
//                base,
//                config
//        );


    }

    public static void compare(
            int start,
            int stop,
            String gtPattern,
            String comparePattern,
            String targetPattern,
            String diffPattern,
            int windowRange )
            throws NotEnoughDataPointsException, IllDefinedDataPointsException {
//        ArrayList<PointMatch>[] matchess = new ArrayList[ stop - start ];
//        for ( int i = 0; i < matchess.length; ++i )
//            matchess[i] = new ArrayList<>();

        ArrayList<PointMatch> matches = new ArrayList<PointMatch>();

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

            ImagePlus gtImp = new ImagePlus(String.format(gtPattern, z));
            ImagePlus compareImp = new ImagePlus(String.format(comparePattern, z));

            ArrayImg<FloatType, FloatArray> gtImg =
                    ArrayImgs.floats((float[]) gtImp.getProcessor().convertToFloatProcessor().getPixels(), gtImp.getWidth(), gtImp.getHeight());
            ArrayImg<FloatType, FloatArray> compareImg =
                    ArrayImgs.floats((float[]) compareImp.getProcessor().convertToFloatProcessor().getPixels(), compareImp.getWidth(), compareImp.getHeight());

            for(Cursor<FloatType> compareC = compareImg.cursor(), gtC = gtImg.cursor(); gtC.hasNext(); )
            {
                double gtPos = gtC.next().getRealDouble();
                double comparePos = compareC.next().getRealDouble();
                if ( Double.isNaN(comparePos) || comparePos == 0.0 || compareC.getDoublePosition( 1 ) < 10.0 )
                    continue;
                PointMatch match =
                        new PointMatch(
                                new Point(new double[]{comparePos} ),
                                new Point(new double[]{gtPos} )
                                );
//                for ( int lower = Math.max(start, z - windowRange ); lower < Math.min( stop, z + windowRange + 1 ); ++lower )
//                    matchess[ lower - start ].add( match );
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


        AffineModel1D m = new AffineModel1D();
        m.fit( matches );
//        AffineModel1D[] ms = new AffineModel1D[stop - start];
//        for ( int i = 0; i < ms.length; ++i )
//        {
//            ms[i] = new AffineModel1D();
//            ms[i].fit( matchess[i] );
//        }

        for( int z = start; z < stop; ++z )
        {

            System.out.println( "Transforming section z=" + z );

            ImagePlus gtImp = new ImagePlus(String.format(gtPattern, z));
            ImagePlus compareImp = new ImagePlus(String.format(comparePattern, z));

            ArrayImg<FloatType, FloatArray> gtImg =
                    ArrayImgs.floats((float[]) gtImp.getProcessor().convertToFloatProcessor().getPixels(), gtImp.getWidth(), gtImp.getHeight());
            ArrayImg<FloatType, FloatArray> compareImg =
                    ArrayImgs.floats((float[]) compareImp.getProcessor().convertToFloatProcessor().getPixels(), compareImp.getWidth(), compareImp.getHeight());

            FloatProcessor targetProcessor = new FloatProcessor(compareImp.getWidth(), compareImp.getHeight());
            ArrayImg<FloatType, FloatArray> target = ArrayImgs.floats( (float[]) targetProcessor.getPixels(), compareImp.getWidth(), compareImp.getHeight() );

            FloatProcessor diffProcessor = new FloatProcessor(compareImp.getWidth(), compareImp.getHeight());
            ArrayImg<FloatType, FloatArray> diff = ArrayImgs.floats( (float[]) diffProcessor.getPixels(), compareImp.getWidth(), compareImp.getHeight() );

            double[] dummy = new double[1];

            for(Cursor<FloatType> compareC = compareImg.cursor(), gtC = gtImg.cursor(), t = target.cursor(), d = diff.cursor();
                compareC.hasNext(); )
            {
                double dub = compareC.next().getRealDouble();
                t.fwd();
                d.fwd();
                if ( Double.isNaN( dub ) || dub == 0.0 )
                {
                    t.get().setReal( Double.NaN );
                    d.get().setReal( Double.NaN );
                }
                else {
                    dummy[0] = dub;
                    double applied = m.apply( dummy )[0];
//                    double applied = ms[z-start].apply(dummy)[0];
//                double applied = modelMap.get( Utility.tuple2( c2.getIntPosition( 0 ), c2.getIntPosition( 1 ) ) ).apply( dummy )[0];
                    t.get().setReal(applied);
                    d.get().setReal(applied - gtC.next().getRealDouble());
                }
            }

            String path = String.format(targetPattern, z);
            new File(path).getParentFile().mkdirs();
            new FileSaver( new ImagePlus("",targetProcessor) ).saveAsTiff( path );

            String diffPath = String.format(diffPattern, z);
            new File(diffPath).getParentFile().mkdirs();
            new FileSaver( new ImagePlus("",diffProcessor) ).saveAsTiff( diffPath );


        }



//        double[] matrix = new double[2];
//        System.out.println( Arrays.toString(m.getMatrix(matrix) ));

        System.out.println("Done.");

    }


    public static void compare_no_affine(
            int start,
            int stop,
            String pattern1,
            String pattern2,
            String diffPattern )
            throws NotEnoughDataPointsException, IllDefinedDataPointsException {


        for( int z = start; z < stop; ++z )
        {

            System.out.println( "Transforming section z=" + z );

            ImagePlus imp1 = new ImagePlus(String.format(pattern1, z));
            ImagePlus imp2 = new ImagePlus(String.format(pattern2, z));

            ArrayImg<FloatType, FloatArray> img1 =
                    ArrayImgs.floats((float[]) imp1.getProcessor().convertToFloatProcessor().getPixels(), imp1.getWidth(), imp1.getHeight());
            ArrayImg<FloatType, FloatArray> img2 =
                    ArrayImgs.floats((float[]) imp2.getProcessor().convertToFloatProcessor().getPixels(), imp2.getWidth(), imp2.getHeight());


            FloatProcessor diffProcessor = new FloatProcessor(imp2.getWidth(), imp2.getHeight());
            ArrayImg<FloatType, FloatArray> diff = ArrayImgs.floats( (float[]) diffProcessor.getPixels(), imp2.getWidth(), imp2.getHeight() );

            for(Cursor<FloatType> c2 = img2.cursor(), c1 = img1.cursor(), d = diff.cursor();
                c2.hasNext(); )
            {
                double dub = c2.next().getRealDouble();
                d.fwd();
                if ( Double.isNaN( dub ) || dub == 0.0 )
                {
                    d.get().setReal( Double.NaN );
                }
                else {
                    d.get().setReal(dub - c1.next().getRealDouble());
                }
            }

            String diffPath = String.format(diffPattern, z);
            new File(diffPath).getParentFile().mkdirs();
            new FileSaver( new ImagePlus("",diffProcessor) ).saveAsTiff( diffPath );


        }



//        double[] matrix = new double[2];
//        System.out.println( Arrays.toString(m.getMatrix(matrix) ));

        System.out.println("Done.");

    }

}
