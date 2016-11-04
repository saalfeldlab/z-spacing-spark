package org.janelia.thickness;

import static org.janelia.thickness.SparkInterpolation.interpolate;

import java.util.ArrayList;
import java.util.List;

import org.apache.spark.SparkConf;
import org.apache.spark.api.java.JavaPairRDD;
import org.apache.spark.api.java.JavaSparkContext;
import org.apache.spark.api.java.function.PairFunction;
import org.janelia.thickness.utility.Utility;
import org.junit.Assert;

import net.imglib2.RandomAccess;
import net.imglib2.RandomAccessible;
import net.imglib2.RandomAccessibleInterval;
import net.imglib2.img.array.ArrayImg;
import net.imglib2.img.array.ArrayImgs;
import net.imglib2.img.array.ArrayRandomAccess;
import net.imglib2.img.basictypeaccess.array.DoubleArray;
import net.imglib2.interpolation.InterpolatorFactory;
import net.imglib2.interpolation.randomaccess.NLinearInterpolatorFactory;
import net.imglib2.interpolation.randomaccess.NearestNeighborInterpolatorFactory;
import net.imglib2.realtransform.InvertibleRealTransform;
import net.imglib2.realtransform.RealViews;
import net.imglib2.realtransform.ScaleAndTranslation;
import net.imglib2.type.numeric.real.DoubleType;
import net.imglib2.view.Views;
import scala.Tuple2;

/**
 * @author Philipp Hanslovsky
 */
public class SparkInterpolationTest {

    private JavaSparkContext sc;
    private JavaPairRDD<Tuple2<Integer, Integer>, double[]> rdd;

//    private InvertibleRealTransform  tf;

    private final ArrayList<Tuple2<Tuple2<Integer, Integer>, Tuple2<Double, Double>>> mapping =
            new ArrayList<Tuple2<Tuple2<Integer, Integer>, Tuple2<Double, Double>>>();

    private final int[] dim = new int[]{20, 20};
    private final int[] radii1 = new int[]{5, 5};
    private final int[] radii2 = new int[]{2, 2};
    private final int[] steps1 = new int[]{5, 5};
    private final int[] steps2 = new int[]{2, 2};
    private final int[] sourceDim = new int[] { 3 , 3 };
    private final CorrelationBlocks cbs1 = new CorrelationBlocks(radii1, steps1);
    private final CorrelationBlocks cbs2 = new CorrelationBlocks(radii2, steps2);
    private final ArrayImg< DoubleType, DoubleArray > source = ArrayImgs.doubles( sourceDim[0], sourceDim[1] );

    private final ScaleAndTranslation tf = new ScaleAndTranslation(
            new double[]{steps1[0] * 1.0 / steps2[0], steps1[1] * 1.0 / steps2[1]},
            new double[]{(radii1[0] - radii2[0]) * 1.0 / steps2[0], (radii1[1] - radii2[1]) * 1.0 / steps2[1]}
    );

    public static void compare(
            List<Tuple2<Tuple2<Integer, Integer>, double[]>> interpolated,
            RandomAccessibleInterval<DoubleType> source,
            InterpolatorFactory< DoubleType, RandomAccessible< DoubleType >> fac,
            InvertibleRealTransform tf,
            double allowedError)
    {

        RandomAccess<DoubleType> ref = RealViews.transform(Views.interpolate(Views.extendBorder(source), fac ), tf).randomAccess();

        for ( Tuple2<Tuple2<Integer, Integer>, double[]> iCol : interpolated )
        {

            ref.setPosition(new int[]{iCol._1()._1(), iCol._1()._2()});
            Assert.assertEquals( ref.get().get(), iCol._2()[0], allowedError );
//            if ( Math.abs( iCol._2()[0] - ref.get().get() ) > 1e-10 )
//                System.out.println( iCol._1() + Arrays.toString( iCol._2() ) + " " + ref.get().get() );
        }
    }

    public void testNLiner()
    {
        // inteprolate using map from cbs2 into cbs1
        JavaPairRDD<Tuple2<Integer, Integer>, double[]> interpol = interpolate(
                sc,
                rdd,
                sc.broadcast(mapping),
                sourceDim,
                new SparkInterpolation.MatchCoordinates.NLinearMatcher());

        List<Tuple2<Tuple2<Integer, Integer>, double[]>> interpolCollected = interpol.collect();

        System.out.flush();
        System.out.println( interpolCollected.size() );
        System.out.println( "Comparing nlinear ..." );

        compare( interpolCollected, source, new NLinearInterpolatorFactory<DoubleType>(), tf, 1e-10 );
        Assert.assertTrue( false );
    }

    public void testNearestNeighbor()
    {
        // inteprolate using map from cbs2 into cbs1
        JavaPairRDD<Tuple2<Integer, Integer>, double[]> interpol = interpolate(
                sc,
                rdd,
                sc.broadcast(mapping),
                sourceDim,
                new SparkInterpolation.MatchCoordinates.NearestNeighborMatcher());

        List<Tuple2<Tuple2<Integer, Integer>, double[]>> interpolCollected = interpol.collect();

        System.out.flush();
        System.out.println( interpolCollected.size() );
        System.out.println( "Comparing nlinear ..." );

        compare( interpolCollected, source, new NearestNeighborInterpolatorFactory<DoubleType>(), tf, 0.0 );
    }

    @org.junit.Before
    public void setUp() throws Exception {

        SparkConf conf = new SparkConf().setAppName("InterpolationTest").setMaster("local");
        sc = new JavaSparkContext( conf );


        // create rdd with local coordinates of r = [5,5], s = [5,5] and values =
        ArrayList<CorrelationBlocks.Coordinate> init = cbs1.generateFromBoundingBox(dim);
        rdd = sc
                .parallelize(init)
                .mapToPair(new PairFunction<CorrelationBlocks.Coordinate, Tuple2<Integer, Integer>, double[]>() {
                    @Override
                    public Tuple2<Tuple2<Integer, Integer>, double[]> call(CorrelationBlocks.Coordinate coordinate) throws Exception {
                        Tuple2<Integer, Integer> wc = coordinate.getWorldCoordinates();
                        return Utility.tuple2(coordinate.getLocalCoordinates(), new double[]{wc._1() + wc._2() * dim[0]});
                    }
                })
                ;

        List<Tuple2<Tuple2<Integer, Integer>, double[]>> rddCollected = rdd.collect();

        // map from cbs2 into cbs1
        ArrayList<CorrelationBlocks.Coordinate> newCoords = cbs2.generateFromBoundingBox(dim);
        for( CorrelationBlocks.Coordinate n : newCoords )
        {
            mapping.add( Utility.tuple2(n.getLocalCoordinates(), cbs1.translateCoordinateIntoThisBlockCoordinates(n)) );
        }

        ArrayImg<DoubleType, DoubleArray> sourceImg = ArrayImgs.doubles(sourceDim[0], sourceDim[1]);


        ArrayRandomAccess<DoubleType> ra = source.randomAccess();
        for( Tuple2<Tuple2<Integer, Integer>, double[]> rddC : rddCollected )
        {
            ra.setPosition( new int[] { rddC._1()._1(), rddC._1()._2() } );
            ra.get().set( rddC._2()[0] );
        }
    }

    @org.junit.After
    public void tearDown() throws Exception {
        sc.close();
    }

}