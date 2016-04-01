package org.janelia.thickness;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.Iterator;
import java.util.List;

import net.imglib2.RandomAccess;
import net.imglib2.img.array.ArrayImg;
import net.imglib2.img.array.ArrayImgs;
import net.imglib2.img.array.ArrayRandomAccess;
import net.imglib2.img.basictypeaccess.array.DoubleArray;
import net.imglib2.interpolation.randomaccess.NLinearInterpolatorFactory;
import net.imglib2.realtransform.InverseRealTransform;
import net.imglib2.realtransform.RealTransformRandomAccessible;
import net.imglib2.realtransform.RealViews;
import net.imglib2.realtransform.ScaleAndTranslation;
import net.imglib2.type.numeric.real.DoubleType;
import net.imglib2.view.Views;
import org.apache.spark.SparkConf;
import org.apache.spark.api.java.JavaPairRDD;
import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.api.java.JavaSparkContext;
import org.apache.spark.api.java.function.Function2;
import org.apache.spark.api.java.function.PairFlatMapFunction;
import org.apache.spark.api.java.function.PairFunction;

import org.apache.spark.broadcast.Broadcast;
import org.janelia.thickness.utility.Utility;
import scala.Tuple2;

public class SparkInterpolation {

    public static JavaPairRDD< Tuple2< Integer, Integer >, double[] > interpolate(
            final JavaSparkContext sc,
            final JavaPairRDD< Tuple2< Integer, Integer >, double[] > source,
            final Broadcast<? extends List<Tuple2<Tuple2<Integer,Integer>,Tuple2<Double,Double>>>> newCoordinatesToOldCoordinates,
            int[] dim )
    {
//        JavaPairRDD<Tuple2<Integer, Integer>, double[]> result = source
        JavaPairRDD<Tuple2<Tuple2<Integer, Integer>, double[]>, Tuple2<Tuple2<Integer, Integer>, Double>> coordinateMatches =
                source.flatMapToPair(new MatchCoordinates(newCoordinatesToOldCoordinates, dim));
        JavaPairRDD<Tuple2<Integer, Integer>, Tuple2<double[], Double>> mapNewToOld = coordinateMatches.mapToPair(new SwapKey());
//        for( Tuple2<Tuple2<Integer, Integer>, Tuple2<double[], Double>> mnto : mapNewToOld.collect() )
//        {
//            System.out.println( mnto._1() + Arrays.toString(mnto._2()._1()) + mnto._2()._2() );
//        }
        JavaPairRDD<Tuple2<Integer, Integer>, Tuple2<double[], Double>> weightedArrays = mapNewToOld.mapToPair(new WeightedArrays());
        JavaPairRDD<Tuple2<Integer, Integer>, Tuple2<double[], Double>> reducedArrays = weightedArrays.reduceByKey(new ReduceArrays());
        JavaPairRDD<Tuple2<Integer, Integer>, double[]> result = reducedArrays.mapToPair(new NormalizeBySumOfWeights());

        return result;
    }

    public static class MatchCoordinates implements PairFlatMapFunction<
            Tuple2<Tuple2<Integer, Integer>, double[]>,
            Tuple2<Tuple2<Integer, Integer>, double[]>,
            Tuple2<Tuple2<Integer, Integer>, Double>> {

        private final Broadcast<? extends List<Tuple2<Tuple2<Integer,Integer>,Tuple2<Double,Double>>>> newCoordinatesToOldCoordinates;
        private final int[] dim;

        public MatchCoordinates(
                Broadcast<? extends List<Tuple2<Tuple2<Integer, Integer>, Tuple2<Double, Double>>>> newCoordinatesToOldCoordinates,
                final int[] dim ) {
            this.newCoordinatesToOldCoordinates = newCoordinatesToOldCoordinates;
            this.dim = dim;
        }

        public double restrictToDimension( double val, int dimension )
        {
            return Math.min( Math.max( val, 0 ), dim[dimension] - 1 );
        }

        @Override
        public Iterable<Tuple2<Tuple2<Tuple2<Integer, Integer>, double[]>, Tuple2<Tuple2<Integer, Integer>, Double>>>
        call(final Tuple2<Tuple2<Integer, Integer>, double[]> t) throws Exception {
            final Tuple2<Integer, Integer> oldSpaceInt = t._1();
            final ArrayList<Tuple2<Tuple2<Integer, Integer>, Double>> associations = new ArrayList<Tuple2<Tuple2<Integer, Integer>, Double>>();
            for (Tuple2<Tuple2<Integer, Integer>, Tuple2<Double, Double>> c : newCoordinatesToOldCoordinates.getValue()) {
                Tuple2<Double, Double> oldSpaceDouble = c._2();
                double diff1 = Math.abs(oldSpaceDouble._1() - oldSpaceInt._1().doubleValue());
                double diff2 = Math.abs(oldSpaceDouble._2() - oldSpaceInt._2().doubleValue());
                if (diff1 <= 1.0 && diff2 <= 1.0) {
                    associations.add(Utility.tuple2(c._1(), (1.0 - diff1) * (1.0 - diff2)));
                }
            }

            return new Iterable<Tuple2<Tuple2<Tuple2<Integer, Integer>, double[]>, Tuple2<Tuple2<Integer, Integer>, Double>>>() {
                @Override
                public Iterator<Tuple2<Tuple2<Tuple2<Integer, Integer>, double[]>, Tuple2<Tuple2<Integer, Integer>, Double>>> iterator() {
                    final Iterator<Tuple2<Tuple2<Integer, Integer>, Double>> it = associations.iterator();
                    return new Iterator<Tuple2<Tuple2<Tuple2<Integer, Integer>, double[]>, Tuple2<Tuple2<Integer, Integer>, Double>>>() {
                        @Override
                        public boolean hasNext() {
                            return it.hasNext();
                        }

                        @Override
                        public Tuple2<Tuple2<Tuple2<Integer, Integer>, double[]>, Tuple2<Tuple2<Integer, Integer>, Double>> next() {
                            return Utility.tuple2(t, it.next());
                        }

                        @Override
                        public void remove() {
                            throw new UnsupportedOperationException();
                        }
                    };
                }
            };
        }
    }

    public static class SwapKey implements PairFunction<
            Tuple2<Tuple2<Tuple2<Integer, Integer>, double[]>, Tuple2<Tuple2<Integer, Integer>, Double> >,
            Tuple2<Integer, Integer>, Tuple2<double[], Double>> {
        @Override
        public Tuple2<Tuple2<Integer, Integer>, Tuple2<double[], Double>>
        call(Tuple2<Tuple2<Tuple2<Integer, Integer>, double[]>, Tuple2<Tuple2<Integer, Integer>, Double>> t) throws Exception {
            Tuple2<Integer, Integer> newCoord = t._2()._1();
            return Utility.tuple2(newCoord, Utility.tuple2(t._1()._2(), t._2()._2()));
        }
    }

    public static class WeightedArrays implements PairFunction<
            Tuple2<Tuple2<Integer, Integer>, Tuple2<double[], Double>>,
            Tuple2<Integer, Integer>, Tuple2<double[], Double>> {
        @Override
        public Tuple2<Tuple2<Integer, Integer>, Tuple2<double[], Double>> call(Tuple2<Tuple2<Integer, Integer>, Tuple2<double[], Double>> t) throws Exception {
            Tuple2<double[], Double> arrWithWeight = t._2();
            double[] arr = arrWithWeight._1().clone(); // TODO clone here? YES! otherwise garbage output
            double weight = arrWithWeight._2();
            for (int i = 0; i < arr.length; ++i) {
                arr[i] *= weight;
            }
            return Utility.tuple2(t._1(), Utility.tuple2(arr, weight));
        }
    }

    public static class ReduceArrays implements Function2<Tuple2<double[], Double>, Tuple2<double[], Double>, Tuple2<double[], Double>> {
        @Override
        public Tuple2<double[], Double> call(Tuple2<double[], Double> t1, Tuple2<double[], Double> t2) throws Exception {
            double[] arr1 = t1._1();
            double[] arr2 = t2._1();
            for (int i = 0; i < arr1.length; ++i)
                arr1[i] += arr2[i];

            return Utility.tuple2(arr1, t1._2() + t2._2());
        }
    }

    public static class NormalizeBySumOfWeights implements PairFunction<Tuple2<Tuple2<Integer, Integer>, Tuple2<double[], Double>>, Tuple2<Integer, Integer>, double[]> {
        @Override
        public Tuple2<Tuple2<Integer, Integer>, double[]> call(Tuple2<Tuple2<Integer, Integer>, Tuple2<double[], Double>> t) throws Exception {
            double[] arr = t._2()._1();
            double weight = t._2()._2();
            for (int i = 0; i < arr.length; ++i)
                arr[i] /= weight;
            return Utility.tuple2(t._1(), arr);
        }
    }


    public static JavaPairRDD< Tuple2< Integer, Integer >, double[] > interpolate2D(
            JavaSparkContext sc,
            JavaPairRDD< Tuple2< Integer, Integer >, double[] > source,
            JavaRDD< Tuple2< Integer, Integer > > target,
            Tuple2< Integer, Integer > xMinMax,
            Tuple2< Integer, Integer > yMinMax,
            final int[] step,
            final int[] offset,
            final int[] otherStep,
            final int[] otherOffset,
            final int size
    )
    {
        JavaPairRDD<Tuple2<Integer, Integer>, double[]> result = target
                .flatMapToPair( new NearestNeighborsAtPreviousResolution(xMinMax, yMinMax, step, offset, otherStep, otherOffset) ) // current -> ( previous, weight )
                .mapToPair( new Utility.Swap< Tuple2< Integer, Integer >, Tuple2< Integer, Integer >, Double >() ) // previous -> ( current, weight )
                .join( source ) // previous -> ( ( current, weight ), coordinates )
                .mapToPair( new SwapAndDrop() )// current -> ( coordinates, weight )
                .aggregateByKey( new double[ size ], new SeqFunc(), new CombFunc() ) // current -> interpolated coordinate
                ;
        return result;
    }


    public static class NearestNeighborsAtPreviousResolution implements PairFlatMapFunction<
            Tuple2<Integer,Integer>,
            Tuple2<Integer,Integer>,
            Tuple2< Tuple2<Integer,Integer>, Double> >
    {

        private static final long serialVersionUID = 3959200717225103805L;
        private final Tuple2< Integer, Integer > xMinMax;
        private final Tuple2< Integer, Integer > yMinMax;
        private final int[] step;
        private final int[] offset;
        private final int[] otherStep;
        private final int[] otherOffset;

        public NearestNeighborsAtPreviousResolution(Tuple2<Integer, Integer> xMinMax,
                                                    Tuple2<Integer, Integer> yMinMax, int[] step, int[] offset, int[] otherStep, int[] otherOffset) {
            super();
            this.xMinMax = xMinMax;
            this.yMinMax = yMinMax;
            this.step = step;
            this.offset = offset;
            this.otherStep = otherStep;
            this.otherOffset = otherOffset;
        }

        public Iterable<Tuple2<Tuple2<Integer, Integer>, Tuple2<Tuple2<Integer, Integer>, Double>>> call(
                Tuple2<Integer, Integer> t) throws Exception {
            int x = t._1();
            int y = t._2();

            double xPrev = 1.0/( otherStep[0] ) * ( step[0]*x + offset[0] - otherOffset[0] );
            double yPrev = 1.0/( otherStep[1] ) * ( step[1]*y + offset[1] - otherOffset[1] );
            double xFloor = Math.floor( xPrev );
            double yFloor = Math.floor( yPrev );
            double xWeightUpper = xPrev - xFloor;
            double yWeightUpper = yPrev - yFloor;
            double xWeightLower = 1.0 - xWeightUpper;
            double yWeightLower = 1.0 - yWeightUpper;

            // check if calculated coordinates are within scope
            int xLower = (int) Math.max( xMinMax._1(), Math.min( xMinMax._2(), xFloor ) );
            int yLower = (int) Math.max( yMinMax._1(), Math.min( yMinMax._2(), yFloor ) );

            int xUpper = Math.max( xMinMax._1(), Math.min( xMinMax._2(), xLower ) );
            int yUpper = Math.max( yMinMax._1(), Math.min( yMinMax._2(), yLower ) );

            ArrayList<Tuple2<Tuple2<Integer, Integer>, Tuple2<Tuple2<Integer, Integer>, Double>>> result =
                    new ArrayList<Tuple2<Tuple2<Integer,Integer>,Tuple2<Tuple2<Integer,Integer>,Double>>>();

            result.add( Utility.tuple2( t, Utility.tuple2( Utility.tuple2( xLower, yLower ), xWeightLower*yWeightLower ) ) );
            result.add( Utility.tuple2( t, Utility.tuple2( Utility.tuple2( xUpper, yLower ), xWeightUpper*yWeightLower ) ) );
            result.add( Utility.tuple2( t, Utility.tuple2( Utility.tuple2( xLower, yUpper ), xWeightLower*yWeightUpper ) ) );
            result.add( Utility.tuple2( t, Utility.tuple2( Utility.tuple2( xUpper, yUpper ), xWeightUpper*yWeightUpper ) ) );

            return result;
        }


    }


    public static class SwapAndDrop implements PairFunction<Tuple2<Tuple2<Integer,Integer>,Tuple2<Tuple2<Tuple2<Integer,Integer>,Double>,double[]>>, Tuple2<Integer,Integer>, Tuple2<double[],Double>>
    {
        private static final long serialVersionUID = 3144085356295505868L;

        public Tuple2<Tuple2<Integer, Integer>, Tuple2<double[], Double>> call(
                Tuple2<Tuple2<Integer, Integer>, Tuple2<Tuple2<Tuple2<Integer, Integer>, Double>, double[]>> t)
                throws Exception {
            Tuple2<Tuple2<Tuple2<Integer, Integer>, Double>, double[]> t2 = t._2();
            double[] coord = t2._2();
            Tuple2<Tuple2<Integer, Integer>, Double> t3 = t2._1();
            return Utility.tuple2( t3._1(), Utility.tuple2( coord, t3._2() ) );
        }
    }


    public static class SeqFunc implements Function2< double[], Tuple2<double[], Double>, double[]>
    {
        private static final long serialVersionUID = -151563405590411586L;

        public double[] call(double[] a1, Tuple2<double[], Double> t)
                throws Exception {
            double[] a2 = t._1();
            double w = t._2();
            for ( int i = 0; i < a1.length; ++i )
                a1[ i ] += w*a2[ i ];
            return a1;
        }
    }


    public static class CombFunc implements	Function2< double[], double[], double[]>
    {
        private static final long serialVersionUID = -8993745973702877369L;

        public double[] call(double[] a1, double[] a2)
                throws Exception {
            for ( int i = 0; i < a1.length; ++i )
                a1[ i ] += a2[ i ];
            return a1;
        }
    }

    public static void main(String[] args) throws InterruptedException {
        SparkConf conf = new SparkConf().setAppName("InterpolationTest").setMaster("local");
        JavaSparkContext sc = new JavaSparkContext(conf);
        final int[] dim = new int[]{20, 20};
        int[] radii1 = new int[]{5, 5};
        int[] radii2 = new int[]{2, 2};
        int[] steps1 = new int[]{5, 5};
        int[] steps2 = new int[]{2, 2};
        int[] sourceDim = new int[] { 3 , 3 };
        CorrelationBlocks cbs1 = new CorrelationBlocks(radii1, steps1);
        CorrelationBlocks cbs2 = new CorrelationBlocks(radii2, steps2);

        // create rdd with local coordinates of r = [5,5], s = [5,5] and values =
        ArrayList<CorrelationBlocks.Coordinate> init = cbs1.generateFromBoundingBox(dim);
        JavaPairRDD<Tuple2<Integer, Integer>, double[]> rdd = sc
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
        ArrayList<Tuple2<Tuple2<Integer, Integer>, Tuple2<Double, Double>>> mapping = new ArrayList<Tuple2<Tuple2<Integer, Integer>, Tuple2<Double, Double>>>();
        for( CorrelationBlocks.Coordinate n : newCoords )
        {
            mapping.add( Utility.tuple2(n.getLocalCoordinates(), cbs1.translateCoordinateIntoThisBlockCoordinates(n)) );
        }


        // inteprolate using map from cbs2 into cbs1
        JavaPairRDD<Tuple2<Integer, Integer>, double[]> interpol = interpolate(sc, rdd, sc.broadcast(mapping),sourceDim);

        List<Tuple2<Tuple2<Integer, Integer>, double[]>> interpolCollected = interpol.collect();

        sc.close();

        ArrayImg<DoubleType, DoubleArray> sourceImg = ArrayImgs.doubles(sourceDim[0], sourceDim[1]);


        ArrayRandomAccess<DoubleType> ra = sourceImg.randomAccess();
        for( Tuple2<Tuple2<Integer, Integer>, double[]> rddC : rddCollected )
        {
            ra.setPosition( new int[] { rddC._1()._1(), rddC._1()._2() } );
            ra.get().set( rddC._2()[0] );
        }

        ScaleAndTranslation tf = new ScaleAndTranslation(
                new double[]{steps1[0] * 1.0 / steps2[0], steps1[1] * 1.0 / steps2[1]},
                new double[]{(radii1[0] - radii2[0]) * 1.0 / steps2[0], (radii1[1] - radii2[1]) * 1.0 / steps2[1]}
        );

        RealTransformRandomAccessible<DoubleType, InverseRealTransform> transformed =
                RealViews.transform(Views.interpolate(Views.extendBorder(sourceImg), new NLinearInterpolatorFactory<DoubleType>()), tf);


//        for( Tuple2<Tuple2<Integer, Integer>, double[]> bla : rddCollected )
//        {
//            System.out.println( bla._1() + Arrays.toString( bla._2() ) );
//        }
//        System.out.println(interpolCollected.size());

        Thread.sleep( 3000 );
        System.out.println( interpolCollected.size() );
        System.out.println( "Comparing..." );

        RandomAccess<DoubleType> t = transformed.randomAccess();
        for ( Tuple2<Tuple2<Integer, Integer>, double[]> iCol : interpolCollected )
        {
            t.setPosition(new int[]{iCol._1()._1(), iCol._1()._2()});
            if ( Math.abs( iCol._2()[0] - t.get().get() ) > 1e-10 )
                System.out.println( iCol._1() + Arrays.toString( iCol._2() ) + " " + t.get().get() );
        }

    }


}
