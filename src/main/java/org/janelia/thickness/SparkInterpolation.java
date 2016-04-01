package org.janelia.thickness;

import java.io.Serializable;
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
import net.imglib2.interpolation.randomaccess.NearestNeighborInterpolatorFactory;
import net.imglib2.realtransform.InverseRealTransform;
import net.imglib2.realtransform.RealTransformRandomAccessible;
import net.imglib2.realtransform.RealViews;
import net.imglib2.realtransform.ScaleAndTranslation;
import net.imglib2.type.numeric.real.DoubleType;
import net.imglib2.view.Views;
import org.apache.spark.SparkConf;
import org.apache.spark.api.java.JavaPairRDD;
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
            int[] dim,
            MatchCoordinates.Matcher matcher )
    {

        JavaPairRDD<Tuple2<Tuple2<Integer, Integer>, double[]>, Tuple2<Tuple2<Integer, Integer>, Double>> coordinateMatches =
                source.flatMapToPair(new MatchCoordinates(newCoordinatesToOldCoordinates, dim, matcher));
        JavaPairRDD<Tuple2<Integer, Integer>, Tuple2<double[], Double>> mapNewToOld = coordinateMatches.mapToPair(new SwapKey());

        JavaPairRDD<Tuple2<Integer, Integer>, Tuple2<double[], Double>> weightedArrays = mapNewToOld.mapToPair(new WeightedArrays());
        JavaPairRDD<Tuple2<Integer, Integer>, Tuple2<double[], Double>> reducedArrays = weightedArrays.reduceByKey(new ReduceArrays());
        JavaPairRDD<Tuple2<Integer, Integer>, double[]> result = reducedArrays.mapToPair(new NormalizeBySumOfWeights());

        return result;
    }

    public static class MatchCoordinates implements PairFlatMapFunction<
            Tuple2<Tuple2<Integer, Integer>, double[]>,
            Tuple2<Tuple2<Integer, Integer>, double[]>,
            Tuple2<Tuple2<Integer, Integer>, Double>> {

        public static interface Matcher extends Serializable
        {
            public void call(
                    Tuple2< Double, Double > newPointInOldCoordinates,
                    Tuple2<Integer, Integer> newCoordinateGrid,
                    Tuple2< Integer, Integer > oldCoordinateGrid,
                    List< Tuple2< Tuple2< Integer, Integer >, Double > > associateWithNewCoordinatesGridAndWeights,
                    int[] dim
                    );
        }

        public static class NearestNeighborMatcher implements Matcher
        {

            @Override
            public void call(
                    Tuple2<Double, Double> newPointInOldCoordinates,
                    Tuple2<Integer, Integer> newCoordinateGrid,
                    Tuple2<Integer, Integer> oldCoordinateGrid,
                    List<Tuple2<Tuple2<Integer, Integer>, Double>> associateWithNewCoordinatesGridAndWeights,
                    int[] dim) {

                if (
                        Math.round( newPointInOldCoordinates._1() ) == oldCoordinateGrid._1().intValue()  &&
                        Math.round( newPointInOldCoordinates._2() ) == oldCoordinateGrid._2().intValue()
                        )
                    associateWithNewCoordinatesGridAndWeights.add( Utility.tuple2( newCoordinateGrid, 1.0 ) );

            }
        }

        public static class NLinearMatcher implements Matcher
        {

            @Override
            public void call(
                    Tuple2<Double, Double> newPointInOldCoordinates,
                    Tuple2<Integer, Integer> newCoordinateGrid,
                    Tuple2<Integer, Integer> oldCoordinateGrid,
                    List<Tuple2<Tuple2<Integer, Integer>, Double>> associateWithNewCoordinatesGridAndWeights,
                    int[] dim) {

                double diff1 = Math.abs(newPointInOldCoordinates._1() - oldCoordinateGrid._1().doubleValue());
                double diff2 = Math.abs(newPointInOldCoordinates._2() - oldCoordinateGrid._2().doubleValue());
                if (diff1 <= 1.0 && diff2 <= 1.0) {
                    associateWithNewCoordinatesGridAndWeights.add(Utility.tuple2(newCoordinateGrid, (1.0 - diff1) * (1.0 - diff2)));
                }

            }
        }

        private final Broadcast<? extends List<Tuple2<Tuple2<Integer,Integer>,Tuple2<Double,Double>>>> newCoordinatesToOldCoordinates;
        private final int[] dim;
        private final Matcher matcher;

        public MatchCoordinates(
                Broadcast<? extends List<Tuple2<Tuple2<Integer, Integer>, Tuple2<Double, Double>>>> newCoordinatesToOldCoordinates,
                final int[] dim,
                final Matcher matcher ) {
            this.newCoordinatesToOldCoordinates = newCoordinatesToOldCoordinates;
            this.dim = dim;
            this.matcher = matcher;
        }

        @Override
        public Iterable<Tuple2<Tuple2<Tuple2<Integer, Integer>, double[]>, Tuple2<Tuple2<Integer, Integer>, Double>>>
        call(final Tuple2<Tuple2<Integer, Integer>, double[]> t) throws Exception {
            final Tuple2<Integer, Integer> oldCoordinateGrid = t._1();
            final ArrayList<Tuple2<Tuple2<Integer, Integer>, Double>> associations = new ArrayList<Tuple2<Tuple2<Integer, Integer>, Double>>();
            for (Tuple2<Tuple2<Integer, Integer>, Tuple2<Double, Double>> c : newCoordinatesToOldCoordinates.getValue()) {
                Tuple2<Integer, Integer> newCoordinateGrid = c._1();
                Tuple2<Double, Double> newPointInOldCoordinates = c._2();
                matcher.call( newPointInOldCoordinates,  newCoordinateGrid, oldCoordinateGrid, associations, dim );
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
        JavaPairRDD<Tuple2<Integer, Integer>, double[]> interpol = interpolate(sc, rdd, sc.broadcast(mapping),sourceDim, new MatchCoordinates.NLinearMatcher());

        List<Tuple2<Tuple2<Integer, Integer>, double[]>> interpolCollected = interpol.collect();


        JavaPairRDD<Tuple2<Integer, Integer>, double[]> interpol2 = interpolate(sc, rdd, sc.broadcast(mapping), sourceDim, new MatchCoordinates.NearestNeighborMatcher());

        List<Tuple2<Tuple2<Integer, Integer>, double[]>> interpolCollected2 = interpol2.collect();


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

        RealTransformRandomAccessible<DoubleType, InverseRealTransform> transformed2 =
                RealViews.transform(Views.interpolate(Views.extendBorder(sourceImg), new NearestNeighborInterpolatorFactory<DoubleType>()), tf);


        Thread.sleep( 3000 );
        System.out.flush();
        System.out.println( interpolCollected.size() );
        System.out.println( "Comparing nlinear ..." );

        RandomAccess<DoubleType> t = transformed.randomAccess();
        for ( Tuple2<Tuple2<Integer, Integer>, double[]> iCol : interpolCollected )
        {
            t.setPosition(new int[]{iCol._1()._1(), iCol._1()._2()});
            if ( Math.abs( iCol._2()[0] - t.get().get() ) > 1e-10 )
                System.out.println( iCol._1() + Arrays.toString( iCol._2() ) + " " + t.get().get() );
        }


        System.out.flush();
        System.out.println( interpolCollected.size() );
        System.out.println( "Comparing nearest neighbor ..." );

        RandomAccess<DoubleType> t2 = transformed2.randomAccess();
        for ( Tuple2<Tuple2<Integer, Integer>, double[]> iCol : interpolCollected2 )
        {
            t2.setPosition(new int[]{iCol._1()._1(), iCol._1()._2()});
            if ( Math.abs( iCol._2()[0] - t2.get().get() ) > 1e-10 )
                System.out.println( iCol._1() + Arrays.toString( iCol._2() ) + " " + t2.get().get() );
        }
    }


}
