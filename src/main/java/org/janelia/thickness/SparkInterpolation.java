package org.janelia.thickness;

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
import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.api.java.JavaSparkContext;
import org.apache.spark.api.java.function.Function2;
import org.apache.spark.api.java.function.PairFlatMapFunction;
import org.apache.spark.api.java.function.PairFunction;
import org.apache.spark.broadcast.Broadcast;
import org.janelia.thickness.utility.Utility;
import scala.Tuple2;

import java.io.Serializable;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Iterator;
import java.util.List;

public class SparkInterpolation {

    public static JavaPairRDD< Tuple2< Integer, Integer >, double[] > interpolate(
            final JavaSparkContext sc,
            final JavaPairRDD< Tuple2< Integer, Integer >, double[] > source,
            final Broadcast<? extends List<Tuple2<Tuple2<Integer,Integer>,Tuple2<Double,Double>>>> newCoordinatesToOldCoordinates,
            int[] dim,
            AssociationPolicy association )
    {
//        JavaPairRDD<Tuple2<Integer, Integer>, double[]> result = source
        JavaPairRDD<Tuple2<Tuple2<Integer, Integer>, double[]>, Tuple2<Tuple2<Integer, Integer>, Double>> coordinateMatches =
                source.flatMapToPair(new MatchCoordinates(newCoordinatesToOldCoordinates, dim, association));
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
    Tuple2<Tuple2<Integer, Integer>, Double>> 
    {

		/**
		 * 
		 */
		private static final long serialVersionUID = 663820550414953261L;
		protected final Broadcast<? extends List<Tuple2<Tuple2<Integer,Integer>,Tuple2<Double,Double>>>> newCoordinatesToOldCoordinates;
        protected final int[] dim;
        protected final AssociationPolicy policy;
        
    	public MatchCoordinates(
                Broadcast<? extends List<Tuple2<Tuple2<Integer, Integer>, Tuple2<Double, Double>>>> newCoordinatesToOldCoordinates,
                final int[] dim,
                final AssociationPolicy policy ) {
            this.newCoordinatesToOldCoordinates = newCoordinatesToOldCoordinates;
            this.dim = dim;
            this.policy = policy;
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
            	policy.associate( c, this.dim, oldSpaceInt, associations );
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
    
    public static interface AssociationPolicy extends Serializable {
    	public void associate(
				final Tuple2<Tuple2<Integer, Integer>, Tuple2<Double, Double>> newCoordinatesToOldCoordinates,
				final int[] dim,
				final Tuple2<Integer, Integer> oldSpaceInt,
				final ArrayList<Tuple2<Tuple2<Integer, Integer>, Double>> associations
				);
    }

    public static class NearestNeighbor implements AssociationPolicy {


        
		/**
		 * 
		 */
		private static final long serialVersionUID = -5068962347061980225L;

		public void associate(
				final Tuple2<Tuple2<Integer, Integer>, Tuple2<Double, Double>> newCoordinatesToOldCoordinates,
				final int[] dim,
				final Tuple2<Integer, Integer> oldSpaceInt,
				final ArrayList<Tuple2<Tuple2<Integer, Integer>, Double>> associations
				)
		{
			Tuple2<Double, Double> oldSpaceDouble = newCoordinatesToOldCoordinates._2();
            Tuple2<Long, Long> oldCoordinatesRound = Utility.tuple2(
                    Math.min(Math.max(Math.round(oldSpaceDouble._1()), 0), dim[0] - 1),
                    Math.min(Math.max(Math.round(oldSpaceDouble._2()), 0), dim[1] - 1)
            );
            if (    oldCoordinatesRound._1().intValue() == oldSpaceInt._1().intValue() &&
                    oldCoordinatesRound._2().intValue() == oldSpaceInt._2().intValue() ) {
                associations.add(Utility.tuple2(newCoordinatesToOldCoordinates._1(), 1.0 ));
            }
		}
        

        
    }

    public static class SwapKey implements PairFunction<
            Tuple2<Tuple2<Tuple2<Integer, Integer>, double[]>, Tuple2<Tuple2<Integer, Integer>, Double> >,
            Tuple2<Integer, Integer>, Tuple2<double[], Double>> {
        /**
		 * 
		 */
		private static final long serialVersionUID = 5948366161924347320L;

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
        /**
		 * 
		 */
		private static final long serialVersionUID = -2321684262329844915L;

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
        /**
		 * 
		 */
		private static final long serialVersionUID = 302989376509175864L;

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
        /**
		 * 
		 */
		private static final long serialVersionUID = 2199139375758027071L;

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


}
