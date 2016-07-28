package org.janelia.thickness;

import java.io.Serializable;
import java.util.ArrayList;
import java.util.Iterator;
import java.util.List;

import org.apache.spark.api.java.JavaPairRDD;
import org.apache.spark.api.java.JavaSparkContext;
import org.apache.spark.api.java.function.Function2;
import org.apache.spark.api.java.function.PairFlatMapFunction;
import org.apache.spark.api.java.function.PairFunction;
import org.apache.spark.broadcast.Broadcast;
import org.janelia.thickness.utility.Utility;

import scala.Tuple2;

/**
 * 
 * @author Philipp Hanslovsky &lt;hanslovskyp@janelia.hhmi.org&gt;
 *
 */
public class SparkInterpolation {

    public static JavaPairRDD< Tuple2< Integer, Integer >, double[] > interpolate(
            final JavaSparkContext sc,
            final JavaPairRDD< Tuple2< Integer, Integer >, double[] > source,
            final Broadcast<? extends List<Tuple2<Tuple2<Integer,Integer>,Tuple2<Double,Double>>>> newCoordinatesToOldCoordinates,
            int[] dim,
            AssociationPolicy association )
    {
        JavaPairRDD<Tuple2<Tuple2<Integer, Integer>, double[]>, Tuple2<Tuple2<Integer, Integer>, Double>> coordinateMatches =
                source.flatMapToPair(new MatchCoordinates(newCoordinatesToOldCoordinates, dim, association));
        JavaPairRDD<Tuple2<Integer, Integer>, Tuple2<double[], Double>> mapNewToOld = coordinateMatches.mapToPair(new SwapKey<>());
        JavaPairRDD<Tuple2<Integer, Integer>, Tuple2<double[], Double>> weightedArrays = mapNewToOld.mapToPair(new WeightedArrays<>());
        JavaPairRDD<Tuple2<Integer, Integer>, Tuple2<double[], Double>> reducedArrays = weightedArrays.reduceByKey(new ReduceArrays());
        JavaPairRDD<Tuple2<Integer, Integer>, double[]> result = reducedArrays.mapToPair(new NormalizeBySumOfWeights<>());

        return result;
    }
    
    
    /**
     * 
     * Match coordinates in new coordinate space to coordinates in old coordinate space and associate weights, e.g.
     * {0,1} for nearest neighbor interpolation or [0,1] for bilinear interpolation.
     * 
     * 
     * @author Philipp Hanslovsky &lt;hanslovskyp@janelia.hhmi.org&gt;
     *
     */
    public static class MatchCoordinates implements PairFlatMapFunction<
    	Tuple2<Tuple2<Integer, Integer>, double[]>,
    	Tuple2<Tuple2<Integer, Integer>, double[]>,
    	Tuple2<Tuple2<Integer, Integer>, Double>
    >
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
    
    
    public static class BiLinear implements AssociationPolicy
    {

        /**
		 * 
		 */
		private static final long serialVersionUID = -1211730761623302862L;

		@Override
        public void associate(
				final Tuple2<Tuple2<Integer, Integer>, Tuple2<Double, Double>> newCoordinatesToOldCoordinates,
				final int[] dim,
				final Tuple2<Integer, Integer> oldSpaceInt,
				final ArrayList<Tuple2<Tuple2<Integer, Integer>, Double>> associations
				) {

        	final Tuple2<Double, Double> newPointInOldCoordinates = newCoordinatesToOldCoordinates._2();
            double diff1 = Math.abs(newPointInOldCoordinates._1() - oldSpaceInt._1().doubleValue());
            double diff2 = Math.abs(newPointInOldCoordinates._2() - oldSpaceInt._2().doubleValue());
            if (diff1 < 1.0 && diff2 < 1.0)
            	associations.add(Utility.tuple2(newCoordinatesToOldCoordinates._1(), (1.0 - diff1) * (1.0 - diff2)));

        }
    }

    /**
     * 
     * make key of second key-value pare key for pair of both values V1,V2.
     * 
     * @author Philipp Hanslovsky &lt;hanslovskyp@janelia.hhmi.org&gt;
     *
     */
    public static class SwapKey< K1, K2, V1, V2 > implements PairFunction<
            Tuple2<Tuple2<K1, V1>, Tuple2<K2, V2> >,
            K2, Tuple2<V1, V2>> {
        /**
		 * 
		 */
		private static final long serialVersionUID = 5948366161924347320L;

		@Override
        public Tuple2<K2, Tuple2<V1, V2>>
        call(Tuple2<Tuple2<K1, V1>, Tuple2<K2, V2>> t) throws Exception {
            K2 newCoord = t._2()._1();
            return Utility.tuple2(newCoord, Utility.tuple2(t._1()._2(), t._2()._2()));
        }
    }

    /**
     * 
     * Multiply arrays with weight.
     * 
     * @author Philipp Hanslovsky &lt;hanslovskyp@janelia.hhmi.org&gt;
     *
     * @param <K> key
     */
    public static class WeightedArrays<K> implements PairFunction<
            Tuple2<K, Tuple2<double[], Double>>,
            K, Tuple2<double[], Double>> {
        /**
		 * 
		 */
		private static final long serialVersionUID = -2321684262329844915L;

		@Override
        public Tuple2<K, Tuple2<double[], Double>> call(Tuple2<K, Tuple2<double[], Double>> t) throws Exception {
            Tuple2<double[], Double> arrWithWeight = t._2();
            double[] arr = arrWithWeight._1().clone(); // TODO clone here? YES! otherwise garbage output
            double weight = arrWithWeight._2();
            for (int i = 0; i < arr.length; ++i) {
                arr[i] *= weight;
            }
            return Utility.tuple2(t._1(), Utility.tuple2(arr, weight));
        }
    }

    /**
     * 
     * Sum up arrays and weights.
     * 
     * @author Philipp Hanslovsky &lt;hanslovskyp@janelia.hhmi.org&gt;
     *
     */
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

    /**
     * 
     * Divide summed arrays by weight sum.
     * 
     * @author Philipp Hanslovsky &lt;hanslovskyp@janelia.hhmi.org&gt;
     *
     * @param <K> key
     */
    public static class NormalizeBySumOfWeights<K> implements PairFunction<Tuple2<K, Tuple2<double[], Double>>, K, double[]> {
        /**
		 * 
		 */
		private static final long serialVersionUID = 2199139375758027071L;

		@Override
        public Tuple2<K, double[]> call(Tuple2<K, Tuple2<double[], Double>> t) throws Exception {
            double[] arr = t._2()._1();
            double weight = t._2()._2();
            for (int i = 0; i < arr.length; ++i)
                arr[i] /= weight;
            return Utility.tuple2(t._1(), arr);
        }
    }


}
