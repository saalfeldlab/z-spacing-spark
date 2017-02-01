package org.janelia.thickness.weight;

import java.util.Arrays;
import java.util.HashMap;
import java.util.Iterator;
import java.util.Map.Entry;
import java.util.stream.IntStream;

import org.apache.spark.api.java.JavaPairRDD;

import ij.process.FloatProcessor;
import net.imglib2.util.RealSum;
import scala.Tuple2;

public class AllWeightsCalculator implements WeightsCalculator
{

	private final JavaPairRDD< Long, Tuple2< FloatProcessor, FloatProcessor > > bothWeights;

	private final int[] dims;

	public AllWeightsCalculator(
			final JavaPairRDD< Long, Tuple2< FloatProcessor, FloatProcessor > > shiftWeights,
			final int... dims )
	{
		super();
		this.bothWeights = shiftWeights;
		this.dims = dims;
	}

	@Override
	public JavaPairRDD< Tuple2< Long, Long >, Weights > calculate(
			final int[] radius,
			final int[] step )
	{

		final long nSections = bothWeights.count();

		final JavaPairRDD< Long, HashMap< Tuple2< Long, Long >, Tuple2< Double, Double > > > weightsBySection = bothWeights
				.mapToPair( t -> {

					final int[] o = radius.clone();

					final int[] indices = IntStream.range( 0, o.length ).toArray();

					final HashMap< Tuple2< Long, Long >, Tuple2< Double, Double > > weights = new HashMap<>();

					final FloatProcessor fpEstimate = t._2()._1();
					final FloatProcessor fpShift = t._2()._2();

					for ( int d = 0; d < o.length; )
					{

						final int[] min = Arrays.stream( indices ).map( i -> Math.max( o[ i ] - radius[ i ], 0 ) ).toArray();
						final int[] max = Arrays.stream( indices ).map( i -> Math.min( o[ i ] + radius[ i ], dims[ i ] ) ).toArray();
						final long[] localIndices = Arrays.stream( indices ).mapToLong( i -> ( o[ i ] - radius[ i ] ) / step[ i ] ).toArray();
						final int size = Arrays.stream( indices ).map( i -> max[ i ] - min[ i ] ).reduce( 1, ( v1, v2 ) -> v1 * v2 );

						final RealSum sumEstimate = new RealSum();
						final RealSum sumShift = new RealSum();

						for ( int y = min[1]; y < max[1]; ++y )
							for ( int x = min[ 0 ]; x < max[ 0 ]; ++x )
							{
								sumEstimate.add( fpEstimate.getf( x, y ) );
								sumShift.add( fpShift.getf( x, y ) );
							}
						weights.put(
								new Tuple2<>( localIndices[ 0 ], localIndices[ 1 ] ),
								new Tuple2<>( sumEstimate.getSum() / size, sumShift.getSum() / size ) );

						for ( d = 0; d < o.length; ++d )
						{
							o[ d ] += step[ d ];
							if ( o[ d ] < dims[ d ] )
								break;
							else
								o[ d ] = radius[ d ];
						}
					}

					return new Tuple2<>( t._1(), weights );
				} );

		final JavaPairRDD< Tuple2< Long, Long >, Tuple2< Long, Tuple2< Double, Double > > > weights = weightsBySection
				.flatMapToPair( t -> {
					final Long c = t._1();
					final Iterator< Entry< Tuple2< Long, Long >, Tuple2< Double, Double > > > it = t._2().entrySet().iterator();
					return new Iterator< Tuple2< Tuple2< Long, Long >, Tuple2< Long, Tuple2< Double, Double > > > >()
					{

						@Override
						public boolean hasNext()
						{
							return it.hasNext();
						}

						@Override
						public Tuple2< Tuple2< Long, Long >, Tuple2< Long, Tuple2< Double, Double > > > next()
						{
							final Entry< Tuple2< Long, Long >, Tuple2< Double, Double > > e = it.next();
							return new Tuple2<>( e.getKey(), new Tuple2<>( c, e.getValue() ) );
						}

					};
				} );

		final JavaPairRDD< Tuple2< Long, Long >, Weights > combinedWeights = weights.aggregateByKey(
				new Weights(
						Arrays.stream( new double[ ( int ) nSections ] ).map( d -> Double.NaN ).toArray(),
						Arrays.stream( new double[ ( int ) nSections ] ).map( d -> Double.NaN ).toArray() ),
				( w, v ) -> {
					w.estimateWeights[ v._1().intValue() ] = v._2()._1();
					w.shiftWeights[ v._1().intValue() ] = v._2()._2();
					return w;
				},
				( w1, w2 ) -> {
					for ( int i = 0; i < w2.estimateWeights.length; ++i )
					{
						if ( !Double.isNaN( w2.estimateWeights[ i ] ) )
							w1.estimateWeights[ i ] = w2.estimateWeights[ i ];
						if ( !Double.isNaN( w2.shiftWeights[ i ] ) )
							w1.shiftWeights[ i ] = w2.shiftWeights[ i ];
					}
					return w1;
				} );

		return combinedWeights;
	}

}
