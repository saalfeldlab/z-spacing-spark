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

public class OnlyEstimateWeightsCalculator implements WeightsCalculator
{

	private final JavaPairRDD< Long, FloatProcessor > estimateWeights;

	private final int[] dims;

	public OnlyEstimateWeightsCalculator( final JavaPairRDD< Long, FloatProcessor > estimateWeights, final int... dims )
	{
		super();
		this.estimateWeights = estimateWeights;
		this.dims = dims;
	}

	@Override
	public JavaPairRDD< Tuple2< Long, Long >, Weights > calculate(
			final int[] radius,
			final int[] step )
	{

		final long nSections = estimateWeights.count();

		final JavaPairRDD< Long, HashMap< Tuple2< Long, Long >, Double > > weightsBySection = estimateWeights
				.mapToPair( t -> {

					final int[] o = radius.clone();

					final int[] indices = IntStream.range( 0, o.length ).toArray();

					final HashMap< Tuple2< Long, Long >, Double > weights = new HashMap<>();

					final FloatProcessor fp = t._2();

					for ( int d = 0; d < o.length; )
					{

						final int[] min = Arrays.stream( indices ).map( i -> Math.max( o[ i ] - radius[ i ], 0 ) ).toArray();
						final int[] max = Arrays.stream( indices ).map( i -> Math.min( o[ i ] + radius[ i ], dims[ i ] ) ).toArray();
						final long[] localIndices = Arrays.stream( indices ).mapToLong( i -> ( o[ i ] - radius[ i ] ) / step[ i ] ).toArray();
						final int size = Arrays.stream( indices ).map( i -> max[ i ] - min[ i ] ).reduce( 1, ( v1, v2 ) -> v1 * v2 );

						final RealSum sum = new RealSum();
						for ( int y = min[1]; y < max[1]; ++y )
							for ( int x = min[0]; x < max[0]; ++x )
								sum.add( fp.getf( x, y ) );
						weights.put( new Tuple2<>( localIndices[ 0 ], localIndices[ 1 ] ), sum.getSum() / size );

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

		final JavaPairRDD< Tuple2< Long, Long >, Tuple2< Long, Double > > weights = weightsBySection
				.flatMapToPair( t -> {
					final Long c = t._1();
					final Iterator< Entry< Tuple2< Long, Long >, Double > > it = t._2().entrySet().iterator();
					return new Iterator< Tuple2< Tuple2< Long, Long >, Tuple2< Long, Double > > >()
					{

						@Override
						public boolean hasNext()
						{
							return it.hasNext();
						}

						@Override
						public Tuple2< Tuple2< Long, Long >, Tuple2< Long, Double > > next()
						{
							final Entry< Tuple2< Long, Long >, Double > e = it.next();
							return new Tuple2<>( e.getKey(), new Tuple2<>( c, e.getValue() ) );
						}

					};
				} );

		final JavaPairRDD< Tuple2< Long, Long >, double[] > combinedWeights = weights.aggregateByKey(
				Arrays.stream( new double[ ( int ) nSections ] ).map( d -> Double.NaN ).toArray(),
				( arr, v ) -> {
					arr[ v._1().intValue() ] = v._2();
					return arr;
				},
				( a1, a2 ) -> {
					for ( int i = 0; i < a2.length; ++i )
						if ( !Double.isNaN( a2[ i ] ) )
							a1[ i ] = a2[ i ];
					return a1;
				} );

		return combinedWeights.mapToPair( t -> {
			return new Tuple2<>( t._1(), new Weights(
					Arrays.stream( new double[ t._2().length ] ).map( d -> 1.0 ).toArray(),
					t._2() ) );
		});
	}

}
