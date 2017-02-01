package org.janelia.thickness.weight;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.stream.IntStream;

import org.apache.spark.api.java.JavaPairRDD;
import org.apache.spark.api.java.JavaSparkContext;

import scala.Tuple2;

public class NoWeightsCalculator implements WeightsCalculator
{

	private final JavaSparkContext sc;

	private final long nSections;

	private final int[] dims;

	public NoWeightsCalculator( final JavaSparkContext sc, final long nSections, final int... dims )
	{
		super();
		this.sc = sc;
		this.nSections = nSections;
		this.dims = dims;
	}

	@Override
	public JavaPairRDD< Tuple2< Long, Long >, Weights > calculate(
			final int[] radius,
			final int[] step )
	{


		final int[] o = radius.clone();

		final int[] indices = IntStream.range( 0, o.length ).toArray();

		final ArrayList< Tuple2< Long, Long > > weightsLocations = new ArrayList<>();


		for ( int d = 0; d < o.length; )
		{

			final long[] localIndices = Arrays.stream( indices ).mapToLong( i -> ( o[ i ] - radius[ i ] ) / step[ i ] ).toArray();

			weightsLocations.add( new Tuple2<>( localIndices[ 0 ], localIndices[ 1 ] ) );

			for ( d = 0; d < o.length; ++d )
			{
				o[ d ] += step[ d ];
				if ( o[ d ] < dims[ d ] )
					break;
				else
					o[ d ] = radius[ d ];
			}
		}

		final long nSections = this.nSections;

		return sc
				.parallelize( weightsLocations )
				.mapToPair( l -> {
					final double[] w = Arrays.stream( new double[ ( int ) nSections ] ).map( d -> 1.0 ).toArray();
					return new Tuple2<>( l, new Weights( w, w ) );
				} );

	}

}
