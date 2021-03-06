package org.janelia.thickness;

import java.util.ArrayList;
import java.util.HashMap;
import java.util.Iterator;
import java.util.Map;

import org.apache.spark.api.java.JavaPairRDD;
import org.apache.spark.api.java.JavaSparkContext;
import org.apache.spark.api.java.function.Function;
import org.apache.spark.api.java.function.Function2;
import org.apache.spark.api.java.function.PairFlatMapFunction;
import org.apache.spark.api.java.function.PairFunction;
import org.apache.spark.broadcast.Broadcast;
import org.janelia.thickness.utility.Utility;

import ij.process.FloatProcessor;
import scala.Tuple2;

/**
 * @author Philipp Hanslovsky
 */
public class MatrixGenerationFromImagePairs
{

	private final JavaSparkContext sc;

	// assume only one of (i,j),(j,i) is present
	private JavaPairRDD< Tuple2< Integer, Integer >, Tuple2< FloatProcessor, FloatProcessor > > sectionPairs;

	private final int[] dim;

	private final int size;

	public MatrixGenerationFromImagePairs( final JavaSparkContext sc, final JavaPairRDD< Tuple2< Integer, Integer >, Tuple2< FloatProcessor, FloatProcessor > > sectionPairs, final int[] dim, final int size )
	{
		this.sc = sc;
		this.sectionPairs = sectionPairs;
		this.dim = dim;
		this.size = size;
	}

	public JavaPairRDD< Tuple2< Integer, Integer >, FloatProcessor > generateMatrices( final int[] stride, final int[] correlationBlockRadius, final int range )
	{

		final JavaPairRDD< Tuple2< Integer, Integer >, Tuple2< FloatProcessor, FloatProcessor > > pairsWithinRange = sectionPairs.filter( new SelectInRange< Tuple2< FloatProcessor, FloatProcessor > >( range ) );
		pairsWithinRange.persist( sectionPairs.getStorageLevel() );
		sectionPairs.unpersist();
		sectionPairs = pairsWithinRange;
		System.out.println( "Filtered pairs." );

		final CorrelationBlocks correlationBlocks = new CorrelationBlocks( correlationBlockRadius, stride );

		final Broadcast< ArrayList< CorrelationBlocks.Coordinate > > coordinates = sc.broadcast( correlationBlocks.generateFromBoundingBox( dim ) );

		final JavaPairRDD< Tuple2< Integer, Integer >, HashMap< Tuple2< Integer, Integer >, Double > > pairwiseCorrelations = pairsWithinRange.mapToPair( new SubSectionCorrelations( coordinates, dim ) );

		final JavaPairRDD< Tuple2< Integer, Integer >, FloatProcessor > matrices = pairwiseCorrelations.flatMapToPair( new ExchangeIndexOrder() ).reduceByKey( new ReduceMaps() ).mapToPair( new MapToFloatProcessor( size ) );

		return matrices;
	}

	public static class SelectInRange< V > implements Function< Tuple2< Tuple2< Integer, Integer >, V >, Boolean >
	{

		private final int range;

		public SelectInRange( final int range )
		{
			this.range = range;
		}

		@Override
		public Boolean call( final Tuple2< Tuple2< Integer, Integer >, V > t ) throws Exception
		{
			final Tuple2< Integer, Integer > indices = t._1();
			final int diff = indices._1().intValue() - indices._2().intValue();
			return Math.abs( diff ) <= range;
		}
	}

	public static class SubSectionCorrelations implements PairFunction< Tuple2< Tuple2< Integer, Integer >, Tuple2< FloatProcessor, FloatProcessor > >, Tuple2< Integer, Integer >, HashMap< Tuple2< Integer, Integer >, Double > >
	{

		private final Broadcast< ArrayList< CorrelationBlocks.Coordinate > > coordinates;

		private final int[] dim;

		public SubSectionCorrelations( final Broadcast< ArrayList< CorrelationBlocks.Coordinate > > coordinates, final int[] dim )
		{
			this.coordinates = coordinates;
			this.dim = dim;
		}

		@Override
		public Tuple2< Tuple2< Integer, Integer >, HashMap< Tuple2< Integer, Integer >, Double > > call( final Tuple2< Tuple2< Integer, Integer >, Tuple2< FloatProcessor, FloatProcessor > > t ) throws Exception
		{
			final FloatProcessor fp1 = t._2()._1();
			final FloatProcessor fp2 = t._2()._2();
			final int[] min = new int[] { 0, 0 };
			final int[] currentStart = new int[ 2 ];
			final int[] currentStop = new int[ 2 ];
			final HashMap< Tuple2< Integer, Integer >, Double > result = new HashMap<>();
			for ( final CorrelationBlocks.Coordinate coord : coordinates.getValue() )
			{
				final Tuple2< Integer, Integer > local = coord.getLocalCoordinates();
				final Tuple2< Integer, Integer > global = coord.getWorldCoordinates();
				final Tuple2< Integer, Integer > radius = coord.getRadius();
				currentStart[ 0 ] = Math.max( min[ 0 ], global._1() - radius._1() );
				currentStart[ 1 ] = Math.max( min[ 1 ], global._2() - radius._2() );
				currentStop[ 0 ] = Math.min( dim[ 0 ], global._1() + radius._1() );
				currentStop[ 1 ] = Math.min( dim[ 1 ], global._2() + radius._2() );
				final int[] targetDim = new int[] { currentStop[ 0 ] - currentStart[ 0 ], currentStop[ 1 ] - currentStart[ 1 ] };
				final FloatProcessor target1 = new FloatProcessor( targetDim[ 0 ], targetDim[ 1 ] );
				final FloatProcessor target2 = new FloatProcessor( targetDim[ 0 ], targetDim[ 1 ] );
				for ( int ySource = currentStart[ 1 ], yTarget = 0; ySource < currentStop[ 1 ]; ++ySource, ++yTarget )
					for ( int xSource = currentStart[ 0 ], xTarget = 0; xSource < currentStop[ 0 ]; ++xSource, ++xTarget )
					{
						target1.setf( xTarget, yTarget, fp1.getf( xSource, ySource ) );
						target2.setf( xTarget, yTarget, fp2.getf( xSource, ySource ) );
					}
				final double correlation = Correlations.calculate( target1, target2 );
				result.put( local, correlation );
			}
			return Utility.tuple2( t._1(), result );
		}
	}

	public static class ExchangeIndexOrder implements PairFlatMapFunction< Tuple2< Tuple2< Integer, Integer >, HashMap< Tuple2< Integer, Integer >, Double > >, Tuple2< Integer, Integer >, HashMap< Tuple2< Integer, Integer >, Double > >
	{
		@Override
		public Iterator< Tuple2< Tuple2< Integer, Integer >, HashMap< Tuple2< Integer, Integer >, Double > > > call( final Tuple2< Tuple2< Integer, Integer >, HashMap< Tuple2< Integer, Integer >, Double > > t ) throws Exception
		{
			// z coordinate of sections
			final Tuple2< Integer, Integer > zz = t._1();
			final HashMap< Tuple2< Integer, Integer >, Double > corrs = t._2();

			final Iterable< Tuple2< Tuple2< Integer, Integer >, HashMap< Tuple2< Integer, Integer >, Double > > > it = () -> new Iterator< Tuple2< Tuple2< Integer, Integer >, HashMap< Tuple2< Integer, Integer >, Double > > >()
			{
				Iterator< Map.Entry< Tuple2< Integer, Integer >, Double > > it = corrs.entrySet().iterator();

				@Override
				public boolean hasNext()
				{
					return it.hasNext();
				}

				@Override
				public Tuple2< Tuple2< Integer, Integer >, HashMap< Tuple2< Integer, Integer >, Double > > next()
				{
					final Map.Entry< Tuple2< Integer, Integer >, Double > nextCorr = it.next();
					final Tuple2< Integer, Integer > xy = nextCorr.getKey();
					final HashMap< Tuple2< Integer, Integer >, Double > result = new HashMap<>();
					result.put( zz, nextCorr.getValue() );
					return Utility.tuple2( xy, result );
				}

				@Override
				public void remove()
				{
					throw new UnsupportedOperationException();
				}
			};
			return it.iterator();
		}
	}

	public static class ReduceMaps implements Function2< HashMap< Tuple2< Integer, Integer >, Double >, HashMap< Tuple2< Integer, Integer >, Double >, HashMap< Tuple2< Integer, Integer >, Double > >
	{
		@Override
		public HashMap< Tuple2< Integer, Integer >, Double > call( final HashMap< Tuple2< Integer, Integer >, Double > hm1, final HashMap< Tuple2< Integer, Integer >, Double > hm2 ) throws Exception
		{
			hm1.putAll( hm2 );
			return hm1;
		}
	}

	public static class MapToFloatProcessor implements PairFunction< Tuple2< Tuple2< Integer, Integer >, HashMap< Tuple2< Integer, Integer >, Double > >, Tuple2< Integer, Integer >, FloatProcessor >
	{

		private final int size;

		public MapToFloatProcessor( final int size )
		{
			this.size = size;
		}

		@Override
		public Tuple2< Tuple2< Integer, Integer >, FloatProcessor > call( final Tuple2< Tuple2< Integer, Integer >, HashMap< Tuple2< Integer, Integer >, Double > > t ) throws Exception
		{
			final FloatProcessor result = new FloatProcessor( size, size );
			result.add( Double.NaN );
			for ( int z = 0; z < size; ++z )
				result.setf( z, z, 1.0f );
			for ( final Map.Entry< Tuple2< Integer, Integer >, Double > entry : t._2().entrySet() )
			{
				final Tuple2< Integer, Integer > xy = entry.getKey();
				final int x = xy._1();
				final int y = xy._2();
				final float val = entry.getValue().floatValue();
				result.setf( x, y, val );
				result.setf( y, x, val );
			}
			return Utility.tuple2( t._1(), result );
		}
	}
}
