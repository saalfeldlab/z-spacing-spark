package org.janelia.thickness.similarity;

import java.util.ArrayList;
import java.util.Arrays;
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
import org.janelia.thickness.BlockCoordinates;
import org.janelia.thickness.utility.Utility;

import ij.process.FloatProcessor;
import net.imglib2.FinalInterval;
import net.imglib2.RandomAccessible;
import net.imglib2.img.array.ArrayImg;
import net.imglib2.img.array.ArrayImgs;
import net.imglib2.img.basictypeaccess.array.FloatArray;
import net.imglib2.type.numeric.real.FloatType;
import net.imglib2.util.Pair;
import net.imglib2.view.Views;
import scala.Tuple2;

/**
 * @author Philipp Hanslovsky &lt;hanslovskyp@janelia.hhmi.org&gt;
 */
public class DefaultMatrixGenerator implements MatrixGenerator
{

	public static class Factory implements MatrixGenerator.Factory
	{

		private final int[] dim;

		public Factory( final int[] dim )
		{
			super();
			this.dim = dim;
		}

		@Override
		public MatrixGenerator create()
		{
			return new DefaultMatrixGenerator( dim );
		}

	}

	private final int[] dim;

	public DefaultMatrixGenerator( final int[] dim )
	{
		super();
		this.dim = dim;
	}

	@Override
	public JavaPairRDD< Tuple2< Integer, Integer >, Tuple2< FloatProcessor, FloatProcessor > > generateMatrices(
			final JavaSparkContext sc,
			final JavaPairRDD< Tuple2< Integer, Integer >, Tuple2< ImageAndMask, ImageAndMask > > sectionPairs,
			final int[] blockRadius,
			final int[] stepSize,
			final int range,
			final int startIndex,
			final int size )
	{

		final JavaPairRDD< Tuple2< Integer, Integer >, Tuple2< ImageAndMask, ImageAndMask > > pairsWithinRange =
				sectionPairs.filter( new SelectInRange<>( range ) );

		final BlockCoordinates correlationBlocks = new BlockCoordinates( blockRadius, stepSize );

		final Broadcast< ArrayList< BlockCoordinates.Coordinate > > coordinates = sc.broadcast( correlationBlocks.generateFromBoundingBox( dim ) );

		final JavaPairRDD< Tuple2< Integer, Integer >, HashMap< Tuple2< Integer, Integer >, CorrelationAndWeight > > pairwiseCorrelations = pairsWithinRange
				.mapToPair( new SubSectionCorrelations( coordinates, dim ) );

		final JavaPairRDD< Tuple2< Integer, Integer >, Tuple2< FloatProcessor, FloatProcessor > > matrices = pairwiseCorrelations
				.flatMapToPair( new ExchangeIndexOrder() )
				.reduceByKey( new ReduceMaps() )
				.mapToPair( new MapToFloatProcessor( size, startIndex ) );

		return matrices;
	}

	public static class SelectInRange< V > implements Function< Tuple2< Tuple2< Integer, Integer >, V >, Boolean >
	{

		/**
		 *
		 */
		private static final long serialVersionUID = 4484476583576256519L;

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

	public static class SubSectionCorrelations implements PairFunction< Tuple2< Tuple2< Integer, Integer >, Tuple2< ImageAndMask, ImageAndMask > >, Tuple2< Integer, Integer >, HashMap< Tuple2< Integer, Integer >, CorrelationAndWeight > >
	{

		/**
		 *
		 */
		private static final long serialVersionUID = 4914446108059613538L;

		private final Broadcast< ArrayList< BlockCoordinates.Coordinate > > coordinates;

		private final int[] dim;

		public SubSectionCorrelations( final Broadcast< ArrayList< BlockCoordinates.Coordinate > > coordinates, final int[] dim )
		{
			this.coordinates = coordinates;
			this.dim = dim;
		}

		@Override
		public Tuple2< Tuple2< Integer, Integer >, HashMap< Tuple2< Integer, Integer >, CorrelationAndWeight > >
		call( final Tuple2< Tuple2< Integer, Integer >, Tuple2< ImageAndMask, ImageAndMask > > t ) throws Exception
		{
			final ImageAndMask fp1 = t._2()._1();
			final ImageAndMask fp2 = t._2()._2();

			final int w = fp1.image.getWidth();
			final int h = fp1.image.getHeight();

			final int[] min = new int[] { 0, 0 };
			final int[] currentStart = new int[ 2 ];
			final int[] currentStop = new int[ 2 ];
			final HashMap< Tuple2< Integer, Integer >, CorrelationAndWeight > result = new HashMap<>();

			final ArrayImg< FloatType, FloatArray > i1 = ArrayImgs.floats( ( float[] ) fp1.image.getPixels(), w, h );
			final ArrayImg< FloatType, FloatArray > i2 = ArrayImgs.floats( ( float[] ) fp2.image.getPixels(), w, h );
			final ArrayImg< FloatType, FloatArray > w1 = ArrayImgs.floats( ( float[] ) fp1.mask.getPixels(), w, h );
			final ArrayImg< FloatType, FloatArray > w2 = ArrayImgs.floats( ( float[] ) fp2.mask.getPixels(), w, h );

			final RandomAccessible< Pair< FloatType, FloatType > > p1 = Views.pair( i1, w1 );
			final RandomAccessible< Pair< FloatType, FloatType > > p2 = Views.pair( i2, w2 );

			for ( final BlockCoordinates.Coordinate coord : coordinates.getValue() )
			{
				final Tuple2< Integer, Integer > local = coord.getLocalCoordinates();
				final Tuple2< Integer, Integer > global = coord.getWorldCoordinates();
				final Tuple2< Integer, Integer > radius = coord.getRadius();
				currentStart[ 0 ] = Math.max( min[ 0 ], global._1() - radius._1() );
				currentStart[ 1 ] = Math.max( min[ 1 ], global._2() - radius._2() );
				currentStop[ 0 ] = Math.min( dim[ 0 ], global._1() + radius._1() );
				currentStop[ 1 ] = Math.min( dim[ 1 ], global._2() + radius._2() );
				final long[] currentMax = Arrays.stream( currentStop ).mapToLong( v -> v - 1 ).toArray();

				final FinalInterval interval = new FinalInterval( Arrays.stream( currentStart ).mapToLong( i -> i ).toArray(), currentMax );

				final CorrelationAndWeight correlation = CorrelationsImgLib.calculate( p1, p2, interval );

				result.put( local, correlation );
			}
			return Utility.tuple2( t._1(), result );
		}
	}

	public static class ExchangeIndexOrder implements PairFlatMapFunction< Tuple2< Tuple2< Integer, Integer >, HashMap< Tuple2< Integer, Integer >, CorrelationAndWeight > >, Tuple2< Integer, Integer >, HashMap< Tuple2< Integer, Integer >, CorrelationAndWeight > >
	{
		/**
		 *
		 */
		private static final long serialVersionUID = -6042060930096104068L;

		@Override
		public Iterator< Tuple2< Tuple2< Integer, Integer >, HashMap< Tuple2< Integer, Integer >, CorrelationAndWeight > > > call( final Tuple2< Tuple2< Integer, Integer >, HashMap< Tuple2< Integer, Integer >, CorrelationAndWeight > > t ) throws Exception
		{
			// z coordinate of sections
			final Tuple2< Integer, Integer > zz = t._1();
			final HashMap< Tuple2< Integer, Integer >, CorrelationAndWeight > corrs = t._2();

			return new Iterator< Tuple2< Tuple2< Integer, Integer >, HashMap< Tuple2< Integer, Integer >, CorrelationAndWeight > > >()
			{
				Iterator< Map.Entry< Tuple2< Integer, Integer >, CorrelationAndWeight > > it = corrs.entrySet().iterator();

				@Override
				public boolean hasNext()
				{
					return it.hasNext();
				}

				@Override
				public Tuple2< Tuple2< Integer, Integer >, HashMap< Tuple2< Integer, Integer >, CorrelationAndWeight > > next()
				{
					final Map.Entry< Tuple2< Integer, Integer >, CorrelationAndWeight > nextCorr = it.next();
					final Tuple2< Integer, Integer > xy = nextCorr.getKey();
					final HashMap< Tuple2< Integer, Integer >, CorrelationAndWeight > result = new HashMap<>();
					result.put( zz, nextCorr.getValue() );
					return Utility.tuple2( xy, result );
				}

			};
		}
	}

	public static class ReduceMaps implements Function2< HashMap< Tuple2< Integer, Integer >, CorrelationAndWeight >, HashMap< Tuple2< Integer, Integer >, CorrelationAndWeight >, HashMap< Tuple2< Integer, Integer >, CorrelationAndWeight > >
	{
		/**
		 *
		 */
		private static final long serialVersionUID = 7642037831468553986L;

		@Override
		public HashMap< Tuple2< Integer, Integer >, CorrelationAndWeight > call( final HashMap< Tuple2< Integer, Integer >, CorrelationAndWeight > hm1, final HashMap< Tuple2< Integer, Integer >, CorrelationAndWeight > hm2 ) throws Exception
		{
			hm1.putAll( hm2 );
			return hm1;
		}
	}

	public static class MapToFloatProcessor implements PairFunction< Tuple2< Tuple2< Integer, Integer >, HashMap< Tuple2< Integer, Integer >, CorrelationAndWeight > >, Tuple2< Integer, Integer >, Tuple2< FloatProcessor, FloatProcessor > >
	{

		/**
		 *
		 */
		private static final long serialVersionUID = 6293715394528119288L;

		private final int size;

		private final int startIndex;

		public MapToFloatProcessor( final int size, final int startIndex )
		{
			this.size = size;
			this.startIndex = startIndex;
		}

		@Override
		public Tuple2< Tuple2< Integer, Integer >, Tuple2< FloatProcessor, FloatProcessor > > call( final Tuple2< Tuple2< Integer, Integer >, HashMap< Tuple2< Integer, Integer >, CorrelationAndWeight > > t ) throws Exception
		{
			final FloatProcessor result = new FloatProcessor( size, size );
			final FloatProcessor weight = new FloatProcessor( size, size );
			result.add( Double.NaN );
			for ( int z = 0; z < size; ++z )
			{
				result.setf( z, z, 1.0f );
				weight.setf( z, z, 1.0f );
			}
			for ( final Map.Entry< Tuple2< Integer, Integer >, CorrelationAndWeight > entry : t._2().entrySet() )
			{
				final Tuple2< Integer, Integer > xy = entry.getKey();
				final int x = xy._1() - startIndex;
				final int y = xy._2() - startIndex;
				final float val = ( float ) entry.getValue().corr;
				final float w = ( float ) entry.getValue().weight;
				result.setf( x, y, val );
				result.setf( y, x, val );
				weight.setf( x, y, w );
				weight.setf( y, x, w );
			}
			return Utility.tuple2( t._1(), new Tuple2<>( result, weight ) );
		}
	}
}
