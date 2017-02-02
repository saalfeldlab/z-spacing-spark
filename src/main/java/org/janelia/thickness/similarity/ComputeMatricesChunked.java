package org.janelia.thickness.similarity;

import java.lang.invoke.MethodHandles;
import java.util.ArrayList;
import java.util.HashMap;

import org.apache.log4j.Level;
import org.apache.log4j.LogManager;
import org.apache.log4j.Logger;
import org.apache.spark.api.java.JavaPairRDD;
import org.apache.spark.api.java.JavaSparkContext;
import org.apache.spark.api.java.function.PairFunction;
import org.apache.spark.storage.StorageLevel;
import org.janelia.thickness.JoinFromList;
import org.janelia.thickness.utility.Utility;

import ij.process.FloatProcessor;
import scala.Tuple2;
import scala.Tuple5;

/**
 * @author Philipp Hanslovsky &lt;hanslovskyp@janelia.hhmi.org&gt;
 */
public class ComputeMatricesChunked
{

	public static Logger LOG = LogManager.getLogger( MethodHandles.lookup().lookupClass() );
	static
	{
		LOG.setLevel( Level.INFO );
	}

	private final JavaSparkContext sc;

	private final ArrayList< JavaPairRDD< Tuple2< Integer, Integer >, Tuple2< ImageAndMask, ImageAndMask > > > pairs;

	private final ArrayList< Tuple5< Integer, Integer, Integer, Integer, Integer > > bounds;

	private final int chunkStepSize;

	private final int maxRange;

	private final int[] dim;

	private final long nImages;

	public ComputeMatricesChunked(
			final JavaSparkContext sc,
			final JavaPairRDD< Integer, ImageAndMask > files,
			final int stepSize,
			final int maxRange,
			final int[] dim,
			final long nImages,
			final StorageLevel pairPersistenceLevel )
	{
		this.sc = sc;
		this.chunkStepSize = stepSize;
		this.maxRange = maxRange;

		this.pairs = new ArrayList<>();
		this.bounds = new ArrayList<>();
		this.dim = dim;
		this.nImages = nImages;

		final Logger log = LOG;// LogManager.getRootLogger();

		final int stop = ( int ) nImages;
		for ( int z = 0; z < stop; z += this.chunkStepSize )
		{
			final int lower = Math.max( z - this.maxRange, 0 );
			final int upper = Math.min( z + this.maxRange + stepSize, stop );
			final int size = upper - lower;
			final JavaPairRDD< Integer, ImageAndMask > rdd = files.filter( new Utility.FilterRange<>( lower, upper ) );
			final HashMap< Integer, ArrayList< Integer > > keyPairList = new HashMap< >();
			for ( int i = lower; i < upper; ++i )
			{
				final ArrayList< Integer > al = new ArrayList< >();
				for ( int k = i + 1; k < upper && k - i <= maxRange; ++k )
					al.add( k );
				keyPairList.put( i, al );
			}
			final JavaPairRDD< Tuple2< Integer, Integer >, Tuple2< ImageAndMask, ImageAndMask > > pairs =
					JoinFromList.projectOntoSelf( rdd, sc.broadcast( keyPairList ) ).persist( pairPersistenceLevel );
			this.pairs.add( pairs );
			this.bounds.add( Utility.tuple5( z, Math.min( z + stepSize, stop ), z - lower, lower, size ) );
		}
		log.info( MethodHandles.lookup().lookupClass().getSimpleName() + ": Join stepsize: " + stepSize );
		log.info( MethodHandles.lookup().lookupClass().getSimpleName() + ": Number of chunks: " + this.pairs.size() );
	}

	public JavaPairRDD< Tuple2< Integer, Integer >, FloatProcessor > run(
			final MatrixGenerator.Factory factory,
			final int range,
			final int[] stepSize,
			final int[] blockRadius )
	{

		final ArrayList< JavaPairRDD< Tuple2< Integer, Integer >, FloatProcessor > > rdds = new ArrayList<>();
		final int maxIndex = ( int ) ( this.nImages - 1 );
		final int stop = maxIndex + 1;
		for ( int i = 0; i < this.pairs.size(); ++i )
		{
			final JavaPairRDD< Tuple2< Integer, Integer >, Tuple2< ImageAndMask, ImageAndMask > > pair = this.pairs.get( i );
			final MatrixGenerator matrixGenerator = factory.create();
			final Tuple5< Integer, Integer, Integer, Integer, Integer > bound = this.bounds.get( i );
			final JavaPairRDD< Tuple2< Integer, Integer >, FloatProcessor > matrices =
					matrixGenerator.generateMatrices( sc, pair, blockRadius, stepSize, range, bound._4(), bound._5() );
			final JavaPairRDD< Tuple2< Integer, Integer >, FloatProcessor > strip =
					matrices.mapToPair( new MatrixToStrip< Tuple2< Integer, Integer > >( bound._3(), Math.min( chunkStepSize, stop - bound._1() ), range ) );
			rdds.add( strip );
		}

		JavaPairRDD< Tuple2< Integer, Integer >, FloatProcessor > result = rdds.get( 0 )
				.mapToPair( new PutIntoGlobalContext< Tuple2< Integer, Integer > >( stop, bounds.get( 0 )._1(), bounds.get( 0 )._2() ) );

		for ( int i = 1; i < rdds.size(); ++i )
		{
			final JavaPairRDD< Tuple2< Integer, Integer >, FloatProcessor > rdd = rdds.get( i );
			final Tuple5< Integer, Integer, Integer, Integer, Integer > bound = bounds.get( i );

			final int offset = bound._1();

			result = result.join( rdd ).mapToPair( new JoinStrips< Tuple2< Integer, Integer > >( offset ) );
		}

		return result;

	}

	/**
	 *
	 * Convert matrix to strip.
	 *
	 * @author Philipp Hanslovsky &lt;hanslovskyp@janelia.hhmi.org&gt;
	 *
	 * @param <K>
	 *            key
	 */
	public static class MatrixToStrip< K > implements PairFunction< Tuple2< K, FloatProcessor >, K, FloatProcessor >
	{
		/**
		 *
		 */
		private static final long serialVersionUID = -6780567502737253053L;

		private final int offset;

		private final int size;

		private final int range;

		public MatrixToStrip( final int offset, final int size, final int range )
		{
			this.offset = offset;
			this.size = size;
			this.range = range;
		}

		@Override
		public Tuple2< K, FloatProcessor > call( final Tuple2< K, FloatProcessor > t ) throws Exception
		{

			final FloatProcessor matrix = t._2();
			final FloatProcessor strip = new FloatProcessor( 2 * range + 1, this.size );
			final int w = matrix.getWidth();

			for ( int z = offset, stripZ = 0; stripZ < size; ++z, ++stripZ )
				for ( int r = -range; r <= range; ++r )
				{
					final int k = r + z;
					if ( k < 0 || k >= w )
						continue;
					strip.setf( range + r, stripZ, matrix.getf( z, k ) );
				}

			return Utility.tuple2( t._1(), strip );
		}
	}

	/**
	 *
	 * Create complete empty strip and add chunk at position lower
	 *
	 * @author Philipp Hanslovsky &lt;hanslovskyp@janelia.hhmi.org&gt;
	 *
	 * @param <K>
	 *            key
	 */
	public static class PutIntoGlobalContext< K > implements PairFunction< Tuple2< K, FloatProcessor >, K, FloatProcessor >
	{

		/**
		 *
		 */
		private static final long serialVersionUID = -126790284079446439L;

		private final int size;

		private final int lower;

		private final int upper;

		public PutIntoGlobalContext( final int size, final int lower, final int upper )
		{
			this.size = size;
			this.lower = lower;
			this.upper = upper;
		}

		@Override
		public Tuple2< K, FloatProcessor > call( final Tuple2< K, FloatProcessor > t ) throws Exception
		{
			final FloatProcessor source = t._2();
			final int w = source.getWidth();
			final FloatProcessor result = new FloatProcessor( w, this.size );
			result.add( Double.NaN );
			for ( int y = this.lower, sourceY = 0; y < this.upper; ++y, ++sourceY )
				for ( int x = 0; x < w; ++x )
					result.setf( x, y, source.getf( x, sourceY ) );
			return Utility.tuple2( t._1(), result );
		}
	}

	/**
	 *
	 * Insert chunk into complete strip at position specified by offset
	 *
	 * @author Philipp Hanslovsky &lt;hanslovskyp@janelia.hhmi.org&gt;
	 *
	 * @param <K>
	 *            key
	 */
	public static class JoinStrips< K >
	implements PairFunction< Tuple2< K, Tuple2< FloatProcessor, FloatProcessor > >, K, FloatProcessor >
	{

		/**
		 *
		 */
		private static final long serialVersionUID = -7559889575641624904L;

		private final int offset;

		public JoinStrips( final int offset )
		{
			this.offset = offset;
		}

		@Override
		public Tuple2< K, FloatProcessor > call( final Tuple2< K, Tuple2< FloatProcessor, FloatProcessor > > t ) throws Exception
		{
			final Tuple2< FloatProcessor, FloatProcessor > fps = t._2();
			final FloatProcessor source = fps._2();
			final FloatProcessor target = fps._1();
			final int sourceH = source.getHeight();
			final int sourceW = source.getWidth();
			for ( int sourceY = 0, y = offset; sourceY < sourceH; ++sourceY, ++y )
				for ( int x = 0; x < sourceW; ++x )
					target.setf( x, y, source.getf( x, sourceY ) );

			return Utility.tuple2( t._1(), target );
		}
	}

}
