package org.janelia.thickness.similarity;

import java.lang.invoke.MethodHandles;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.Iterator;
import java.util.List;
import java.util.concurrent.ExecutionException;
import java.util.stream.Collectors;

import org.apache.log4j.Level;
import org.apache.log4j.LogManager;
import org.apache.log4j.Logger;
import org.apache.spark.Partitioner;
import org.apache.spark.api.java.JavaPairRDD;
import org.apache.spark.api.java.JavaRDDLike;
import org.apache.spark.api.java.JavaSparkContext;
import org.apache.spark.api.java.function.PairFunction;
import org.apache.spark.storage.StorageLevel;
import org.janelia.thickness.JoinFromList;
import org.janelia.thickness.utility.Utility;

import ij.ImagePlus;
import ij.io.FileSaver;
import ij.process.FloatProcessor;
import ij.process.ImageProcessor;
import scala.Tuple2;
import scala.Tuple5;

/**
 * @author Philipp Hanslovsky &lt;hanslovskyp@janelia.hhmi.org&gt;
 */
public class ComputeMatricesChunked
{

	public static interface PartitionerFactory
	{

		public Partitioner create( int min, int max, int defaultParallelism );

	}

	public static class RangeBasedPartitionerFactory implements PartitionerFactory
	{
		@Override
		public Partitioner create( final int min, final int max, final int numPartitions )
		{
			return new RangeBasedPartitioner( min, max, numPartitions );
		}

	}

	public static class RangeBasedPartitioner extends Partitioner
	{
		public static final Logger LOG = LogManager.getLogger( MethodHandles.lookup().lookupClass() );
		static
		{
			LOG.setLevel( Level.INFO );
		}

		private final int min;

		private final int max;

		private final int n;

		private final int numPartitions;

		private final double rangePerPartition;

		public RangeBasedPartitioner( final int min, final int max, final int numPartitions )
		{
			super();
			this.min = min;
			this.max = max;
			this.n = max - min;
			this.numPartitions = Math.min( numPartitions, this.n );
			this.rangePerPartition = n * 1.0 / this.numPartitions;
			LOG.info( this.min + " " + this.max + " " + this.n + " " + this.numPartitions + " " + this.rangePerPartition );
		}

		@Override
		public int numPartitions()
		{
			return numPartitions;
		}

		@Override
		public int getPartition( final Object o )
		{

			LOG.debug( "Getting partition for " + o + ": " + o.getClass().getSimpleName() );
			if ( o instanceof Integer )
			{
				final int part = ( int ) ( ( ( ( Integer ) o ).intValue() - min ) / rangePerPartition );
				LOG.debug( "Getting partition for " + o + ": " + part );
				return part;
			}

			if ( o instanceof Tuple2 )
				return getPartition( ( ( Tuple2< ?, ? > ) o )._1() );

			return 0;
		}
	}

	public static final Logger LOG = LogManager.getLogger( MethodHandles.lookup().lookupClass() );
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
			final PartitionerFactory partitionerFactory,
			final StorageLevel pairPersistenceLevel ) throws InterruptedException, ExecutionException
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

			if ( LOG.isDebugEnabled() )
			{

				final List< Tuple2< Integer, Long > > its = recordsPerPartition( rdd );

				LOG.debug( "Record distribution: " + " " + lower + " " + upper + " " + size + " " + rdd.count() );
				for ( final Tuple2< Integer, Long > it : its )
					LOG.debug( it );
			}

			LOG.debug( "Projecting onto self for rdd with: " + rdd.count() + " records." );

			final JavaPairRDD< Tuple2< Integer, Integer >, Tuple2< ImageAndMask, ImageAndMask > > pairs = JoinFromList.projectOntoSelf( rdd, sc.broadcast( keyPairList ), sc.defaultParallelism() )
					//					.mapToPair( t -> new Tuple2<>( t._1()._1() < t._1()._2() ? t._1() : t._1().swap(), t._2() ) )
					// is this partitionby bad?
					//					.partitionBy( new ShearedGridPartitioner2D( new int[] { 0, 1 }, new int[] { stop, maxRange }, sc.defaultParallelism() ) )
					.persist( pairPersistenceLevel );

			this.pairs.add( pairs );
			this.bounds.add( Utility.tuple5( z, Math.min( z + stepSize, stop ), z - lower, lower, size ) );
		}

		final List< Long > counts = this.pairs.stream().map( p -> p.count() ).collect( Collectors.toList() );

		if ( LOG.isDebugEnabled() )
			for ( int z = 0; z < pairs.size(); ++z )
			{
				final Tuple2< Tuple2< Integer, Integer >, Tuple2< ImageAndMask, ImageAndMask > > val = pairs.get( z ).first();
				final Tuple2< ImageAndMask, ImageAndMask > p = val._2();
				final String home = System.getProperty( "user.home" );
				final ImageProcessor[] fps = new ImageProcessor[] { p._1().image, p._1().mask, p._2().image, p._2().mask };
				final String[] names = new String[] { "debug-img1", "debug-mask1", "debug-img2", "debug-mask2" };
				for ( int i = 0; i < fps.length; ++i )
					new FileSaver( new ImagePlus( "", fps[ i ] ) ).saveAsTiff( home + "/" + names[ i ] + "-" + z + ".tif" );
				final CorrelationAndWeight saw = Correlations.calculate( p._1(), p._2() );
				LOG.debug( "Got similarity=" + saw.corr + " and weight=" + saw.weight + " for exemplary image pair: " + val._1() );
			}

		// make sure this is done before any other work happens! otherwise
		// performance suffers?
		LOG.info( "Holding image pairs: " + counts );


		log.info( MethodHandles.lookup().lookupClass().getSimpleName() + ": Join stepsize: " + stepSize );
		log.info( MethodHandles.lookup().lookupClass().getSimpleName() + ": Number of chunks: " + this.pairs.size() );
	}

	public JavaPairRDD< Tuple2< Integer, Integer >, Tuple2< FloatProcessor, FloatProcessor > > run(
			final MatrixGenerator.Factory factory,
			final int range,
			final int[] stepSize,
			final int[] blockRadius )
	{

		final ArrayList< JavaPairRDD< Tuple2< Integer, Integer >, Tuple2< FloatProcessor, FloatProcessor > > > rdds = new ArrayList<>();
		final int maxIndex = ( int ) ( this.nImages - 1 );
		final int stop = maxIndex + 1;
		for ( int i = 0; i < this.pairs.size(); ++i )
		{
			final JavaPairRDD< Tuple2< Integer, Integer >, Tuple2< ImageAndMask, ImageAndMask > > pair = this.pairs.get( i );
			final MatrixGenerator matrixGenerator = factory.create();
			final Tuple5< Integer, Integer, Integer, Integer, Integer > bound = this.bounds.get( i );
			final JavaPairRDD< Tuple2< Integer, Integer >, Tuple2< FloatProcessor, FloatProcessor > > matrices =
					matrixGenerator.generateMatrices( sc, pair, blockRadius, stepSize, range, bound._4(), bound._5() );
			final JavaPairRDD< Tuple2< Integer, Integer >, Tuple2< FloatProcessor, FloatProcessor > > strip =
					matrices.mapToPair( new MatrixToStrip< Tuple2< Integer, Integer > >( bound._3(), Math.min( chunkStepSize, stop - bound._1() ), range ) );
			rdds.add( strip );
		}

		JavaPairRDD< Tuple2< Integer, Integer >, Tuple2< FloatProcessor, FloatProcessor > > result = rdds.get( 0 )
				.mapToPair( new PutIntoGlobalContext< Tuple2< Integer, Integer > >( stop, bounds.get( 0 )._1(), bounds.get( 0 )._2() ) );

		for ( int i = 1; i < rdds.size(); ++i )
		{
			final JavaPairRDD< Tuple2< Integer, Integer >, Tuple2< FloatProcessor, FloatProcessor > > rdd = rdds.get( i );
			final Tuple5< Integer, Integer, Integer, Integer, Integer > bound = bounds.get( i );

			final int offset = bound._1();

			result = result.join( rdd ).mapToPair( new JoinStrips<>( offset ) );
		}

		return result;

	}

	public static List< Tuple2< Integer, Long > > recordsPerPartition( final JavaRDDLike< ?, ? > rdd )
	{
		final List< Tuple2< Integer, Long > > its = rdd.mapPartitionsWithIndex( ( i, it ) -> {
			return new Iterator< Tuple2< Integer, Long > >()
			{

				@Override
				public boolean hasNext()
				{
					return it.hasNext();
				}

				@Override
				public Tuple2< Integer, Long > next()
				{
					it.next();
					return new Tuple2<>( i, 1l );
				}
			};
		}, true ).mapToPair( t -> t ).reduceByKey( ( l1, l2 ) -> l1 + l2 ).collect();
		return its;
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
	public static class MatrixToStrip< K > implements PairFunction< Tuple2< K, Tuple2< FloatProcessor, FloatProcessor > >, K, Tuple2< FloatProcessor, FloatProcessor > >
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
		public Tuple2< K, Tuple2< FloatProcessor, FloatProcessor > > call( final Tuple2< K, Tuple2< FloatProcessor, FloatProcessor > > t ) throws Exception
		{

			final FloatProcessor matrix = t._2()._1();
			final FloatProcessor weight = t._2()._2();
			final FloatProcessor strip = new FloatProcessor( 2 * range + 1, this.size );
			final FloatProcessor weightStrip = new FloatProcessor( 2 * range + 1, this.size );
			final int w = matrix.getWidth();

			for ( int z = offset, stripZ = 0; stripZ < size; ++z, ++stripZ )
				for ( int r = -range; r <= range; ++r )
				{
					final int k = r + z;
					if ( k < 0 || k >= w )
						continue;
					final int pos = range + r;
					strip.setf( pos, stripZ, matrix.getf( z, k ) );
					weightStrip.setf( pos, stripZ, weight.getf( z, k ) );
				}

			return Utility.tuple2( t._1(), new Tuple2<>( strip, weightStrip ) );
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
	public static class PutIntoGlobalContext< K > implements PairFunction< Tuple2< K, Tuple2< FloatProcessor, FloatProcessor > >, K, Tuple2< FloatProcessor, FloatProcessor > >
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
		public Tuple2< K, Tuple2< FloatProcessor, FloatProcessor > > call( final Tuple2< K, Tuple2< FloatProcessor, FloatProcessor > > t ) throws Exception
		{
			final FloatProcessor source = t._2()._1();
			final FloatProcessor weight = t._2()._2();
			final int w = source.getWidth();
			final FloatProcessor result = new FloatProcessor( w, this.size );
			final FloatProcessor resultWeights = new FloatProcessor( w, this.size );
			result.add( Double.NaN );
			for ( int y = this.lower, sourceY = 0; y < this.upper; ++y, ++sourceY )
				for ( int x = 0; x < w; ++x )
				{
					result.setf( x, y, source.getf( x, sourceY ) );
					resultWeights.setf( x, y, weight.getf( x, sourceY ) );
				}
			return Utility.tuple2( t._1(), new Tuple2<>( result, resultWeights ) );
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
	implements PairFunction< Tuple2< K, Tuple2< Tuple2< FloatProcessor, FloatProcessor >, Tuple2< FloatProcessor, FloatProcessor > > >, K, Tuple2< FloatProcessor, FloatProcessor > >
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
		public Tuple2< K, Tuple2< FloatProcessor, FloatProcessor > > call( final Tuple2< K, Tuple2< Tuple2< FloatProcessor, FloatProcessor >, Tuple2< FloatProcessor, FloatProcessor > > > t ) throws Exception
		{
			final FloatProcessor source = t._2()._2()._1();
			final FloatProcessor target = t._2()._1()._1();
			final FloatProcessor sourceWeight = t._2()._2()._2();
			final FloatProcessor targetWeight = t._2()._1()._2();
			final int sourceH = source.getHeight();
			final int sourceW = source.getWidth();
			for ( int sourceY = 0, y = offset; sourceY < sourceH; ++sourceY, ++y )
				for ( int x = 0; x < sourceW; ++x )
				{
					target.setf( x, y, source.getf( x, sourceY ) );
					targetWeight.setf( x, y, sourceWeight.getf( x, sourceY ) );
				}

			return Utility.tuple2( t._1(), new Tuple2<>( target, targetWeight ) );
		}
	}

}
