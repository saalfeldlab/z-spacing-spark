package org.janelia.thickness.similarity;

import java.util.ArrayList;
import java.util.HashMap;

import org.apache.spark.api.java.JavaPairRDD;
import org.apache.spark.api.java.JavaSparkContext;
import org.apache.spark.api.java.function.PairFunction;
import org.janelia.thickness.JoinFromList;
import org.janelia.thickness.utility.Utility;

import ij.process.FloatProcessor;
import scala.Tuple2;
import scala.Tuple3;

/**
 * @author Philipp Hanslovsky &lt;hanslovskyp@janelia.hhmi.org&gt;
 */
public class ComputeMatricesChunked
{
	private final JavaPairRDD< Integer, FloatProcessor > files;

	private final ArrayList< MatrixGenerationFromImagePairs > generators;

	private final ArrayList< Tuple3< Integer, Integer, Integer > > bounds;

	private final int stepSize;

	private final int maxRange;

	private final int[] dim;

	public ComputeMatricesChunked( JavaSparkContext sc, JavaPairRDD< Integer, FloatProcessor > files, int stepSize, int maxRange, int[] dim, boolean ensurePersistence )
	{
		this.files = files;
		this.stepSize = stepSize;
		this.maxRange = maxRange;

		this.generators = new ArrayList<>();
		this.bounds = new ArrayList<>();
		this.dim = dim;

		int stop = ( int ) files.count();
		for ( int z = 0; z < stop; z += this.stepSize )
		{
			final int lower = Math.max( z - this.maxRange, 0 );
			final int upper = Math.min( z + this.maxRange + stepSize, stop );
			final int size = upper - lower;
			JavaPairRDD< Integer, FloatProcessor > rdd = files.filter( new Utility.FilterRange<>( lower, upper ) ).cache();
			final HashMap< Integer, ArrayList< Integer > > keyPairList = new HashMap< Integer, ArrayList< Integer > >();
			for ( int i = lower; i < upper; ++i )
			{
				ArrayList< Integer > al = new ArrayList< Integer >();
				for ( int k = i + 1; k < upper && k - i <= maxRange; ++k )
				{
					al.add( k );
				}
				keyPairList.put( i, al );
			}
			JavaPairRDD< Tuple2< Integer, Integer >, Tuple2< FloatProcessor, FloatProcessor > > pairs =
					JoinFromList.projectOntoSelf( rdd, sc.broadcast( keyPairList ) );
			MatrixGenerationFromImagePairs matrixGenerator = new MatrixGenerationFromImagePairs( sc, pairs, this.dim, size, lower );
			if ( ensurePersistence )
				matrixGenerator.ensurePersistence();
			this.generators.add( matrixGenerator );
			this.bounds.add( Utility.tuple3( z, Math.min( z + stepSize, stop ), z - lower ) );
		}
	}

	public JavaPairRDD< Tuple2< Integer, Integer >, FloatProcessor > run(
			int range,
			int stride[],
			int[] correlationBlockRadius )
	{
		ArrayList< JavaPairRDD< Tuple2< Integer, Integer >, FloatProcessor > > rdds = new ArrayList<>();
		final int maxIndex = ( int ) ( files.count() - 1 );
		final int stop = maxIndex + 1;
		for ( int i = 0; i < generators.size(); ++i )
		{
			MatrixGenerationFromImagePairs matrixGenerator = this.generators.get( i );
			Tuple3< Integer, Integer, Integer > bound = this.bounds.get( i );
			JavaPairRDD< Tuple2< Integer, Integer >, FloatProcessor > matrices =
					matrixGenerator.generateMatrices( stride, correlationBlockRadius, range ).cache();
			System.out.println( "SmallerJoinTest: " + matrices.count() + " matrices." );
			JavaPairRDD< Tuple2< Integer, Integer >, FloatProcessor > strip =
					matrices.mapToPair( new MatrixToStrip<>( bound._3(), Math.min( stepSize, stop - bound._1() ), range ) );
			rdds.add( strip );
		}

		JavaPairRDD< Tuple2< Integer, Integer >, FloatProcessor > result = rdds.get( 0 )
				.mapToPair( new PutIntoGlobalContext< Tuple2< Integer, Integer > >( stop, bounds.get( 0 )._1(), bounds.get( 0 )._2() ) );

		for ( int i = 1; i < rdds.size(); ++i )
		{
			JavaPairRDD< Tuple2< Integer, Integer >, FloatProcessor > rdd = rdds.get( i );
			Tuple3< Integer, Integer, Integer > bound = bounds.get( i );

			final int offset = bound._1();

			result = result.join( rdd ).mapToPair( new JoinStrips<>( offset ) );
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

		public MatrixToStrip( int offset, int size, int range )
		{
			this.offset = offset;
			this.size = size;
			this.range = range;
		}

		@Override
		public Tuple2< K, FloatProcessor > call( Tuple2< K, FloatProcessor > t ) throws Exception
		{

			FloatProcessor matrix = t._2();
			FloatProcessor strip = new FloatProcessor( 2 * range + 1, this.size );
			int w = matrix.getWidth();

			for ( int z = offset, stripZ = 0; stripZ < size; ++z, ++stripZ )
			{
				for ( int r = -range; r <= range; ++r )
				{
					int k = r + z;
					if ( k < 0 || k >= w )
						continue;
					strip.setf( range + r, stripZ, matrix.getf( z, k ) );
				}
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

		public PutIntoGlobalContext( int size, int lower, int upper )
		{
			this.size = size;
			this.lower = lower;
			this.upper = upper;
		}

		@Override
		public Tuple2< K, FloatProcessor > call( Tuple2< K, FloatProcessor > t ) throws Exception
		{
			FloatProcessor source = t._2();
			int w = source.getWidth();
			FloatProcessor result = new FloatProcessor( w, this.size );
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

		public JoinStrips( int offset )
		{
			this.offset = offset;
		}

		@Override
		public Tuple2< K, FloatProcessor > call( Tuple2< K, Tuple2< FloatProcessor, FloatProcessor > > t ) throws Exception
		{
			Tuple2< FloatProcessor, FloatProcessor > fps = t._2();
			FloatProcessor source = fps._2();
			FloatProcessor target = fps._1();
			int sourceH = source.getHeight();
			int sourceW = source.getWidth();
			for ( int sourceY = 0, y = offset; sourceY < sourceH; ++sourceY, ++y )
			{
				for ( int x = 0; x < sourceW; ++x )
				{
					target.setf( x, y, source.getf( x, sourceY ) );
				}
			}

			return Utility.tuple2( t._1(), target );
		}
	}

}
