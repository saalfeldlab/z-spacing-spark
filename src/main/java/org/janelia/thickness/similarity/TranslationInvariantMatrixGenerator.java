package org.janelia.thickness.similarity;

import java.util.Arrays;
import java.util.HashMap;
import java.util.HashSet;

import org.apache.spark.api.java.JavaPairRDD;
import org.apache.spark.api.java.JavaSparkContext;
import org.apache.spark.api.java.function.PairFunction;
import org.janelia.thickness.utility.Utility;

import ij.process.FloatProcessor;
import mpicbg.ij.integral.BlockPMCC;
import scala.Tuple2;

/**
 * @author Philipp Hanslovsky &lt;hanslovskyp@janelia.hhmi.org&gt;
 */
public class TranslationInvariantMatrixGenerator implements MatrixGenerator
{

	public static class Factory implements MatrixGenerator.Factory
	{

		private final int[] correlationBlockRadius;

		private final int[] maxOffset;

		public Factory( final int[] correlationBlockRadius, final int[] maxOffset )
		{
			super();
			this.correlationBlockRadius = correlationBlockRadius;
			this.maxOffset = maxOffset;
		}

		@Override
		public MatrixGenerator create()
		{
			return new TranslationInvariantMatrixGenerator( correlationBlockRadius, maxOffset );
		}

	}

	private final int[] correlationBlockRadius;

	private final int[] maxOffset;

	public TranslationInvariantMatrixGenerator( final int[] correlationBlockRadius, final int[] maxOffset )
	{
		super();
		this.correlationBlockRadius = correlationBlockRadius;
		this.maxOffset = maxOffset;
	}

	@Override
	public JavaPairRDD< Tuple2< Integer, Integer >, FloatProcessor > generateMatrices(
			final JavaSparkContext sc,
			final JavaPairRDD< Tuple2< Integer, Integer >, Tuple2< FloatProcessor, FloatProcessor > > sectionPairs,
			final int[] blockRadius,
			final int[] stepSize,
			final int range,
			final int startIndex,
			final int size )
	{

		final JavaPairRDD< Tuple2< Integer, Integer >, Tuple2< FloatProcessor, FloatProcessor > > sections = sectionPairs
				.filter( new DefaultMatrixGenerator.SelectInRange< Tuple2< FloatProcessor, FloatProcessor > >( range ) );

		System.out.println( "sections: " + sections.count() );

		final JavaPairRDD< Tuple2< Integer, Integer >, Tuple2< FloatProcessor, FloatProcessor > > maxProjections = sections
				.mapToPair( new FPToSimilarities< Tuple2< Integer, Integer > >(
						maxOffset,
						correlationBlockRadius ) )
				.cache();

		System.out.println( "maxOffset=" + Arrays.toString( maxOffset ) );
		System.out.println( "correlationBlockRadius=" + Arrays.toString( correlationBlockRadius ) );

		System.out.println( "maxProjections: " + maxProjections.count() );

		final JavaPairRDD< Tuple2< Integer, Integer >, HashMap< Tuple2< Integer, Integer >, Double > > averages = maxProjections
				.mapToPair( new AverageBlocks< Tuple2< Integer, Integer > >( blockRadius, stepSize ) )
				.cache();

		System.out.println( "averages: " + averages.count() );

		final JavaPairRDD< Tuple2< Integer, Integer >, Tuple2< Tuple2< Integer, Integer >, Double > > flatAverages = averages
				.flatMapToPair(
						new Utility.FlatmapMap< Tuple2< Integer, Integer >, Tuple2< Integer, Integer >, Double, HashMap< Tuple2< Integer, Integer >, Double > >() )
				.cache();

		System.out.println( "flatAverages: " + flatAverages.count() );

		final JavaPairRDD< Tuple2< Integer, Integer >, HashMap< Tuple2< Integer, Integer >, Double > > averagesIndexedByXYTuples = flatAverages
				.mapToPair( new Utility.SwapKeyKey< Tuple2< Integer, Integer >, Tuple2< Integer, Integer >, Double >() )
				.mapToPair( new Utility.ValueAsMap< Tuple2< Integer, Integer >, Tuple2< Integer, Integer >, Double >() )
				.cache();

		averagesIndexedByXYTuples.count();

		final JavaPairRDD< Tuple2< Integer, Integer >, FloatProcessor > matrices = averagesIndexedByXYTuples
				.reduceByKey( new Utility.ReduceMapsByUnion< Tuple2< Integer, Integer >, Double, HashMap< Tuple2< Integer, Integer >, Double > >() )
				.mapToPair( new DefaultMatrixGenerator.MapToFloatProcessor( size, startIndex ) )
				.cache();

		matrices.count();

		return matrices;

	}

	public static class FPToSimilarities< K >
	implements PairFunction< Tuple2< K, Tuple2< FloatProcessor, FloatProcessor > >, K, Tuple2< FloatProcessor, FloatProcessor > >
	{

		/**
		 *
		 */
		private static final long serialVersionUID = 572711725174439812L;

		private final int[] maxOffsets;

		private final int[] blockRadius;

		public FPToSimilarities( final int[] maxOffsets, final int[] blockRadius )
		{
			this.maxOffsets = maxOffsets;
			this.blockRadius = blockRadius;
		}

		@SuppressWarnings( "rawtypes" )
		@Override
		public Tuple2< K, Tuple2< FloatProcessor, FloatProcessor > >
		call( final Tuple2< K, Tuple2< FloatProcessor, FloatProcessor > > t ) throws Exception
		{
			final FloatProcessor fixed = t._2()._1();
			final FloatProcessor moving = t._2()._2();

			final K k = t._1();

			int x = 0, y = 0;
			if ( k instanceof Tuple2 )
			{
				if ( ( ( Tuple2 ) k )._1() instanceof Integer )
					x = ( ( Integer ) ( ( Tuple2 ) k )._1() ).intValue();
				if ( ( ( Tuple2 ) k )._2() instanceof Integer )
					y = ( ( Integer ) ( ( Tuple2 ) k )._2() ).intValue();
			}

			final Tuple2< FloatProcessor, FloatProcessor > ccs = tolerantNCC(
					( FloatProcessor ) fixed.duplicate(),
					( FloatProcessor ) moving.duplicate(),
					maxOffsets,
					blockRadius,
					x,
					y );
			return Utility.tuple2( t._1(), Utility.tuple2( ccs._1(), ccs._2() ) );
		}
	}

	public static class AverageBlocks< K >
	implements PairFunction< Tuple2< K, Tuple2< FloatProcessor, FloatProcessor > >, K, HashMap< Tuple2< Integer, Integer >, Double > >
	{

		/**
		 *
		 */
		private static final long serialVersionUID = 6067709319074903557L;

		private final int[] blockRadius;

		private final int[] stepSize;

		public AverageBlocks( final int[] blockRadius, final int[] stepSize )
		{
			this.blockRadius = blockRadius;
			this.stepSize = stepSize;
		}

		@Override
		public Tuple2< K, HashMap< Tuple2< Integer, Integer >, Double > > call( final Tuple2< K, Tuple2< FloatProcessor, FloatProcessor > > t ) throws Exception
		{
			return Utility.tuple2( t._1(), average( t._2()._1(), t._2()._2(), blockRadius, stepSize ) );
		}
	}

	public static FloatProcessor generateMask( final FloatProcessor img, final HashSet< Float > values )
	{
		final FloatProcessor mask = new FloatProcessor( img.getWidth(), img.getHeight() );
		final float[] i = ( float[] ) img.getPixels();
		final float[] m = ( float[] ) mask.getPixels();
		for ( int k = 0; k < i.length; ++k )
			m[ k ] = values.contains( i[ k ] ) ? 0.0f : 1.0f;
		return mask;
	}

	public static FloatProcessor generateMask( final FloatProcessor fp )
	{
		final FloatProcessor weights = new FloatProcessor( fp.getWidth(), fp.getHeight() );
		final float[] weightsPixels = ( float[] ) weights.getPixels();
		final float[] fpPixels = ( float[] ) fp.getPixels();
		for ( int i = 0; i < fpPixels.length; ++i )
		{
			final boolean isNaN = Float.isNaN( fpPixels[ i ] );
			// ignore NaNs (leave them 0.0f in mask)
			// still need to replace NaNs in image because 0.0 * NaN = NaN
			if ( isNaN )
				fpPixels[ i ] = 0.0f;
			else
				weightsPixels[ i ] = 1.0f;
		}
		return weights;
	}

	public static Tuple2< FloatProcessor, FloatProcessor > tolerantNCC(
			final FloatProcessor fixed,
			final FloatProcessor moving,
			final int[] maxOffsets,
			final int[] blockRadiusInput,
			final int z1,
			final int z2 )
	{
		final int width = moving.getWidth();
		final int height = moving.getHeight();

		final int[] blockRadius = new int[] {
				Math.min( blockRadiusInput[ 0 ], width - 1 ),
				Math.min( blockRadiusInput[ 1 ], height - 1 )
		};

		final FloatProcessor maxCorrelations = new FloatProcessor( width, height );
		//        maxCorrelations.add( Double.NaN );

		final int xStart = -1 * maxOffsets[ 0 ];
		final int yStart = -1 * maxOffsets[ 1 ];

		final int xStop = 1 * maxOffsets[ 0 ]; // inclusive
		final int yStop = 1 * maxOffsets[ 1 ]; // inclusive

		final BlockPMCC pmcc = new BlockPMCC( fixed, moving );
		final FloatProcessor tp = pmcc.getTargetProcessor();

		final FloatProcessor weights = new FloatProcessor( width, height );

		for ( int yOff = yStart; yOff <= yStop; ++yOff )
			for ( int xOff = xStart; xOff <= xStop; ++xOff )
			{
				pmcc.setOffset( xOff, yOff );
				pmcc.r( blockRadius[ 0 ], blockRadius[ 1 ] );

				for ( int y = 0; y < height; ++y )
					for ( int x = 0; x < width; ++x )
					{
						// if full correlation block is not contained within
						// image, ignore it!
						if ( x + xOff - blockRadius[ 0 ] < 0 || y + yOff - blockRadius[ 1 ] < 0 ||
								x + xOff + blockRadius[ 0 ] > width || y + yOff + blockRadius[ 1 ] > height )
							continue;

						// if full correlation block is not contained within
						// moving image, ignore it!
						if ( x - blockRadius[ 0 ] < 0 || y - blockRadius[ 1 ] < 0 ||
								x + blockRadius[ 0 ] > width || y + blockRadius[ 1 ] > height )
							continue;

						final float val = tp.getf( x, y );
						if ( !Double.isNaN( val ) && val > maxCorrelations.getf( x, y ) )
							maxCorrelations.setf( x, y, val );
					}

			}

		for ( int y = 0; y < height; ++y )
			for ( int x = 0; x < width; ++x )
			{
				final float weight = x < blockRadius[ 0 ] || x >= width - blockRadius[ 0 ] ||
						y < blockRadius[ 1 ] || y >= height - blockRadius[ 1 ] ? Float.NaN : 1.0f;
						weights.setf( x, y, weight );
			}

		return Utility.tuple2( maxCorrelations, weights );
	}

	public static HashMap< Tuple2< Integer, Integer >, Double > average(
			final FloatProcessor maxCorrelations,
			final FloatProcessor weights,
			final int[] blockSize,
			final int[] stepSize )
	{
		final HashMap< Tuple2< Integer, Integer >, Double > hm = new HashMap< >();
		final int width = maxCorrelations.getWidth();
		final int height = maxCorrelations.getHeight();

		final int maxX = width - 1;
		final int maxY = height - 1;

		for ( int y = blockSize[ 1 ], yIndex = 0; y < height; y += stepSize[ 1 ], ++yIndex )
		{
			final int lowerY = y - blockSize[ 1 ];
			final int upperY = Math.min( y + blockSize[ 1 ], maxY );
			for ( int x = blockSize[ 0 ], xIndex = 0; x < width; x += stepSize[ 0 ], ++xIndex )
			{
				final int lowerX = x - blockSize[ 0 ];
				final int upperX = Math.min( x + blockSize[ 0 ], maxX );
				double sum = 0.0;
				double weightSum = 0.0;
				for ( int yLocal = lowerY; yLocal <= upperY; ++yLocal )
					for ( int xLocal = lowerX; xLocal <= upperX; ++xLocal )
					{
						final double weight = weights.getf( xLocal, yLocal );
						final double corr = maxCorrelations.getf( xLocal, yLocal );
						if ( Double.isNaN( weight ) || Double.isNaN( corr ) )
							continue;
						sum += weight * corr;
						weightSum += weight;
					}
				if ( weightSum > 0.0 )
				{
					sum /= weightSum;
					hm.put( Utility.tuple2( xIndex, yIndex ), sum );
				}
			}
		}
		return hm;
	}

}
