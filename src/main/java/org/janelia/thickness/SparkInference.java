package org.janelia.thickness;

import org.apache.spark.api.java.JavaPairRDD;
import org.apache.spark.api.java.JavaSparkContext;
import org.apache.spark.api.java.function.PairFunction;
import org.janelia.thickness.inference.InferFromMatrix;
import org.janelia.thickness.inference.Options;
import org.janelia.thickness.inference.fits.GlobalCorrelationFitAverage;
import org.janelia.thickness.inference.visitor.LazyVisitor;
import org.janelia.thickness.inference.visitor.Visitor;
import org.janelia.thickness.utility.Utility;
import org.janelia.utility.MatrixStripConversion;

import ij.ImagePlus;
import ij.process.FloatProcessor;
import mpicbg.models.NotEnoughDataPointsException;
import net.imglib2.Cursor;
import net.imglib2.FinalInterval;
import net.imglib2.RandomAccessibleInterval;
import net.imglib2.img.display.imagej.ImageJFunctions;
import net.imglib2.transform.integer.shear.AbstractShearTransform;
import net.imglib2.transform.integer.shear.ShearTransform;
import net.imglib2.type.Type;
import net.imglib2.type.numeric.RealType;
import net.imglib2.type.numeric.real.DoubleType;
import net.imglib2.type.numeric.real.FloatType;
import net.imglib2.view.ExtendedRandomAccessibleInterval;
import net.imglib2.view.IntervalView;
import net.imglib2.view.TransformView;
import net.imglib2.view.Views;
import scala.Tuple2;

public class SparkInference
{

	public static JavaPairRDD< Tuple2< Integer, Integer >, double[] > inferCoordinates(
			final JavaSparkContext sc,
			final JavaPairRDD< Tuple2< Integer, Integer >, FloatProcessor > matrices,
			final JavaPairRDD< Tuple2< Integer, Integer >, double[] > startingCoordinates,
			final Chunk chunk,
			final Options options,
			final String pattern )
	{
		final JavaPairRDD< Tuple2< Integer, Integer >, Tuple2< FloatProcessor, double[] > > matricesWithStartingCoordinates = matrices
				.join( startingCoordinates );

		System.out.flush();
		final JavaPairRDD< Tuple2< Integer, Integer >, Tuple2< Integer, Tuple2< FloatProcessor, double[] > > > chunked =
				chunk.getChunks( matricesWithStartingCoordinates );
		final JavaPairRDD< Tuple2< Integer, Integer >, Tuple2< Integer, double[] > > results = chunked
				.mapToPair( new Inference<Tuple2<Integer, Integer>>( options ) );

		return chunk.mergeTransforms( results );
	}

	public static < T extends Type< T > > RandomAccessibleInterval< T > stripToMatrix( final RandomAccessibleInterval< T > strip, final T dummy )
	{
		final ExtendedRandomAccessibleInterval< T, RandomAccessibleInterval< T > > extended = Views.extendValue( strip, dummy );
		final AbstractShearTransform tf = new ShearTransform( 2, 0, 1 ).inverse();
		final long w = strip.dimension( 0 ) / 2;
		final long h = strip.dimension( 1 );
		final FinalInterval interval = new FinalInterval( new long[] { w, 0 }, new long[] { h + w - 1, h - 1 } );
		final IntervalView< T > transformed = Views.offsetInterval( new TransformView<>( extended, tf ), interval );
		return transformed;
	}

	public static < T extends Type< T > > RandomAccessibleInterval< T > matrixToStrip( final RandomAccessibleInterval< T > matrix, final int range, final T dummy )
	{
		final ExtendedRandomAccessibleInterval< T, RandomAccessibleInterval< T > > extended = Views.extendValue( matrix, dummy );
		final AbstractShearTransform tf = new ShearTransform( 2, 0, 1 );
		final long h = matrix.dimension( 1 );
		final FinalInterval interval = new FinalInterval( new long[] { -range, 0 }, new long[] { range, h } );
		final IntervalView< T > transformed = Views.offsetInterval( new TransformView<>( extended, tf ), interval );
		return transformed;
	}

	public static class Inference< K >
			implements PairFunction< Tuple2< K, Tuple2< Integer, Tuple2< FloatProcessor, double[] > > >, K, Tuple2< Integer, double[] > >
	{
		private static final long serialVersionUID = 8094812748656050753L;

		private final Options options;

		public Inference( final Options options )
		{
			super();
			this.options = options;
		}

		public Tuple2< K, Tuple2< Integer, double[] > > call( final Tuple2< K, Tuple2< Integer, Tuple2< FloatProcessor, double[] > > > it )
				throws Exception
		{
			final Tuple2< Integer, Tuple2< FloatProcessor, double[] > > t = it._2();
			final ImagePlus imp = new ImagePlus( "", t._2()._1() );
			final int w = imp.getWidth();
			final int h = imp.getHeight();
			RandomAccessibleInterval< FloatType > matrix = ImageJFunctions.wrapFloat( imp );
			matrix = h == w ? matrix : MatrixStripConversion.stripToMatrix( matrix, new FloatType( Float.NaN ) );
			// return starting coordinates if one of the values is nan or zero
			for ( final Cursor< FloatType > c = Views.iterable( matrix ).cursor(); c.hasNext(); )
			{
				final float val = c.next().get();
				final long x = c.getLongPosition( 0 );
				final long y = c.getLongPosition( 1 );
				if ( Math.abs( x - y ) <= options.comparisonRange && ( Float.isNaN( val ) || val == 0.0f ) )
					return Utility.tuple2( it._1(), Utility.tuple2( t._1(), t._2()._2() ) );
			}

			final InferFromMatrix inference = new InferFromMatrix( new GlobalCorrelationFitAverage() );
			final Visitor visitor = new LazyVisitor();
//            ArrayImg<DoubleType, DoubleArray> img = ArrayImgs.doubles(t._2()._2().length, options.nIterations + 1);
//            visitor = new WriteTransformationVisitor(img);
			try
			{
				final double[] coordinates = inference.estimateZCoordinates( matrix, t._2()._2(), visitor, options );
				for ( int i = 0; i < coordinates.length; ++i )
				{
					final double c = coordinates[ i ];
					if ( Double.isNaN( c ) ) { return Utility.tuple2( it._1(), Utility.tuple2( t._1(), t._2()._2() ) ); }
				}
				return Utility.tuple2( it._1(), Utility.tuple2( t._1(), coordinates ) );
			}
			catch ( final NotEnoughDataPointsException e )
			{
				System.err.println( "Fail at inference for coordinate " + t._1() );
				e.printStackTrace( System.err );
				return Utility.tuple2( it._1(), null );
			}
		}
	}

	public static class WriteTransformationVisitor implements Visitor
	{

		private final RandomAccessibleInterval< DoubleType > img;

		public WriteTransformationVisitor( final RandomAccessibleInterval< DoubleType > img )
		{
			this.img = img;
		}

		@Override
		public < T extends RealType< T > > void act(
				final int iteration,
				final RandomAccessibleInterval< T > matrix,
				final RandomAccessibleInterval< T > scaledMatrix,
				final double[] lut,
				final int[] permutation,
				final int[] inversePermutation,
				final double[] weights,
				final RandomAccessibleInterval< double[] > estimatedFit )
		{
			final Cursor< DoubleType > current = Views.flatIterable( Views.hyperSlice( img, 1, iteration ) ).cursor();
			for ( int z = 0; current.hasNext(); ++z )
				current.next().set( lut[ z ] );
		}
	}

}
