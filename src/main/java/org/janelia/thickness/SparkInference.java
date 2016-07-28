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

import ij.process.FloatProcessor;
import mpicbg.models.NotEnoughDataPointsException;
import net.imglib2.Cursor;
import net.imglib2.RandomAccessibleInterval;
import net.imglib2.img.array.ArrayImgs;
import net.imglib2.type.numeric.RealType;
import net.imglib2.type.numeric.real.DoubleType;
import net.imglib2.type.numeric.real.FloatType;
import net.imglib2.view.Views;
import scala.Tuple2;

/**
 * 
 * @author Philipp Hanslovsky &lt;hanslovskyp@janelia.hhmi.org&gt;
 *
 */
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
		final JavaPairRDD< Tuple2< Integer, Integer >, double[] > results = matricesWithStartingCoordinates
				.mapToPair( new Inference<Tuple2<Integer, Integer>>( options ) );
		return results;
	}

	public static class Inference< K >
			implements PairFunction< Tuple2< K, Tuple2< FloatProcessor, double[] > >, K, double[] >
	{
		private static final long serialVersionUID = 8094812748656050753L;

		private final Options options;

		public Inference( final Options options )
		{
			super();
			this.options = options;
		}

		public Tuple2< K, double[] > 
		call( final Tuple2< K, Tuple2< FloatProcessor, double[] > > xyAndMatrixAndCoordinates )
				throws Exception
		{
			final Tuple2< FloatProcessor, double[] > matrixAndCoordinates = xyAndMatrixAndCoordinates._2();
			final FloatProcessor fp = matrixAndCoordinates._1();
			final float[] data = (float[])fp.getPixels();
			final int w = fp.getWidth();
			final int h = fp.getHeight();
			RandomAccessibleInterval< FloatType > matrix = ArrayImgs.floats( data, w, h );
			matrix = h == w ? matrix : MatrixStripConversion.stripToMatrix( matrix, new FloatType( Float.NaN ) );
			// return starting coordinates if one of the values is nan or zero
			for ( final Cursor< FloatType > c = Views.iterable( matrix ).cursor(); c.hasNext(); )
			{
				final float val = c.next().get();
				final long x = c.getLongPosition( 0 );
				final long y = c.getLongPosition( 1 );
				if ( Math.abs( x - y ) <= options.comparisonRange && ( Float.isNaN( val ) || val == 0.0f ) )
					return Utility.tuple2( xyAndMatrixAndCoordinates._1(), matrixAndCoordinates._2() );
			}

			final InferFromMatrix inference = new InferFromMatrix( new GlobalCorrelationFitAverage() );
			final Visitor visitor = new LazyVisitor();
//            ArrayImg<DoubleType, DoubleArray> img = ArrayImgs.doubles(t._2()._2().length, options.nIterations + 1);
//            visitor = new WriteTransformationVisitor(img);
			try
			{
				final double[] coordinates = inference.estimateZCoordinates( matrix, matrixAndCoordinates._2(), visitor, options );
				for ( int i = 0; i < coordinates.length; ++i )
				{
					final double c = coordinates[ i ];
					if ( Double.isNaN( c ) ) { return Utility.tuple2( xyAndMatrixAndCoordinates._1(), matrixAndCoordinates._2() ); }
				}
				return Utility.tuple2( xyAndMatrixAndCoordinates._1(), coordinates );
			}
			catch ( final NotEnoughDataPointsException e )
			{
				System.err.println( "Fail at inference for coordinate " + matrixAndCoordinates._1() );
				e.printStackTrace( System.err );
				return Utility.tuple2( xyAndMatrixAndCoordinates._1(), matrixAndCoordinates._2() );
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
