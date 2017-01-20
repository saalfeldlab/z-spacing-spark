package org.janelia.thickness;

import org.apache.spark.api.java.JavaPairRDD;
import org.apache.spark.api.java.JavaSparkContext;
import org.apache.spark.api.java.function.PairFunction;
import org.janelia.thickness.inference.InferFromMatrix;
import org.janelia.thickness.inference.Options;
import org.janelia.thickness.inference.fits.AbstractCorrelationFit;
import org.janelia.thickness.inference.fits.GlobalCorrelationFitAverage;
import org.janelia.thickness.inference.fits.LocalCorrelationFitAverage;
import org.janelia.thickness.inference.visitor.LazyVisitor;
import org.janelia.thickness.inference.visitor.Visitor;
import org.janelia.thickness.utility.FPTuple;
import org.janelia.thickness.utility.Utility;

import ij.ImagePlus;
import mpicbg.models.NotEnoughDataPointsException;
import net.imglib2.Cursor;
import net.imglib2.RandomAccessibleInterval;
import net.imglib2.img.Img;
import net.imglib2.img.array.ArrayImg;
import net.imglib2.img.array.ArrayImgs;
import net.imglib2.img.basictypeaccess.array.DoubleArray;
import net.imglib2.img.display.imagej.ImageJFunctions;
import net.imglib2.type.numeric.RealType;
import net.imglib2.type.numeric.real.DoubleType;
import net.imglib2.type.numeric.real.FloatType;
import net.imglib2.view.Views;
import scala.Tuple2;

public class SparkInference
{

	public static JavaPairRDD< Tuple2< Integer, Integer >, double[] > inferCoordinates( final JavaSparkContext sc, final JavaPairRDD< Tuple2< Integer, Integer >, FPTuple > matrices, final JavaPairRDD< Tuple2< Integer, Integer >, double[] > startingCoordinates, final Options options, final String pattern )
	{
		final JavaPairRDD< Tuple2< Integer, Integer >, Tuple2< FPTuple, double[] > > matricesWithStartingCoordinates = matrices.join( startingCoordinates );
		final JavaPairRDD< Tuple2< Integer, Integer >, double[] > result = matricesWithStartingCoordinates.mapToPair( new Inference< Tuple2< Integer, Integer > >( options, pattern ) );

		return result;
	}

	public static class Inference< K > implements PairFunction< Tuple2< K, Tuple2< FPTuple, double[] > >, K, double[] >
	{
		private static final long serialVersionUID = 8094812748656050753L;

		private final Options options;

		private final String pattern;

		public Inference( final Options options, final String pattern )
		{
			super();
			this.options = options;
			this.pattern = pattern;
		}

		@Override
		public Tuple2< K, double[] > call( final Tuple2< K, Tuple2< FPTuple, double[] > > t ) throws Exception
		{

			//            if ( t._1().equals( Utility.tuple2( 10, 0 ) ) )
			//                new FileSaver( new ImagePlus( "", t._2()._1().rebuild() ) ).saveAsTiff("/groups/saalfeld/home/hanslovskyp/matrix-spark.tif");
			final Img< FloatType > matrix = ImageJFunctions.wrapFloat( new ImagePlus( "", t._2()._1().rebuild() ) );
			final AbstractCorrelationFit corrFit = options.estimateWindowRadius < 0 ? new GlobalCorrelationFitAverage() : new LocalCorrelationFitAverage( ( int ) matrix.dimension( 1 ), options );;
			final InferFromMatrix inference = new InferFromMatrix( corrFit );
			// InferFromMatrix inference = new
			// InferFromMatrix(LocalizedCorrelationFitConstant.generateTranslation1D(),
			// new OpinionMediatorWeightedAverage());
			//            Visitor visitor = new Visitor() {
			//                @Override
			//                public <T extends RealType<T>> void act(int iteration, RandomAccessibleInterval<T> matrix, double[] lut, int[] permutation, int[] inversePermutation, double[] multipliers, double[] weights, RandomAccessibleInterval<double[]> estimatedFit) {
			//                    System.out.println( "VISITOR: " + t._1() + Arrays.toString(lut) + " " + iteration);
			//                    System.out.flush();
			//                }
			//            };
			Visitor visitor = new LazyVisitor();
			final ArrayImg< DoubleType, DoubleArray > img = ArrayImgs.doubles( t._2()._2().length, options.nIterations + 1 );
			visitor = new WriteTransformationVisitor( img );
			try
			{
				final double[] coordinates = inference.estimateZCoordinates( matrix, t._2()._2(), visitor, options );
				for ( final double c : coordinates )
					if ( Double.isNaN( c ) )
					{
						System.err.println( "Inferred NaN value for coordinate " + t._1() );
						return Utility.tuple2( t._1(), null );
					}
				//				final String path = String.format( pattern, t._1().toString() );
				//				Files.createDirectories( new File( path ).getParentFile().toPath() );
				//				new FileSaver( ImageJFunctions.wrapFloat( img, "" ) ).saveAsTiff( path );
				return Utility.tuple2( t._1(), coordinates );
			}
			catch ( final NotEnoughDataPointsException e )
			{
				//                String msg = e.getMessage();
				//                new ImagePlus(t._1().toString(),t._2()._1().rebuild()).show();
				System.err.println( "Fail at inference for coordinate " + t._1() );
				e.printStackTrace( System.err );
				return Utility.tuple2( t._1(), null );
				//                throw e;
				//                throw new NotEnoughDataPointsException( t._1() + " " + msg );
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
		public < T extends RealType< T > > void act( final int iteration, final RandomAccessibleInterval< T > matrix, final RandomAccessibleInterval< T > scaledMatrix, final double[] lut, final int[] permutation, final int[] inversePermutation, final double[] multipliers, final RandomAccessibleInterval< double[] > estimatedFit )
		{
			final Cursor< DoubleType > current = Views.flatIterable( Views.hyperSlice( img, 1, iteration ) ).cursor();
			for ( int z = 0; current.hasNext(); ++z )
				current.next().set( lut[ z ] );
		}

	}
}
