package org.janelia.thickness;

import ij.ImagePlus;
import ij.io.FileSaver;
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

import java.io.File;
import java.nio.file.Files;

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
import scala.Tuple2;

public class SparkInference
{

	public static JavaPairRDD< Tuple2< Integer, Integer >, double[] > inferCoordinates( JavaSparkContext sc, JavaPairRDD< Tuple2< Integer, Integer >, FPTuple > matrices, JavaPairRDD< Tuple2< Integer, Integer >, double[] > startingCoordinates, final Options options, final String pattern )
	{
		JavaPairRDD< Tuple2< Integer, Integer >, Tuple2< FPTuple, double[] > > matricesWithStartingCoordinates = matrices.join( startingCoordinates );

		System.out.flush();
		JavaPairRDD< Tuple2< Integer, Integer >, double[] > result = matricesWithStartingCoordinates.mapToPair( new Inference< Tuple2< Integer, Integer > >( options, pattern ) );

		return result;
	}

	public static class Inference< K > implements PairFunction< Tuple2< K, Tuple2< FPTuple, double[] > >, K, double[] >
	{
		private static final long serialVersionUID = 8094812748656050753L;

		private final Options options;

		private final String pattern;

		public Inference( Options options, String pattern )
		{
			super();
			this.options = options;
			this.pattern = pattern;
		}

		public Tuple2< K, double[] > call( final Tuple2< K, Tuple2< FPTuple, double[] > > t ) throws Exception
		{

//            if ( t._1().equals( Utility.tuple2( 10, 0 ) ) )
//                new FileSaver( new ImagePlus( "", t._2()._1().rebuild() ) ).saveAsTiff("/groups/saalfeld/home/hanslovskyp/matrix-spark.tif");
			Img< FloatType > matrix = ImageJFunctions.wrapFloat( new ImagePlus( "", t._2()._1().rebuild() ) );
			AbstractCorrelationFit corrFit = options.estimateWindowRadius < 0 ? new GlobalCorrelationFitAverage() : new LocalCorrelationFitAverage( ( int ) matrix.dimension( 1 ), options );;
			InferFromMatrix inference = new InferFromMatrix( corrFit );
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
			ArrayImg< DoubleType, DoubleArray > img = ArrayImgs.doubles( t._2()._2().length, options.nIterations + 1 );
			visitor = new WriteTransformationVisitor( img );
			try
			{
				double[] coordinates = inference.estimateZCoordinates( matrix, t._2()._2(), visitor, options );
				for ( double c : coordinates )
				{
					if ( Double.isNaN( c ) )
					{
						System.err.println( "Inferred NaN value for coordinate " + t._1() );
						return Utility.tuple2( t._1(), null );
					}
				}
				String path = String.format( pattern, t._1().toString() );
				Files.createDirectories( new File( path ).getParentFile().toPath() );
				new FileSaver( ImageJFunctions.wrapFloat( img, "" ) ).saveAsTiff( path );
				return Utility.tuple2( t._1(), coordinates );
			}
			catch ( NotEnoughDataPointsException e )
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

		public WriteTransformationVisitor( RandomAccessibleInterval< DoubleType > img )
		{
			this.img = img;
		}

		@Override
		public < T extends RealType< T > > void act( int iteration, RandomAccessibleInterval< T > matrix, RandomAccessibleInterval< T > scaledMatrix, double[] lut, int[] permutation, int[] inversePermutation, double[] multipliers, RandomAccessibleInterval< double[] > estimatedFit )
		{
			Cursor< DoubleType > current = Views.flatIterable( Views.hyperSlice( img, 1, iteration ) ).cursor();
			for ( int z = 0; current.hasNext(); ++z )
				current.next().set( lut[ z ] );
		}

	}
}
