package org.janelia.thickness;

import java.io.File;
import java.io.IOException;
import java.nio.file.Files;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;

import org.apache.spark.SparkConf;
import org.apache.spark.api.java.JavaPairRDD;
import org.apache.spark.api.java.JavaSparkContext;
import org.apache.spark.api.java.function.Function;
import org.apache.spark.api.java.function.PairFunction;
import org.janelia.thickness.inference.Options;
import org.janelia.thickness.utility.DPTuple;
import org.janelia.thickness.utility.FPTuple;
import org.janelia.thickness.utility.Utility;

import ij.ImagePlus;
import ij.io.FileSaver;
import ij.process.ByteProcessor;
import loci.formats.FormatException;
import scala.Tuple2;

public class ZSpacing
{

	public static void main( final String[] args ) throws FormatException, IOException
	{
		final SparkConf conf = new SparkConf().setAppName( "ZSpacing" );

		final JavaSparkContext sc = new JavaSparkContext( conf );

		final String scaleOptionsPath = args[ 0 ];
		final ScaleOptions scaleOptions = ScaleOptions.createFromFile( scaleOptionsPath );

		run( sc, scaleOptions );

	}

	public static void run( final JavaSparkContext sc, final ScaleOptions scaleOptions ) throws FormatException, IOException
	{

		final String sourcePattern = scaleOptions.source;

		final String root = scaleOptions.target;
		final String outputFolder = root + "/%02d";
		final int imageScaleLevel = scaleOptions.scale;

		final int start = scaleOptions.start;
		final int stop = scaleOptions.stop;
		final int size = stop - start;

		final ArrayList< Integer > indices = Utility.arange( size );
		final JavaPairRDD< Integer, FPTuple > sections = sc.parallelize( indices ).mapToPair( new PairFunction< Integer, Integer, Integer >()
		{

			@Override
			public Tuple2< Integer, Integer > call( final Integer arg0 ) throws Exception
			{
				return Utility.tuple2( arg0, arg0 );
			}
		} ).sortByKey().map( new Function< Tuple2< Integer, Integer >, Integer >()
		{

			@Override
			public Integer call( final Tuple2< Integer, Integer > arg0 ) throws Exception
			{
				return arg0._1();
			}
		} ).mapToPair( new Utility.LoadFileFromPattern( sourcePattern ) ).mapToPair( new Utility.DownSample< Integer >( imageScaleLevel ) );

		final FPTuple firstImg = sections.take( 1 ).get( 0 )._2();
		final int width = firstImg.width;
		final int height = firstImg.height;

		final double[] startingCoordinates = new double[ size ];
		for ( int i = 0; i < size; ++i )
			startingCoordinates[ i ] = i;

		final ArrayList< Tuple2< Tuple2< Integer, Integer >, double[] > > coordinatesList = new ArrayList<>();
		coordinatesList.add( Utility.tuple2( Utility.tuple2( 0, 0 ), startingCoordinates ) );
		JavaPairRDD< Tuple2< Integer, Integer >, double[] > coordinates = sc.parallelizePairs( coordinatesList, 1 );

		final int[][] radiiArray = scaleOptions.radii;
		final int[][] stepsArray = scaleOptions.steps;
		final int[][] correlationBlockRadiiArray = scaleOptions.correlationBlockRadii;
		final int[][] maxOffsetsArray = scaleOptions.maxOffsets;
		final Options[] options = scaleOptions.inference;

		int maxRange = 0;
		for ( int i = 0; i < options.length; ++i )
			maxRange = Math.max( maxRange, options[ i ].comparisonRange );

		final int[] dim = new int[] { width, height };

		final ArrayList< Tuple2< Long, Long > > times = new ArrayList<>();

		final HashMap< Integer, ArrayList< Integer > > indexPairs = new HashMap<>();
		for ( int i = 0; i < size; ++i )
			indexPairs.put( i, Utility.arange( i + 1, Math.min( i + maxRange + 1, size ) ) );

		final JavaPairRDD< Tuple2< Integer, Integer >, Tuple2< FPTuple, FPTuple > > sectionPairs = JoinFromList.projectOntoSelf( sections, sc.broadcast( indexPairs ) );
		sectionPairs.cache();
		sectionPairs.count();

		final MatrixGenerationFromImagePairs matrixGenerator = new MatrixGenerationFromImagePairs( sc, sectionPairs, dim, size );

		for ( int i = 0; i < radiiArray.length; ++i )
		{
			// IJ.log( "i=" + i );
			System.out.println( "i=" + i );
			System.out.println( "Options [" + i + "] = \n" + options[ i ].toString() );

			final long tStart = System.currentTimeMillis();

			final int[] currentOffset = radiiArray[ i ];
			final int[] currentStep = stepsArray[ i ];
			final int[] currentDim = new int[] { Math.max( 1, ( int ) Math.ceil( ( dim[ 0 ] - currentOffset[ 0 ] ) * 1.0 / currentStep[ 0 ] ) ), Math.max( 1, ( int ) Math.ceil( ( dim[ 1 ] - currentOffset[ 1 ] ) * 1.0 / currentStep[ 1 ] ) )
			};
			// IJ.log( "i=" + i + ": " + Arrays.toString( currentDim) +
			// Arrays.toString( currentOffset ) + Arrays.toString( currentStep )
			// + " " + size );

			final int[] previousOffset = i > 0 ? radiiArray[ i - 1 ] : new int[] { 0, 0 };
			final int[] previousStep = i > 0 ? stepsArray[ i - 1 ] : new int[] { width, height };
			final int[] previousDim = new int[] { Math.max( 1, ( int ) Math.ceil( ( dim[ 0 ] - previousOffset[ 0 ] ) * 1.0 / previousStep[ 0 ] ) ), Math.max( 1, ( int ) Math.ceil( ( dim[ 1 ] - previousOffset[ 1 ] ) * 1.0 / previousStep[ 1 ] ) )
			};

			final CorrelationBlocks cbs = new CorrelationBlocks( currentOffset, currentStep );
			final ArrayList< CorrelationBlocks.Coordinate > xyCoordinatesLocalAndWorld = cbs.generateFromBoundingBox( dim );
			final ArrayList< Tuple2< Integer, Integer > > xyCoordinates = new ArrayList<>();
			for ( final CorrelationBlocks.Coordinate xy : xyCoordinatesLocalAndWorld )
				xyCoordinates.add( xy.getLocalCoordinates() );

			// min and max of previous step
			final Tuple2< Integer, Integer > xMinMax;
			final Tuple2< Integer, Integer > yMinMax;
			if ( xyCoordinates.size() == 0 )
			{
				final Tuple2< Integer, Integer > t = Utility.tuple2( currentOffset[ 0 ], currentOffset[ 1 ] );
				xyCoordinates.add( t );

				xMinMax = Utility.tuple2( previousOffset[ 0 ], previousOffset[ 0 ] );
				yMinMax = Utility.tuple2( previousOffset[ 1 ], previousOffset[ 1 ] );
			}
			else
			{
				xMinMax = Utility.tuple2( 0, ( dim[ 0 ] - 2 * previousOffset[ 0 ] - 1 ) / previousStep[ 0 ] * previousStep[ 0 ] );
				yMinMax = Utility.tuple2( 0, ( dim[ 1 ] - 2 * previousOffset[ 1 ] - 1 ) / previousStep[ 1 ] * previousStep[ 1 ] );
			}

			JavaPairRDD< Tuple2< Integer, Integer >, double[] > currentCoordinates;
			if ( i == 0 )
				currentCoordinates = coordinates;
			else
			{
				final CorrelationBlocks cbs1 = new CorrelationBlocks( previousOffset, previousStep );
				final CorrelationBlocks cbs2 = new CorrelationBlocks( currentOffset, currentStep );
				final ArrayList< CorrelationBlocks.Coordinate > newCoords = cbs2.generateFromBoundingBox( dim );
				final ArrayList< Tuple2< Tuple2< Integer, Integer >, Tuple2< Double, Double > > > mapping = new ArrayList<>();
				for ( final CorrelationBlocks.Coordinate n : newCoords )
					mapping.add( Utility.tuple2( n.getLocalCoordinates(), cbs1.translateCoordinateIntoThisBlockCoordinates( n ) ) );
				currentCoordinates = SparkInterpolation.interpolate( sc, coordinates, sc.broadcast( mapping ), previousDim, new SparkInterpolation.MatchCoordinates.NearestNeighborMatcher() );
			}
			coordinates.unpersist();

			final JavaPairRDD< Tuple2< Integer, Integer >, FPTuple > matrices = matrixGenerator.generateMatrices( currentStep, currentOffset, options[ i ].comparisonRange );
			matrices.cache();

			System.out.println( "Calculated " + matrices.count() + " matrices" );

			final String outputFormatMatrices = String.format( outputFolder, i ) + "/matrices/%s.tif";
			// Write matrices
			// matrices.mapToPair( new Utility.WriteToFormatString< Tuple2<
			// Integer, Integer > >( outputFormatMatrices ) ).collect();

			System.out.println( "Before inference." );
			System.out.println( options[ i ].toString() + " " + options[ i ].comparisonRange );

			final String lutPattern = String.format( outputFolder, i ) + "/luts/%s.tif";

			final JavaPairRDD< Tuple2< Integer, Integer >, double[] > result = SparkInference.inferCoordinates( sc, matrices, currentCoordinates, options[ i ], lutPattern );
			result.cache();
			final long t0Inference = System.nanoTime();
			result.count();
			final long t1Inference = System.nanoTime();
			final long dtInference = t1Inference - t0Inference;
			System.out.println( "Inference done! (" + dtInference + "ns)" );

			// last occurence of matrices, unpersist!
			matrices.unpersist();

			// log success and failure
			final String successAndFailurePath = String.format( outputFolder, i ) + "/successAndFailure.tif";
			final ByteProcessor ip = LogSuccessAndFailure.log( sc, result, currentDim );
			Files.createDirectories( new File( successAndFailurePath ).getParentFile().toPath() );
			System.out.println( "Wrote status image? " + new FileSaver( new ImagePlus( "", ip ) ).saveAsTiff( successAndFailurePath ) );

			final ColumnsAndSections columnsAndSections = new ColumnsAndSections( currentDim, size );
			final JavaPairRDD< Integer, DPTuple > coordinateSections = columnsAndSections.columnsToSections( sc, result );

			// last occurence of result, unpersist!
			result.unpersist();

			final JavaPairRDD< Integer, DPTuple > diffused = coordinateSections
					// TODO write something like diffusion: done?
					.mapToPair( new PairFunction< Tuple2< Integer, DPTuple >, Integer, DPTuple >()
					{
						@Override
						public Tuple2< Integer, DPTuple > call( final Tuple2< Integer, DPTuple > t ) throws Exception
						{
							return Utility.tuple2( t._1(), FillHoles.fill( t._2().clone() ) );
						}
					} );

			coordinates = columnsAndSections.sectionsToColumns( sc, diffused );
			coordinates.cache();
			final JavaPairRDD< Tuple2< Integer, Integer >, double[] > backward = Render.invert( sc, coordinates ).cache();
			final JavaPairRDD< Integer, DPTuple > backwardImages = columnsAndSections.columnsToSections( sc, backward );

			final String outputFormat = String.format( outputFolder, i ) + "/forward/%04d.tif";
			final List< Tuple2< Integer, Boolean > > success = diffused.mapToPair( new Utility.WriteToFormatStringDouble< Integer >( outputFormat ) ).collect();

			final String outputFormatBackward = String.format( outputFolder, i ) + "/backward/%04d.tif";
			final List< Tuple2< Integer, Boolean > > successBackward = backwardImages.mapToPair( new Utility.WriteToFormatStringDouble< Integer >( outputFormatBackward ) ).collect();

			// write transformed matrices
			//			final String outputFormatTransformedMatrices = String.format( outputFolder, i ) + "/transformed-matrices/%s.tif";
			//			matrices.join( backward ).mapToPair( new Utility.Transform< Tuple2< Integer, Integer > >() ).mapToPair( new Utility.WriteToFormatString< Tuple2< Integer, Integer > >( outputFormatTransformedMatrices ) ).collect();

			// last occurence of backward, unpersist!
			backward.unpersist();

			int count = 0;
			for ( final Tuple2< Integer, Boolean > s : success )
			{
				if ( s._2().booleanValue() )
					continue;
				++count;
				System.out.println( "Failed to write forward image " + s._1().intValue() );
			}

			int countBackward = 0;
			for ( final Tuple2< Integer, Boolean > s : successBackward )
			{
				if ( s._2().booleanValue() )
					continue;
				++countBackward;
				System.out.println( "Failed to write backward image " + s._1().intValue() );
			}

			final long tEnd = System.currentTimeMillis();

			System.out.println( "Successfully wrote " + ( success.size() - count ) + "/" + success.size() + " (forward) and " + ( successBackward.size() - countBackward ) + "/" + successBackward.size() + " (backward) " + "images at iteration " + i + String.format( " in %25dms", tEnd - tStart ) );

			times.add( Utility.tuple2( tStart, tEnd ) );
		}

		for ( final Tuple2< Long, Long > t : times )
		{
			final long diff = t._2().longValue() - t._1().longValue();
			System.out.println( String.format( "Run time for complete iteration: %25dms", diff ) );
		}

		sc.close();
	}

}
