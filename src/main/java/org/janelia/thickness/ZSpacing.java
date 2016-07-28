package org.janelia.thickness;

import java.io.File;
import java.io.IOException;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.HashMap;
import java.util.List;

import org.apache.spark.SparkConf;
import org.apache.spark.api.java.JavaPairRDD;
import org.apache.spark.api.java.JavaSparkContext;
import org.apache.spark.api.java.function.PairFunction;
import org.janelia.thickness.inference.Options;
import org.janelia.thickness.similarity.ComputeMatricesChunked;
import org.janelia.thickness.utility.DPTuple;
import org.janelia.thickness.utility.Utility;
import org.kohsuke.args4j.Argument;
import org.kohsuke.args4j.CmdLineException;
import org.kohsuke.args4j.CmdLineParser;

import ij.ImagePlus;
import ij.io.FileSaver;
import ij.process.ByteProcessor;
import ij.process.FloatProcessor;
import loci.formats.FormatException;
import scala.Tuple2;

/**
 * 
 * @author Philipp Hanslovsky &lt;hanslovskyp@janelia.hhmi.org&gt;
 *
 */
public class ZSpacing
{
	
	private static class Parameters {
		
		@Argument( metaVar = "CONFIG_PATH" )
		private String configPath;
		
		private boolean parsedSuccessfully;
	}

	public static void main( final String[] args ) throws FormatException, IOException
	{
		
		Parameters p = new Parameters();
		CmdLineParser parser = new CmdLineParser( p );
    	try {
			parser.parseArgument(args);
			p.parsedSuccessfully = true;
		} catch (CmdLineException e) {
            // handling of wrong arguments
			System.err.println(e.getMessage());
			parser.printUsage(System.err);
			p.parsedSuccessfully = false;
		}

        if ( p.parsedSuccessfully )

        {
			final SparkConf conf = new SparkConf()
					.setAppName( "ZSpacing" )
					.set( "spark.network.timeout", "600" )
					.set("spark.serializer", "org.apache.spark.serializer.KryoSerializer")
					.set("spark.kryo.registrator", KryoSerialization.Registrator.class.getName())
					;
	
			final JavaSparkContext sc = new JavaSparkContext( conf );
			final ScaleOptions scaleOptions = ScaleOptions.createFromFile( p.configPath );
	
			run( sc, scaleOptions );
	
			sc.close();
        }

	}

	public static void run(
			final JavaSparkContext sc,
			final ScaleOptions scaleOptions ) throws FormatException, IOException
	{
		final String sourcePattern = scaleOptions.source;
		final String root = scaleOptions.target;
		final String outputFolder = root + "/%02d";
		final int imageScaleLevel = scaleOptions.scale;
		final int start = scaleOptions.start;
		final int stop = scaleOptions.stop;
		final int size = stop - start;

		final ArrayList< Integer > indices = Utility.arange( start, stop );
		final JavaPairRDD< Integer, FloatProcessor > sections = sc
				.parallelize( indices )
				.mapToPair( new Utility.Duplicate<>() )
				.sortByKey()
				.map( new Utility.DropValue<>() )
				.mapToPair( new Utility.LoadFileFromPattern( sourcePattern ) )
				.mapToPair( new Utility.DownSample<>( imageScaleLevel ) )
				.cache();

		final FloatProcessor firstImg = sections.take( 1 ).get( 0 )._2();
		final int width = firstImg.getWidth();
		final int height = firstImg.getHeight();

		final double[] startingCoordinates = new double[ size ];
		for ( int i = 0; i < size; ++i )
			startingCoordinates[ i ] = i;

		final ArrayList< Tuple2< Tuple2< Integer, Integer >, double[] > > coordinatesList = new ArrayList< Tuple2< Tuple2< Integer, Integer >, double[] > >();
		coordinatesList.add( Utility.tuple2( Utility.tuple2( 0, 0 ), startingCoordinates ) );
		JavaPairRDD< Tuple2< Integer, Integer >, double[] > coordinates = sc.parallelizePairs( coordinatesList, 1 );

		final int[][] radiiArray = scaleOptions.radii;
		final int[][] stepsArray = scaleOptions.steps;
		final Options[] options = scaleOptions.inference;

		int maxRange = 0;
		for ( int i = 0; i < options.length; ++i )
			maxRange = Math.max( maxRange, options[ i ].comparisonRange );

		final int[] dim = new int[] { width, height };

		final ArrayList< Tuple2< Long, Long > > times = new ArrayList< Tuple2< Long, Long > >();

		final HashMap< Integer, ArrayList< Integer > > indexPairs = new HashMap< Integer, ArrayList< Integer > >();
		for ( int i = 0; i < size; ++i )
			indexPairs.put( i, Utility.arange( i + 1, Math.min( i + maxRange + 1, size ) ) );

		// huge join here!
//        JavaPairRDD<Tuple2<Integer, Integer>, Tuple2<Tuple2<FPTuple, FPTuple>, Tuple2<FPTuple, FPTuple>>> sectionPairs =
//                JoinFromList.projectOntoSelf(sections, sc.broadcast(indexPairs)).cache();
//
//        JavaPairRDD<Tuple2<Integer, Integer>, Tuple2<FPTuple, FPTuple>> sectionPairsDropWeights = sectionPairs.mapToPair(new PairFunction<Tuple2<Tuple2<Integer, Integer>, Tuple2<Tuple2<FPTuple, FPTuple>, Tuple2<FPTuple, FPTuple>>>, Tuple2<Integer, Integer>, Tuple2<FPTuple, FPTuple>>() {
//            @Override
//            public Tuple2<Tuple2<Integer, Integer>, Tuple2<FPTuple, FPTuple>> call(Tuple2<Tuple2<Integer, Integer>, Tuple2<Tuple2<FPTuple, FPTuple>, Tuple2<FPTuple, FPTuple>>> t) throws Exception {
//                return Utility.tuple2(
//                        t._1(),
//                        Utility.tuple2( t._2()._1()._1(), t._2()._2()._1() )
//                );
//            }
//        });
//        MatrixGenerationFromImagePairs matrixGenerator = new MatrixGenerationFromImagePairs(sc, sectionPairsDropWeights, dim, size, 0);
//        matrixGenerator.ensurePersistence();
//        TolerantNCC tolerantNCC = new TolerantNCC(sectionPairs);
//        tolerantNCC.ensurePersistence();
//		final JavaPairRDD< Integer, FloatProcessor > sectionsDropWeights = sections.mapToPair(new DropWeights<>());
		final ComputeMatricesChunked generator = new ComputeMatricesChunked( sc, sections, scaleOptions.joinStepSize, maxRange, dim, true );

		for ( int i = 0; i < radiiArray.length; ++i )
		{
			System.out.println( "i=" + i );
			System.out.println( "Options [" + i + "] = \n" + options[ i ].toString() );

			new File( String.format( outputFolder, i ) ).mkdirs();

			final long tStart = System.currentTimeMillis();

			final int[] currentOffset = radiiArray[ i ];
			final int[] currentStep = stepsArray[ i ];
			final int[] currentDim = getDimensionsForStepAndRadius( dim, currentStep, currentOffset );

			final int[] previousOffset = i > 0 ? radiiArray[ i - 1 ] : new int[] { 0, 0 };
			final int[] previousStep = i > 0 ? stepsArray[ i - 1 ] : new int[] { width, height };
			final int[] previousDim = getDimensionsForStepAndRadius( dim, previousStep, previousOffset );

			final JavaPairRDD< Tuple2< Integer, Integer >, double[] > currentCoordinates;
			if ( i == 0 )
			{
				currentCoordinates = coordinates;
			}
			else
			{
				Tuple2<Double, Double> min = Utility.tuple2( 0.0, 0.0 );
				Tuple2<Double, Double> max = Utility.tuple2( (double)previousDim[0]-1, (double)previousDim[1]-1);
				final BlockCoordinates cbs1 = new BlockCoordinates( previousOffset, previousStep );
				final BlockCoordinates cbs2 = new BlockCoordinates( currentOffset, currentStep );
				final ArrayList< BlockCoordinates.Coordinate > newCoords = cbs2.generateFromBoundingBox( dim );

				final ArrayList< Tuple2< Tuple2< Integer, Integer >, Tuple2< Double, Double > > > mapping = new ArrayList< Tuple2< Tuple2< Integer, Integer >, Tuple2< Double, Double > > >();
				for ( final BlockCoordinates.Coordinate n : newCoords )
				{
					mapping.add( Utility.tuple2( 
							n.getLocalCoordinates(), 
							Utility.max( Utility.min( cbs1.translateOtherLocalCoordiantesIntoLocalSpace(n), max ), min )
							) );
				}

				currentCoordinates = SparkInterpolation.interpolate( sc, coordinates, sc.broadcast( mapping ), previousDim, new SparkInterpolation.NearestNeighbor() );
			}

			currentCoordinates.cache();
			System.out.println( "Calculated " + currentCoordinates.count() + " coordinates" );

			final JavaPairRDD< Tuple2< Integer, Integer >, FloatProcessor > matrices =
					generator.run( options[ i ].comparisonRange, currentStep, currentOffset );

			System.out.println( "Calculated " + matrices.cache().count() + " matrices" );
			System.out.println( Arrays.toString( dim ) + " " + Arrays.toString( currentOffset ) + " " + Arrays.toString( currentStep ) + matrices.take( 1 ).get( 0 )._1() );

			if ( scaleOptions.logMatrices[ i ] )
			{
				final String outputFormatMatrices = String.format( outputFolder, i ) + "/matrices/%s.tif";
				matrices
						.mapToPair( new Utility.WriteToFormatString< Tuple2< Integer, Integer > >( outputFormatMatrices ) )
						.collect();
			}

			System.out.println( currentCoordinates.take( 1 ).get( 0 )._1() );

			System.out.println( "Before inference." );
			System.out.println( options[ i ].toString() + " " + options[ i ].comparisonRange );

			final String lutPattern = String.format( outputFolder, i ) + "/luts/%s.tif";

			final Chunk chunk = new Chunk( scaleOptions.chunkSizes[ i ], scaleOptions.overlaps[ i ], size, options[ i ].comparisonRange );

			final JavaPairRDD< Tuple2< Integer, Integer >, double[] > result =
					SparkInference.inferCoordinates(sc, matrices, currentCoordinates, chunk, options[ i ], lutPattern ).cache();
			result.count();
			System.out.println( "Inference done!" );

			// log success and failure
			final String successAndFailurePath = String.format( outputFolder, i ) + "/successAndFailure.tif";
			final ByteProcessor ip = LogSuccessAndFailure.log( sc, result, currentDim );
			System.out.println( "Wrote status image? " + new FileSaver( new ImagePlus( "", ip ) ).saveAsTiff( successAndFailurePath ) );

			final ColumnsAndSections columnsAndSections = new ColumnsAndSections( currentDim, size );
			final JavaPairRDD< Integer, DPTuple > coordinateSections = columnsAndSections.columnsToSections( sc, result );

			coordinates = columnsAndSections.sectionsToColumns( sc, coordinateSections );
			final JavaPairRDD< Tuple2< Integer, Integer >, double[] > backward = coordinates.mapToPair( new Utility.InvertLut<>() );
			final JavaPairRDD< Integer, DPTuple > backwardImages = columnsAndSections.columnsToSections( sc, backward );

			final String outputFormat = String.format( outputFolder, i ) + "/forward/%04d.tif";
			final List< Tuple2< Integer, Boolean > > success = coordinateSections
					.mapToPair( new Utility.WriteToFormatStringDouble< Integer >( outputFormat ) )
					.collect();

			final String outputFormatBackward = String.format( outputFolder, i ) + "/backward/%04d.tif";
			final List< Tuple2< Integer, Boolean > > successBackward = backwardImages
					.mapToPair( new Utility.WriteToFormatStringDouble< Integer >( outputFormatBackward ) )
					.collect();
//
			if ( i < 3 )
			{
				final String outputFormatTransformedMatrices = String.format( outputFolder, i ) + "/transformed-matrices/%s.tif";
				matrices
						.join( backward )
						.mapToPair( new Utility.Transform< Tuple2< Integer, Integer > >() )
						.mapToPair( new Utility.WriteToFormatString< Tuple2< Integer, Integer > >( outputFormatTransformedMatrices ) )
						.collect();
			}

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

			System.out.println(
					"Successfully wrote " + ( success.size() - count ) + "/" + success.size() + " (forward) and " +
							( successBackward.size() - countBackward ) + "/" + successBackward.size() + " (backward) " +
							"images at iteration " + i + String.format( " in %25dms", ( tEnd - tStart ) ) );

			times.add( Utility.tuple2( tStart, tEnd ) );
		}

		for ( final Tuple2< Long, Long > t : times )
		{
			final long diff = t._2().longValue() - t._1().longValue();
			System.out.println( String.format( "Run time for complete iteration: %25dms", diff ) );
		}
	}

	public static int[] getDimensionsForStepAndRadius( int[] dim, int[] step, int[] radius )
	{
		return new int[] {
				Math.max( 1, ( int ) Math.ceil( ( dim[ 0 ] - radius[ 0 ] ) * 1.0 / step[ 0 ] ) ),
				Math.max( 1, ( int ) Math.ceil( ( dim[ 1 ] - radius[ 1 ] ) * 1.0 / step[ 1 ] ) )
		};
	}
	
	public static class DropWeights< K, V1, V2 > implements PairFunction< Tuple2< K, Tuple2< V1, V2 > >, K, V1 >
	{

		/**
		 * 
		 */
		private static final long serialVersionUID = -2378364940202047703L;

		@Override
		public Tuple2<K, V1> call(Tuple2<K, Tuple2<V1, V2>> t) throws Exception {
			return Utility.tuple2( t._1(), t._2()._1() );
		}
		
	}

}
