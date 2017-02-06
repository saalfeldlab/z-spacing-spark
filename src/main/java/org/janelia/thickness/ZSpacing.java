package org.janelia.thickness;

import java.io.File;
import java.io.IOException;
import java.lang.invoke.MethodHandles;
import java.lang.reflect.InvocationTargetException;
import java.nio.file.Files;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.HashMap;
import java.util.List;

import org.apache.log4j.Level;
import org.apache.log4j.LogManager;
import org.apache.log4j.Logger;
import org.apache.spark.SparkConf;
import org.apache.spark.api.java.JavaPairRDD;
import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.api.java.JavaSparkContext;
import org.apache.spark.api.java.function.Function;
import org.apache.spark.broadcast.Broadcast;
import org.apache.spark.storage.StorageLevel;
import org.janelia.thickness.SparkInference.Variables;
import org.janelia.thickness.inference.Options;
import org.janelia.thickness.similarity.ComputeMatricesChunked;
import org.janelia.thickness.similarity.DefaultMatrixGenerator;
import org.janelia.thickness.similarity.ImageAndMask;
import org.janelia.thickness.utility.DPTuple;
import org.janelia.thickness.utility.Utility;
import org.janelia.thickness.weight.NoWeightsCalculator;
import org.janelia.thickness.weight.OnlyShiftWeightsCalculator;
import org.janelia.thickness.weight.Weights;
import org.janelia.thickness.weight.WeightsCalculator;
import org.kohsuke.args4j.Argument;
import org.kohsuke.args4j.CmdLineException;
import org.kohsuke.args4j.CmdLineParser;

import ij.ImagePlus;
import ij.io.FileSaver;
import ij.process.ByteProcessor;
import ij.process.FloatProcessor;
import loci.formats.FormatException;
import pl.joegreen.lambdaFromString.LambdaCreationException;
import pl.joegreen.lambdaFromString.LambdaFactory;
import pl.joegreen.lambdaFromString.TypeReference;
import scala.Tuple2;

/**
 *
 * @author Philipp Hanslovsky
 *
 */
public class ZSpacing
{

	public static Logger LOG = LogManager.getLogger( MethodHandles.lookup().lookupClass() );
	static
	{
		LOG.setLevel( Level.INFO );
	}

	private static class Parameters
	{

		@Argument( metaVar = "CONFIG_PATH" )
		private String configPath;

		private boolean parsedSuccessfully;
	}

	public static void main( final String[] args ) throws FormatException, IOException, InstantiationException, IllegalAccessException, IllegalArgumentException, InvocationTargetException, NoSuchMethodException, SecurityException, ClassNotFoundException, LambdaCreationException
	{

		final Parameters p = new Parameters();
		final CmdLineParser parser = new CmdLineParser( p );
		try
		{
			parser.parseArgument( args );
			p.parsedSuccessfully = true;
		}
		catch ( final CmdLineException e )
		{
			System.err.println( e.getMessage() );
			parser.printUsage( System.err );
			p.parsedSuccessfully = false;
		}

		if ( p.parsedSuccessfully )

		{
			final SparkConf conf = new SparkConf()
					.setAppName( "ZSpacing" )
					.set( "spark.network.timeout", "600" )
					.set( "spark.serializer", "org.apache.spark.serializer.KryoSerializer" )
					.set( "spark.kryoserializer.buffer.max", "1g" )
					.set( "spark.kryo.unsafe", "true" ) // supposed to give huge
														// performance boost for
														// primitive arrays
					.set( "spark.kryo.registrator", KryoSerialization.Registrator.class.getName() );

			final JavaSparkContext sc = new JavaSparkContext( conf );
			final ScaleOptions scaleOptions = ScaleOptions.createFromFile( p.configPath );

			run( sc, scaleOptions );


			sc.close();
		}
	}

	public static void run( final JavaSparkContext sc, final ScaleOptions scaleOptions ) throws FormatException, IOException, InstantiationException, IllegalAccessException, IllegalArgumentException, InvocationTargetException, NoSuchMethodException, SecurityException, ClassNotFoundException, LambdaCreationException
	{
		if ( scaleOptions.fileOpener == null )
			run( sc, scaleOptions, new Utility.LoadFileFromPattern( scaleOptions.source ) );
		else if ( Utility.classExists( scaleOptions.fileOpener ) )
		{
			LOG.info( "Using existing class for fileOpener: " + scaleOptions.fileOpener );
			// eg "fileOpener" :
			// "org.janelia.thickness.utility.Utility$LoadFileFromPattern"
			run( sc, scaleOptions, ( Function< Integer, FloatProcessor > ) Class.forName( scaleOptions.fileOpener ).getConstructor( String.class ).newInstance( scaleOptions.source ) );
		}
		else
		{
			LOG.info( "Using lambda for fileOpener: " + scaleOptions.fileOpener );
			// eg "fileOpener" :
			// "i -> new ij.ImagePlus(String.format(\"%04d.tif\", i )
			// ).getProcessor().convertToFloatProcessor()"
			// problem: Lambda or class containing lambda expression not
			// available on remote side? Serialization issues? Solve! TODO
			final Function< Integer, FloatProcessor > fileOpener = LambdaFactory.get().createLambda( scaleOptions.fileOpener, new TypeReference< Function< Integer, FloatProcessor > >()
			{} );
			run( sc, scaleOptions, fileOpener );
		}
	}


	public static void run( final JavaSparkContext sc, final ScaleOptions scaleOptions, final Function< Integer, FloatProcessor > fileOpener ) throws FormatException, IOException
	{

		final ArrayList< Object > globalUnpersistList = new ArrayList<>();

		final Logger log = LOG;// LogManager.getRootLogger();

		final String root = scaleOptions.target;
		final String outputFolder = root + "/%02d";
		final int imageScaleLevel = scaleOptions.scale;
		final int start = scaleOptions.start;
		final int stop = scaleOptions.stop;
		final int step = scaleOptions.step;
		final ArrayList< Integer > indices = Utility.arange( start, stop, step );
		final int size = indices.size();
		final Broadcast< ArrayList< Integer > > indicesBC = sc.broadcast( indices );
		globalUnpersistList.add( indicesBC );

		final JavaRDD< Integer > sortedIndices = sc.parallelize( Utility.arange( size ) ).mapToPair( i -> Utility.tuple2( i, i ) ).sortByKey().map( arg0 -> arg0._1() ).cache();
		final JavaPairRDD< Integer, FloatProcessor > sections = sortedIndices.mapToPair( i -> new Tuple2<>( i, indicesBC.getValue().get( i ) ) ).mapValues( fileOpener ).mapToPair( new Utility.DownSample< Integer >( imageScaleLevel ) );
		sections.cache();
		sections.count();
		globalUnpersistList.add( sortedIndices );

		final FloatProcessor firstImg = sections.take( 1 ).get( 0 )._2();
		final int width = firstImg.getWidth();
		final int height = firstImg.getHeight();

		final JavaPairRDD< Integer, FloatProcessor > sectionMasks;
		if ( scaleOptions.estimateMask == null )
			sectionMasks = sections.mapValues( v -> {
				final float[] pixels = new float[ width* height];
				Arrays.fill( pixels, 1.0f );
				return new FloatProcessor( width, height, pixels );
			} );
		else
			sectionMasks = sortedIndices.mapToPair( i -> new Tuple2<>( i, indicesBC.getValue().get( i ) ) ).mapValues( new Utility.LoadFileFromPattern( scaleOptions.estimateMask ) ).mapToPair( new Utility.DownSample< Integer >( imageScaleLevel ) );

		final double[] startingCoordinates = new double[ size ];
		for ( int i = 0; i < size; ++i )
			startingCoordinates[ i ] = i;

		final ArrayList< Tuple2< Tuple2< Integer, Integer >, double[] > > coordinatesList = new ArrayList<>();
		coordinatesList.add( Utility.tuple2( Utility.tuple2( 0, 0 ), startingCoordinates ) );
		JavaPairRDD< Tuple2< Integer, Integer >, SparkInference.Variables > variables = sc
				.parallelizePairs( coordinatesList, 1 )
				.mapValues( c -> new SparkInference.Variables( c, Utility.singleValueArray( size, 1.0 ), new double[ 0 ] ) );
		globalUnpersistList.add( variables );

		final int[][] radiiArray = scaleOptions.radii;
		final int[][] stepsArray = scaleOptions.steps;
		final Options[] options = scaleOptions.inference;

		int maxRange = 0;
		for ( int i = 0; i < options.length; ++i )
			maxRange = Math.max( maxRange, options[ i ].comparisonRange );

		final int[] dim = new int[] { width, height };

		final ArrayList< Tuple2< Long, Long > > times = new ArrayList<>();

		final HashMap< Integer, ArrayList< Integer > > indexPairs = new HashMap<>();
		for ( int i = 0; i < size; ++i )
			indexPairs.put( i, Utility.arange( i + 1, Math.min( i + maxRange + 1, size ) ) );

		final JavaPairRDD< Integer, ImageAndMask > maskedSections = sections.join( sectionMasks ).mapValues( fps -> new ImageAndMask( fps._1(), fps._2() ) );

		final ComputeMatricesChunked computer = new ComputeMatricesChunked( sc, maskedSections, scaleOptions.joinStepSize, maxRange, dim, size, StorageLevel.MEMORY_AND_DISK() );
		sections.unpersist();

		final JavaPairRDD< Integer, FloatProcessor > estimateMask = scaleOptions.estimateMask == null ? null : sortedIndices.mapToPair( i -> new Tuple2<>( i, indicesBC.getValue().get( i ) ) ).mapValues( new Utility.LoadFileFromPattern( scaleOptions.estimateMask ) ).mapToPair( new Utility.DownSample< Integer >( imageScaleLevel ) );
		final JavaPairRDD< Integer, FloatProcessor > shiftMask = scaleOptions.shiftMask == null ? null : sortedIndices.mapToPair( i -> new Tuple2<>( i, indicesBC.getValue().get( i ) ) ).mapValues( new Utility.LoadFileFromPattern( scaleOptions.shiftMask ) ).mapToPair( new Utility.DownSample< Integer >( imageScaleLevel ) );

		final WeightsCalculator wc = shiftMask == null ? new NoWeightsCalculator( sc, size, dim ) : new OnlyShiftWeightsCalculator( shiftMask, dim );

		for ( int i = 0; i < radiiArray.length; ++i )
		{

			final ArrayList< Object > unpersistList = new ArrayList<>();

			log.info( MethodHandles.lookup().lookupClass().getSimpleName() + ": i=" + i );
			log.info( MethodHandles.lookup().lookupClass().getSimpleName() + ": Options [" + i + "] = \n" + options[ i ].toString() );

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

			if ( xyCoordinates.size() == 0 )
				xyCoordinates.add( Utility.tuple2( currentOffset[ 0 ], currentOffset[ 1 ] ) );

			JavaPairRDD< Tuple2< Integer, Integer >, SparkInference.Variables > currentVariables;
			if ( i == 0 )
				currentVariables = variables;
			else
			{
				final CorrelationBlocks cbs1 = new CorrelationBlocks( previousOffset, previousStep );
				final CorrelationBlocks cbs2 = new CorrelationBlocks( currentOffset, currentStep );
				final ArrayList< CorrelationBlocks.Coordinate > newCoords = cbs2.generateFromBoundingBox( dim );
				final ArrayList< Tuple2< Tuple2< Integer, Integer >, Tuple2< Double, Double > > > mapping = new ArrayList<>();
				for ( final CorrelationBlocks.Coordinate n : newCoords )
					mapping.add( Utility.tuple2( n.getLocalCoordinates(), cbs1.translateCoordinateIntoThisBlockCoordinates( n ) ) );
				currentVariables = SparkInterpolation.interpolate( sc, variables, sc.broadcast( mapping ), previousDim, new SparkInterpolation.MatchCoordinates.NearestNeighborMatcher() );
			}
			variables.unpersist();

			final JavaPairRDD< Tuple2< Integer, Integer >, Weights > masks = wc.calculate( currentOffset, currentStep );
			masks.persist( StorageLevel.MEMORY_AND_DISK() );
			unpersistList.add( masks );

			//			final JavaPairRDD< Tuple2< Integer, Integer >, FloatProcessor > matrices = matrixGenerator.generateMatrices( currentStep, currentOffset, options[ i ].comparisonRange );
			final JavaPairRDD< Tuple2< Integer, Integer >, Tuple2< FloatProcessor, FloatProcessor > > matrices = computer.run( new DefaultMatrixGenerator.Factory( dim ), options[ i ].comparisonRange, currentStep, currentOffset );
			matrices.persist( StorageLevel.MEMORY_AND_DISK() );
			unpersistList.add( matrices );

			LOG.info( "Calculated " + matrices.count() + " matrices" );

			if ( scaleOptions.logMatrices[ i ] )
			{
				final String outputFormatMatrices = String.format( outputFolder, i ) + "/matrices/%s.tif";
				// Write matrices
				matrices.mapValues( t -> t._1() ).mapToPair( new Utility.WriteToFormatString< Tuple2< Integer, Integer > >( outputFormatMatrices ) ).collect();
			}

			final String lutPattern = String.format( outputFolder, i ) + "/luts/%s.tif";

			final JavaPairRDD< Tuple2< Integer, Integer >, Tuple2< Variables, Weights > > variablesAndWeights = currentVariables.join( masks );

			final JavaPairRDD< Tuple2< Integer, Integer >, SparkInference.Variables > result =
					SparkInference.inferCoordinates( sc, matrices, variablesAndWeights, options[ i ], lutPattern );
			result.persist( StorageLevel.MEMORY_AND_DISK() );
			final long t0Inference = System.nanoTime();
			result.count();
			final long t1Inference = System.nanoTime();
			final long dtInference = t1Inference - t0Inference;
			LOG.info( "Inference done! (" + dtInference + "ns) " );


			// log success and failure
			final String successAndFailurePath = String.format( outputFolder, i ) + "/successAndFailure.tif";
			final ByteProcessor ip = LogSuccessAndFailure.log( sc, result, currentDim );
			Files.createDirectories( new File( successAndFailurePath ).getParentFile().toPath() );
			LOG.info( "Wrote status image? " + new FileSaver( new ImagePlus( "", ip ) ).saveAsTiff( successAndFailurePath ) );

			final ColumnsAndSections columnsAndSections = new ColumnsAndSections( currentDim, size );
			variables.unpersist();
			variables = result;
			variables.persist( StorageLevel.MEMORY_AND_DISK() );
			variables.count();

			final JavaPairRDD< Tuple2< Integer, Integer >, double[] > coordinates = variables.mapValues( v -> v.coordinates );

			final JavaPairRDD< Tuple2< Integer, Integer >, double[] > backward = Render.invert( sc, coordinates ).persist( StorageLevel.MEMORY_AND_DISK() );
			unpersistList.add( backward );
			final JavaPairRDD< Integer, DPTuple > forwardImages = columnsAndSections.columnsToSections( sc, coordinates );
			final JavaPairRDD< Integer, DPTuple > backwardImages = columnsAndSections.columnsToSections( sc, backward );

			final String outputFormat = String.format( outputFolder, i ) + "/forward/%04d.tif";
			final List< Tuple2< Integer, Boolean > > success = forwardImages.mapToPair( t -> new Tuple2<>( indicesBC.getValue().get( t._1() ), t._2() ) ).mapToPair( new Utility.WriteToFormatStringDouble< Integer >( outputFormat ) ).collect();

			final String outputFormatBackward = String.format( outputFolder, i ) + "/backward/%04d.tif";
			final List< Tuple2< Integer, Boolean > > successBackward = backwardImages.mapToPair( t -> new Tuple2<>( indicesBC.getValue().get( t._1() ), t._2() ) ).mapToPair( new Utility.WriteToFormatStringDouble< Integer >( outputFormatBackward ) ).collect();

			if ( scaleOptions.logMatrices[ i ] )
			{
				// write transformed matrices
				final String outputFormatTransformedMatrices = String.format( outputFolder, i ) + "/transformed-matrices/%s.tif";
				matrices.mapValues( t -> t._1() ).join( backward ).mapToPair( new Utility.Transform< Tuple2< Integer, Integer > >() ).mapToPair( new Utility.WriteToFormatString< Tuple2< Integer, Integer > >( outputFormatTransformedMatrices ) ).collect();
			}

			// last occurence of backward, unpersist!
			backward.unpersist();

			int count = 0;
			for ( final Tuple2< Integer, Boolean > s : success )
			{
				if ( s._2().booleanValue() )
					continue;
				++count;
				log.warn( MethodHandles.lookup().lookupClass().getSimpleName() + ": Failed to write forward image " + s._1().intValue() );
			}

			int countBackward = 0;
			for ( final Tuple2< Integer, Boolean > s : successBackward )
			{
				if ( s._2().booleanValue() )
					continue;
				++countBackward;
				log.warn( MethodHandles.lookup().lookupClass().getSimpleName() + ": Failed to write backward image " + s._1().intValue() );
			}

			final long tEnd = System.currentTimeMillis();

			log.info( MethodHandles.lookup().lookupClass().getSimpleName() + ": Successfully wrote " + ( success.size() - count ) + "/" + success.size() + " (forward) and " + ( successBackward.size() - countBackward ) + "/" + successBackward.size() + " (backward) " + "images at iteration " + i + String.format( " in %25dms", tEnd - tStart ) );

			times.add( Utility.tuple2( tStart, tEnd ) );

			// unpersist rdds
			for ( final Object o : unpersistList )
			{
				if ( o instanceof JavaPairRDD< ?, ? > )
					( ( JavaPairRDD< ?, ? > ) o ).unpersist();
				if ( o instanceof JavaRDD< ? > )
					( ( JavaRDD< ? > ) o ).unpersist();
			}

		}

		for ( final Object o : globalUnpersistList )
		{
			if ( o instanceof JavaPairRDD< ?, ? > )
				( ( JavaPairRDD< ?, ? > ) o ).unpersist();
			if ( o instanceof JavaRDD< ? > )
				( ( JavaRDD< ? > ) o ).unpersist();
			if ( o instanceof Broadcast< ? > )
				( ( Broadcast< ? > ) o ).doDestroy( false );
		}

		for ( final Tuple2< Long, Long > t : times )
		{
			final long diff = t._2().longValue() - t._1().longValue();
			log.info( String.format( "%s: Run time for complete iteration: %25dms", MethodHandles.lookup().lookupClass().getSimpleName(), diff ) );
		}

	}

}
