package org.janelia.thickness.matrix;

import java.io.File;
import java.io.IOException;
import java.lang.invoke.MethodHandles;
import java.lang.reflect.InvocationTargetException;
import java.nio.file.Files;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Comparator;
import java.util.List;
import java.util.Map;
import java.util.Map.Entry;
import java.util.concurrent.ExecutionException;
import java.util.stream.Collectors;
import java.util.stream.IntStream;

import org.apache.log4j.Level;
import org.apache.log4j.LogManager;
import org.apache.log4j.Logger;
import org.apache.spark.SparkConf;
import org.apache.spark.api.java.JavaPairRDD;
import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.api.java.JavaSparkContext;
import org.apache.spark.api.java.function.Function;
import org.apache.spark.api.java.function.Function2;
import org.apache.spark.api.java.function.PairFunction;
import org.apache.spark.broadcast.Broadcast;
import org.apache.spark.storage.StorageLevel;
import org.janelia.thickness.BlockCoordinates;
import org.janelia.thickness.BlockCoordinates.Coordinate;
import org.janelia.thickness.ScaleOptions;
import org.janelia.thickness.inference.Options;
import org.janelia.thickness.kryo.KryoSerialization;
import org.janelia.thickness.similarity.CorrelationAndWeight;
import org.janelia.thickness.similarity.ImageAndMask;
import org.janelia.thickness.utility.Utility;
import org.kohsuke.args4j.Argument;
import org.kohsuke.args4j.CmdLineException;
import org.kohsuke.args4j.CmdLineParser;

import ij.ImagePlus;
import ij.io.FileSaver;
import ij.process.FloatProcessor;
import ij.process.ImageProcessor;
import loci.formats.FormatException;
import mpicbg.trakem2.util.Downsampler;
import net.imglib2.Cursor;
import net.imglib2.FinalInterval;
import net.imglib2.img.array.ArrayCursor;
import net.imglib2.img.array.ArrayImg;
import net.imglib2.img.array.ArrayImgFactory;
import net.imglib2.img.array.ArrayImgs;
import net.imglib2.img.basictypeaccess.array.FloatArray;
import net.imglib2.img.display.imagej.ImageJFunctions;
import net.imglib2.type.NativeType;
import net.imglib2.type.numeric.real.FloatType;
import net.imglib2.util.Intervals;
import net.imglib2.util.Pair;
import net.imglib2.view.IntervalView;
import net.imglib2.view.Views;
import pl.joegreen.lambdaFromString.LambdaCreationException;
import pl.joegreen.lambdaFromString.LambdaFactory;
import pl.joegreen.lambdaFromString.TypeReference;
import scala.Tuple2;
import scala.Tuple3;

/**
 *
 * @author Philipp Hanslovsky
 *
 */
public class ComputeMatrices
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

	public static void main( final String[] args ) throws FormatException, IOException, InstantiationException, IllegalAccessException, IllegalArgumentException, InvocationTargetException, NoSuchMethodException, SecurityException, ClassNotFoundException, LambdaCreationException, InterruptedException, ExecutionException
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
					.setAppName( MethodHandles.lookup().lookupClass().getSimpleName() )
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

	public static void run( final JavaSparkContext sc, final ScaleOptions scaleOptions ) throws FormatException, IOException, InstantiationException, IllegalAccessException, IllegalArgumentException, InvocationTargetException, NoSuchMethodException, SecurityException, ClassNotFoundException, LambdaCreationException, InterruptedException, ExecutionException
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


	public static void run( final JavaSparkContext sc, final ScaleOptions scaleOptions, final Function< Integer, FloatProcessor > fileOpener ) throws FormatException, IOException, InterruptedException, ExecutionException
	{

		final ArrayList< Object > globalUnpersistList = new ArrayList<>();

		final Logger log = LOG;// LogManager.getRootLogger();

		final String root = scaleOptions.target;
		final String outputFolder = root + "/%02d";
		final int imageScaleLevel = scaleOptions.scale;
		final int start = scaleOptions.start;
		final int stop = scaleOptions.stop;
		final int step = scaleOptions.step;
		final int[] indices = Utility.arange( start, stop, step ).stream().mapToInt( i -> i ).toArray();
		final int size = indices.length;
		final int[][] radiiArray = scaleOptions.radii;
		final int[][] stepsArray = scaleOptions.steps;
		final Options[] options = scaleOptions.inference;
		final String imagePattern = scaleOptions.source;
		final String maskPattern = scaleOptions.estimateMask;
		final int joinStepSize = scaleOptions.joinStepSize;
		final int maxRange = Arrays.stream( options ).mapToInt( o -> o.comparisonRange ).max().getAsInt();

		final Broadcast< int[] > indicesBC = sc.broadcast( indices );
		globalUnpersistList.add( indicesBC );

		//		final List< Integer > zeroBasedIndices = IntStream.range( 0, size ).mapToObj( i -> i ).collect( Collectors.toList() );
		//		final JavaRDD< Tuple2< Integer, Integer > > indexPairs = sc.parallelize( zeroBasedIndices ).flatMap( i -> {
		//			return IntStream.range( i + 1, Math.min( i + 1 + maxRange, size ) ).mapToObj( k -> new Tuple2<>( i, k ) ).iterator();
		//		} ).repartition( sc.defaultParallelism() );

		LOG.info( "joinStepSize: " + joinStepSize );

		final ArrayList< JavaRDD< Tuple2< Integer, Integer > > > indexPairRDDs = new ArrayList< >();
		final ArrayList< Tuple2< Integer, Integer > > bounds = new ArrayList<>();
		for ( int z = 0; z < size; z += joinStepSize )
		{
			final int s = Math.max( z - maxRange, 0 );
			final int S = Math.min( z + maxRange + joinStepSize, size );
			final List< Integer > idxs = IntStream.range( z, z + joinStepSize ).mapToObj( i -> i ).collect( Collectors.toList() );
			final JavaRDD< Tuple2< Integer, Integer > > indexPairs = sc.parallelize( idxs ).flatMap( i -> {
				return IntStream.range( i + 1, Math.min( i + 1 + maxRange, S ) ).mapToObj( k -> new Tuple2<>( i, k ) ).iterator();
			} ).repartition( sc.defaultParallelism() );
			indexPairs.cache();
			indexPairRDDs.add( indexPairs );
			bounds.add( new Tuple2<>( z, S ) );
		}
		final Broadcast< ArrayList< Tuple2< Integer, Integer > > > boundsBC = sc.broadcast( bounds );
		globalUnpersistList.add( boundsBC );
		globalUnpersistList.addAll( indexPairRDDs );
		final List< Integer > sizes = bounds.stream().map( t -> Math.min( t._1() + joinStepSize, size - 1 ) - t._1() ).collect( Collectors.toList() );

		LOG.info( "Persisted " + indexPairRDDs.stream().map( rdd -> rdd.count() ).collect( Collectors.toList() ) + " index pairs. " );
		LOG.info( indicesBC.getValue().length );
		LOG.info( indexPairRDDs.stream().map( rdd -> rdd.map( t -> Math.min( t._1(), t._1() ) ).min( Comparator.naturalOrder() ) ).collect( Collectors.toList() ) );
		LOG.info( indexPairRDDs.stream().map( rdd -> rdd.map( t -> Math.max( t._1(), t._1() ) ).max( Comparator.naturalOrder() ) ).collect( Collectors.toList() ) );
		LOG.info( "Bounds: " + bounds );
		LOG.info( "Sizes: " + sizes );
		LOG.info( "Min distance: " + indexPairRDDs.stream().map( rdd -> rdd.map( t -> Math.abs( t._1() - t._2() ) ).min( Comparator.naturalOrder() ) ) );
		LOG.info( "Max distance: " + indexPairRDDs.stream().map( rdd -> rdd.map( t -> Math.abs( t._1() - t._2() ) ).max( Comparator.naturalOrder() ) ) );

		final ImageProcessor ip0 = Downsampler.downsampleImageProcessor( new ImagePlus( String.format( imagePattern, start ) ).getProcessor(), scaleOptions.scale );
		final int w = ip0.getWidth();
		final int h = ip0.getHeight();
		final int[] dim = new int[] { w, h };
		final Broadcast< int[] > dimBC = sc.broadcast( dim );

		LOG.info( "Image size after downsampling: " + Arrays.toString( dim ) );

		LOG.info( "Source pattern: " + imagePattern );
		LOG.info( "Mask pattern:   " + maskPattern );

		final List< JavaPairRDD< Tuple2< Integer, Integer >, Tuple2< ImageAndMask, ImageAndMask > > > maskedImagePairs = indexPairRDDs.stream().map( rdd -> rdd.mapToPair( new LoadImages( indicesBC, imagePattern, maskPattern, w, h, imageScaleLevel ) ) ).collect( Collectors.toList() );
		globalUnpersistList.addAll( maskedImagePairs );
		indexPairRDDs.stream().map( rdd -> rdd.unpersist() ).mapToInt( v -> 1 ).count();

		final ArrayList< Tuple2< Long, Long > > times = new ArrayList<>();

		final ArrayList< BlockCoordinates.Coordinate >[] coordinates = new ArrayList[ radiiArray.length ];
		for ( int i = 0; i < coordinates.length; ++i )
		{
			final int[] currentOffset = radiiArray[ i ];
			final int[] currentStep = stepsArray[ i ];
			final BlockCoordinates correlationBlocks = new BlockCoordinates( currentOffset, currentStep );
			coordinates[ i ] = correlationBlocks.generateFromBoundingBox( dim );
		}
		final Broadcast< ArrayList< Coordinate >[] > coordinatesBC = sc.broadcast( coordinates );
		final Broadcast< int[] > rBC = sc.broadcast( Arrays.stream( options ).mapToInt( o -> o.comparisonRange ).toArray() );

		final Broadcast< long[] > blockSize = sc.broadcast( new long[] { 5, 5 } );

		final List< JavaPairRDD< Tuple3< Integer, Integer, Integer >, Tuple2< ArrayImg< FloatType, ? >, ArrayImg< FloatType, ? > > > > matrixChunks = IntStream
				.range( 0, maskedImagePairs.size())
				.mapToObj( i -> generateMatrices( sc, maskedImagePairs.get( i ), coordinatesBC, dimBC, rBC, sc.broadcast( radiiArray ), sc.broadcast( stepsArray ), sizes.get( i ), blockSize, bounds.get( i ) ).persist( StorageLevel.DISK_ONLY() ) )
				.collect( Collectors.toList() );
		globalUnpersistList.addAll( matrixChunks );

		final long t0 = System.currentTimeMillis();
		final long nChunks = matrixChunks.stream().mapToLong( rdd -> rdd.count() ).reduce( ( l1, l2 ) -> l1 + l2 ).getAsLong();
		final long t1 = System.currentTimeMillis();
		LOG.info( "Similarity calculation time: " + ( t1 - t0 ) + "ms (" + nChunks + "chunks)" );

		for ( int iteration = 0; iteration < coordinates.length; ++iteration )
		{

			final ArrayList< Object > unpersistList = new ArrayList<>();

			final int fIteration = iteration;
			final int r = options[ iteration ].comparisonRange;
			final List< JavaPairRDD< Tuple2< Integer, Integer >, Tuple2< ArrayImg< FloatType, ? >, ArrayImg< FloatType, ? > > > > msAt = matrixChunks.stream().map( rdd -> rdd.filter( t -> t._1()._1() == fIteration ).mapToPair( t -> new Tuple2<>( new Tuple2<>( t._1()._2(), t._1()._3() ), t._2() ) ) ).collect( Collectors.toList() );
			final List< Tuple2< Integer, JavaPairRDD< Tuple2< Integer, Integer >, Tuple2< ArrayImg< FloatType, ? >, ArrayImg< FloatType, ? > > > > > matrixChunksWithOffset = IntStream.range( 0, msAt.size() ).mapToObj( k -> new Tuple2<>( k * joinStepSize, msAt.get( k ) ) ).collect( Collectors.toList() );

			// if ( iteration == 0 )
			// {
			// new ImageJ();
			// matrixChunks.forEach( rdd -> ImageJFunctions.show(
			// Views.hyperSlice( Views.hyperSlice( rdd.values().map( p -> p._1()
			// ).collect().get( 0 ), 1, 0 ), 0, 0 ) ) );
			// }

			JavaPairRDD< Tuple2< Integer, Integer >, Tuple2< ArrayImg< FloatType, ? >, ArrayImg< FloatType, ? > > > matrices = msAt.get( 0 ).mapValues( v -> {
				final long d0 = v._1().dimension( 0 );
				final long d1 = v._1().dimension( 1 );
				final FloatType f = new FloatType( Float.NaN );
				return new Tuple2<>( Utility.setConstantVal( ArrayImgs.floats( d0, d1, r, size ), f ), Utility.setConstantVal( ArrayImgs.floats( d0, d1, r, size ), f ) );
			} );
			for ( int k = 0; k < matrixChunksWithOffset.size(); ++k )
			{
				final Tuple2< Integer, JavaPairRDD< Tuple2< Integer, Integer >, Tuple2< ArrayImg< FloatType, ? >, ArrayImg< FloatType, ? > > > > chunk = matrixChunksWithOffset.get( k );
				final int s = chunk._1();
				final int S = Math.min( s + joinStepSize, size - 1 );
				matrices = matrices.join( chunk._2() ).mapValues( t -> {
					final ArrayImg< FloatType, ? > target = t._1()._1();
					final ArrayImg< FloatType, ? > targetWeight = t._1()._2();

					final ArrayImg< FloatType, ? > source = t._2()._1();
					final ArrayImg< FloatType, ? > sourceWeight = t._2()._2();

					final long[] matMin = Intervals.minAsLongArray( target );
					final long[] matMax = Intervals.maxAsLongArray( target );
					final long[] matDim = Intervals.dimensionsAsLongArray( target );

					matMin[ 3 ] = s;
					matMax[ 3 ] = S - 1;
					matDim[ 3 ] = S - s;

					final FinalInterval fi = new FinalInterval( matDim );

					for ( final Pair< FloatType, FloatType > p : Views.interval( Views.pair( source, Views.offsetInterval( target, matMin, matDim ) ), fi ) )
						if ( !Float.isNaN( p.getA().get() ) )
							p.getB().set( p.getA() );

					for ( final Pair< FloatType, FloatType > p : Views.interval( Views.pair( sourceWeight, Views.offsetInterval( targetWeight, matMin, matDim ) ), fi ) )
						if ( !Float.isNaN( p.getA().get() ) )
							p.getB().set( p.getA() );

					return t._1();
				} );
			} ;
			matrices.persist( StorageLevel.DISK_ONLY() );
			unpersistList.add( matrices );
			matrices.count();

			final String outputFormatMatrices = String.format( outputFolder, iteration ) + "/matrices/%s.tif";
			final String outputFormatWeightMatrices = String.format( outputFolder, iteration ) + "/weight-matrices/%s.tif";
			// Write matrices
			final JavaPairRDD< Tuple2< Integer, Integer >, ArrayImg< FloatType, ? > > onlyMatrices = matrices.mapValues( t -> t._1() );
			final JavaPairRDD< Tuple2< Integer, Integer >, ArrayImg< FloatType, ? > > onlyWeights = matrices.mapValues( t -> t._2() );

			final long c1 = onlyMatrices.mapToPair( new WriteToFormatString( outputFormatMatrices ) ).values().treeReduce( ( l1, l2 ) -> l1 + l2 );
			final long c2 = onlyWeights.mapToPair( new WriteToFormatString( outputFormatWeightMatrices ) ).values().treeReduce( ( l1, l2 ) -> l1 + l2 );
			// matrices.mapValues( t -> t._2() ).mapToPair( new
			// Utility.WriteToFormatString<>( outputFormatWeightMatrices )
			// ).collect();

			final long tEnd = System.currentTimeMillis();

			log.info( "Successfully wrote " + c1 + " matrices at iteration " + iteration );

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

	public static void addValue( final FloatProcessor fp, final int min, final int max, final int range, final int diff, final float value )
	{
		fp.setf( range + diff, min, value );
		fp.setf( range - diff, max, value );
	}

	public static FloatProcessor merge( final FloatProcessor fp1, final FloatProcessor fp2 )
	{
		final float[] d1 = ( float[] ) fp1.getPixels();
		final float[] d2 = ( float[] ) fp2.getPixels();
		for ( int i = 0; i < d1.length; ++i )
			if ( !Float.isNaN( d1[ i ] ) )
				d2[ i ] = d1[ i ];
		return fp2;
	}

	public static final JavaPairRDD< Tuple3< Integer, Integer, Integer >, Tuple2< ArrayImg< FloatType, ? >, ArrayImg< FloatType, ? > > > generateMatrices(
			final JavaSparkContext sc,
			final JavaPairRDD< Tuple2< Integer, Integer >, Tuple2< ImageAndMask, ImageAndMask > > maskedImagePairs,
			final Broadcast< ArrayList< BlockCoordinates.Coordinate >[] > coordinates,
			final Broadcast< int[] > dim,
			final Broadcast< int[] > r,
			final Broadcast< int[][] > radiiArray,
			final Broadcast< int[][] > stepsArray,
			final int size,
			final Broadcast< long[] > blockSize,
			final Tuple2< Integer, Integer > bounds )
	{
		final JavaPairRDD< IterationXYZ, IterationAndTwoCoordinatesAndTwoArrayImgs< FloatType, FloatType > > correlations = maskedImagePairs
				.mapToPair( new SubSectionCorrelations( coordinates, dim, r ) )
				.flatMapValues( v -> {
					return IntStream.range( 0, v.length ).mapToObj( i -> new Tuple2<>( i, v[ i ] ) ).collect( Collectors.toList() );
				} ).flatMapValues( new ExtractBlocks<>( blockSize ) ).mapToPair( new SwitchZZAndIXYIndexByFirstZ<>() );
		;

		final JavaPairRDD< Tuple3< Integer, Integer, Integer >, Tuple2< ArrayImg< FloatType, ? >, ArrayImg< FloatType, ? > > > matrices = correlations
				// combine by iteration, x, y, and first z index
				.combineByKey( v -> {
					@SuppressWarnings( "unchecked" )
					final ArrayImg< FloatType, ? >[] imgs = new ArrayImg[ 2 * r.getValue()[ v.i ] ];
					final int dz = v.c2 - v.c1 - 1;
					imgs[ dz ] = v.img1;
					imgs[ dz + r.getValue()[ v.i ] ] = v.img2;
					return imgs;
				}, ( imgs, v ) -> {
					final int dz = v.c2 - v.c1 - 1;
					imgs[ dz ] = v.img1;
					imgs[ dz + r.getValue()[ v.i ] ] = v.img2;
					return imgs;
				}, ( imgs1, imgs2 ) -> {
					for ( int i = 0; i < imgs2.length; ++i )
						if ( imgs2[ i ] != null )
							imgs1[ i ] = imgs2[ i ];
					return imgs1;
				}, sc.defaultParallelism() )
				//
				.mapToPair( t -> {
					final ArrayImg< FloatType, ? >[] imgs = t._2();
					final int radius = r.getValue()[ t._1().i ];
					final ArrayImg< FloatType, FloatArray > img1 = ArrayImgs.floats( imgs[ 0 ].dimension( 0 ), imgs[ 0 ].dimension( 1 ), radius );
					final ArrayImg< FloatType, FloatArray > img2 = ArrayImgs.floats( imgs[ 0 ].dimension( 0 ), imgs[ 0 ].dimension( 1 ), radius );
					for ( int i = 0; i < radius; ++i )
					{
						final Cursor< FloatType > c1 = Views.hyperSlice( img1, 2, i ).cursor();
						final Cursor< FloatType > c2 = Views.hyperSlice( img2, 2, i ).cursor();
						if ( imgs[ i ] == null || imgs[ i + radius ] == null )
							while ( c1.hasNext() )
							{
								c1.next().set( Float.NaN );
								c2.next().set( Float.NaN );
							}
						else
						{
							final ArrayCursor< FloatType > s1 = imgs[ i ].cursor();
							final ArrayCursor< FloatType > s2 = imgs[ i + radius ].cursor();
							while ( c1.hasNext() )
							{
								c1.next().set( s1.next() );
								c2.next().set( s2.next() );
							}
						}
					}
					return new Tuple2<>( new Tuple3<>( t._1().i, t._1().x, t._1().y ), new Tuple3<>( t._1().z, img1, img2 ) );
				} ).combineByKey( v -> {
					final int localIndex = v._1() - bounds._1();
					final ArrayImg< FloatType, ? >[] cs = new ArrayImg[ 2 * size ];
					cs[ localIndex ] = v._2();
					cs[ localIndex + size ] = v._3();
					return cs;
				}, ( cs, v ) -> {
					final int localIndex = v._1() - bounds._1();
					cs[ localIndex ] = v._2();
					cs[ localIndex + size ] = v._3();
					return cs;
				}, ( cs1, cs2 ) -> {
					for ( int i = 0; i < cs2.length; ++i )
						if ( cs2[ i ] != null )
							cs1[ i ] = cs2[ i ];

					return cs1;
				}, sc.defaultParallelism() ).mapValues( cs -> {
					final ArrayImg< FloatType, FloatArray > img1 = ArrayImgs.floats( cs[ 0 ].dimension( 0 ), cs[ 0 ].dimension( 1 ), cs[ 0 ].dimension( 2 ), cs.length / 2 );
					final ArrayImg< FloatType, FloatArray > img2 = ArrayImgs.floats( cs[ 0 ].dimension( 0 ), cs[ 0 ].dimension( 1 ), cs[ 0 ].dimension( 2 ), cs.length / 2 );

					for ( int i = 0, k = size; i < size; ++i, ++k )
					{
						final IntervalView< FloatType > hs1 = Views.hyperSlice( img1, 3, i );
						final IntervalView< FloatType > hs2 = Views.hyperSlice( img2, 3, i );
						if ( cs[ i ] == null )
						{
							for ( final FloatType h : hs1 )
								h.setReal( Float.NaN );
							for ( final FloatType h : hs2 )
								h.setReal( Float.NaN );
						}
						else
						{
							for ( final Pair< FloatType, FloatType > p : Views.interval( Views.pair( cs[ i ], hs1 ), cs[ i ] ) )
								p.getB().set( p.getA() );
							for ( final Pair< FloatType, FloatType > p : Views.interval( Views.pair( cs[ k ], hs2 ), cs[ k ] ) )
								p.getB().set( p.getA() );
						}
					}

					return new Tuple2<>( img1, img2 );
				} );

		return matrices;
	}

	public static class GetIterator< T, V, M extends Map< T, V > > implements Function< M, Iterable< Map.Entry< T, V > > >
	{
		public static final Logger LOG = LogManager.getLogger( MethodHandles.lookup().lookupClass() );
		static
		{
			LOG.setLevel( Level.INFO );
		}

		@Override
		public Iterable< Entry< T, V > > call( final M m ) throws Exception
		{
			LOG.debug( "Flat mapping map: " + m.entrySet() );
			return m.entrySet();
		}

	}

	public static class SwapKey< K1, K2, V > implements PairFunction< Tuple2< K1, Tuple2< K2, V > >, K2, Tuple2< K1, V > >
	{
		public static final Logger LOG = LogManager.getLogger( MethodHandles.lookup().lookupClass() );
		static
		{
			LOG.setLevel( Level.INFO );
		}
		@Override
		public Tuple2< K2, Tuple2< K1, V > > call( final Tuple2< K1, Tuple2< K2, V > > t ) throws Exception
		{
			LOG.debug( "Swapping: " + t );
			return new Tuple2<>( t._2()._1(), new Tuple2<>( t._1(), t._2()._2() ) );
		}
	}

	public static class FirstValue implements Function< Tuple2< Tuple2< Integer, Integer >, CorrelationAndWeight >, Tuple2< FloatProcessor, FloatProcessor > >
	{
		public static final Logger LOG = LogManager.getLogger( MethodHandles.lookup().lookupClass() );
		static
		{
			LOG.setLevel( Level.INFO );
		}
		private final int r;

		private final int size;

		public FirstValue( final int r, final int size )
		{
			super();
			this.r = r;
			this.size = size;
		}

		@Override
		public Tuple2< FloatProcessor, FloatProcessor > call( final Tuple2< Tuple2< Integer, Integer >, CorrelationAndWeight > v1 ) throws Exception
		{
			final FloatProcessor matrix = Utility.constValueFloatProcessor( 2 * r + 1, size, Float.NaN );
			final FloatProcessor mask = Utility.constValueFloatProcessor( 2 * r + 1, size, Float.NaN );
			final int min = Math.min( v1._1()._1(), v1._1()._2() );
			final int max = Math.max( v1._1()._1(), v1._1()._2() );
			final int diff = max - min;
			LOG.debug( "Setting first value correlation and weight to " + v1._2() + " at" + v1._1() + " (diff=" + diff + ")" );
			addValue( matrix, min, max, r, diff, ( float ) v1._2().corr );
			addValue( mask, min, max, r, diff, ( float ) v1._2().weight );
			return new Tuple2<>( matrix, mask );
		}

	}

	public static class AddValue implements Function2< Tuple2< FloatProcessor, FloatProcessor >, Tuple2< Tuple2< Integer, Integer >, CorrelationAndWeight >, Tuple2< FloatProcessor, FloatProcessor > >
	{

		public static final Logger LOG = LogManager.getLogger( MethodHandles.lookup().lookupClass() );
		static
		{
			LOG.setLevel( Level.INFO );
		}

		private final int r;

		public AddValue( final int r )
		{
			super();
			this.r = r;
		}

		@Override
		public Tuple2< FloatProcessor, FloatProcessor > call( final Tuple2< FloatProcessor, FloatProcessor > fp, final Tuple2< Tuple2< Integer, Integer >, CorrelationAndWeight > e ) throws Exception
		{
			final int min = Math.min( e._1()._1(), e._1()._2() );
			final int max = Math.max( e._1()._1(), e._1()._2() );
			final int diff = max - min;
			LOG.debug( "Setting correlation and weight to " + e._2() + " at" + e._1() + " (diff=" + diff + ")" );
			addValue( fp._1(), min, max, r, diff, ( float ) e._2().corr );
			addValue( fp._2(), min, max, r, diff, ( float ) e._2().weight );
			return fp;
		}

	}

	public static class MergeFunc implements Function2< Tuple2< FloatProcessor, FloatProcessor >, Tuple2< FloatProcessor, FloatProcessor >, Tuple2< FloatProcessor, FloatProcessor > >
	{

		@Override
		public Tuple2< FloatProcessor, FloatProcessor > call( final Tuple2< FloatProcessor, FloatProcessor > fp1, final Tuple2< FloatProcessor, FloatProcessor > fp2 ) throws Exception
		{
			merge( fp1._1(), fp2._1() );
			merge( fp1._2(), fp2._2() );
			return fp2;
		}

	}

	public static class LoadImages implements PairFunction< Tuple2< Integer, Integer >, Tuple2< Integer, Integer >, Tuple2< ImageAndMask, ImageAndMask > >
	{

		final Broadcast< int[] > indicesBC;

		final String imagePattern;

		final String maskPattern;

		final int w;

		final int h;

		final int imageScaleLevel;

		public LoadImages( final Broadcast< int[] > indicesBC, final String imagePattern, final String maskPattern, final int w, final int h, final int imageScaleLevel )
		{
			super();
			this.indicesBC = indicesBC;
			this.imagePattern = imagePattern;
			this.maskPattern = maskPattern;
			this.w = w;
			this.h = h;
			this.imageScaleLevel = imageScaleLevel;
		}

		@Override
		public Tuple2< Tuple2< Integer, Integer >, Tuple2< ImageAndMask, ImageAndMask > > call( final Tuple2< Integer, Integer > t ) throws Exception
		{
			final int i1 = indicesBC.getValue()[ t._1() ];
			final int i2 = indicesBC.getValue()[ t._2() ];

			LOG.debug( "Loading img1: " + String.format( imagePattern, i1 ) );
			LOG.debug( "Loading img2: " + String.format( imagePattern, i2 ) );
			final ImageProcessor img1 = new ImagePlus( String.format( imagePattern, i1 ) ).getProcessor();
			final ImageProcessor img2 = new ImagePlus( String.format( imagePattern, i2 ) ).getProcessor();

			final ImageProcessor mask1, mask2;
			if ( maskPattern == null )
			{
				LOG.debug( "Mask pattern is null using ByteProcessor with constant value 1 instead." );
				mask1 = Utility.constValueByteProcessor( w, h, ( byte ) 1 );
				mask2 = Utility.constValueByteProcessor( w, h, ( byte ) 1 );
			}
			else
			{
				LOG.debug( "Loading mask1: " + String.format( imagePattern, i1 ) );
				LOG.debug( "Loading mask2: " + String.format( imagePattern, i2 ) );
				mask1 = Downsampler.downsampleImageProcessor( new ImagePlus( String.format( maskPattern, i1 ) ).getProcessor(), imageScaleLevel );
				mask2 = Downsampler.downsampleImageProcessor( new ImagePlus( String.format( maskPattern, i2 ) ).getProcessor(), imageScaleLevel );
			}
			return new Tuple2<>( t, new Tuple2<>( new ImageAndMask( Downsampler.downsampleImageProcessor( img1, imageScaleLevel ), mask1 ), new ImageAndMask( Downsampler.downsampleImageProcessor( img2, imageScaleLevel ), mask2 ) ) );
		}

	}

	public static class SetColumnConstant implements Function< FloatProcessor, FloatProcessor >
	{

		final int column;

		final float val;

		public SetColumnConstant( final int column, final float val )
		{
			super();
			this.column = column;
			this.val = val;
		}

		@Override
		public FloatProcessor call( final FloatProcessor fp ) throws Exception
		{
			final int stride = fp.getWidth();
			final float[] pixels = ( float[] ) fp.getPixels();
			for ( int i = column; i < pixels.length; i += stride )
				pixels[ i ] = val;
			return fp;
		}

	}

	public static class IterationAndTwoCoordinatesAndTwoArrayImgs< T extends NativeType< T >, U extends NativeType< U > >
	{

		public final int i;

		public final int c1;

		public final int c2;

		public final ArrayImg< T, ? > img1;

		public final ArrayImg< U, ? > img2;

		public IterationAndTwoCoordinatesAndTwoArrayImgs( final int i, final int x, final int y, final ArrayImg< T, ? > img1, final ArrayImg< U, ? > img2 )
		{
			super();
			this.i = i;
			this.c1 = x;
			this.c2 = y;
			this.img1 = img1;
			this.img2 = img2;
		}
	}

	public static class ExtractBlocks< T extends NativeType< T >, U extends NativeType< U > > implements Function< Tuple2< Integer, Tuple2< ArrayImg< T, ? >, ArrayImg< U, ? > > >, Iterable< IterationAndTwoCoordinatesAndTwoArrayImgs< T, U > > >
	{

		public static Logger LOG = LogManager.getLogger( MethodHandles.lookup().lookupClass() );
		static
		{
			LOG.setLevel( Level.INFO );
		}

		public static final int N_DIM = 2;



		private final Broadcast< long[] > blockSize;

		public ExtractBlocks( final Broadcast< long[] > blockSize )
		{
			super();
			this.blockSize = blockSize;
		}

		@Override
		public Iterable< IterationAndTwoCoordinatesAndTwoArrayImgs< T, U > > call( final Tuple2< Integer, Tuple2< ArrayImg< T, ? >, ArrayImg< U, ? > > > v ) throws Exception
		{
			if ( v == null || v._2() == null )
				return new ArrayList<>();
			final Tuple2< ArrayImg< T, ? >, ArrayImg< U, ? > > v1 = v._2();
			final ArrayImg< T, ? > i1 = v1._1();
			final ArrayImg< U, ? > i2 = v1._2();
			final ArrayImgFactory< T > f1 = i1.factory();
			final ArrayImgFactory< U > f2 = i2.factory();
			final T t = i1.firstElement();
			final U u = i2.firstElement();
			final long[] blockSize = this.blockSize.getValue();

			final ArrayList< IterationAndTwoCoordinatesAndTwoArrayImgs< T, U > > blocks = new ArrayList<>();

			final long[] offset = new long[ N_DIM ];
			final long[] dim = Intervals.dimensionsAsLongArray( i1 );

			for ( int d = 0; d < N_DIM; ) {
				final long[] upper = IntStream.range( 0, N_DIM ).mapToLong( i -> Math.min( offset[ i ] + blockSize[ i ], dim[ i ] ) ).toArray();
				final long[] blockDim = IntStream.range( 0, N_DIM ).mapToLong( i -> upper[ i ] - offset[ i ] ).toArray();
				final ArrayImg< T, ? > ii1 = f1.create( blockDim, t );
				final ArrayImg< U, ? > ii2 = f2.create( blockDim, u );

				for ( final Pair< T, T > p : Views.interval( Views.pair( Views.offsetInterval( i1, offset, blockDim ), ii1 ), ii1 ) )
					p.getB().set( p.getA() );

				for ( final Pair< U, U > p : Views.interval( Views.pair( Views.offsetInterval( i2, offset, blockDim ), ii2 ), ii2 ) )
					p.getB().set( p.getA() );

				blocks.add( new IterationAndTwoCoordinatesAndTwoArrayImgs<>( v._1(), ( int ) offset[ 0 ], ( int ) offset[ 1 ], ii1, ii2 ) );

				for ( d = 0; d < N_DIM; ++d ) {
					offset[ d ] += blockSize[ d ];
					if ( offset[ d ] < dim[ d ] )
						break;
					else
						offset[ d ] = 0;
				}
			}

			LOG.debug( blocks );

			return blocks;
		}
	}

	public static class IterationXYZ
	{
		public final int i;

		public final int x;

		public final int y;

		public final int z;

		public IterationXYZ( final int i, final int x, final int y, final int z )
		{
			super();
			this.i = i;
			this.x = x;
			this.y = y;
			this.z = z;
		}

		@Override
		public final int hashCode()
		{
			// list:
			// return 31*hashCode + (e==null ? 0 : e.hashCode())
			return Arrays.hashCode( new int[] { i, x, y, z } );
		}

		@Override
		public final boolean equals( final Object o )
		{
			if ( o instanceof IterationXYZ )
			{
				final IterationXYZ ob = ( IterationXYZ ) o;
				return ob.i == i && ob.x == x && ob.y == y && ob.z == z;
			}
			return false;
		}
	}


	public static class SwitchZZAndIXYIndexByFirstZ< T extends NativeType< T >, U extends NativeType< U > > implements PairFunction< Tuple2< Tuple2< Integer, Integer >, IterationAndTwoCoordinatesAndTwoArrayImgs< T, U > >, IterationXYZ, IterationAndTwoCoordinatesAndTwoArrayImgs< T, U > >
	{

		@Override
		public Tuple2< IterationXYZ, IterationAndTwoCoordinatesAndTwoArrayImgs< T, U > > call( final Tuple2< Tuple2< Integer, Integer >, IterationAndTwoCoordinatesAndTwoArrayImgs< T, U > > t ) throws Exception
		{
			return new Tuple2<>( new IterationXYZ( t._2().i, t._2().c1, t._2().c2, t._1()._1() ), new IterationAndTwoCoordinatesAndTwoArrayImgs<>( t._2().i, t._1()._1(), t._1()._2(), t._2().img1, t._2().img2 ) );
		}

	}

	public static class MergeMatrices implements Function2< Tuple2< ArrayImg< FloatType, ? >, ArrayImg< FloatType, ? > >, Tuple2< ArrayImg< FloatType, ? >, ArrayImg< FloatType, ? > >, Tuple2< ArrayImg< FloatType, ? >, ArrayImg< FloatType, ? > > >
	{

		@Override
		public Tuple2< ArrayImg< FloatType, ? >, ArrayImg< FloatType, ? > > call( final Tuple2< ArrayImg< FloatType, ? >, ArrayImg< FloatType, ? > > v1, final Tuple2< ArrayImg< FloatType, ? >, ArrayImg< FloatType, ? > > v2 ) throws Exception
		{
			for ( final Pair< FloatType, FloatType > p : Views.interval( Views.pair( v1._1(), v2._1() ), v1._1() ) )
				if ( !Float.isNaN( p.getB().get() ) )
					p.getA().set( p.getB() );

			for ( final Pair< FloatType, FloatType > p : Views.interval( Views.pair( v1._2(), v2._2() ), v1._2() ) )
				if ( !Float.isNaN( p.getB().get() ) )
					p.getA().set( p.getB() );
			return v1;
		}

	}

	public static class WriteToFormatString implements PairFunction< Tuple2< Tuple2< Integer, Integer >, ArrayImg< FloatType, ? > >, Tuple2< Integer, Integer >, Long >
	{
		private final String outputFormat;

		public WriteToFormatString( final String outputFormat )
		{
			super();
			this.outputFormat = outputFormat;
		}

		@Override
		public Tuple2< Tuple2< Integer, Integer >, Long > call( final Tuple2< Tuple2< Integer, Integer >, ArrayImg< FloatType, ? > > t ) throws Exception
		{
			final int ox = t._1()._1();
			final int oy = t._1()._2();
			final ArrayImg< FloatType, ? > img = t._2();
			final long d0 = img.dimension( 0 );
			final long d1 = img.dimension( 1 );

			Files.createDirectories( new File( String.format( outputFormat, "" ) ).getParentFile().toPath() );

			for ( long y = 0; y < d1; ++y )
			{
				final long Y = oy + y;
				final IntervalView< FloatType > hsY = Views.hyperSlice( img, 1, y );
				for ( long x = 0; x < d0; ++x )
				{
					final long X = ox + x;
					final IntervalView< FloatType > hsX = Views.hyperSlice( hsY, 0, x );
					final String path = String.format( outputFormat, new Tuple2<>( X, Y ) );
					new FileSaver( ImageJFunctions.wrap( hsX, "" ) ).saveAsTiff( path );
				}
			}
			return Utility.tuple2( t._1(), d1 * d0 );
		}
	}

}
