package org.janelia.thickness;

import java.io.IOException;
import java.lang.invoke.MethodHandles;
import java.lang.reflect.InvocationTargetException;
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
import org.janelia.thickness.inference.Options;
import org.janelia.thickness.kryo.KryoSerialization;
import org.janelia.thickness.similarity.CorrelationAndWeight;
import org.janelia.thickness.similarity.CorrelationsImgLib;
import org.janelia.thickness.similarity.DefaultMatrixGenerator;
import org.janelia.thickness.similarity.ImageAndMask;
import org.janelia.thickness.utility.Utility;
import org.kohsuke.args4j.Argument;
import org.kohsuke.args4j.CmdLineException;
import org.kohsuke.args4j.CmdLineParser;

import ij.ImagePlus;
import ij.process.FloatProcessor;
import ij.process.ImageProcessor;
import loci.formats.FormatException;
import mpicbg.trakem2.util.Downsampler;
import net.imglib2.FinalInterval;
import net.imglib2.RandomAccessible;
import net.imglib2.img.Img;
import net.imglib2.img.display.imagej.ImageJFunctions;
import net.imglib2.type.numeric.RealType;
import net.imglib2.util.Intervals;
import net.imglib2.util.Pair;
import net.imglib2.view.Views;
import pl.joegreen.lambdaFromString.LambdaCreationException;
import pl.joegreen.lambdaFromString.LambdaFactory;
import pl.joegreen.lambdaFromString.TypeReference;
import scala.Tuple2;

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

		final ArrayList< JavaRDD< Tuple2< Integer, Integer > > > indexPairRDDs = new ArrayList< >();
		final ArrayList< Tuple2< Integer, Integer > > bounds = new ArrayList<>();
		for ( int z = 0; z < size; z += joinStepSize )
		{
			final int s = Math.max( z - maxRange, 0 );
			final int S = Math.min( z + maxRange + joinStepSize, size );
			final List< Integer > idxs = IntStream.range( s, z + joinStepSize ).mapToObj( i -> i ).collect( Collectors.toList() );
			final JavaRDD< Tuple2< Integer, Integer > > indexPairs = sc.parallelize( idxs ).flatMap( i -> {
				return IntStream.range( i + 1, Math.min( i + 1 + maxRange, S ) ).mapToObj( k -> new Tuple2<>( i, k ) ).iterator();
			} ).repartition( sc.defaultParallelism() );
			indexPairs.cache();
			indexPairRDDs.add( indexPairs );
			bounds.add( new Tuple2<>( s, S ) );
		}
		final Broadcast< ArrayList< Tuple2< Integer, Integer > > > boundsBC = sc.broadcast( bounds );
		globalUnpersistList.add( boundsBC );
		globalUnpersistList.addAll( indexPairRDDs );

		LOG.info( "Persisted " + indexPairRDDs.stream().map( rdd -> rdd.count() ).collect( Collectors.toList() ) + " index pairs. " );
		LOG.info( indicesBC.getValue().length );
		LOG.info( indexPairRDDs.stream().map( rdd -> rdd.map( t -> Math.min( t._1(), t._1() ) ).min( Comparator.naturalOrder() ) ).collect( Collectors.toList() ) );
		LOG.info( indexPairRDDs.stream().map( rdd -> rdd.map( t -> Math.max( t._1(), t._1() ) ).max( Comparator.naturalOrder() ) ).collect( Collectors.toList() ) );
		LOG.info( "Bounds: " + bounds );
		LOG.info( "Min distance: " + indexPairRDDs.stream().map( rdd -> rdd.map( t -> Math.abs( t._1() - t._2() ) ).min( Comparator.naturalOrder() ) ) );
		LOG.info( "Max distance: " + indexPairRDDs.stream().map( rdd -> rdd.map( t -> Math.abs( t._1() - t._2() ) ).max( Comparator.naturalOrder() ) ) );

		final ImageProcessor ip0 = Downsampler.downsampleImageProcessor( new ImagePlus( String.format( imagePattern, start ) ).getProcessor() );
		final int w = ip0.getWidth();
		final int h = ip0.getHeight();
		final int[] dim = new int[] { w, h };
		final Broadcast< int[] > dimBC = sc.broadcast( dim );

		LOG.info( "Image size after downsampling: " + Arrays.toString( dim ) );

		LOG.info( "Source pattern: " + imagePattern );
		LOG.info( "Mask pattern:   " + maskPattern );

		List< JavaPairRDD< Tuple2< Integer, Integer >, Tuple2< ImageAndMask, ImageAndMask > > > maskedImagePairs = indexPairRDDs.stream().map( rdd -> rdd.mapToPair( new LoadImages( indicesBC, imagePattern, maskPattern, w, h, imageScaleLevel ) ).persist( StorageLevel.DISK_ONLY() ) ).collect( Collectors.toList() );
		globalUnpersistList.addAll( maskedImagePairs );
		LOG.info( "Loaded " + maskedImagePairs.stream().map( rdd -> rdd.count() ).collect( Collectors.toList() ) + " image and weight pairs." );
		indexPairRDDs.stream().map( rdd -> rdd.unpersist() ).mapToInt( v -> 1 ).count();

		final ArrayList< Tuple2< Long, Long > > times = new ArrayList<>();

		for ( int iteration = 0; iteration < radiiArray.length; ++iteration )
		{

			final ArrayList< Object > unpersistList = new ArrayList<>();

			LOG.info( "i=" + iteration );
			LOG.info( "Options [" + iteration + "] = \n" + options[ iteration ].toString() );

			final long tStart = System.currentTimeMillis();

			final int[] currentOffset = radiiArray[ iteration ];
			final int[] currentStep = stepsArray[ iteration ];
			final int r = options[ iteration ].comparisonRange;

			final List< JavaPairRDD< Tuple2< Integer, Integer >, Tuple2< ImageAndMask, ImageAndMask > > > mip = maskedImagePairs.stream().map( rdd -> rdd.filter( new DefaultMatrixGenerator.SelectInRange<>( r ) ).persist( StorageLevel.DISK_ONLY() ) ).collect( Collectors.toList() );
			mip.forEach( rdd -> rdd.count() );
			maskedImagePairs.forEach( rdd -> rdd.unpersist() );
			maskedImagePairs = mip;

			final BlockCoordinates correlationBlocks = new BlockCoordinates( currentOffset, currentStep );
			final Broadcast< ArrayList< BlockCoordinates.Coordinate > > coordinates = sc.broadcast( correlationBlocks.generateFromBoundingBox( dim ) );

			//			final JavaPairRDD< Tuple2< Integer, Integer >, Tuple2< FloatProcessor, FloatProcessor > > matrices = generateMatrices( maskedImagePairs, coordinates, dimBC, r, size );
			// why is this slow at dense resolution? and consumes a ton of
			// memory!
			final List< JavaPairRDD< Tuple2< Integer, Integer >, Tuple2< FloatProcessor, FloatProcessor > > > matrixChunks = mip.stream().map( rdd -> generateMatrices( sc, rdd, coordinates, dimBC, r, size ).persist( StorageLevel.DISK_ONLY() ) ).collect( Collectors.toList() );
			unpersistList.addAll( matrixChunks );
			LOG.info( "Calculated " + matrixChunks.stream().map( rdd -> rdd.count() ).collect( Collectors.toList() ) + " matrix chunks" );

			final List< Tuple2< Integer, JavaPairRDD< Tuple2< Integer, Integer >, Tuple2< FloatProcessor, FloatProcessor > > > > matrixChunksWithOffset = IntStream.range( 0, matrixChunks.size() ).mapToObj( k -> new Tuple2<>( k * joinStepSize, matrixChunks.get( k ) ) ).collect( Collectors.toList() );

			JavaPairRDD< Tuple2< Integer, Integer >, Tuple2< FloatProcessor, FloatProcessor > > matrices = matrixChunks.get( 0 ).mapValues( v -> new Tuple2<>( Utility.constValueFloatProcessor( 2 * r + 1, size, Float.NaN ), Utility.constValueFloatProcessor( 2 * r + 1, size, Float.NaN ) ) );
			for ( int k = 0; k < matrixChunksWithOffset.size(); ++k )
			{
				final Tuple2< Integer, JavaPairRDD< Tuple2< Integer, Integer >, Tuple2< FloatProcessor, FloatProcessor > > > chunk = matrixChunksWithOffset.get( k );
				final int s = chunk._1();
				final int S = Math.min( s + joinStepSize, size );
				matrices = matrices.join( chunk._2() ).mapValues( t -> {
					final FloatProcessor target = t._1()._1();
					final FloatProcessor targetWeight = t._1()._2();

					final FloatProcessor source = t._2()._1();
					final FloatProcessor sourceWeight = t._2()._2();

					final int width = target.getWidth();

					for ( int y = s; y < S; ++y )
						for ( int x = 0; x < width; ++x )
						{
							target.setf( x, y, source.getf( x, y ) );
							targetWeight.setf( x, y, sourceWeight.getf( x, y ) );
						}
					return t._1();
				} );
			} ;
			matrices.persist( StorageLevel.DISK_ONLY() );
			unpersistList.add( matrices );
			matrices.context();

			final String outputFormatMatrices = String.format( outputFolder, iteration ) + "/matrices/%s.tif";
			final String outputFormatWeightMatrices = String.format( outputFolder, iteration ) + "/weight-matrices/%s.tif";
			// Write matrices
			matrices.mapValues( t -> t._1() ).mapValues( new SetColumnConstant( r, 1.0f ) ).mapToPair( new Utility.WriteToFormatString<>( outputFormatMatrices ) ).collect();
			matrices.mapValues( t -> t._2() ).mapValues( new SetColumnConstant( r, 1.0f ) ).mapToPair( new Utility.WriteToFormatString<>( outputFormatWeightMatrices ) ).collect();

			final long tEnd = System.currentTimeMillis();

			log.info( MethodHandles.lookup().lookupClass().getSimpleName() + ": Successfully wrote " + matrices.count() + "matrices at iteration " + iteration + String.format( " in %25dms", tEnd - tStart ) );

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

	public static final JavaPairRDD< Tuple2< Integer, Integer >, Tuple2< FloatProcessor, FloatProcessor > > generateMatrices( final JavaSparkContext sc, final JavaPairRDD< Tuple2< Integer, Integer >, Tuple2< ImageAndMask, ImageAndMask > > maskedImagePairs, final Broadcast< ArrayList< BlockCoordinates.Coordinate > > coordinates, final Broadcast< int[] > dim, final int r, final int size )
	{
		final Broadcast< List< Tuple2< Integer, Integer > > > localCoordinates = sc.broadcast( coordinates.getValue().stream().map( c -> c.getLocalCoordinates() ).collect( Collectors.toList() ) );

		final JavaPairRDD< Tuple2< Integer, Integer >, Tuple2< FloatProcessor, FloatProcessor > > matrices = maskedImagePairs
				.mapValues( new SubSectionCorrelations( coordinates, dim.getValue() ) )
				.flatMapValues( t -> Utility.combineAsStream( localCoordinates.getValue(), t ).collect( Collectors.toList() ) )
				.mapToPair( new SwapKey<>() )
				.mapValues( new FirstValue( r, size ) )
				.reduceByKey( new MergeFunc() );
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

	public static class SubSectionCorrelations implements Function< Tuple2< ImageAndMask, ImageAndMask >, ArrayList< CorrelationAndWeight > >
	{

		public static final Logger LOG = LogManager.getLogger( MethodHandles.lookup().lookupClass() );
		static
		{
			LOG.setLevel( Level.DEBUG );
		}

		/**
		 *
		 */
		private static final long serialVersionUID = 4914446108059613538L;

		private final Broadcast< ArrayList< BlockCoordinates.Coordinate > > coordinates;

		private final int[] dim;

		public SubSectionCorrelations( final Broadcast< ArrayList< BlockCoordinates.Coordinate > > coordinates, final int[] dim )
		{
			this.coordinates = coordinates;
			this.dim = dim;
		}

		@Override
		public ArrayList< CorrelationAndWeight > call( final Tuple2< ImageAndMask, ImageAndMask > t ) throws Exception
		{
			final ImageAndMask fp1 = t._1();
			final ImageAndMask fp2 = t._2();

			final int w = fp1.image.getWidth();
			final int h = fp1.image.getHeight();

			final int[] min = new int[] { 0, 0 };
			final int[] currentStart = new int[ 2 ];
			final int[] currentStop = new int[ 2 ];
			final ArrayList< CorrelationAndWeight > result = new ArrayList<>();

			final Img< ? extends RealType< ? > > i1 = ImageJFunctions.wrapReal( new ImagePlus( "", fp1.image ) );
			final Img< ? extends RealType< ? > > i2 = ImageJFunctions.wrapReal( new ImagePlus( "", fp2.image ) );
			final Img< ? extends RealType< ? > > w1 = ImageJFunctions.wrapReal( new ImagePlus( "", fp1.mask ) );
			final Img< ? extends RealType< ? > > w2 = ImageJFunctions.wrapReal( new ImagePlus( "", fp2.mask ) );

			assert i1.dimension( 0 ) == w && i1.dimension( 1 ) == h;
			assert i2.dimension( 0 ) == w && i2.dimension( 1 ) == h;
			assert w1.dimension( 0 ) == w && w1.dimension( 1 ) == h;
			assert w2.dimension( 0 ) == w && w2.dimension( 1 ) == h;

			LOG.debug( "Calculating correlations for images and masks: " + Arrays.toString( Intervals.dimensionsAsLongArray( i1 ) ) + "\n" + Arrays.toString( Intervals.dimensionsAsLongArray( i2 ) ) + "\n" + Arrays.toString( Intervals.dimensionsAsLongArray( w1 ) ) + "\n" + Arrays.toString( Intervals.dimensionsAsLongArray( w2 ) ) + "\n" );

			final RandomAccessible< ? extends Pair< ? extends RealType< ? >, ? extends RealType< ? > > > p1 = Views.pair( i1, w1 );
			final RandomAccessible< ? extends Pair< ? extends RealType< ? >, ? extends RealType< ? > > > p2 = Views.pair( i2, w2 );

			for ( final BlockCoordinates.Coordinate coord : coordinates.getValue() )
			{
				final Tuple2< Integer, Integer > local = coord.getLocalCoordinates();
				final Tuple2< Integer, Integer > global = coord.getWorldCoordinates();
				final Tuple2< Integer, Integer > radius = coord.getRadius();
				currentStart[ 0 ] = Math.max( min[ 0 ], global._1() - radius._1() );
				currentStart[ 1 ] = Math.max( min[ 1 ], global._2() - radius._2() );
				currentStop[ 0 ] = Math.min( dim[ 0 ], global._1() + radius._1() );
				currentStop[ 1 ] = Math.min( dim[ 1 ], global._2() + radius._2() );
				final long[] currentMax = Arrays.stream( currentStop ).mapToLong( v -> v - 1 ).toArray();

				final FinalInterval interval = new FinalInterval( Arrays.stream( currentStart ).mapToLong( i -> i ).toArray(), currentMax );

				final CorrelationAndWeight correlation = CorrelationsImgLib.calculate( p1, p2, interval );
				LOG.debug( "Got correlation " + correlation + " at position " + coord.toString() );
				result.add( correlation );
			}
			LOG.debug( "Returning " + result.size() + "similarities with weight" );
			return result;
		}
	}

}
