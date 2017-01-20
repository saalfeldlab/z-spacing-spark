package org.janelia.thickness.utility;

import java.io.File;
import java.io.Serializable;
import java.nio.file.Files;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.Iterator;
import java.util.Map;
import java.util.concurrent.Callable;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;

import org.apache.spark.SparkConf;
import org.apache.spark.api.java.JavaSparkContext;
import org.apache.spark.api.java.function.Function;
import org.apache.spark.api.java.function.Function2;
import org.apache.spark.api.java.function.PairFlatMapFunction;
import org.apache.spark.api.java.function.PairFunction;
import org.janelia.thickness.lut.LUTRealTransform;
import org.janelia.thickness.lut.SingleDimensionLUTRealTransformField;

import ij.ImagePlus;
import ij.io.FileSaver;
import ij.process.FloatProcessor;
import ij.process.ImageProcessor;
import loci.formats.FileStitcher;
import loci.plugins.util.ImageProcessorReader;
import mpicbg.trakem2.util.Downsampler;
import net.imglib2.Cursor;
import net.imglib2.RandomAccessible;
import net.imglib2.RandomAccessibleInterval;
import net.imglib2.RealPoint;
import net.imglib2.RealRandomAccess;
import net.imglib2.RealRandomAccessible;
import net.imglib2.converter.RealFloatConverter;
import net.imglib2.converter.read.ConvertedRandomAccessibleInterval;
import net.imglib2.img.Img;
import net.imglib2.img.array.ArrayCursor;
import net.imglib2.img.array.ArrayImg;
import net.imglib2.img.array.ArrayImgs;
import net.imglib2.img.basictypeaccess.array.DoubleArray;
import net.imglib2.img.basictypeaccess.array.FloatArray;
import net.imglib2.img.display.imagej.ImageJFunctions;
import net.imglib2.interpolation.randomaccess.NLinearInterpolatorFactory;
import net.imglib2.realtransform.RealTransformRealRandomAccessible;
import net.imglib2.type.numeric.RealType;
import net.imglib2.type.numeric.real.DoubleType;
import net.imglib2.type.numeric.real.FloatType;
import net.imglib2.view.IntervalView;
import net.imglib2.view.Views;
import net.imglib2.view.composite.RealComposite;
import scala.Tuple2;
import scala.Tuple3;
import scala.Tuple4;
import scala.Tuple5;
import scala.Tuple6;

/**
 * Created by hanslovskyp on 9/18/15.
 */
public class Utility
{

	public static < U, V > Tuple2< U, V > tuple2( final U u, final V v )
	{
		return new Tuple2<>( u, v );
	}

	public static < U, V, W > Tuple3< U, V, W > tuple3( final U u, final V v, final W w )
	{
		return new Tuple3<>( u, v, w );

	}

	public static < U, V, W, X > Tuple4< U, V, W, X > tuple4( final U u, final V v, final W w, final X x )
	{
		return new Tuple4<>( u, v, w, x );
	}

	public static < U, V, W, X, Y > Tuple5< U, V, W, X, Y > tuple5( final U u, final V v, final W w, final X x, final Y y )
	{
		return new Tuple5<>( u, v, w, x, y );
	}

	public static < U, V, W, X, Y, Z > Tuple6< U, V, W, X, Y, Z > tuple6( final U u, final V v, final W w, final X x, final Y y, final Z z )
	{
		return new Tuple6<>( u, v, w, x, y, z );
	}

	public static ArrayList< Integer > arange( final int stop )
	{
		return arange( 0, stop );
	}

	public static ArrayList< Integer > arange( final int start, final int stop )
	{
		final ArrayList< Integer > numbers = new ArrayList<>();
		for ( int i = start; i < stop; ++i )
			numbers.add( i );
		return numbers;
	}

	public static ArrayList< Long > arange( final long stop )
	{
		return arange( 0l, stop );
	}

	public static ArrayList< Long > arange( final long start, final long stop )
	{
		final ArrayList< Long > numbers = new ArrayList<>();
		for ( long i = start; i < stop; ++i )
			numbers.add( i );
		return numbers;
	}

	/**
	 *
	 * rdd in: ( K -> V ) rdd out: ( V -> K )
	 *
	 * @param <K>
	 * @param <V>
	 */
	public static class SwapKeyValue< K, V > implements PairFunction< Tuple2< K, V >, V, K >
	{
		private static final long serialVersionUID = -4173593608656179460L;

		@Override
		public Tuple2< V, K > call( final Tuple2< K, V > t ) throws Exception
		{
			return t.swap();
		}
	}

	/**
	 * rdd in: ( K -> G,V ) rdd out: ( G -> K,V )
	 *
	 * @param <K>
	 * @param <G>
	 * @param <V>
	 */
	public static class Swap< K, G, V > implements PairFunction< Tuple2< K, Tuple2< G, V > >, G, Tuple2< K, V > >
	{
		private static final long serialVersionUID = -4848162685420711301L;

		@Override
		public Tuple2< G, Tuple2< K, V > > call( final Tuple2< K, Tuple2< G, V > > t ) throws Exception
		{
			final Tuple2< G, V > t2 = t._2();
			return Utility.tuple2( t2._1(), Utility.tuple2( t._1(), t2._2() ) );
		}
	}

	public static class StackOpener implements PairFunction< Integer, Integer, FloatProcessor >
	{

		private static final long serialVersionUID = 6238343817172855495L;

		private final String fileName;

		private final boolean isPattern;

		public StackOpener( final String fileName, final boolean isPattern )
		{
			super();
			this.fileName = fileName;
			this.isPattern = isPattern;
		}

		@Override
		public Tuple2< Integer, FloatProcessor > call( final Integer t ) throws Exception
		{
			final ImageProcessorReader reader = new ImageProcessorReader();
			final ImageProcessor ip;
			if ( isPattern )
			{
				final FileStitcher stitcher = new FileStitcher( isPattern );
				stitcher.setId( fileName );
				reader.setId( stitcher.getFilePattern().getFiles()[ t ] );
				ip = reader.openProcessors( 0 )[ 0 ];
				stitcher.close();
			}
			else
			{
				reader.setId( fileName );
				ip = reader.openProcessors( t )[ 0 ];
			}
			// assume only one channel right now
			final FloatProcessor fp = ip.convertToFloatProcessor();
			reader.close();
			return Utility.tuple2( t, fp );
		}
	}

	public static class FileListOpener implements PairFunction< Integer, Integer, FPTuple[] >
	{
		private final String[] formats;

		public FileListOpener( final String[] formats )
		{
			this.formats = formats;
		}

		@Override
		public Tuple2< Integer, FPTuple[] > call( final Integer index ) throws Exception
		{
			final FPTuple[] images = new FPTuple[ formats.length ];
			for ( int i = 0; i < formats.length; ++i )
				images[ i ] = new FPTuple( new ImagePlus( String.format( formats[ i ], index ) ).getProcessor().convertToFloatProcessor() );
			return Utility.tuple2( index, images );
		}
	}

	public static class WriteToFormatString< K > implements PairFunction< Tuple2< K, FloatProcessor >, K, Boolean >
	{
		private static final long serialVersionUID = -4095400790949311166L;

		private final String outputFormat;

		public WriteToFormatString( final String outputFormat )
		{
			super();
			this.outputFormat = outputFormat;
		}

		@Override
		public Tuple2< K, Boolean > call( final Tuple2< K, FloatProcessor > t ) throws Exception
		{
			final K index = t._1();
			final ImagePlus imp = new ImagePlus( "", t._2() );
			final String path = String.format( outputFormat, index );
			Files.createDirectories( new File( path ).getParentFile().toPath() );
			final boolean success = new FileSaver( imp ).saveAsTiff( path );
			return Utility.tuple2( index, success );
		}
	}

	public static class WriteToFormatStringDouble< K > implements PairFunction< Tuple2< K, DPTuple >, K, Boolean >
	{
		private static final long serialVersionUID = -4095400790949311166L;

		private final String outputFormat;

		public WriteToFormatStringDouble( final String outputFormat )
		{
			super();
			this.outputFormat = outputFormat;
		}

		@Override
		public Tuple2< K, Boolean > call( final Tuple2< K, DPTuple > t ) throws Exception
		{
			final K index = t._1();
			final DPTuple dp = t._2();
			final ArrayImg< DoubleType, DoubleArray > img = ArrayImgs.doubles( dp.pixels, dp.width, dp.height );
			final ConvertedRandomAccessibleInterval< DoubleType, FloatType > floats = new ConvertedRandomAccessibleInterval<>( img, new RealFloatConverter< DoubleType >(), new FloatType() );
			final ImagePlus imp = ImageJFunctions.wrap( floats, "" );
			final String path = String.format( outputFormat, index );
			Files.createDirectories( new File( path ).getParentFile().toPath() );
			final boolean success = new FileSaver( imp ).saveAsTiff( path );
			return Utility.tuple2( index, success );
		}
	}

	public static class DownSample< K > implements PairFunction< Tuple2< K, FloatProcessor >, K, FloatProcessor >
	{

		private final int sampleScale;

		/**
		 * @param sampleScale
		 */
		public DownSample( final int sampleScale )
		{
			super();
			this.sampleScale = sampleScale;
		}

		private static final long serialVersionUID = -8634964011671381854L;

		@Override
		public Tuple2< K, FloatProcessor > call( final Tuple2< K, FloatProcessor > indexedFloatProcessor ) throws Exception
		{
			final FloatProcessor downsampled = ( FloatProcessor ) Downsampler.downsampleImageProcessor( indexedFloatProcessor._2(), sampleScale );
			return Utility.tuple2( indexedFloatProcessor._1(), downsampled );
		}

	}

	public static class DownSampleImages< K > implements PairFunction< Tuple2< K, Tuple2< FPTuple, FPTuple > >, K, Tuple2< FPTuple, FPTuple > >
	{

		private final int sampleScale;

		/**
		 * @param sampleScale
		 */
		public DownSampleImages( final int sampleScale )
		{
			super();
			this.sampleScale = sampleScale;
		}

		private static final long serialVersionUID = -8634964011671381854L;

		@Override
		public Tuple2< K, Tuple2< FPTuple, FPTuple > > call( final Tuple2< K, Tuple2< FPTuple, FPTuple > > indexedFloatProcessorTuple ) throws Exception
		{
			final FloatProcessor downsampled = ( FloatProcessor ) Downsampler.downsampleImageProcessor( indexedFloatProcessorTuple._2()._1().rebuild(), sampleScale );
			final FloatProcessor downsampledMask = ( FloatProcessor ) Downsampler.downsampleImageProcessor( indexedFloatProcessorTuple._2()._2().rebuild(), sampleScale );
			return Utility.tuple2( indexedFloatProcessorTuple._1(), Utility.tuple2( new FPTuple( downsampled ), new FPTuple( downsampledMask ) ) );
		}

	}

	public static class GaussianBlur< K > implements PairFunction< Tuple2< K, FPTuple >, K, FPTuple >
	{

		private final double[] sigma;

		public GaussianBlur( final double[] sigma )
		{
			this.sigma = sigma;
		}

		public GaussianBlur( final double sigmaX, final double sigmaY )
		{
			this( new double[] { sigmaX, sigmaY } );
		}

		public GaussianBlur( final double sigma )
		{
			this( sigma, sigma );
		}

		@Override
		public Tuple2< K, FPTuple > call( final Tuple2< K, FPTuple > t ) throws Exception
		{
			final FloatProcessor fp = ( FloatProcessor ) t._2().rebuild().duplicate();
			new ij.plugin.filter.GaussianBlur().blurFloat( fp, sigma[ 0 ], sigma[ 1 ], 1e-4 );
			return Utility.tuple2( t._1(), new FPTuple( fp ) );
		}
	}

	public static int xyToLinear( final int width, final int x, final int y )
	{
		return width * y + x;
	}

	public static int[] linearToXY( final int width, final int i, final int[] xy )
	{
		xy[ 1 ] = i / width;
		xy[ 0 ] = i - xy[ 1 ] * width;
		return xy;
	}

	// takes backward transform from source to target
	public static class Transform< K > implements PairFunction< Tuple2< K, Tuple2< FloatProcessor, double[] > >, K, FloatProcessor >
	{
		@Override
		public Tuple2< K, FloatProcessor > call( final Tuple2< K, Tuple2< FloatProcessor, double[] > > t ) throws Exception
		{
			final Tuple2< FloatProcessor, double[] > t2 = t._2();
			final int width = t2._1().getWidth();
			final int height = t2._1().getHeight();
			final Img< FloatType > matrix = ImageJFunctions.wrapFloat( new ImagePlus( "", t2._1() ) );
			final float[] data = new float[ width * height ];
			final ArrayImg< FloatType, FloatArray > transformed = ArrayImgs.floats( data, width, height );
			final RealRandomAccessible< FloatType > source = Views.interpolate( Views.extendValue( matrix, new FloatType( Float.NaN ) ), new NLinearInterpolatorFactory< FloatType >() );
			final LUTRealTransform transform = new LUTRealTransform( t2._2(), 2, 2 );
			final Cursor< FloatType > s = Views.flatIterable( Views.interval( Views.raster( new RealTransformRealRandomAccessible<>( source, transform ) ), matrix ) ).cursor();
			final ArrayCursor< FloatType > tc = transformed.cursor();
			while ( tc.hasNext() )
				tc.next().set( s.next() );

			return Utility.tuple2( t._1(), new FloatProcessor( width, height, data ) );
		}
	}

	public static class FlatmapMap< K1, K2, V, M extends Map< K2, V > > implements PairFlatMapFunction< Tuple2< K1, M >, K1, Tuple2< K2, V > >
	{
		@Override
		public Iterator< Tuple2< K1, Tuple2< K2, V > > > call( final Tuple2< K1, M > t ) throws Exception
		{
			return new IterableWithConstKeyFromMap<>( t._1(), t._2() ).iterator();
		}
	}

	public static class EntryToTuple< K, V1, V2, E extends Map.Entry< V1, V2 > > implements PairFunction< Tuple2< K, E >, K, Tuple2< V1, V2 > >
	{

		@Override
		public Tuple2< K, Tuple2< V1, V2 > > call( final Tuple2< K, E > t ) throws Exception
		{
			return Utility.tuple2( t._1(), Utility.tuple2( t._2().getKey(), t._2().getValue() ) );
		}
	}

	public static class IterableWithConstKeyFromMap< K1, K2, V > implements Iterable< Tuple2< K1, Tuple2< K2, V > > >, Serializable
	{

		private final K1 k;

		private final Map< K2, V > m;

		public IterableWithConstKeyFromMap( final K1 k, final Map< K2, V > m )
		{
			this.k = k;
			this.m = m;
		}

		@Override
		public Iterator< Tuple2< K1, Tuple2< K2, V > > > iterator()
		{
			final Iterator< Map.Entry< K2, V > > it = m.entrySet().iterator();
			return new Iterator< Tuple2< K1, Tuple2< K2, V > > >()
			{
				@Override
				public boolean hasNext()
				{
					return it.hasNext();
				}

				@Override
				public Tuple2< K1, Tuple2< K2, V > > next()
				{
					final Map.Entry< K2, V > entry = it.next();
					return Utility.tuple2( k, Utility.tuple2( entry.getKey(), entry.getValue() ) );
				}

				@Override
				public void remove()
				{
					throw new UnsupportedOperationException();
				}
			};
		}
	}

	public static class IterableWithConstKey< K, V > implements Iterable< Tuple2< K, V > >, Serializable
	{

		private final K key;

		private final Iterable< V > iterable;

		public IterableWithConstKey( final K key, final Iterable< V > iterable )
		{
			this.key = key;
			this.iterable = iterable;
		}

		@Override
		public Iterator< Tuple2< K, V > > iterator()
		{
			return new Iterator< Tuple2< K, V > >()
			{
				final Iterator< V > it = iterable.iterator();

				@Override
				public boolean hasNext()
				{
					return it.hasNext();
				}

				@Override
				public Tuple2< K, V > next()
				{
					return Utility.tuple2( key, it.next() );
				}

				@Override
				public void remove()
				{
					throw new UnsupportedOperationException();
				}
			};
		}
	}

	public static class ValueAsMap< K1, K2, V > implements PairFunction< Tuple2< K1, Tuple2< K2, V > >, K1, HashMap< K2, V > >
	{

		@Override
		public Tuple2< K1, HashMap< K2, V > > call( final Tuple2< K1, Tuple2< K2, V > > t ) throws Exception
		{
			final HashMap< K2, V > hm = new HashMap<>();
			hm.put( t._2()._1(), t._2()._2() );
			return Utility.tuple2( t._1(), hm );
		}
	}

	public static class ReduceMapsByUnion< K, V, M extends Map< K, V > > implements Function2< M, M, M >
	{

		@Override
		public M call( final M m1, final M m2 ) throws Exception
		{
			m1.putAll( m2 );
			return m1;
		}
	}

	// public static class SwapMultiKey<K1,K2,V> implements
	// PairFunction<Tuple2<K1,Tuple2<K2,V>>,K2,Tuple2<K1,V>>
	// {
	//
	// @Override
	// public Tuple2<K2, Tuple2<K1, V>> call(Tuple2<K1, Tuple2<K2, V>> t) throws
	// Exception {
	// return Utility.tuple2( t._2()._1(), Utility.tuple2( t._1(), t._2()._2() )
	// );
	// }
	// }

	public static void main( final String[] args )
	{
		final SparkConf conf = new SparkConf().setAppName( "BLA" ).setMaster( "local" );
		final JavaSparkContext sc = new JavaSparkContext( conf );
	}

	public static class Format< K > implements PairFunction< K, K, String >
	{

		private final String pattern;

		public Format( final String pattern )
		{
			this.pattern = pattern;
		}

		@Override
		public Tuple2< K, String > call( final K k ) throws Exception
		{
			return Utility.tuple2( k, String.format( pattern, k ) );
		}
	}

	public static class LoadFileFromPattern implements PairFunction< Integer, Integer, FloatProcessor >
	{

		private final String pattern;

		public LoadFileFromPattern( final String pattern )
		{
			this.pattern = pattern;
		}

		@Override
		public Tuple2< Integer, FloatProcessor > call( final Integer k ) throws Exception
		{
			final String path = String.format( pattern, k.intValue() );
			final FloatProcessor fp = new ImagePlus( path ).getProcessor().convertToFloatProcessor();
			return Utility.tuple2( k, fp );
		}
	}

	public static class LoadFileTupleFromPatternTuple implements PairFunction< Integer, Integer, Tuple2< FPTuple, FPTuple > >
	{

		private final Tuple2< String, String > patternTuple;

		public LoadFileTupleFromPatternTuple( final String p1, final String p2 )
		{
			this( Utility.tuple2( p1, p2 ) );
		}

		public LoadFileTupleFromPatternTuple( final Tuple2< String, String > patternTuple )
		{
			this.patternTuple = patternTuple;
		}

		@Override
		public Tuple2< Integer, Tuple2< FPTuple, FPTuple > > call( final Integer k ) throws Exception
		{
			final String path = String.format( patternTuple._1(), k.intValue() );
			final FloatProcessor fp = new ImagePlus( path ).getProcessor().convertToFloatProcessor();
			final FloatProcessor fpMask = generateMask( patternTuple._2(), k.intValue(), fp );
			return Utility.tuple2( k, Utility.tuple2( new FPTuple( fp ), new FPTuple( fpMask ) ) );
		}

		public static FloatProcessor generateMask( final String pattern, final int z, final FloatProcessor image )
		{
			if ( pattern.isEmpty() )
			{
				final FloatProcessor result = new FloatProcessor( image.getWidth(), image.getHeight() );
				result.add( 1.0 );
				return result;
			}
			else
				return new ImagePlus( String.format( pattern, z ) ).getProcessor().convertToFloatProcessor();
		}
	}

	public static class LoadFile implements PairFunction< Tuple2< Integer, String >, Integer, FPTuple >
	{

		private static final long serialVersionUID = -5220501390575963707L;

		@Override
		public Tuple2< Integer, FPTuple > call( final Tuple2< Integer, String > tuple ) throws Exception
		{
			final Integer index = tuple._1();
			final String filename = tuple._2();
			final FloatProcessor fp = new ImagePlus( filename ).getProcessor().convertToFloatProcessor();
			return Utility.tuple2( index, new FPTuple( fp ) );
		}
	}

	public static class ReplaceValue implements PairFunction< Tuple2< Integer, FPTuple >, Integer, FPTuple >
	{

		private final float value;

		private final float replacement;

		/**
		 * @param value
		 * @param replacement
		 */
		public ReplaceValue( final float value, final float replacement )
		{
			super();
			this.value = value;
			this.replacement = replacement;
		}

		private static final long serialVersionUID = 5642948550521110112L;

		@Override
		public Tuple2< Integer, FPTuple > call( final Tuple2< Integer, FPTuple > indexedFloatProcessor ) throws Exception
		{
			final FloatProcessor fp = ( FloatProcessor ) indexedFloatProcessor._2().rebuild().duplicate();
			final float[] pixels = ( float[] ) fp.getPixels();
			for ( int i = 0; i < pixels.length; ++i )
				if ( pixels[ i ] == this.value )
					pixels[ i ] = this.replacement;
			return new Tuple2<>( indexedFloatProcessor._1(), new FPTuple( fp ) );
		}

	}

	public static < T extends RealType< T > > RandomAccessibleInterval< T > transform( final RandomAccessible< T > source, final RandomAccessibleInterval< T > target, final RandomAccessibleInterval< DoubleType > lut ) throws InterruptedException
	{
		final Cursor< RealComposite< DoubleType > > lutCursor = Views.flatIterable( Views.collapseReal( lut ) ).cursor();
		final ArrayList< Callable< Void > > callables = new ArrayList<>();
		final ExecutorService es = Executors.newFixedThreadPool( Runtime.getRuntime().availableProcessors() );
		final long size = lut.dimension( 2 );
		while ( lutCursor.hasNext() )
		{
			lutCursor.fwd();
			final long x = lutCursor.getLongPosition( 0 );
			final long y = lutCursor.getLongPosition( 1 );
			final RealRandomAccessible< T > sourceColumn = Views.interpolate( Views.hyperSlice( Views.hyperSlice( source, 1, y ), 0, x ), new NLinearInterpolatorFactory< T >() );
			final IntervalView< T > targetColumn = Views.hyperSlice( Views.hyperSlice( target, 1, y ), 0, x );
			final IntervalView< DoubleType > lutColumn = Views.hyperSlice( Views.hyperSlice( lut, 1, y ), 0, x );

			final IntervalView< T > targetColumn3D = Views.offsetInterval( target, new long[] { x, y, 0 }, new long[] { 1, 1, size } );
			final IntervalView< DoubleType > lutColumn3D = Views.offsetInterval( lut, new long[] { x, y, 0 }, new long[] { 1, 1, size } );
			final RealPoint p = new RealPoint( targetColumn3D.numDimensions() );

			final SingleDimensionLUTRealTransformField transform = new SingleDimensionLUTRealTransformField( 3, 3, lutColumn3D );
			callables.add( () -> {
				final RealRandomAccess< T > ra = sourceColumn.realRandomAccess();
				final Cursor< T > c = Views.flatIterable( targetColumn3D ).cursor();
				while ( c.hasNext() )
				{
					final T t = c.next();
					transform.apply( c, p );
					ra.setPosition( p.getDoublePosition( 2 ), 0 );
					t.set( ra.get() );
				}
				return null;
			} );
		}
		System.out.println( "Invoking all column jobs." );
		System.out.flush();
		es.invokeAll( callables );
		return target;
	}

	/**
	 * Transform RDD into PairRDD with same value as key:
	 *
	 * @author Philipp Hanslovsky &lt;hanslovskyp@janelia.hhmi.org&gt;
	 *
	 * @param <T>
	 */
	public static class Duplicate< T > implements PairFunction< T, T, T >
	{

		/**
		 *
		 */
		private static final long serialVersionUID = -2287251828070115337L;

		@Override
		public Tuple2< T, T > call( final T t ) throws Exception
		{
			return Utility.tuple2( t, t );
		}

	}

	/**
	 *
	 * Accept all entries with an integer key k: start &lt;= k &lt; stop
	 *
	 * @author Philipp Hanslovsky &lt;hanslovskyp@janelia.hhmi.org&gt;
	 *
	 * @param <V>
	 *            value
	 */
	public static class FilterRange< V > implements Function< Tuple2< Integer, V >, Boolean >
	{

		/**
		 *
		 */
		private static final long serialVersionUID = -2112302031715609728L;

		private final long start;

		private final long stop;

		public FilterRange( final long start, final long stop )
		{
			this.start = start;
			this.stop = stop;
		}

		@Override
		public Boolean call( final Tuple2< Integer, V > t ) throws Exception
		{
			final int unboxed = t._1().intValue();
			return unboxed >= start && unboxed < stop;
		}
	}

	/**
	 * swap keys K1, K2 for key K1 pointing to a key-value pair K2, V
	 *
	 * rdd in: ( K1 -> K2,V ) rdd out: ( K2 -> K1,V )
	 *
	 * @param <K1>
	 * @param <K2>
	 * @param <V>
	 */
	public static class SwapKeyKey< K1, K2, V > implements PairFunction< Tuple2< K1, Tuple2< K2, V > >, K2, Tuple2< K1, V > >
	{
		private static final long serialVersionUID = -4848162685420711301L;

		@Override
		public Tuple2< K2, Tuple2< K1, V > > call( final Tuple2< K1, Tuple2< K2, V > > t ) throws Exception
		{
			final Tuple2< K2, V > t2 = t._2();
			return Utility.tuple2( t2._1(), Utility.tuple2( t._1(), t2._2() ) );
		}
	}
}
