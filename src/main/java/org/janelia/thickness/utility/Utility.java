package org.janelia.thickness.utility;

import java.io.File;
import java.io.Serializable;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.Iterator;
import java.util.Map;

import org.apache.spark.api.java.function.Function;
import org.apache.spark.api.java.function.Function2;
import org.apache.spark.api.java.function.PairFlatMapFunction;
import org.apache.spark.api.java.function.PairFunction;
import org.janelia.thickness.lut.LUTRealTransform;
import org.janelia.thickness.lut.SingleDimensionLUTRealTransform;

import ij.ImagePlus;
import ij.io.FileSaver;
import ij.process.ByteProcessor;
import ij.process.ColorProcessor;
import ij.process.FloatProcessor;
import ij.process.ImageProcessor;
import ij.process.ShortProcessor;
import mpicbg.trakem2.util.Downsampler;
import net.imglib2.Cursor;
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
import net.imglib2.type.numeric.real.DoubleType;
import net.imglib2.type.numeric.real.FloatType;
import net.imglib2.view.Views;
import scala.Tuple2;
import scala.Tuple3;
import scala.Tuple4;
import scala.Tuple5;
import scala.Tuple6;

/**
 * @author Philipp Hanslovsky &lt;hanslovskyp@janelia.hhmi.org&gt;
 */
public class Utility
{

	public static < U, V > Tuple2< U, V > tuple2( final U u, final V v )
	{
		return new Tuple2< U, V >( u, v );
	}

	public static Tuple2< Integer, Integer > tuple2( int[] arr )
	{
		return Utility.tuple2( arr[ 0 ], arr[ 1 ] );
	}

	public static Tuple2< Long, Long > tuple2( long[] arr )
	{
		return Utility.tuple2( arr[ 0 ], arr[ 1 ] );
	}

	public static int[] arrayInt( Tuple2< Integer, Integer > t )
	{
		return arrayInt( t, new int[ 2 ] );
	}

	public static int[] arrayInt( Tuple2< Integer, Integer > t, int[] array )
	{
		array[ 0 ] = t._1();
		array[ 1 ] = t._2();
		return array;
	}

	public static long[] arrayLong( Tuple2< Long, Long > t )
	{
		return arrayLong( t, new long[ 2 ] );
	}

	public static long[] arrayLong( Tuple2< Long, Long > t, long[] array )
	{
		array[ 0 ] = t._1();
		array[ 1 ] = t._2();
		return array;
	}

	public static double[] arrayDouble( Tuple2< Double, Double > t )
	{
		return arrayDouble( t, new double[ 2 ] );
	}

	public static double[] arrayDouble( Tuple2< Double, Double > t, double[] array )
	{
		array[ 0 ] = t._1();
		array[ 1 ] = t._2();
		return array;
	}

	public static Tuple2< Double, Double > min( Tuple2< Double, Double > t1, Tuple2< Double, Double > t2 )
	{
		return Utility.tuple2( Math.min( t1._1(), t2._1() ), Math.min( t1._2(), t2._2() ) );
	}

	public static Tuple2< Double, Double > max( Tuple2< Double, Double > t1, Tuple2< Double, Double > t2 )
	{
		return Utility.tuple2( Math.max( t1._1(), t2._1() ), Math.max( t1._2(), t2._2() ) );
	}

	public static < U, V, W > Tuple3< U, V, W > tuple3( final U u, final V v, final W w )
	{
		return new Tuple3< U, V, W >( u, v, w );

	}

	public static < U, V, W, X > Tuple4< U, V, W, X > tuple4( final U u, final V v, final W w, final X x )
	{
		return new Tuple4< U, V, W, X >( u, v, w, x );
	}

	public static < U, V, W, X, Y > Tuple5< U, V, W, X, Y > tuple5( final U u, final V v, final W w, final X x, final Y y )
	{
		return new Tuple5< U, V, W, X, Y >( u, v, w, x, y );
	}

	public static < U, V, W, X, Y, Z > Tuple6< U, V, W, X, Y, Z > tuple6( final U u, final V v, final W w, final X x, final Y y, final Z z )
	{
		return new Tuple6< U, V, W, X, Y, Z >( u, v, w, x, y, z );
	}

	public static ArrayList< Integer > arange( final int stop )
	{
		return ( arange( 0, stop ) );
	}

	public static ArrayList< Integer > arange( final int start, final int stop )
	{
		final ArrayList< Integer > numbers = new ArrayList< Integer >();
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
		final ArrayList< Long > numbers = new ArrayList< Long >();
		for ( long i = start; i < stop; ++i )
			numbers.add( i );
		return numbers;
	}

	public static class ConvertImageProcessor
	{
		public static enum TYPE
		{
			BYTE_PROCESSOR,
			SHORT_PROCESSOR,
			FLOAT_PROCESSOR,
			COLOR_PROCESSOR
		}

		public static TYPE getType( final ImageProcessor ip )
		{
			if ( ip instanceof ByteProcessor )
				return TYPE.BYTE_PROCESSOR;
			else if ( ip instanceof ShortProcessor )
				return TYPE.SHORT_PROCESSOR;
			else if ( ip instanceof FloatProcessor )
				return TYPE.FLOAT_PROCESSOR;
			else if ( ip instanceof ColorProcessor )
				return TYPE.COLOR_PROCESSOR;
			else
				return null;
		}

		public static ImageProcessor convert( final ImageProcessor ip, final TYPE type )
		{
			final ImageProcessor result;
			switch ( type )
			{
			case BYTE_PROCESSOR:
				result = ip.convertToByteProcessor();
				break;
			case SHORT_PROCESSOR:
				result = ip.convertToShortProcessor();
				break;
			case FLOAT_PROCESSOR:
				result = ip.convertToFloatProcessor();
				break;
			case COLOR_PROCESSOR:
				result = ip.convertToColorProcessor();
				break;
			default:
				result = null;
				break;
			}
			return result;
		}

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

		public Tuple2< V, K > call( final Tuple2< K, V > t ) throws Exception
		{
			return tuple2( t._2(), t._1() );
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

		public Tuple2< K2, Tuple2< K1, V > > call( final Tuple2< K1, Tuple2< K2, V > > t )
				throws Exception
		{
			final Tuple2< K2, V > t2 = t._2();
			return Utility.tuple2( t2._1(), Utility.tuple2( t._1(), t2._2() ) );
		}
	}

	public static class WriteToFormatString< K > implements PairFunction< Tuple2< K, FloatProcessor >, K, Boolean >
	{
		private static final long serialVersionUID = -4095400790949311166L;

		private final String outputFormat;

		@SuppressWarnings( "unused" )
		private final ConvertImageProcessor.TYPE ipType;

		public WriteToFormatString( final String outputFormat )
		{
			this( outputFormat, ConvertImageProcessor.TYPE.FLOAT_PROCESSOR );
		}

		public WriteToFormatString( final String outputFormat, final ConvertImageProcessor.TYPE ipType )
		{
			super();
			this.outputFormat = outputFormat;
			this.ipType = ipType;
		}

		public Tuple2< K, Boolean > call( final Tuple2< K, FloatProcessor > t )
				throws Exception
		{
			final K index = t._1();
			final ImagePlus imp = new ImagePlus( "", t._2() );
			final String path = String.format( outputFormat, index );
			new File( path ).getParentFile().mkdirs();
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

		public Tuple2< K, Boolean > call( final Tuple2< K, DPTuple > t )
				throws Exception
		{
			final K index = t._1();
			final DPTuple dp = t._2();
			final ArrayImg< DoubleType, DoubleArray > img = ArrayImgs.doubles( dp.pixels, dp.width, dp.height );
			final ConvertedRandomAccessibleInterval< DoubleType, FloatType > floats =
					new ConvertedRandomAccessibleInterval< DoubleType, FloatType >( img, new RealFloatConverter< DoubleType >(), new FloatType() );
			final ImagePlus imp = ImageJFunctions.wrap( floats, "" );
			final String path = String.format( outputFormat, index );
			new File( path ).getParentFile().mkdirs();
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
		public Tuple2< K, FloatProcessor > call(
				final Tuple2< K, FloatProcessor > indexedFloatProcessor ) throws Exception
		{
			final FloatProcessor downsampled = ( FloatProcessor ) Downsampler.downsampleImageProcessor( indexedFloatProcessor._2(), sampleScale );
			return Utility.tuple2( indexedFloatProcessor._1(), downsampled );
		}

	}

	public static int xyToLinear( final int width, final int x, final int y )
	{
		return width * y + x;
	}

	public static int[] linearToXY( final int width, final int i, final int[] xy )
	{
		xy[ 1 ] = i / width;
		xy[ 0 ] = i - ( xy[ 1 ] * width );
		return xy;
	}

	// takes backward transform from source to target
	public static class Transform< K > implements PairFunction< Tuple2< K, Tuple2< FloatProcessor, double[] > >, K, FloatProcessor >
	{
		/**
		 * 
		 */
		private static final long serialVersionUID = -7333177858409257925L;

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
			final Cursor< FloatType > s =
					Views.flatIterable(
							Views.interval(
									Views.raster(
											new RealTransformRealRandomAccessible< FloatType, LUTRealTransform >( source, transform ) ),
									matrix ) )
							.cursor();
			final ArrayCursor< FloatType > tc = transformed.cursor();
			while ( tc.hasNext() )
				tc.next().set( s.next() );

			return Utility.tuple2( t._1(), new FloatProcessor( width, height, data ) );
		}
	}

	public static class FlatmapMap< K1, K2, V, M extends Map< K2, V > > implements PairFlatMapFunction< Tuple2< K1, M >, K1, Tuple2< K2, V > >
	{
		/**
		 * 
		 */
		private static final long serialVersionUID = 5871607022107221948L;

		@Override
		public Iterable< Tuple2< K1, Tuple2< K2, V > > > call( final Tuple2< K1, M > t ) throws Exception
		{
			return new IterableWithConstKeyFromMap< K1, K2, V >( t._1(), t._2() );
		}
	}

	public static class IterableWithConstKeyFromMap< K1, K2, V > implements Iterable< Tuple2< K1, Tuple2< K2, V > > >, Serializable
	{

		/**
		 * 
		 */
		private static final long serialVersionUID = 144940735137963745L;

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

		/**
		 * 
		 */
		private static final long serialVersionUID = -1483858948362400784L;

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

		/**
		 * 
		 */
		private static final long serialVersionUID = -6733180103222389437L;

		@Override
		public Tuple2< K1, HashMap< K2, V > > call( final Tuple2< K1, Tuple2< K2, V > > t ) throws Exception
		{
			final HashMap< K2, V > hm = new HashMap< K2, V >();
			hm.put( t._2()._1(), t._2()._2() );
			return Utility.tuple2( t._1(), hm );
		}
	}

	public static class ReduceMapsByUnion< K, V, M extends Map< K, V > > implements Function2< M, M, M >
	{

		/**
		 * 
		 */
		private static final long serialVersionUID = -1493458715879120485L;

		@Override
		public M call( final M m1, final M m2 ) throws Exception
		{
			m1.putAll( m2 );
			return m1;
		}
	}

	public static class LoadFileFromPattern implements PairFunction< Integer, Integer, FloatProcessor >
	{

		/**
		 * 
		 */
		private static final long serialVersionUID = 3090174862493622888L;

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

	public static class LoadFile implements PairFunction< Tuple2< Integer, String >, Integer, FloatProcessor >
	{

		private static final long serialVersionUID = -5220501390575963707L;

		@Override
		public Tuple2< Integer, FloatProcessor > call( final Tuple2< Integer, String > tuple ) throws Exception
		{
			final Integer index = tuple._1();
			final String filename = tuple._2();
			final FloatProcessor fp = new ImagePlus( filename ).getProcessor().convertToFloatProcessor();
			return Utility.tuple2( index, fp );
		}
	}

	public static class ReplaceValue implements PairFunction< Tuple2< Integer, FloatProcessor >, Integer, FloatProcessor >
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
		public Tuple2< Integer, FloatProcessor > call(
				final Tuple2< Integer, FloatProcessor > indexedFloatProcessor ) throws Exception
		{
			final FloatProcessor fp = ( FloatProcessor ) indexedFloatProcessor._2().duplicate();
			final float[] pixels = ( float[] ) fp.getPixels();
			for ( int i = 0; i < pixels.length; ++i )
				if ( pixels[ i ] == this.value )
					pixels[ i ] = this.replacement;
			return Utility.tuple2( indexedFloatProcessor._1(), fp );
		}

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
		public Tuple2< T, T > call( T t ) throws Exception
		{
			return Utility.tuple2( t, t );
		}

	}

	/**
	 * 
	 * Drop value of PairRDD
	 * 
	 * @author Philipp Hanslovsky &lt;hanslovskyp@janelia.hhmi.org&gt;
	 *
	 * @param <K>
	 * @param <V>
	 */
	public static class DropValue< K, V > implements Function< Tuple2< K, V >, K >
	{

		/**
		 *
		 */
		private static final long serialVersionUID = -2208916056673976309L;

		@Override
		public K call( Tuple2< K, V > t ) throws Exception
		{
			return t._1();
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

		public FilterRange( long start, long stop )
		{
			this.start = start;
			this.stop = stop;
		}

		@Override
		public Boolean call( Tuple2< Integer, V > t ) throws Exception
		{
			int unboxed = t._1().intValue();
			return unboxed >= start && unboxed < stop;
		}
	}

	/**
	 * 
	 * Invert look-up table
	 * 
	 * @author Philipp Hanslovsky &lt;hanslovskyp@janelia.hhmi.org&gt;
	 *
	 * @param <K>
	 *            key
	 */
	public static class InvertLut< K > implements PairFunction< Tuple2< K, double[] >, K, double[] >
	{
		/**
		 * 
		 */
		private static final long serialVersionUID = 6958801646350755543L;

		@Override
		public Tuple2< K, double[] > call( Tuple2< K, double[] > t ) throws Exception
		{
			final double[] lut = t._2();
			SingleDimensionLUTRealTransform transform =
					new SingleDimensionLUTRealTransform( lut, 1, 1, 0 );
			double[] inverse = new double[ lut.length ];
			double[] source = new double[ 1 ];
			double[] target = new double[ 1 ];
			for ( int i = 0; i < inverse.length; ++i )
			{
				target[ 0 ] = i;
				transform.applyInverse( source, target );
				inverse[ i ] = source[ 0 ];
			}
			return Utility.tuple2( t._1(), inverse );
		}
	}

	public static class AddOffsetToKey< V > implements PairFunction< Tuple2< Integer, V >, Integer, V >
	{

		/**
		 * 
		 */
		private static final long serialVersionUID = 4422574591898855269L;

		private final int offset;

		public AddOffsetToKey( int offset )
		{
			super();
			this.offset = offset;
		}

		@Override
		public Tuple2< Integer, V > call( Tuple2< Integer, V > t ) throws Exception
		{
			return Utility.tuple2( t._1() + offset, t._2() );
		}

	}

}
