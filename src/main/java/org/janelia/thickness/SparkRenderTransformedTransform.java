/**/package org.janelia.thickness;

import java.io.IOException;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.HashSet;
import java.util.Iterator;
import java.util.List;
import java.util.regex.Pattern;

import org.apache.spark.SparkConf;
import org.apache.spark.api.java.JavaPairRDD;
import org.apache.spark.api.java.JavaSparkContext;
import org.apache.spark.api.java.function.Function;
import org.apache.spark.api.java.function.Function2;
import org.apache.spark.api.java.function.PairFlatMapFunction;
import org.apache.spark.api.java.function.PairFunction;
import org.janelia.thickness.utility.FPTuple;
import org.janelia.thickness.utility.Utility;

import ij.ImagePlus;
import ij.process.FloatProcessor;
import loci.formats.FileStitcher;
import loci.formats.FormatException;
import net.imglib2.Cursor;
import net.imglib2.FinalInterval;
import net.imglib2.RealRandomAccess;
import net.imglib2.RealRandomAccessible;
import net.imglib2.img.array.ArrayCursor;
import net.imglib2.img.array.ArrayImg;
import net.imglib2.img.array.ArrayImgs;
import net.imglib2.img.basictypeaccess.array.FloatArray;
import net.imglib2.interpolation.randomaccess.NLinearInterpolatorFactory;
import net.imglib2.realtransform.InverseRealTransform;
import net.imglib2.realtransform.RealTransformRandomAccessible;
import net.imglib2.realtransform.RealViews;
import net.imglib2.realtransform.ScaleAndTranslation;
import net.imglib2.type.numeric.real.FloatType;
import net.imglib2.view.Views;
import scala.Tuple2;

/**
 * Created by hanslovskyp on 10/22/15.
 */
public class SparkRenderTransformedTransform
{

	public static void main( final String[] args ) throws IOException, FormatException
	{

		final SparkConf sparkConf = new SparkConf().setAppName( "Render" );
		final JavaSparkContext sc = new JavaSparkContext( sparkConf );

		// final String sourceFormat = args[0];
		// final String transformFormat = args[1];
		final String configPath = args[ 0 ];
		final int iteration = Integer.parseInt( args[ 1 ] );
		final String outputFormat = args[ 2 ];
		final ScaleOptions options = ScaleOptions.createFromFile( configPath );
		final int[] radii = options.radii[ iteration ];
		final int[] steps = options.steps[ iteration ];
		final String sourceFormat = options.target + String.format( "/%02d/forward/", iteration ) + "<0000-2100>.tif";
		final String transformFormat = options.target + String.format( "/%02d/backward/", iteration ) + "%04d.tif";

		// final String outputFormat = args[4];

		final boolean isPattern = Pattern.compile( ".*<\\d+-\\d+>.*" ).matcher( sourceFormat ).matches(); // fileName.indexOf(
																											// "%"
																											// )
																											// >
																											// -1;
		final FileStitcher reader = new FileStitcher( isPattern );
		reader.setId( sourceFormat );

		final int width = reader.getSizeX();
		final int height = reader.getSizeY();
		final int size = reader.getSizeZ(); // 201;//reader.getSizeZ();

		final long[] dim = new long[] { width, height, size };

		reader.close();

		final double[] radiiDouble = new double[] { radii[ 0 ], radii[ 1 ] };
		final double[] stepsDouble = new double[] { steps[ 0 ], steps[ 1 ] };

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
		} ).mapToPair( new Utility.StackOpener( sourceFormat, isPattern ) );

		final JavaPairRDD< Integer, FPTuple > transforms = sc.parallelize( indices ).mapToPair( new PairFunction< Integer, Integer, Integer >()
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
		} ).mapToPair( new PairFunction< Integer, Integer, FPTuple >()
		{
			@Override
			public Tuple2< Integer, FPTuple > call( final Integer integer ) throws Exception
			{
				final ImagePlus imp = new ImagePlus( String.format( transformFormat, integer.intValue() ) );
				final FloatProcessor fp = imp.getProcessor().convertToFloatProcessor();
				return Utility.tuple2( integer, new FPTuple( fp ) );
			}
		} );

		final JavaPairRDD< Integer, FPTuple > transformed = render( sc, sections, transforms, stepsDouble, radiiDouble, dim );

		final List< Tuple2< Integer, Boolean > > successOnWrite = transformed.mapToPair( new Utility.WriteToFormatString< Integer >( outputFormat ) ).collect();
		int count = 0;
		for ( final Tuple2< Integer, Boolean > s : successOnWrite )
		{
			if ( s._2().booleanValue() )
				continue;
			++count;
			System.out.println( "Failed to write forward image " + s._1().intValue() );
		}
		System.out.println( "Successfully transformed and wrote " + ( size - count ) + "/" + size + " images." );

	}

	public static JavaPairRDD< Integer, FPTuple > render( final JavaSparkContext sc, final JavaPairRDD< Integer, FPTuple > sections, final JavaPairRDD< Integer, FPTuple > transforms, final double[] stepsDouble, final double[] radiiDouble, final long[] dim )
	{

		final int width = ( int ) dim[ 0 ];
		final int height = ( int ) dim[ 1 ];

		final JavaPairRDD< Integer, Tuple2< FPTuple, Integer > > transformsToRequiredSectionsMapping = transforms.mapToPair( new PairFunction< Tuple2< Integer, FPTuple >, Integer, Tuple2< FPTuple, HashSet< Integer > > >()
		{
			@Override
			public Tuple2< Integer, Tuple2< FPTuple, HashSet< Integer > > > call( final Tuple2< Integer, FPTuple > t ) throws Exception
			{
				final HashSet< Integer > s = new HashSet<>();
				// why need to generate high res image?
				final FPTuple transformFP = t._2();
				final ArrayImg< FloatType, FloatArray > transformImg = ArrayImgs.floats( transformFP.pixels, transformFP.width, transformFP.height );
				final RealRandomAccessible< FloatType > extendedAndInterpolatedTransform = Views.interpolate( Views.extendBorder( transformImg ), new NLinearInterpolatorFactory< FloatType >() );
				final RealTransformRandomAccessible< FloatType, InverseRealTransform > scaledTransformImg = RealViews.transform( extendedAndInterpolatedTransform, new ScaleAndTranslation( stepsDouble, radiiDouble ) );
				final Cursor< FloatType > c = Views.flatIterable( Views.interval( Views.raster( scaledTransformImg ), new FinalInterval( width, height ) ) ).cursor();
				// for (float p : t._2().pixels) {
				while ( c.hasNext() )
				{
					final float p = c.next().get();
					if ( !Float.isNaN( p ) && !Float.isInfinite( p ) )
					{
						s.add( ( int ) Math.floor( p ) );
						s.add( ( int ) Math.ceil( p ) );
					}
				}
				return Utility.tuple2( t._1(), Utility.tuple2( t._2(), s ) );
			}
		} ).flatMapToPair( new PairFlatMapFunction< Tuple2< Integer, Tuple2< FPTuple, HashSet< Integer > > >, Integer, Tuple2< FPTuple, Integer > >()
		{
			@Override
			public Iterator< Tuple2< Integer, Tuple2< FPTuple, Integer > > > call( final Tuple2< Integer, Tuple2< FPTuple, HashSet< Integer > > > t ) throws Exception
			{
				final Integer idx = t._1();
				final FPTuple data = t._2()._1();
				final HashSet< Integer > s = t._2()._2();
				final Iterable< Tuple2< Integer, Tuple2< FPTuple, Integer > > > it = new Iterable< Tuple2< Integer, Tuple2< FPTuple, Integer > > >()
				{
					@Override
					public Iterator< Tuple2< Integer, Tuple2< FPTuple, Integer > > > iterator()
					{
						return new Iterator< Tuple2< Integer, Tuple2< FPTuple, Integer > > >()
						{
							final Iterator< Integer > it = s.iterator();

							@Override
							public boolean hasNext()
							{
								return it.hasNext();
							}

							@Override
							public Tuple2< Integer, Tuple2< FPTuple, Integer > > next()
							{
								return Utility.tuple2( idx, Utility.tuple2( data, it.next() ) );
							}

							@Override
							public void remove()
							{

							}
						};
					}
				};
				return it.iterator();
			}
		} )
				// ( target-index -> ... ==> source-index -> ... )
				.mapToPair( new PairFunction< Tuple2< Integer, Tuple2< FPTuple, Integer > >, Integer, Tuple2< FPTuple, Integer > >()
				{
					@Override
					public Tuple2< Integer, Tuple2< FPTuple, Integer > > call( final Tuple2< Integer, Tuple2< FPTuple, Integer > > t ) throws Exception
					{
						return Utility.tuple2( t._2()._2(), Utility.tuple2( t._2()._1(), t._1() ) );
					}
				} );

		final JavaPairRDD< Integer, Tuple2< FPTuple, HashMap< Integer, FPTuple > > > targetsAndSections = sections.join( transformsToRequiredSectionsMapping ).mapToPair( new PairFunction< Tuple2< Integer, Tuple2< FPTuple, Tuple2< FPTuple, Integer > > >, Integer, Tuple2< FPTuple, HashMap< Integer, FPTuple > > >()
		{
			@Override
			public Tuple2< Integer, Tuple2< FPTuple, HashMap< Integer, FPTuple > > > call( final Tuple2< Integer, Tuple2< FPTuple, Tuple2< FPTuple, Integer > > > t ) throws Exception
			{
				final Integer sectionIndex = t._1();
				final Integer targetIndex = t._2()._2()._2();
				final FPTuple transform = t._2()._2()._1();
				final FPTuple section = t._2()._1();
				final HashMap< Integer, FPTuple > m = new HashMap<>();
				m.put( sectionIndex, section );
				return Utility.tuple2( targetIndex, Utility.tuple2( transform, m ) );
			}
		} ).reduceByKey( new Function2< Tuple2< FPTuple, HashMap< Integer, FPTuple > >, Tuple2< FPTuple, HashMap< Integer, FPTuple > >, Tuple2< FPTuple, HashMap< Integer, FPTuple > > >()
		{
			@Override
			public Tuple2< FPTuple, HashMap< Integer, FPTuple > > call( final Tuple2< FPTuple, HashMap< Integer, FPTuple > > t1, final Tuple2< FPTuple, HashMap< Integer, FPTuple > > t2 ) throws Exception
			{
				final HashMap< Integer, FPTuple > m = t1._2();
				m.putAll( t2._2() );
				return Utility.tuple2( t1._1(), m );
			}
		} );

		final JavaPairRDD< Integer, FPTuple > transformed = targetsAndSections.mapToPair( new PairFunction< Tuple2< Integer, Tuple2< FPTuple, HashMap< Integer, FPTuple > > >, Integer, FPTuple >()
		{
			@Override
			public Tuple2< Integer, FPTuple > call( final Tuple2< Integer, Tuple2< FPTuple, HashMap< Integer, FPTuple > > > t ) throws Exception
			{
				final Integer index = t._1();
				final FPTuple transformFP = t._2()._1();
				final HashMap< Integer, FPTuple > m = t._2()._2();
				final ArrayImg< FloatType, FloatArray > transformImg = ArrayImgs.floats( transformFP.pixels, transformFP.width, transformFP.height );
				final RealRandomAccessible< FloatType > extendedAndInterpolatedTransform = Views.interpolate( Views.extendBorder( transformImg ), new NLinearInterpolatorFactory< FloatType >() );
				final RealTransformRandomAccessible< FloatType, InverseRealTransform > scaledTransformImg = RealViews.transform( extendedAndInterpolatedTransform, new ScaleAndTranslation( stepsDouble, radiiDouble ) );
				final RealRandomAccess< FloatType > transformRA = scaledTransformImg.realRandomAccess();
				final float[] targetPixels = new float[ ( int ) dim[ 0 ] * ( int ) dim[ 1 ] ];
				new FloatProcessor( ( int ) dim[ 0 ], ( int ) dim[ 1 ], targetPixels ).add( Double.NaN );
				for ( final ArrayCursor< FloatType > c = ArrayImgs.floats( targetPixels, dim[ 0 ], dim[ 1 ] ).cursor(); c.hasNext(); )
				{
					final FloatType fv = c.next();
					final int x = c.getIntPosition( 0 );
					final int y = c.getIntPosition( 1 );
					transformRA.setPosition( x, 0 );
					transformRA.setPosition( y, 1 );

					final float pos = transformRA.get().get();

					final int lowerPos = ( int ) pos;
					final int upperPos = lowerPos + 1;
					final float lowerWeight = upperPos - pos;
					final float upperWeight = 1 - lowerWeight;

					float weightSum = 0.0f;
					float val = 0.0f;

					final FPTuple lower = m.get( lowerPos );
					if ( lower != null )
					{
						weightSum += lowerWeight;
						val += lower.rebuild().getf( x, y ) * lowerWeight;
					}

					final FPTuple upper = m.get( upperPos );
					if ( upper != null )
					{
						weightSum += upperWeight;
						val += upper.rebuild().getf( x, y ) * upperWeight;
					}

					fv.setReal( val / weightSum );

				}
				return Utility.tuple2( index, new FPTuple( targetPixels, width, height ) );
			}
		} );

		return transformed;

	}

}
