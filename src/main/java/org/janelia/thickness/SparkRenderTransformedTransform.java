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
		final JavaPairRDD< Integer, FloatProcessor > sections = sc.parallelize( indices ).mapToPair( arg0 -> Utility.tuple2( arg0, arg0 ) ).sortByKey().map( arg0 -> arg0._1() ).mapToPair( new Utility.StackOpener( sourceFormat, isPattern ) );

		final JavaPairRDD< Integer, FloatProcessor > transforms = sc.parallelize( indices ).mapToPair( arg0 -> Utility.tuple2( arg0, arg0 ) ).sortByKey().map( arg0 -> arg0._1() ).mapToPair( integer -> {
			final ImagePlus imp = new ImagePlus( String.format( transformFormat, integer.intValue() ) );
			final FloatProcessor fp = imp.getProcessor().convertToFloatProcessor();
			return Utility.tuple2( integer, fp );
		} );

		final JavaPairRDD< Integer, FloatProcessor > transformed = render( sc, sections, transforms, stepsDouble, radiiDouble, dim );

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

	public static JavaPairRDD< Integer, FloatProcessor > render( final JavaSparkContext sc, final JavaPairRDD< Integer, FloatProcessor > sections, final JavaPairRDD< Integer, FloatProcessor > transforms, final double[] stepsDouble, final double[] radiiDouble, final long[] dim )
	{

		final int width = ( int ) dim[ 0 ];
		final int height = ( int ) dim[ 1 ];

		final JavaPairRDD< Integer, Tuple2< FloatProcessor, Integer > > transformsToRequiredSectionsMapping = transforms.mapToPair( t -> {
			final HashSet< Integer > s = new HashSet<>();
			// why need to generate high res image?
			final FloatProcessor transformFP = t._2();
			final ArrayImg< FloatType, FloatArray > transformImg = ArrayImgs.floats( ( float[] ) transformFP.getPixels(), transformFP.getWidth(), transformFP.getHeight() );
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
		} ).flatMapToPair( t -> {
			final Integer idx = t._1();
			final FloatProcessor data = t._2()._1();
			final HashSet< Integer > s = t._2()._2();
			final Iterable< Tuple2< Integer, Tuple2< FloatProcessor, Integer > > > it = () -> new Iterator< Tuple2< Integer, Tuple2< FloatProcessor, Integer > > >()
			{
				final Iterator< Integer > it = s.iterator();

				@Override
				public boolean hasNext()
				{
					return it.hasNext();
				}

				@Override
				public Tuple2< Integer, Tuple2< FloatProcessor, Integer > > next()
				{
					return Utility.tuple2( idx, Utility.tuple2( data, it.next() ) );
				}

				@Override
				public void remove()
				{

				}
			};
			return it.iterator();
		} )
				// ( target-index -> ... ==> source-index -> ... )
				.mapToPair( t -> Utility.tuple2( t._2()._2(), Utility.tuple2( t._2()._1(), t._1() ) ) );

		final JavaPairRDD< Integer, Tuple2< FloatProcessor, HashMap< Integer, FloatProcessor > > > targetsAndSections = sections.join( transformsToRequiredSectionsMapping ).mapToPair( t -> {
			final Integer sectionIndex = t._1();
			final Integer targetIndex = t._2()._2()._2();
			final FloatProcessor transform = t._2()._2()._1();
			final FloatProcessor section = t._2()._1();
			final HashMap< Integer, FloatProcessor > m = new HashMap<>();
			m.put( sectionIndex, section );
			return Utility.tuple2( targetIndex, Utility.tuple2( transform, m ) );
		} ).reduceByKey( ( t1, t2 ) -> {
			final HashMap< Integer, FloatProcessor > m = t1._2();
			m.putAll( t2._2() );
			return Utility.tuple2( t1._1(), m );
		} );

		final JavaPairRDD< Integer, FloatProcessor > transformed = targetsAndSections.mapToPair( t -> {
			final Integer index = t._1();
			final FloatProcessor transformFP = t._2()._1();
			final HashMap< Integer, FloatProcessor > m = t._2()._2();
			final ArrayImg< FloatType, FloatArray > transformImg = ArrayImgs.floats( ( float[] ) transformFP.getPixels(), transformFP.getWidth(), transformFP.getHeight() );
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

				final FloatProcessor lower = m.get( lowerPos );
				if ( lower != null )
				{
					weightSum += lowerWeight;
					val += lower.getf( x, y ) * lowerWeight;
				}

				final FloatProcessor upper = m.get( upperPos );
				if ( upper != null )
				{
					weightSum += upperWeight;
					val += upper.getf( x, y ) * upperWeight;
				}

				fv.setReal( val / weightSum );

			}
			return Utility.tuple2( index, new FloatProcessor( width, height, targetPixels ) );
		} );

		return transformed;

	}

}
