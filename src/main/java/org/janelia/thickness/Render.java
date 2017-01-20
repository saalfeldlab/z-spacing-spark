package org.janelia.thickness;

import ij.ImagePlus;
import ij.io.FileSaver;
import ij.plugin.FolderOpener;
import ij.process.ImageConverter;
import net.imglib2.*;
import net.imglib2.converter.RealDoubleConverter;
import net.imglib2.converter.read.ConvertedRandomAccessibleInterval;
import net.imglib2.img.Img;
import net.imglib2.img.array.ArrayCursor;
import net.imglib2.img.array.ArrayImg;
import net.imglib2.img.array.ArrayImgs;
import net.imglib2.img.basictypeaccess.array.DoubleArray;
import net.imglib2.img.basictypeaccess.array.FloatArray;
import net.imglib2.img.display.imagej.ImageJFunctions;
import net.imglib2.interpolation.randomaccess.NLinearInterpolatorFactory;
import net.imglib2.realtransform.InverseRealTransform;
import net.imglib2.realtransform.RealTransformRealRandomAccessible;
import net.imglib2.realtransform.RealViews;
import net.imglib2.realtransform.ScaleAndTranslation;
import net.imglib2.type.numeric.real.DoubleType;
import net.imglib2.type.numeric.real.FloatType;
import net.imglib2.view.IntervalView;
import net.imglib2.view.Views;
import net.imglib2.view.composite.CompositeIntervalView;
import net.imglib2.view.composite.RealComposite;
import org.apache.spark.api.java.JavaPairRDD;
import org.apache.spark.api.java.JavaSparkContext;
import org.apache.spark.api.java.function.PairFunction;
import org.janelia.thickness.lut.SingleDimensionLUTRealTransform;
import org.janelia.thickness.lut.SingleDimensionLUTRealTransformField;
import org.janelia.thickness.utility.Utility;

import scala.Tuple2;

import java.io.File;
import java.io.FileNotFoundException;
import java.io.IOException;
import java.nio.file.Files;

/**
 * @author Philipp Hanslovsky
 */
public class Render
{

	public static < K > JavaPairRDD< K, double[] > invert( JavaSparkContext sc, JavaPairRDD< K, double[] > lut )
	{
		JavaPairRDD< K, double[] > inverted = lut.mapToPair( new PairFunction< Tuple2< K, double[] >, K, double[] >()
		{
			@Override
			public Tuple2< K, double[] > call( Tuple2< K, double[] > t ) throws Exception
			{
				final double[] lut = t._2();
				SingleDimensionLUTRealTransform transform = new SingleDimensionLUTRealTransform( lut, 1, 1, 0 );
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
		} );
		return inverted;
	}

	public static ArrayImg< DoubleType, DoubleArray > invert( RandomAccessibleInterval< DoubleType > lut )
	{
		SingleDimensionLUTRealTransformField lutTransformField = new SingleDimensionLUTRealTransformField( 3, 3, lut );
		long[] dim = new long[ lut.numDimensions() ];
		lut.dimensions( dim );
		ArrayImg< DoubleType, DoubleArray > target = ArrayImgs.doubles( dim );
		RealPoint targetPoint = new RealPoint( dim.length );
		int lastPosition = dim.length - 1;
		for ( ArrayCursor< DoubleType > c = target.cursor(); c.hasNext(); )
		{
			DoubleType val = c.next();
			lutTransformField.applyInverse( targetPoint, c );
			val.set( targetPoint.getDoublePosition( lastPosition ) );
		}
		return target;
	}

	public static void main( String[] args ) throws InterruptedException, IOException
	{

//        String path = args[0];

//        double[] data = new double[]{0.1, 1.9, 3.5};
//
//        ArrayImg<DoubleType, DoubleArray> img = ArrayImgs.doubles(data, 1, 1, 3);
//        ArrayImg<DoubleType, DoubleArray> inverse = invert(img);
//        for( DoubleType i : inverse )
//            System.out.println( i.get() );
//
//        SparkConf conf = new SparkConf().setAppName("Invert").setMaster("local[*]");
//        JavaSparkContext sc = new JavaSparkContext( conf );
//
//        ArrayList<Tuple2<Integer, double[]>> al = new ArrayList<Tuple2<Integer, double[]> >();
//        al.add( Utility.tuple2( 0, data) );
//
//        double[] inverted = invert(sc, sc.parallelizePairs(al)).collectAsMap().get(0);
//        System.out.println( Arrays.toString(inverted) );

//        String sourcePath = "/nobackup/saalfeld/philipp/AL-Z0613-14/analysis/registration/0-999/export/scale10/762x915+358+376/";
//        String backwardLutPath = "/nobackup/saalfeld/philipp/AL-Z0613-14/analysis/z-spacing/01/out/spark-test-2/04/backward/";
//        String sourcePath = "/nobackup/saalfeld/philipp/AL-Z0613-14/analysis/z-spacing/01/dummy-data";
//        String backwardLutPath = "/nobackup/saalfeld/philipp/AL-Z0613-14/analysis/z-spacing/01/dummy-backward";
//        String sourcePath = "/nobackup/saalfeld/hanslovskyp/spark-confirmation/spark-v2/data-sequence";
//        String backwardLutPath = "/home/hanslovskyp/workspace-idea/z_spacing-spark-scala/out/spark-test-2/03/backward";
//        String forwardLutPath = "/nobackup/saalfeld/philippAL-Z0613-14/analysis/z-spacing/01/dummy-forward";
		String id = args.length > 0 ? args[ 0 ] : "20151022_000000";
//        String root = "/nobackup/saalfeld/philipp/AL-Z0613-14/analysis/z-spacing/" + id;
//        String root = "/home/hanslovskyp/workspace-idea/z_spacing-spark-scala/synthetic-distortion/noise-sigma-20/out/" + id;
//        String root = "/nobackup/saalfeld/hanslovskyp/CutOn4-15-2013_ImagedOn1-27-2014/export/scale10/numbers/1349-3399/953x322+268+120/experiments/" + id;
//        String root = "/nobackup/saalfeld/hanslovskyp/CutOn4-15-2013_ImagedOn1-27-2014/export/crop/1349-3399/experiments/" + id;
		String root = "/nobackup/saalfeld/hanslovskyp/CutOn4-15-2013_ImagedOn1-27-2014/aligned/substacks/1300-3449/4000x2500+5172+1416/downscale-by-2/" + id;
		// String sourcePath =
		// "/nobackup/saalfeld/philipp/AL-Z0613-14/analysis/registration/0-999/export/scale10/329x575x816+658+656+135";
//        String sourcePath = "/nobackup/saalfeld/philipp/AL-Z0613-14/analysis/registration/0-999/export/scale10/762x915+358+376";
//        String sourcePath = "/home/hanslovskyp/workspace-idea/z_spacing-spark-scala/synthetic-distortion/noise-sigma-20/data-0-70";
//        String sourcePath = "/nobackup/saalfeld/hanslovskyp/CutOn4-15-2013_ImagedOn1-27-2014/export/scale10/numbers/1349-3399/953x322+268+120/data";
//        String sourcePath = "/nobackup/saalfeld/hanslovskyp/CutOn4-15-2013_ImagedOn1-27-2014/export/crop/1349-3399/data/";
		String sourcePath = "/nobackup/saalfeld/hanslovskyp/CutOn4-15-2013_ImagedOn1-27-2014/aligned/substacks/1300-3449/4000x2500+5172+1416/downscale-by-2/data";
		String optionsPath = root + "/config.json";

		for ( int i = 0; i < 2; ++i )
		{

			System.out.println( i );

			String iteration = String.format( "%02d", i );
			String base = String.format( root + "/out/spark-test-2/%s", iteration );
			System.out.println( "BASE" + base );
			String backwardLutPath = base + "/backward";
			String backwardTargetPath = base + "/backward-LUT-xy-view/%04d.tif";
			String backwardRenderPath = base + "/render-xy-view/%04d.tif";
			System.out.println( backwardLutPath );
//
			ImagePlus imp = new FolderOpener().openFolder( sourcePath );
			System.out.println( "opened source: " + sourcePath );
			new ImageConverter( imp ).convertToGray32();
			System.out.println( backwardLutPath );

			Img< FloatType > stack = ImageJFunctions.wrapFloat( imp );
			ConvertedRandomAccessibleInterval< FloatType, DoubleType > lut = new ConvertedRandomAccessibleInterval< FloatType, DoubleType >( ImageJFunctions.wrapFloat( new FolderOpener().openFolder( backwardLutPath ) ), new RealDoubleConverter< FloatType >(), new DoubleType() );
			System.out.println( "opened lut" );

			long[] dim = new long[ stack.numDimensions() ];
			stack.dimensions( dim );
			ArrayImg< DoubleType, DoubleArray > correctlySizedLut = ArrayImgs.doubles( dim );
			ArrayImg< FloatType, FloatArray > resultImg = ArrayImgs.floats( dim );

			RealRandomAccessible< DoubleType > extendedLut = Views.interpolate( Views.extendBorder( lut ), new NLinearInterpolatorFactory< DoubleType >() );

			ScaleOptions options = ScaleOptions.createFromFile( optionsPath );

			int[] radii = options.radii[ Integer.parseInt( iteration ) ];
			int[] steps = options.steps[ Integer.parseInt( iteration ) ];

			double[] step = new double[] { steps[ 0 ], steps[ 1 ], 1 };
			double[] radius = new double[] { radii[ 0 ], radii[ 1 ], 0 };

			CompositeIntervalView< DoubleType, RealComposite< DoubleType > > collapsed = Views.collapseReal( lut );
			RealRandomAccessible< RealComposite< DoubleType > > extendedCollapsed = Views.interpolate( Views.extendBorder( collapsed ), new NLinearInterpolatorFactory< RealComposite< DoubleType > >() );

			double[] scale2D = new double[] { step[ 0 ], step[ 1 ] };
			double[] radius2D = new double[] { radius[ 0 ], radius[ 1 ] };

			ScaleAndTranslation collapsedTransform = new ScaleAndTranslation( scale2D, radius2D );

			RealTransformRealRandomAccessible< RealComposite< DoubleType >, InverseRealTransform > collapsedTransformed = RealViews.transformReal( extendedCollapsed, collapsedTransform );

			RealRandomAccess< RealComposite< DoubleType > > collapsedRa = collapsedTransformed.realRandomAccess();
			for ( int y = 0; y < lut.dimension( 1 ); ++y )
			{
				for ( int x = 0; x < lut.dimension( 0 ); ++x )
				{
					collapsedRa.setPosition( new int[] { x, y } );
					RealComposite< DoubleType > comp = collapsedRa.get();
					Cursor< DoubleType > cursor = Views.flatIterable( Views.hyperSlice( Views.hyperSlice( correctlySizedLut, 1, y ), 0, x ) ).cursor();
					for ( int z = 0; cursor.hasNext(); ++z )
					{
						cursor.next().set( comp.get( z ) );
					}
				}
			}

			// x = x'*s + r
			ScaleAndTranslation transform = new ScaleAndTranslation( step, radius );

			RealTransformRealRandomAccessible< DoubleType, InverseRealTransform > transformed = RealViews.transformReal( extendedLut, transform );

			RealRandomAccess< DoubleType > ra = transformed.realRandomAccess();
			for ( ArrayCursor< DoubleType > c = correctlySizedLut.cursor(); c.hasNext(); )
			{
				c.fwd();
				ra.setPosition( c );
				c.get().set( ra.get() );
			}

			RealRandomAccess< FloatType > sourceRa = Views.interpolate( Views.extendValue( stack, new FloatType( Float.NaN ) ), new NLinearInterpolatorFactory< FloatType >() ).realRandomAccess();

			SingleDimensionLUTRealTransformField lutTransform = new SingleDimensionLUTRealTransformField( 3, 3, correctlySizedLut );

//            for (ArrayCursor<FloatType> r = resultImg.cursor(); r.hasNext(); ) {
//                r.fwd();
//                lutTransform.apply(r, sourceRa);
//                r.get().set(sourceRa.get());
//            }

			Utility.transform( Views.extendValue( stack, new FloatType( Float.NaN ) ), resultImg, correctlySizedLut );
			System.out.println( "Transformed image." );

			Files.createDirectories( new File( String.format( backwardTargetPath, 0 ) ).getParentFile().toPath() );
			Files.createDirectories( new File( String.format( backwardRenderPath, 0 ) ).getParentFile().toPath() );
			for ( int z = 0; z < resultImg.dimension( 2 ); ++z )
			{
				IntervalView< DoubleType > lutHS = Views.hyperSlice( correctlySizedLut, 2, z );
				IntervalView< FloatType > resultHS = Views.hyperSlice( resultImg, 2, z );
				String lutPath = String.format( backwardTargetPath, z );
				String resultPath = String.format( backwardRenderPath, z );
				new FileSaver( ImageJFunctions.wrapFloat( lutHS, "" ) ).saveAsTiff( lutPath );
				new FileSaver( ImageJFunctions.wrapFloat( resultHS, "" ) ).saveAsTiff( resultPath );
			}

		}

		System.out.println( "Done." );

	}

}
