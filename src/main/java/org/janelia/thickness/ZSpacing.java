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
import org.apache.spark.api.java.function.Function;
import org.apache.spark.api.java.function.PairFunction;
import org.janelia.thickness.experiments.Render;
import org.janelia.thickness.inference.Options;
import org.janelia.thickness.utility.DPTuple;
import org.janelia.thickness.utility.Utility;

import ij.ImagePlus;
import ij.io.FileSaver;
import ij.process.ByteProcessor;
import ij.process.FloatProcessor;
import loci.formats.FormatException;
import scala.Tuple2;

public class ZSpacing
{

	public static void main( final String[] args ) throws FormatException, IOException
	{
		final SparkConf conf = new SparkConf().setAppName( "ZSpacing" )
				.set( "spark.network.timeout", "600" );

		final JavaSparkContext sc = new JavaSparkContext( conf );

		final String scaleOptionsPath = args[ 0 ];
		final ScaleOptions scaleOptions = ScaleOptions.createFromFile( scaleOptionsPath );

		run( sc, scaleOptions );

	}

	public static void run(
			final JavaSparkContext sc,
			final ScaleOptions scaleOptions ) throws FormatException, IOException
	{
		final String sourcePattern = scaleOptions.source;
		final String maskPattern = scaleOptions.mask;
		final String root = scaleOptions.target;
		final String outputFolder = root + "/%02d";
		final int imageScaleLevel = scaleOptions.scale;
		final int start = scaleOptions.start;
		final int stop = scaleOptions.stop;
		final int size = stop - start;

		final ArrayList< Integer > indices = Utility.arange( size );

		@SuppressWarnings("serial")
		final JavaPairRDD< Integer, Tuple2< FloatProcessor, FloatProcessor > > sections = sc
				.parallelize( indices )
				.mapToPair( new PairFunction< Integer, Integer, Integer >()
				{

					public Tuple2< Integer, Integer > call( final Integer arg0 ) throws Exception
					{
						return Utility.tuple2( arg0, arg0 );
					}
				} )
				.sortByKey()
				.map( new Function< Tuple2< Integer, Integer >, Integer >()
				{

					public Integer call( final Tuple2< Integer, Integer > arg0 ) throws Exception
					{
						return arg0._1();
					}
				} )
				.mapToPair( new Utility.LoadFileTupleFromPatternTuple( sourcePattern, maskPattern ) )
				.mapToPair( new Utility.DownSampleImages< Integer >( imageScaleLevel ) )
				.cache();

		final FloatProcessor firstImg = sections.take( 1 ).get( 0 )._2()._1();
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
		final JavaPairRDD< Integer, FloatProcessor > sectionsDropWeights = sections.mapToPair( 
				new PairFunction< Tuple2< Integer, Tuple2< FloatProcessor, FloatProcessor > >, Integer, FloatProcessor >()
		{
			@Override
			public Tuple2< Integer, FloatProcessor > call( final Tuple2< Integer, Tuple2< FloatProcessor, FloatProcessor > > t ) throws Exception
			{
				return Utility.tuple2( t._1(), t._2()._1() );
			}
		} );
		final SmallerJoinTest generator = new SmallerJoinTest( sc, sectionsDropWeights, scaleOptions.joinStepSize, maxRange, dim, true );

//        new ImageJ();

//        IJ.log( Arrays.toString( dim ) );

		for ( int i = 0; i < radiiArray.length; ++i )
		{
//			IJ.log( "i=" + i );
			System.out.println( "i=" + i );
			System.out.println( "Options [" + i + "] = \n" + options[ i ].toString() );

			new File( String.format( outputFolder, i ) ).mkdirs();

			final long tStart = System.currentTimeMillis();

			final int[] currentOffset = radiiArray[ i ];
			final int[] currentStep = stepsArray[ i ];
			final int[] currentDim = new int[] {
					Math.max( 1, ( int ) Math.ceil( ( dim[ 0 ] - currentOffset[ 0 ] ) * 1.0 / currentStep[ 0 ] ) ),
					Math.max( 1, ( int ) Math.ceil( ( dim[ 1 ] - currentOffset[ 1 ] ) * 1.0 / currentStep[ 1 ] ) )
			};
//            IJ.log( "i=" + i + ": " + Arrays.toString( currentDim) + Arrays.toString( currentOffset ) + Arrays.toString( currentStep ) + " " + size );

			final int[] previousOffset = i > 0 ? radiiArray[ i - 1 ] : new int[] { 0, 0 };
			final int[] previousStep = i > 0 ? stepsArray[ i - 1 ] : new int[] { width, height };
			final int[] previousDim = new int[] {
					Math.max( 1, ( int ) Math.ceil( ( dim[ 0 ] - previousOffset[ 0 ] ) * 1.0 / previousStep[ 0 ] ) ),
					Math.max( 1, ( int ) Math.ceil( ( dim[ 1 ] - previousOffset[ 1 ] ) * 1.0 / previousStep[ 1 ] ) )
			};

			final BlockCoordinates cbs = new BlockCoordinates( currentOffset, currentStep );
			final ArrayList< BlockCoordinates.Coordinate > xyCoordinatesLocalAndWorld = cbs.generateFromBoundingBox( dim );
			final ArrayList< Tuple2< Integer, Integer > > xyCoordinates = new ArrayList< Tuple2< Integer, Integer > >();
			for ( final BlockCoordinates.Coordinate xy : xyCoordinatesLocalAndWorld )
			{
				xyCoordinates.add( xy.getLocalCoordinates() );
			}

			System.out.println( "previousDim: " + Arrays.toString( previousDim ) + "(" + ( previousDim[ 0 ] * previousDim[ 1 ] ) + ")" +
					" " + xyCoordinates.size() );

			// min and max of previous step
			final Tuple2< Integer, Integer > xMinMax;
			final Tuple2< Integer, Integer > yMinMax;
			if ( xyCoordinates.size() == 0 )
			{
				final Tuple2< Integer, Integer > t = Utility.tuple2( currentOffset[ 0 ], currentOffset[ 1 ] );
				xyCoordinates.add( t );
//                globalCoordinatesTolocalCoordinatesMapping.put( t, t );
				xMinMax = Utility.tuple2( previousOffset[ 0 ], previousOffset[ 0 ] );
				yMinMax = Utility.tuple2( previousOffset[ 1 ], previousOffset[ 1 ] );
			}
			else
			{
				xMinMax = Utility.tuple2( 0, ( ( dim[ 0 ] - 2 * previousOffset[ 0 ] - 1 ) / previousStep[ 0 ] ) * previousStep[ 0 ] );
				yMinMax = Utility.tuple2( 0, ( ( dim[ 1 ] - 2 * previousOffset[ 1 ] - 1 ) / previousStep[ 1 ] ) * previousStep[ 1 ] );
			}

			final JavaPairRDD< Tuple2< Integer, Integer >, double[] > currentCoordinates;
			if ( i == 0 )
			{
				currentCoordinates = coordinates;
			}
			else
			{
				final BlockCoordinates cbs1 = new BlockCoordinates( previousOffset, previousStep );
				final BlockCoordinates cbs2 = new BlockCoordinates( currentOffset, currentStep );
				final ArrayList< BlockCoordinates.Coordinate > newCoords = cbs2.generateFromBoundingBox( dim );

				final ArrayList< Tuple2< Tuple2< Integer, Integer >, Tuple2< Double, Double > > > mapping = new ArrayList< Tuple2< Tuple2< Integer, Integer >, Tuple2< Double, Double > > >();
				for ( final BlockCoordinates.Coordinate n : newCoords )
				{
					mapping.add( Utility.tuple2( n.getLocalCoordinates(), cbs1.translateOtherLocalCoordiantesIntoLocalSpace( n ) ) );
				}

				currentCoordinates = SparkInterpolation.interpolate( sc, coordinates, sc.broadcast( mapping ), previousDim );
			}

			currentCoordinates.cache().count();
			System.out.println( "Calculated " + currentCoordinates.count() + " coordinates" );

			final JavaPairRDD< Tuple2< Integer, Integer >, FloatProcessor > matrices =
					generator.run( options[ i ].comparisonRange, currentStep, currentOffset );

			System.out.println( "Calculated " + matrices.cache().count() + " matrices" );
			System.out.println( Arrays.toString( dim ) + " " + Arrays.toString( currentOffset ) + " " + Arrays.toString( currentStep ) + matrices.take( 1 ).get( 0 )._1() );

			if ( scaleOptions.logMatrices[ i ] )
			{
				final String outputFormatMatrices = String.format( outputFolder, i ) + "/matrices/%s.tif";
				final List< Tuple2< Tuple2< Integer, Integer >, Boolean > > successMatrices = matrices
						.mapToPair( new Utility.WriteToFormatString< Tuple2< Integer, Integer > >( outputFormatMatrices ) )
						.collect();
			}

			System.out.println( currentCoordinates.take( 1 ).get( 0 )._1() );
//            new ImagePlus( "", matrices.take(1).get(0)._2().rebuild() ).show();

			System.out.println( "Before inference." );
			System.out.println( options[ i ].toString() + " " + options[ i ].comparisonRange );

			final String lutPattern = String.format( outputFolder, i ) + "/luts/%s.tif";

			final Chunk chunk = new Chunk( scaleOptions.chunkSizes[ i ], scaleOptions.overlaps[ i ], size, options[ i ].comparisonRange );

			final JavaPairRDD< Tuple2< Integer, Integer >, double[] > result = SparkInference.inferCoordinates(
					sc,
					matrices,
					currentCoordinates,
					chunk,
					options[ i ],
					lutPattern )
					.cache();
			result.count();
			System.out.println( "Inference done!" );

			// log success and failure
			final String successAndFailurePath = String.format( outputFolder, i ) + "/successAndFailure.tif";
			final ByteProcessor ip = LogSuccessAndFailure.log( sc, result, currentDim );
//            if ( ip.get( 0, 0 ) != 0 ) {
//                sc.close();
//                return;
//            }
			System.out.println( "Wrote status image? " + new FileSaver( new ImagePlus( "", ip ) ).saveAsTiff( successAndFailurePath ) );

			final ColumnsAndSections columnsAndSections = new ColumnsAndSections( currentDim, size );
			final JavaPairRDD< Integer, DPTuple > coordinateSections = columnsAndSections.columnsToSections( sc, result );

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
			final JavaPairRDD< Tuple2< Integer, Integer >, double[] > backward = Render.invert( sc, coordinates ).cache();
			final JavaPairRDD< Integer, DPTuple > backwardImages = columnsAndSections.columnsToSections( sc, backward );
//            JavaPairRDD<Integer, FPTuple> images = SparkTransformationToImages.toImages( coordinates, currentDim, currentOffset, currentStep );

			final String outputFormat = String.format( outputFolder, i ) + "/forward/%04d.tif";
			final List< Tuple2< Integer, Boolean > > success = diffused
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
				final List< Tuple2< Tuple2< Integer, Integer >, Boolean > > successTransformedMatrices = matrices
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

		sc.close();
	}

}
