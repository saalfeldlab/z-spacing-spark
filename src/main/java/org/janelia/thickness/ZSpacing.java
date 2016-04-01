package org.janelia.thickness;

import ij.ImagePlus;
import ij.io.FileSaver;
import ij.process.ByteProcessor;
import loci.formats.FormatException;
import org.apache.spark.SparkConf;
import org.apache.spark.api.java.JavaPairRDD;
import org.apache.spark.api.java.JavaSparkContext;
import org.apache.spark.api.java.function.Function;
import org.apache.spark.api.java.function.PairFunction;
import org.janelia.thickness.inference.Options;
import org.janelia.thickness.utility.DPTuple;
import org.janelia.thickness.utility.FPTuple;
import org.janelia.thickness.utility.Utility;
import org.janelia.utility.io.IO;
import scala.Tuple2;

import java.io.IOException;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.HashMap;
import java.util.List;

public class ZSpacing {

    public static void main(String[] args) throws FormatException, IOException {
        SparkConf conf = new SparkConf().setAppName("ZSpacing");

        final JavaSparkContext sc = new JavaSparkContext( conf );

        String scaleOptionsPath = args[0];
        ScaleOptions scaleOptions = ScaleOptions.createFromFile(scaleOptionsPath);

        run( sc, scaleOptions );

    }

    public static void run(
            JavaSparkContext sc,
            ScaleOptions scaleOptions ) throws FormatException, IOException
    {

        final String sourcePattern = scaleOptions.source;

        final String root         = scaleOptions.target;
        final String outputFolder = root + "/%02d";
        final int imageScaleLevel = scaleOptions.scale;

        final int start = scaleOptions.start;
        final int stop  = scaleOptions.stop;
        final int size  = stop - start;


        ArrayList<Integer> indices = Utility.arange(size);
        JavaPairRDD<Integer, FPTuple> sections = sc
                .parallelize( indices )
                .mapToPair( new PairFunction<Integer, Integer, Integer>() {

                    public Tuple2<Integer, Integer> call(Integer arg0) throws Exception {
                        return Utility.tuple2( arg0, arg0 );
                    }
                })
                .sortByKey()
                .map( new Function<Tuple2<Integer,Integer>, Integer>() {

                    public Integer call(Tuple2<Integer, Integer> arg0) throws Exception {
                        return arg0._1();
                    }
                })
                .mapToPair( new Utility.LoadFileFromPattern( sourcePattern ) )
                .mapToPair(new Utility.DownSample<Integer>(imageScaleLevel))
                .cache()
                ;

        FPTuple firstImg = sections.take(1).get(0)._2();
        int width = firstImg.width;
        int height = firstImg.height;

        double[] startingCoordinates = new double[ size ];
        for ( int i = 0; i < size; ++i )
            startingCoordinates[ i ] = i;

        ArrayList<Tuple2<Tuple2<Integer, Integer>, double[]>> coordinatesList = new ArrayList< Tuple2< Tuple2< Integer, Integer >, double[] > >();
        coordinatesList.add( Utility.tuple2( Utility.tuple2( 0, 0 ), startingCoordinates ) );
        JavaPairRDD<Tuple2<Integer, Integer>, double[]> coordinates = sc.parallelizePairs(coordinatesList, 1);

        int[][] radiiArray = scaleOptions.radii;
        int[][] stepsArray = scaleOptions.steps;
        int[][] correlationBlockRadiiArray = scaleOptions.correlationBlockRadii;
        int[][] maxOffsetsArray = scaleOptions.maxOffsets;
        Options[] options = scaleOptions.inference;

        int maxRange = 0;
        for( int i = 0; i < options.length; ++i )
            maxRange = Math.max( maxRange, options[i].comparisonRange );


        final int[] dim = new int[] { width, height };

        ArrayList<Tuple2<Long, Long>> times = new ArrayList< Tuple2< Long, Long > >();

        HashMap<Integer, ArrayList<Integer>> indexPairs = new HashMap<Integer, ArrayList<Integer>>();
        for ( int i = 0; i < size; ++i )
            indexPairs.put(i, Utility.arange(i + 1, Math.min(i + maxRange + 1, size)));

        JavaPairRDD<Tuple2<Integer, Integer>, Tuple2<FPTuple, FPTuple>> sectionPairs =
                JoinFromList.projectOntoSelf(sections, sc.broadcast(indexPairs)).cache();

        MatrixGenerationFromImagePairs matrixGenerator = new MatrixGenerationFromImagePairs(sc, sectionPairs, dim, size);
        matrixGenerator.ensurePersistence();


        for ( int i = 0; i < radiiArray.length; ++i )
        {
//			IJ.log( "i=" + i );
            System.out.println( "i=" + i );
            System.out.println( "Options [" + i + "] = \n" + options[i].toString() );

            long tStart = System.currentTimeMillis();

            final int[] currentOffset = radiiArray[ i ];
            final int[] currentStep = stepsArray[ i ];
            final int[] currentDim = new int[] {
                    Math.max( 1, (int) Math.ceil( ( dim[0] - currentOffset[0] ) * 1.0 / currentStep[0] ) ),
                    Math.max( 1, (int) Math.ceil( ( dim[1] - currentOffset[1] ) * 1.0 / currentStep[1] ) )
            };
//            IJ.log( "i=" + i + ": " + Arrays.toString( currentDim) + Arrays.toString( currentOffset ) + Arrays.toString( currentStep ) + " " + size );

            final int[] previousOffset = i > 0 ? radiiArray[ i - 1 ] : new int[] { 0, 0 };
            final int[] previousStep   = i > 0 ? stepsArray[ i - 1 ] : new int[] { width, height };
            final int[] previousDim = new int[] {
                    Math.max( 1, (int) Math.ceil( ( dim[0] - previousOffset[0] ) * 1.0 / previousStep[0] ) ),
                    Math.max( 1, (int) Math.ceil( ( dim[1] - previousOffset[1] ) * 1.0 / previousStep[1] ) )
            };

            CorrelationBlocks cbs = new CorrelationBlocks(currentOffset, currentStep);
            ArrayList<CorrelationBlocks.Coordinate> xyCoordinatesLocalAndWorld = cbs.generateFromBoundingBox(dim);
            ArrayList<Tuple2<Integer, Integer>> xyCoordinates = new ArrayList<Tuple2<Integer, Integer>>();
            for (CorrelationBlocks.Coordinate xy : xyCoordinatesLocalAndWorld )
            {
                xyCoordinates.add( xy.getLocalCoordinates() );
            }


            // min and max of previous step
            final Tuple2<Integer, Integer> xMinMax;
            final Tuple2<Integer, Integer> yMinMax;
            if ( xyCoordinates.size() == 0 )
            {
                Tuple2<Integer, Integer> t = Utility.tuple2(currentOffset[0], currentOffset[1]);
                xyCoordinates.add(t);

                xMinMax = Utility.tuple2( previousOffset[0], previousOffset[0] );
                yMinMax = Utility.tuple2( previousOffset[1], previousOffset[1] );
            }
            else
            {
                xMinMax = Utility.tuple2( 0, ( ( dim[0] - 2*previousOffset[0] - 1 ) / previousStep[0] ) * previousStep[ 0 ] );
                yMinMax = Utility.tuple2( 0, ( ( dim[1] - 2*previousOffset[1] - 1 ) / previousStep[1] ) * previousStep[ 1 ] );
            }

            JavaPairRDD<Tuple2<Integer, Integer>, double[]> currentCoordinates;
            if ( i == 0 )
            {
                currentCoordinates = coordinates;
            } else {
                CorrelationBlocks cbs1 = new CorrelationBlocks(previousOffset, previousStep);
                CorrelationBlocks cbs2 = new CorrelationBlocks(currentOffset, currentStep);
                ArrayList<CorrelationBlocks.Coordinate> newCoords = cbs2.generateFromBoundingBox(dim);
                ArrayList<Tuple2<Tuple2<Integer, Integer>, Tuple2<Double, Double>>> mapping = new ArrayList<Tuple2<Tuple2<Integer, Integer>, Tuple2<Double, Double>>>();
                for( CorrelationBlocks.Coordinate n : newCoords )
                {
                    mapping.add( Utility.tuple2(n.getLocalCoordinates(), cbs1.translateCoordinateIntoThisBlockCoordinates(n)) );
                }
                currentCoordinates = SparkInterpolation.interpolate( sc, coordinates, sc.broadcast( mapping ), previousDim );
            }

            currentCoordinates.cache().count();
            System.out.println( "Calculated " + currentCoordinates.count() + " coordinates" );


             JavaPairRDD<Tuple2<Integer, Integer>, FPTuple> matrices = matrixGenerator.generateMatrices(currentStep, currentOffset, options[i].comparisonRange);


            System.out.println("Calculated " + matrices.cache().count() + " matrices");
            System.out.println(Arrays.toString(dim) + " " + Arrays.toString( currentOffset ) + " " + Arrays.toString( currentStep ) + matrices.take(1).get(0)._1() );

            String outputFormatMatrices = String.format( outputFolder, i ) + "/matrices/%s.tif";
            List<Tuple2<Tuple2<Integer, Integer>, Boolean>> successMatrices = matrices
                    .mapToPair(new Utility.WriteToFormatString<Tuple2<Integer, Integer>>(outputFormatMatrices))
                    .collect()
                    ;

            System.out.println(currentCoordinates.take(1).get(0)._1());

            System.out.println( "Before inference." );
            System.out.println( options[i].toString() + " " + options[i].comparisonRange );

            final String lutPattern = String.format(outputFolder, i) + "/luts/%s.tif";

            JavaPairRDD<Tuple2<Integer, Integer>, double[]> result = SparkInference.inferCoordinates(
                    sc,
                    matrices,
                    currentCoordinates,
                    options[i],
                    lutPattern )
                    .cache();
            ;
            result.count();
            System.out.println("Inference done!");

            // log success and failure
            String successAndFailurePath = String.format(outputFolder, i) + "/successAndFailure.tif";
            ByteProcessor ip = LogSuccessAndFailure.log(sc, result, currentDim);
            IO.createDirectoryForFile( successAndFailurePath );
            System.out.println("Wrote status image? " + new FileSaver(new ImagePlus("", ip)).saveAsTiff(successAndFailurePath));

            ColumnsAndSections columnsAndSections = new ColumnsAndSections(currentDim, size);
            JavaPairRDD<Integer, DPTuple> coordinateSections = columnsAndSections.columnsToSections(sc, result);



            JavaPairRDD<Integer, DPTuple> diffused = coordinateSections
                    // TODO write something like diffusion: done?
                    .mapToPair(new PairFunction<Tuple2<Integer, DPTuple>, Integer, DPTuple>() {
                        @Override
                        public Tuple2<Integer, DPTuple> call(Tuple2<Integer, DPTuple> t) throws Exception {
                            return Utility.tuple2( t._1(), FillHoles.fill( t._2().clone() ) );
                        }
                    }
                    );


//            currentCoordinates = result;
//            coordinates = result;
            coordinates = columnsAndSections.sectionsToColumns( sc, diffused );
            JavaPairRDD<Tuple2<Integer, Integer>, double[]> backward = Render.invert(sc, coordinates).cache();
            JavaPairRDD<Integer, DPTuple> backwardImages = columnsAndSections.columnsToSections(sc, backward);
//            JavaPairRDD<Integer, FPTuple> images = SparkTransformationToImages.toImages( coordinates, currentDim, currentOffset, currentStep );


            String outputFormat = String.format( outputFolder, i ) + "/forward/%04d.tif";
            List<Tuple2<Integer, Boolean>> success = diffused
                    .mapToPair( new Utility.WriteToFormatStringDouble<Integer>( outputFormat ) )
                    .collect()
                    ;

            String outputFormatBackward = String.format( outputFolder, i ) + "/backward/%04d.tif";
            List<Tuple2<Integer, Boolean>> successBackward = backwardImages
                    .mapToPair( new Utility.WriteToFormatStringDouble<Integer>( outputFormatBackward ) )
                    .collect()
                    ;
//
            String outputFormatTransformedMatrices = String.format( outputFolder, i ) + "/transformed-matrices/%s.tif";
            List<Tuple2<Tuple2<Integer, Integer>, Boolean>> successTransformedMatrices = matrices
                    .join( backward )
                    .mapToPair(new Utility.Transform<Tuple2<Integer, Integer>>())
                    .mapToPair(new Utility.WriteToFormatString<Tuple2<Integer, Integer>>(outputFormatTransformedMatrices))
                    .collect()
                    ;

//            // for test purposes
//            String outputFormat2 = String.format( outputFolder, i ) + "/uncorrected/%04d.tif";
//            List<Tuple2<Integer, Boolean>> success2 = coordinateSections
//                    .mapToPair( new Utility.WriteToFormatStringDouble( outputFormat2 ) )
//                    .collect()
//                    ;

            int count = 0;
            for ( Tuple2<Integer, Boolean> s : success )
            {
                if ( s._2().booleanValue() )
                    continue;
                ++count;
                System.out.println( "Failed to write forward image " + s._1().intValue() );
            }

            int countBackward = 0;
            for ( Tuple2<Integer, Boolean> s : successBackward )
            {
                if ( s._2().booleanValue() )
                    continue;
                ++countBackward;
                System.out.println( "Failed to write backward image " + s._1().intValue() );
            }

            long tEnd = System.currentTimeMillis();

            System.out.println(
                    "Successfully wrote " + ( success.size() - count ) + "/" + success.size() + " (forward) and " +
                            (successBackward.size() - countBackward ) + "/" + successBackward.size() + " (backward) " +
                            "images at iteration " + i + String.format( " in %25dms", ( tEnd - tStart ) ) );

            times.add( Utility.tuple2( tStart, tEnd ) );
        }

        for ( Tuple2<Long, Long> t : times )
        {
            long diff = t._2().longValue() - t._1().longValue();
            System.out.println( String.format( "Run time for complete iteration: %25dms", diff ) );
        }





        sc.close();
    }


}
