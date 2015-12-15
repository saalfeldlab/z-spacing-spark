package org.janelia.thickness;

import ij.ImageJ;
import ij.ImagePlus;
import ij.process.FloatProcessor;
import net.imglib2.Cursor;
import net.imglib2.FinalInterval;
import net.imglib2.img.Img;
import net.imglib2.img.display.imagej.ImageJFunctions;
import net.imglib2.type.numeric.real.FloatType;
import net.imglib2.view.ExtendedRandomAccessibleInterval;
import net.imglib2.view.Views;
import org.apache.commons.lang.StringUtils;
import org.apache.hadoop.util.hash.Hash;
import org.apache.spark.SparkConf;
import org.apache.spark.api.java.JavaPairRDD;
import org.apache.spark.api.java.JavaSparkContext;
import org.apache.spark.api.java.function.PairFunction;
import scala.Tuple2;

import java.util.*;

/**
 * Created by hanslovskyp on 9/24/15.
 */
public class MatrixGenerationFromFewLargeBlocks implements MatrixGeneration<Tuple2< Integer, Integer >> {

    private final JavaSparkContext sc;
    private final JavaPairRDD< Integer, FPTuple > sections;
    private final int[] dim;
    private final int size;

    public MatrixGenerationFromFewLargeBlocks(JavaSparkContext sc, JavaPairRDD<Integer, FPTuple> sections) {
        this.sc = sc;
        this.sections = sections;
        FPTuple first = sections.take(1).get(0)._2();
        this.dim = new int[] { first.width, first.height };
        this.size = (int) sections.count();
    }

    @Override
    public JavaPairRDD<Tuple2<Integer, Integer>, FPTuple> generateMatrices( int[] stride, int[] correlationBlockRadius, int range ) {
        CorrelationBlocks blocks = new CorrelationBlocks(correlationBlockRadius, stride);
        System.out.println( Arrays.toString(dim) );
        ArrayList<CorrelationBlocks.Coordinate> coords = blocks.generateFromBoundingBox(dim);

        System.out.println( "Coordinates start" );
        System.out.println( StringUtils.join(coords, "\n" ) );
        System.out.println( "Coordinates end" );

        ArrayList<Tuple2<Tuple2<Integer, Integer>, FPTuple>> matrices = new ArrayList< Tuple2< Tuple2<Integer, Integer>, FPTuple> > ();

        HashMap<Integer, ArrayList<Integer>> comparisonIndices = new HashMap<Integer, ArrayList<Integer>>();
        for( int i = 0; i < size; ++i )
        {
            ArrayList<Integer> indices = new ArrayList<Integer>();
            for ( int k = i + 1; k - i <= range && k < size; ++k )
            {
                indices.add( k );
            }
            comparisonIndices.put( i, indices );
        }

        for( CorrelationBlocks.Coordinate c : coords )
        {
            Tuple2<Integer, Integer> worldCoord = c.getWorldCoordinates();
            int[] worldStart = new int[]{
                    Math.max(worldCoord._1() - correlationBlockRadius[0], 0),
                    Math.max(worldCoord._2() - correlationBlockRadius[1], 0)
            };
            int[] worldStop = new int[]{
                    Math.min(worldCoord._1() + correlationBlockRadius[0], dim[0]),
                    Math.min(worldCoord._2() + correlationBlockRadius[1], dim[1])
            };
            JavaPairRDD<Integer, FPTuple> subsections =
                    sections.mapToPair(new Subsection<Integer>(worldStart, worldStop));
            JavaPairRDD<Tuple2<Integer, Integer>, Tuple2<FPTuple, FPTuple>> subsectionPairs =
                    JoinFromList.projectOntoSelf(subsections, sc.broadcast(comparisonIndices));
            JavaPairRDD<Tuple2<Integer, Integer>, Double> correlations =
                    subsectionPairs.mapToPair(new Correlations.Calculate<Tuple2<Integer, Integer>>());

            FloatProcessor matrix = new FloatProcessor(size, size);
            matrix.add( Double.NaN );
            for ( int z = 0; z < size; ++z )
                matrix.setf( z, z, 1.0f );
            for( Tuple2<Tuple2<Integer, Integer>, Double> indicesAndResult : correlations.collect() )
            {
                Tuple2<Integer, Integer> coord = indicesAndResult._1();
                int x = coord._1();
                int y = coord._2();
                float val = indicesAndResult._2().floatValue();
                matrix.setf( x, y, val );
                matrix.setf( y, x, val );
            }
            matrices.add(Utility.tuple2(c.getWorldCoordinates(), new FPTuple(matrix)));
        }
        return sc.parallelizePairs( matrices );
    }

    public static class Subsection< K > implements PairFunction<Tuple2<K,FPTuple>, K, FPTuple>
    {
        private static final long serialVersionUID = 886079932104665922L;
        private final int[] start;
        private final int[] stop;
        private final int[] dim;

        public Subsection( int[] start, int[] stop ) {
            super();
            this.start = start;
            this.stop = stop;
            this.dim = new int[] { stop[0] - start[0], stop[1] - start[1] };
        }

        public Tuple2<K, FPTuple> call(Tuple2<K, FPTuple> t)
                throws Exception {
            FloatProcessor source = t._2().rebuild();
            int fpWidth = dim[0];
            int fpHeight = dim[1];
            FloatProcessor fp = new FloatProcessor( fpWidth, fpHeight );
            for ( int ySource = start[1], y = 0; ySource < stop[1]; ++ySource, ++y )
                for ( int xSource = start[0], x = 0; xSource < stop[0]; ++xSource, ++x )
                    fp.setf( x, y, source.getf( xSource, ySource ) );
            return Utility.tuple2( t._1(), new FPTuple( fp ) );
        }
    }

    public static void main(String[] args) {
        final String pattern =
                "/home/hanslovskyp/em-sequence-wave/%04d.tif";

        final int start = 0;
        final int stop = 100;
        final int range = 20;
        final int[] correlationBlockSize = { 80, 80 };
        final int[] stride = new int[] { 80, 80 };
        final ArrayList<Integer> indices = Utility.arange(start, stop);

        SparkConf conf = new SparkConf()
                .setAppName("BlockTest")
                .setMaster("local[*]")
                .set("spark.driver.maxResultSize", "0")
                ;
        JavaSparkContext sc = new JavaSparkContext(conf);

        System.out.println( "Before reading data" );

        JavaPairRDD<Integer, FPTuple> data = sc
                .parallelize(indices)
                .mapToPair(new PairFunction<Integer, Integer, FPTuple>() {
                    public Tuple2<Integer, FPTuple> call(Integer integer) throws Exception {
                        String fn = String.format(pattern, integer.intValue());
                        FloatProcessor fp = new ImagePlus(fn).getProcessor().convertToFloatProcessor();
                        return Utility.tuple2(integer, FPTuple.create(fp));
                    }
                })
                .cache();

        MatrixGenerationFromFewLargeBlocks matGen = new MatrixGenerationFromFewLargeBlocks(sc, data);

        JavaPairRDD<Tuple2<Integer, Integer>, FPTuple> matrices = matGen.generateMatrices( stride, correlationBlockSize, range );

        new ImageJ();
        for( Tuple2<Tuple2<Integer, Integer>, FPTuple> m : matrices.collect() )
            new ImagePlus( "" + m._1(), m._2().rebuild() ).show();

        sc.close();

    }
}
