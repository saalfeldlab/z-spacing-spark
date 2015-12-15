package org.janelia.thickness;

import ij.ImageJ;
import ij.ImagePlus;
import ij.process.ByteProcessor;
import ij.process.FloatProcessor;
import mpicbg.models.TranslationModel1D;
import net.imglib2.Cursor;
import net.imglib2.img.Img;
import net.imglib2.img.array.ArrayImg;
import net.imglib2.img.array.ArrayImgs;
import net.imglib2.img.basictypeaccess.array.FloatArray;
import net.imglib2.img.display.imagej.ImageJFunctions;
import net.imglib2.type.numeric.real.FloatType;
import net.imglib2.view.Views;
import org.apache.commons.io.FileUtils;
import org.apache.commons.lang3.StringUtils;
import org.apache.spark.SparkConf;
import org.apache.spark.api.java.JavaPairRDD;
import org.apache.spark.api.java.JavaSparkContext;
import org.apache.spark.api.java.function.Function2;
import org.apache.spark.api.java.function.PairFlatMapFunction;
import org.apache.spark.api.java.function.PairFunction;
import org.apache.spark.broadcast.Broadcast;
import org.apache.spark.mllib.linalg.distributed.GridPartitioner;
import org.janelia.thickness.inference.InferFromMatrix;
import org.janelia.thickness.inference.Options;
import org.janelia.thickness.inference.fits.CorrelationFitAverage;
import org.janelia.thickness.mediator.OpinionMediatorWeightedAverage;
import scala.Tuple2;

import java.io.File;
import java.util.*;

/**
 * Created by hanslovskyp on 9/18/15.
 */
public class BlockData {

    private final int[] blockSize;
    private final DataBlocks dataBlocks;

    public BlockData(int[] blockSize)
    {
        this.blockSize = blockSize;
        this.dataBlocks = new DataBlocks( Utility.tuple2( blockSize[0], blockSize[1] ) );
    }

    public JavaPairRDD< Tuple2< Integer, Integer >, FPTuple[] > blocks(
            JavaPairRDD< Integer, FPTuple> rdd,
            final int width,
            final int height,
            final int nPartitions,
            JavaSparkContext sc
    )
    {

        Broadcast<ArrayList<DataBlocks.Coordinate>> blockCoordinatesBC = sc.broadcast(dataBlocks.generateFromBoundingBox(new int[]{width, height}));

        Tuple2<Integer, Integer> nColsRows = dataBlocks.getNBlocks(new int[]{width, height});
        int nCols = nColsRows._1();
        int nRows = nColsRows._2();


        // for some reason, it seems that GridPartitioner thinks of cols and rows differently
        GridPartitioner partitioner = GridPartitioner.apply(nCols, nRows, nPartitions);

        final int[] blockSize = this.blockSize;

        System.out.println( "blockCoordinatesBC size: " + blockCoordinatesBC.getValue().size() + " " + width + " " + height );
        System.out.println(StringUtils.join( blockCoordinatesBC.getValue(), "\n" ) );
        System.out.flush();

        JavaPairRDD<Tuple2<Integer, Integer>, FPTuple[]> blockedRdd = rdd
                .mapToPair( new Blockify(blockSize, blockCoordinatesBC) )
                .flatMapToPair( new Reindexer() )
                .reduceByKey(
                        partitioner,
                        new Joiner())
                .mapToPair(new ToArray())
                .cache()
                ;

        return blockedRdd;
    }

    public JavaPairRDD<Tuple2<Integer, Integer>, HashMap<Tuple2<Integer, Integer>, FPTuple[]>>
    retrieveBlocks(
            JavaPairRDD<Tuple2<Integer, Integer>, FPTuple[]> rdd,
            JavaSparkContext sc,
            int[] max,
            int[] stride,
            int[] correlationBlockSize )
    {
        CorrelationBlocks cbs = new CorrelationBlocks(
                Utility.tuple2( correlationBlockSize[0], correlationBlockSize[1] ),
                Utility.tuple2( stride[0], stride[1] )
        );
        ArrayList<CorrelationBlocks.Coordinate> correlationBlocks = cbs.generateFromBoundingBox(max);

        HashMap<Tuple2<Integer, Integer>, ArrayList<Tuple2<Integer, Integer>>> positionsToBlocks =
                new HashMap<Tuple2<Integer, Integer>, ArrayList<Tuple2<Integer, Integer>>>();
        final HashMap<Tuple2<Integer, Integer>, ArrayList<Tuple2<Integer, Integer>>> blocksToPositions =
                new HashMap<Tuple2<Integer, Integer>, ArrayList< Tuple2<Integer, Integer>>>();

        // Can be one to many or many to many in both directions, if radius < blockSize!!
        // positions to block mapping
        for ( CorrelationBlocks.Coordinate c : correlationBlocks )
        {
            Tuple2<Integer, Integer> currentWidth = c.getRadius();
            Tuple2<Integer, Integer> worldCoordinates = c.getWorldCoordinates();
            int xWorld = worldCoordinates._1();
            int yWorld = worldCoordinates._2();
            Tuple2<Integer, Integer> currentWorldMin = Utility.tuple2(
                    Math.max( xWorld - currentWidth._1(), 0 ),
                    Math.max( yWorld - currentWidth._2(), 0 )
            );
            Tuple2<Integer, Integer> currentWorldMax = Utility.tuple2(
                    Math.min( xWorld + currentWidth._1(), max[0] ),
                    Math.min( yWorld + currentWidth._2(), max[1] )
            );
            positionsToBlocks.put(worldCoordinates, dataBlocks.getCoverageTuplesLocalCoordinates(currentWorldMin, currentWorldMax));
        }

        // blocks to position mapping
        for ( Map.Entry<Tuple2<Integer, Integer>, ArrayList<Tuple2<Integer, Integer>>> e : positionsToBlocks.entrySet() )
        {
            Tuple2<Integer, Integer> key = e.getKey();
            for ( Tuple2<Integer,Integer> t : e.getValue() )
            {
                ArrayList<Tuple2<Integer, Integer>> currList = blocksToPositions.get(t);
                if ( currList == null )
                {
                    currList = new ArrayList<Tuple2<Integer, Integer>>();
                    blocksToPositions.put( t, currList );
                }
                currList.add( key );
            }
        }

        final Broadcast<HashMap<Tuple2<Integer, Integer>, ArrayList<Tuple2<Integer, Integer>>>> blocksToPositionsBC = sc.broadcast(blocksToPositions);

        System.out.println(StringUtils.join( blocksToPositions, "\n" ) );
        System.out.println(StringUtils.join(correlationBlocks, "\n"));

        JavaPairRDD<Tuple2<Integer, Integer>, HashMap<Tuple2<Integer, Integer>, FPTuple[]>> selections = rdd
                .flatMapToPair(new ToHashMap(blocksToPositionsBC))
                .reduceByKey(new Joiner2())
                .cache()
                ;

        return selections;

    }

    public JavaPairRDD<Tuple2<Integer, Integer>, FPTuple> calculate(
            final JavaPairRDD<Tuple2<Integer, Integer>, HashMap<Tuple2<Integer, Integer>, FPTuple[]>> selections,
            final JavaSparkContext sc,
            final int[] stepSize,
            final int range
    )
    {
        JavaPairRDD<Tuple2<Integer, Integer>, FPTuple> matrices = selections
                .mapToPair( new Calculator(blockSize, stepSize, range) )
                ;

        HashSet<Tuple2<Integer, Integer>> keys1 = new HashSet<Tuple2<Integer, Integer>>();
        HashSet<Tuple2<Integer, Integer>> keys2 = new HashSet<Tuple2<Integer, Integer>>();

        keys1.addAll( selections
                        .mapToPair(new MizzgeClass<Tuple2<Integer,Integer>,HashMap<Tuple2<Integer,Integer>,FPTuple[]>>())
                        .reduceByKey( new MizzgeReduce() )
                        .collectAsMap()
                        .keySet()
        );

        keys2.addAll(
                matrices.mapToPair( new MizzgeClass<Tuple2<Integer, Integer>, FPTuple>()).reduceByKey( new MizzgeReduce() ).collectAsMap().keySet() );

        boolean truth = keys1.equals(keys2);

        return matrices;
    }


    public static class MizzgeClass< K, V > implements PairFunction < Tuple2< K, V >, K, Boolean >
    {
        @Override
        public Tuple2<K, Boolean> call(Tuple2<K, V> kvTuple2) throws Exception {
            return Utility.tuple2( kvTuple2._1(), true );
        }
    }

    public static class MizzgeReduce implements Function2< Boolean, Boolean, Boolean >
    {

        @Override
        public Boolean call(Boolean aBoolean, Boolean aBoolean2) throws Exception {
            return true;
        }
    }

    public static void main( String[] args )
    {
        final String pattern =
                "/home/hanslovskyp/em-sequence-wave/%04d.tif";
                // "/nobackup/saalfeld/hanslovskyp/shan-for-local/ds2/substacks/z=[1950,2150]/substacks/468x477+70+68/substacks/normalized-contrast/larger/crop/%05d.tif"

        final int start = 0;
        final int stop = 100;
        final int range = 20;
        final int[] blockSize = new int[] { 32, 32 };
        final int[] correlationBlockSize = { 10, 10 };
        final int[] stride = new int[] { 10, 10 };
        final int[] max = new int[] { 231, 303 };
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

        System.out.println( "After reading data. Before creating blocks." );

        MatrixGenerationFromManySmallBlocks generator =
                MatrixGenerationFromManySmallBlocks.create(data, blockSize, max, sc.defaultParallelism(), sc).ensureBlockExecution();

        System.out.println("After creating blocks. Before creating matrices.");

        JavaPairRDD<Tuple2<Integer, Integer>, FPTuple> matrices =
                generator.generateMatrices( stride, correlationBlockSize, range ).cache();

        System.out.println("After creating matrices. Before running inference." );

//        new ImageJ();
//        for ( Tuple2< Tuple2< Integer, Integer >, FPTuple> m : matrices.collect().subList( 0, 1 ) )
//            new ImagePlus( String.format( "%d,%d", m._1()._1(), m._1()._2() ), m._2().rebuild() ).show();

        JavaPairRDD<Tuple2<Integer, Integer>, double[]> estimates = matrices
                .mapToPair(new PairFunction<Tuple2<Tuple2<Integer, Integer>, FPTuple>, Tuple2<Integer, Integer>, double[]>() {
                    public Tuple2<Tuple2<Integer, Integer>, double[]> call(Tuple2<Tuple2<Integer, Integer>, FPTuple> t) throws Exception {
                        FloatProcessor fp = t._2().rebuild();
                        InferFromMatrix inf = new InferFromMatrix(new CorrelationFitAverage(), new OpinionMediatorWeightedAverage());
//                        InferFromMatrix inf = new InferFromMatrix(LocalizedCorrelationFitConstant.generateTranslation1D(), new OpinionMediatorWeightedAverage());
                        Img<FloatType> matrix = ImageJFunctions.wrapFloat(new ImagePlus("", fp));
                        double[] startingCoordinates = new double[fp.getWidth()];
                        for (int z = 0; z < startingCoordinates.length; ++z) startingCoordinates[z] = z;
                        Options opt = Options.generateDefaultOptions();
                        opt.comparisonRange = range;
                        opt.nIterations = 10;
                        double[] result = inf.estimateZCoordinates(matrix, startingCoordinates, opt);
                        return Utility.tuple2(t._1(), result);
                    }
                })
                ;

        List<Tuple2<Tuple2<Integer, Integer>, double[]>> collectedEstimates = estimates.collect();

        sc.close();

        CorrelationBlocks cbs = new CorrelationBlocks(Utility.tuple2(correlationBlockSize[0], correlationBlockSize[1]), Utility.tuple2(stride[0], stride[1]));

        int[][] minMax = new int[][] { { Integer.MAX_VALUE, Integer.MIN_VALUE }, { Integer.MAX_VALUE, Integer.MIN_VALUE } };
        for ( Tuple2< Tuple2<Integer, Integer>, double[] > e : collectedEstimates )
        {
            int x = cbs.worldToLocal( e._1() )._1();
            int y = cbs.worldToLocal( e._1() )._2();
            if ( x < minMax[0][0] )
                minMax[0][0] = x;
            if ( x > minMax[0][1] )
                minMax[0][1] = x;
            if ( y < minMax[1][0] )
                minMax[1][0] = y;
            if ( y > minMax[1][1] )
                minMax[1][1] = y;
        }
        int[] nMatrices = new int[]{minMax[0][1] - minMax[0][0] + 1, minMax[1][1] - minMax[1][0] +1};
        ArrayImg<FloatType, FloatArray> result = ArrayImgs.floats( nMatrices[0], nMatrices[1], stop - start );
        for( FloatType r : result )
            r.set( Float.NaN );

        System.out.println();
        System.out.println(Arrays.toString(minMax[0]) + Arrays.toString(minMax[1]));
        System.out.println( Arrays.toString( nMatrices ) +  " " +  ( stop - start ) );
        System.out.println();

        ByteProcessor visitedFP = new ByteProcessor(nMatrices[0], nMatrices[1]);
        visitedFP.set( 0 );
        for( Tuple2<Tuple2<Integer, Integer>, double[]> s : collectedEstimates )
        {

            try {
                Tuple2<Integer, Integer> coord = s._1();
                Tuple2<Integer, Integer> localCoord = cbs.worldToLocal(coord);
                visitedFP.set( localCoord._1(), localCoord._2(), 1 );
            }
            catch( NullPointerException e )
            {
                System.out.println( s._1() );
                System.out.flush();
                throw e;
            }
        }

        HashMap<Tuple2<Integer, Integer>, Boolean> visitedMap = new HashMap<Tuple2<Integer, Integer>, Boolean>();



        for ( Tuple2< Tuple2<Integer, Integer>, double[] > e : collectedEstimates )
        {
            Tuple2<Integer, Integer> coord = e._1();
            Tuple2<Integer, Integer> localCoord = cbs.worldToLocal(coord);
            boolean isInHashMap = visitedMap.get( localCoord ) != null;
            visitedMap.put( localCoord, true );
            int y = localCoord._2();
            int x = localCoord._1();
            double[] map = e._2();
            System.out.println( isInHashMap + " " + localCoord + " " + map.length );
            Cursor<FloatType> cursor =
                    Views.flatIterable(Views.hyperSlice(Views.hyperSlice(result, 1, y), 0, x) ).cursor();
            for( int z = 0; cursor.hasNext(); ++z )
                cursor.next().setReal( map[z] );
        }

//        new ImagePlus( "", visitedFP ).show();
//        ImageJFunctions.show( result );



//        sc.close();

    }



    public static class Blockify implements PairFunction<Tuple2<Integer, FPTuple>, Integer, ArrayList<Tuple2<Tuple2<Integer, Integer>, FPTuple>>> {
        private final int[] blockSize;
        private final Broadcast<ArrayList<DataBlocks.Coordinate>> blocksBroadcast;

        public Blockify(int[] blockSize, Broadcast<ArrayList<DataBlocks.Coordinate>> blocksBroadcast) {
            this.blockSize = blockSize;
            this.blocksBroadcast = blocksBroadcast;
        }

        public Tuple2<Integer, ArrayList<Tuple2<Tuple2<Integer, Integer>, FPTuple>>> call(Tuple2<Integer, FPTuple> t) throws Exception {
                        Integer index = t._1();
                        FloatProcessor source = t._2().rebuild();
                        ArrayList<Tuple2<Tuple2<Integer, Integer>, FPTuple>> blocks2D =
                                new ArrayList<Tuple2<Tuple2<Integer, Integer>, FPTuple>>();
//            FileUtils.writeStringToFile( new File("/groups/saalfeld/home/hanslovskyp/local/tmp/" + index), StringUtils.join(blocksBroadcast.getValue(), "\n"));
                        for ( DataBlocks.Coordinate blockMeta : blocksBroadcast.getValue()) {
                            Tuple2<Integer, Integer> localCoordinates = blockMeta.getLocalCoordinates();
                            Tuple2<Integer, Integer> widthTuple = blockMeta.getStride();
                            Integer xIndex = localCoordinates._1();
                            Integer yIndex = localCoordinates._2();
                            int xOffset = xIndex * blockSize[0];
                            int yOffset = yIndex * blockSize[1];
                            int width = Math.min( widthTuple._1(), source.getWidth() - xOffset );
                            int height = Math.min( widthTuple._2(), source.getHeight() - yOffset);
                            System.out.println( blockMeta );

//                            FileUtils.writeStringToFile(
//                                    new File("/home/hanslovskyp/local/tmp/Blockify-" + t._1() + " " + localCoordinates + ".log"),
//                                    width + " " + height + " " + source.getWidth() + " " + source.getHeight() + " " + xOffset +  " " + yOffset );
                            FloatProcessor fp = new FloatProcessor(width, height);
                            for (int y = 0; y < height; ++y) {
                                for (int x = 0; x < width; ++x) {
                                    fp.setf(x, y, source.getf(xOffset + x, yOffset + y));
                                }
                            }
                            blocks2D.add(Utility.tuple2(Utility.tuple2(xIndex, yIndex), FPTuple.create(fp)));
                        }
            return Utility.tuple2(index, blocks2D);
        }
    }

    public static class Reindexer implements  PairFlatMapFunction<
            Tuple2<Integer, ArrayList<Tuple2<Tuple2<Integer, Integer>, FPTuple>>>,
            Tuple2<Integer, Integer>, HashMap<Integer, FPTuple>
            > {
        public Iterable<Tuple2<Tuple2<Integer, Integer>, HashMap<Integer, FPTuple>>> call(
        final Tuple2<Integer, ArrayList<Tuple2<Tuple2<Integer, Integer>, FPTuple>>> t) throws Exception {
            return new Iterable<Tuple2<Tuple2<Integer, Integer>, HashMap<Integer, FPTuple>>>() {
                private final Integer index = t._1();

                public Iterator<Tuple2<Tuple2<Integer, Integer>, HashMap<Integer, FPTuple>>> iterator() {
                    return new Iterator<Tuple2<Tuple2<Integer, Integer>, HashMap<Integer, FPTuple>>>() {
                        private final Iterator<Tuple2<Tuple2<Integer, Integer>, FPTuple>> it = t._2().iterator();

                        public boolean hasNext() {
                            return it.hasNext();
                        }

                        public Tuple2<Tuple2<Integer, Integer>, HashMap<Integer, FPTuple>> next() {
                            Tuple2<Tuple2<Integer, Integer>, FPTuple> indicesAndImage = it.next();
                            HashMap<Integer, FPTuple> hm = new HashMap<Integer, FPTuple>();
                            hm.put(index, indicesAndImage._2());
                            return Utility.tuple2(
                                    indicesAndImage._1(),
                                    hm
                            );
                        }

                        @Override
                        public void remove() {
                            throw new UnsupportedOperationException();
                        }
                    };
                }
            };
        }
    }

    public static class Joiner implements Function2<HashMap<Integer, FPTuple>, HashMap<Integer, FPTuple>, HashMap<Integer, FPTuple>> {
        public HashMap<Integer, FPTuple> call(
                HashMap<Integer, FPTuple> hm1,
                HashMap<Integer, FPTuple> hm2) throws Exception {
            hm1.putAll(hm2);
            return hm1;
        }
    }

    public static class ToArray implements PairFunction<Tuple2<Tuple2<Integer, Integer>, HashMap<Integer, FPTuple>>, Tuple2<Integer, Integer>, FPTuple[]> {
        public Tuple2<Tuple2<Integer, Integer>, FPTuple[]> call(Tuple2<Tuple2<Integer, Integer>, HashMap<Integer, FPTuple>> t) throws Exception {
            HashMap<Integer, FPTuple> map = t._2();
            FPTuple[] images = new FPTuple[map.size()];
            for (Map.Entry<Integer, FPTuple> e : map.entrySet()) {
                images[e.getKey()] = e.getValue();
            }
            return Utility.tuple2(t._1(), images);
        }
    }

    public static class ToHashMap implements PairFlatMapFunction<Tuple2<Tuple2<Integer, Integer>, FPTuple[]>, Tuple2<Integer, Integer>, HashMap<Tuple2<Integer, Integer>, FPTuple[]>> {
        private final Broadcast<HashMap<Tuple2<Integer, Integer>, ArrayList<Tuple2<Integer, Integer>>>> blocksToPositionsBC;

        public ToHashMap(Broadcast<HashMap<Tuple2<Integer, Integer>, ArrayList<Tuple2<Integer, Integer>>>> blocksToPositionsBC) {
            this.blocksToPositionsBC = blocksToPositionsBC;
        }

        public Iterable<
                Tuple2<Tuple2<Integer, Integer>,
                HashMap<Tuple2<Integer, Integer>, FPTuple[]>>
                > call(final Tuple2<Tuple2<Integer, Integer>, FPTuple[]> t) throws Exception {
            final Tuple2<Integer, Integer> blockPosition = t._1();
            final ArrayList<Tuple2<Integer, Integer>> positions = blocksToPositionsBC.getValue().get(blockPosition);
            if ( positions == null )
                return new ArrayList<Tuple2<Tuple2<Integer, Integer>, HashMap<Tuple2<Integer, Integer>, FPTuple[]>>>();
            else
                return new Iterable<Tuple2<Tuple2<Integer, Integer>, HashMap<Tuple2<Integer, Integer>, FPTuple[]>>>() {
                    public Iterator<Tuple2<Tuple2<Integer, Integer>, HashMap<Tuple2<Integer, Integer>, FPTuple[]>>> iterator() {
                        return new Iterator<Tuple2<Tuple2<Integer, Integer>, HashMap<Tuple2<Integer, Integer>, FPTuple[]>>>() {
                            private final Iterator<Tuple2<Integer, Integer>> it = positions.iterator();
                            public boolean hasNext() {
                                return it.hasNext();
                            }

                            public Tuple2<Tuple2<Integer, Integer>, HashMap<Tuple2<Integer, Integer>, FPTuple[]>> next() {
                                HashMap<Tuple2<Integer, Integer>, FPTuple[]> hm = new HashMap<Tuple2<Integer, Integer>, FPTuple[]>();
                                hm.put( blockPosition, t._2() );
                                return Utility.tuple2(
                                        it.next(),
                                        hm
                                );
                            }

                            @Override
                            public void remove() {
                                throw new UnsupportedOperationException();
                            }
                        };
                    }
                };
        }
    }

    public static class Joiner2 implements Function2<
            HashMap<Tuple2<Integer, Integer>, FPTuple[]>,
            HashMap<Tuple2<Integer, Integer>, FPTuple[]>,
            HashMap<Tuple2<Integer, Integer>, FPTuple[]>> {
        public HashMap<Tuple2<Integer, Integer>, FPTuple[]> call(
                HashMap<Tuple2<Integer, Integer>, FPTuple[]> hm1,
                HashMap<Tuple2<Integer, Integer>, FPTuple[]> hm2) throws Exception {
            HashMap<Tuple2<Integer, Integer>, FPTuple[]> result = new HashMap<Tuple2<Integer, Integer>, FPTuple[]>();
            result.putAll((HashMap<Tuple2<Integer, Integer>, FPTuple[]>) hm1.clone());
            result.putAll((HashMap<Tuple2<Integer, Integer>, FPTuple[]>) hm2.clone());
//            result.put( null, null );
            for ( Map.Entry<Tuple2<Integer, Integer>, FPTuple[]> entry : result.entrySet() )
            {System.out.println( entry );
                if ( entry.getKey() == null )
                    throw new NullPointerException( "IN JOINER2!" );}
            return result;
        }
    }


    public static class Calculator implements
            PairFunction<Tuple2<Tuple2<Integer, Integer>, HashMap<Tuple2<Integer, Integer>, FPTuple[]>>, Tuple2<Integer, Integer>, FPTuple> {
        private final int[] blockSize;
        private final int[] radius;
        private final int range;

        public Calculator(int[] blockSize, int[] radius, int range) {
            this.blockSize = blockSize;
            this.radius = radius;
            this.range = range;
        }

        public Tuple2<Tuple2<Integer, Integer>, FPTuple>
        call(Tuple2<Tuple2<Integer, Integer>, HashMap<Tuple2<Integer, Integer>, FPTuple[]>> t) throws Exception {
            Tuple2<Integer, Integer> pos = t._1();
            int posX = pos._1();
            int posY = pos._2();
            Tuple2<Integer, Integer> min = Utility.tuple2(
                    posX - radius[0],
                    posY - radius[1]
            );
            Tuple2<Integer, Integer> max = Utility.tuple2(
                    posX + radius[0],
                    posY + radius[1]
            );
            FPTuple matrix = Correlations.calculate(t._2(), min, max, blockSize, range);
            return Utility.tuple2(pos, matrix);
        }
    }

}
