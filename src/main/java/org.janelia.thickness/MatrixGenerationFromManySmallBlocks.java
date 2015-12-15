package org.janelia.thickness;

import org.apache.spark.api.java.JavaPairRDD;
import org.apache.spark.api.java.JavaSparkContext;
import scala.Tuple2;

import java.util.HashMap;

/**
 * Created by hanslovskyp on 9/24/15.
 */
public class MatrixGenerationFromManySmallBlocks implements MatrixGeneration<Tuple2< Integer, Integer >> {

    private final JavaSparkContext sc;
    private final JavaPairRDD< Tuple2< Integer, Integer >, FPTuple[] > rawDataBlocks;
    private final BlockData blockData;
    private final int[] dim;

    public MatrixGenerationFromManySmallBlocks(
            JavaSparkContext sc,
            JavaPairRDD<Tuple2<Integer, Integer>, FPTuple[]> rawDataBlocks,
            BlockData blockData,
            int[] dim ) {
        this.sc = sc;
        this.rawDataBlocks = rawDataBlocks;
        this.blockData = blockData;
        this.dim = dim;
    }

    @Override
    public JavaPairRDD<Tuple2<Integer, Integer>, FPTuple> generateMatrices(
            int[] stride,
            int[] correlationBlockRadius,
            int range ) {
        JavaPairRDD<Tuple2<Integer, Integer>, HashMap<Tuple2<Integer, Integer>, FPTuple[]>> selections =
                blockData.retrieveBlocks(rawDataBlocks, sc, dim, stride, correlationBlockRadius);

        JavaPairRDD<Tuple2<Integer, Integer>, FPTuple> matrices =
                blockData.calculate(selections, sc, correlationBlockRadius, range);

        return matrices;
    }

    public MatrixGenerationFromManySmallBlocks ensureBlockExecution()
    {
        rawDataBlocks.count();
        return this;
    }

    public static MatrixGenerationFromManySmallBlocks create(
            JavaPairRDD< Integer, FPTuple > sections,
            int[] blockSize,
            int[] dim,
            int nPartitions,
            JavaSparkContext sc )
    {
        BlockData bd = new BlockData(blockSize);
        JavaPairRDD<Tuple2<Integer, Integer>, FPTuple[]> blocks = bd.blocks(sections, dim[0], dim[1], nPartitions, sc).cache();
        return new MatrixGenerationFromManySmallBlocks( sc, blocks, bd, dim );
    }
}
