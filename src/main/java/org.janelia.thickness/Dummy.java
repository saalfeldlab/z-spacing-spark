package org.janelia.thickness;

import ij.IJ;
import ij.ImageJ;
import ij.ImagePlus;
import net.imglib2.img.display.imagej.ImageJFunctions;
import org.apache.spark.SparkConf;
import org.apache.spark.api.java.JavaPairRDD;
import org.apache.spark.api.java.JavaSparkContext;
import org.janelia.thickness.inference.Options;
import scala.Tuple2;

import java.util.ArrayList;
import java.util.Arrays;

/**
 * Created by hanslovskyp on 10/7/15.
 */
public class Dummy {

    public static void main(String[] args) {

        SparkConf conf = new SparkConf().setAppName("BLAB").setMaster("local");
        JavaSparkContext sc = new JavaSparkContext(conf);
        String path = "/nobackup/saalfeld/philipp/AL-Z0613-14/analysis/z-spacing/16/out/spark-test-2/00/matrices/(0,0).tif";
        FPTuple pixels = new FPTuple(new ImagePlus(path).getProcessor().convertToFloatProcessor());

        double[] doublePixels = new double[pixels.pixels.length];

        for ( int i = 0; i < doublePixels.length; ++i )
        {
            doublePixels[i] = pixels.pixels[i];
        }

        ArrayList<Tuple2<Tuple2<Integer, Integer>, FPTuple>> al = new ArrayList<Tuple2<Tuple2<Integer, Integer>, FPTuple>>();
        al.add( Utility.tuple2( Utility.tuple2( 0, 0 ), pixels ) );

        JavaPairRDD<Tuple2<Integer, Integer>, FPTuple> rdd = sc.parallelizePairs(al);

        double[] startingCoordinates = new double[ pixels.width ];
        for ( int i =0 ; i < startingCoordinates.length; ++i )
            startingCoordinates[i] = i;

        ArrayList<Tuple2<Tuple2<Integer, Integer>, double[]>> al2 = new ArrayList<Tuple2<Tuple2<Integer, Integer>, double[]>>();
        al2.add( Utility.tuple2(Utility.tuple2(0,0),startingCoordinates));
        JavaPairRDD<Tuple2<Integer, Integer>, double[]> rdd2 = sc.parallelizePairs(al2);

        Options options = Options.generateDefaultOptions();
        options.multiplierGenerationRegularizerWeight = 0.1;
        options.coordinateUpdateRegularizerWeight = 0.0;
        options.shiftProportion = 0.6;
        options.nIterations = 1;
        options.comparisonRange = 50;
        options.minimumSectionThickness = 1.0E-8;
        options.withRegularization = true;
        options.multiplierEstimationIterations = 10;
        options.withReorder = false;
        options.nThreads = 16;
        options.forceMonotonicity = false;
        options.estimateWindowRadius = -1;

        JavaPairRDD<Tuple2<Integer, Integer>, double[]> mapping = SparkInference.inferCoordinates(
                sc,
                rdd,
                rdd2,
                options,
                null);

        new ImageJ();
        IJ.log(Arrays.toString(mapping.collect().get(0)._2()));

        sc.close();


    }

}
