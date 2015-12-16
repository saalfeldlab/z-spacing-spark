package org.janelia.thickness.experiments;

import loci.formats.FileStitcher;
import loci.formats.FormatException;
import org.apache.spark.SparkConf;
import org.apache.spark.api.java.JavaPairRDD;
import org.apache.spark.api.java.JavaSparkContext;
import org.apache.spark.api.java.function.Function;
import org.apache.spark.api.java.function.PairFunction;
import org.janelia.thickness.utility.FPTuple;
import org.janelia.thickness.utility.Utility;
import scala.Tuple2;

import java.io.IOException;
import java.util.ArrayList;
import java.util.regex.Pattern;

/**
 * Created by hanslovskyp on 10/21/15.
 */
public class SparkGenerateWave {

    public static void main(String[] args) throws IOException, FormatException {

        final String fn = "/home/hanslovskyp/workspace-idea/z_spacing-spark-scala/synthetic-distortion/noise-sigma-20/raw/<0000-0080>.tif";
        boolean isPattern = Pattern.compile(".*<\\d+-\\d+>.*").matcher( fn ).matches(); // fileName.indexOf( "%" ) > -1;
        FileStitcher reader = new FileStitcher( isPattern );
        reader.setId(fn);


        final int width = reader.getSizeX();
        final int height = reader.getSizeY();
        final int size   = reader.getSizeZ(); // 201;//reader.getSizeZ();

        reader.close();

        SparkConf conf = new SparkConf().setAppName("GenerateWave");
        JavaSparkContext sc = new JavaSparkContext(conf);


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
                .mapToPair( new Utility.StackOpener( fn, isPattern ) )
                .cache()
                ;
    }

}
