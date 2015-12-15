package org.janelia.thickness;

import ij.ImagePlus;
import ij.process.FloatProcessor;
import org.apache.spark.SparkConf;
import org.apache.spark.api.java.JavaPairRDD;
import org.apache.spark.api.java.JavaSparkContext;
import org.apache.spark.api.java.function.PairFunction;
import scala.Tuple2;

import java.util.ArrayList;
import java.util.List;

/**
 * Created by hanslovskyp on 9/30/15.
 */
public class DownsampleFloatProcessor {

//    private final float ignoreValue;
//
//    public DownsampleFloatProcessor()
//    {
//        this( Float.NaN );
//    }
//
//    public DownsampleFloatProcessor(float ignoreValue) {
//        this.ignoreValue = ignoreValue;
//    }

    public static void main(String[] args) {
        final String inputPattern = args[0];
        final String outputPattern = args[1];
        final int start = Integer.parseInt( args[2] );
        final int stop = Integer.parseInt(args[3]);
        final float ignoreValue = Float.parseFloat(args[4]);
        final int levels = Integer.parseInt(args[5]);

        SparkConf conf = new SparkConf().setAppName("Downsampling");
//        conf.setMaster( "local[*]" );
        JavaSparkContext sc = new JavaSparkContext( conf );

        ArrayList<Integer> indices = Utility.arange(start, stop);

        JavaPairRDD<Integer, FPTuple> images = sc
                .parallelize(indices)
                .mapToPair(new PairFunction<Integer, Integer, FPTuple>() {
                    @Override
                    public Tuple2<Integer, FPTuple> call(Integer integer) throws Exception {
                        FloatProcessor fp = new ImagePlus(String.format(inputPattern, integer.intValue())).getProcessor().convertToFloatProcessor();
                        return Utility.tuple2(integer, new FPTuple(fp));
                    }
                })
                ;

        JavaPairRDD<Integer, FPTuple> downscaled = images
                .mapToPair(new ReplaceByNaN(ignoreValue))
                .mapToPair(new Utility.DownSample<Integer>(levels))
                ;

        List<Tuple2<Integer, Boolean>> success = downscaled
                .mapToPair(new Utility.WriteToFormatString(outputPattern))
                .collect();

        sc.close();

        int size = success.size();
        int count = 0;

        for( Tuple2<Integer, Boolean> s : success )
            if( s._2() )
                ++count;

        System.out.println( "Successfully wrote " + count + "/" + size + " images." );

    }

    public static class ReplaceByNaN implements PairFunction< Tuple2< Integer, FPTuple >, Integer, FPTuple >
    {

        private final float replace;

        public ReplaceByNaN(float replace) {
            this.replace = replace;
        }

        @Override
        public Tuple2<Integer, FPTuple> call(Tuple2<Integer, FPTuple> t) throws Exception {
            if ( ! Float.isNaN( replace ) ) {
                float[] px = t._2().pixels;
                for (int i = 0; i < px.length; ++i)
                    if (px[i] == replace)
                        px[i] = Float.NaN;
            }
            return t;
        }
    }


}
