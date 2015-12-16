package org.janelia.thickness;

import ij.process.ByteProcessor;
import org.apache.spark.api.java.JavaPairRDD;
import org.apache.spark.api.java.JavaSparkContext;
import org.apache.spark.api.java.function.Function;
import org.apache.spark.api.java.function.PairFunction;
import org.janelia.thickness.utility.Utility;
import scala.Tuple2;

import java.util.List;

/**
 * Created by hanslovskyp on 10/1/15.
 */
public class LogSuccessAndFailure {

    public static ByteProcessor log(
            JavaSparkContext sc,
            JavaPairRDD<Tuple2< Integer, Integer >, double[] > input,
            final int[] dim )
    {
        return log( sc, input, dim, Byte.MAX_VALUE );
    }

    public static ByteProcessor log(
            JavaSparkContext sc,
            JavaPairRDD<Tuple2< Integer, Integer >, double[] > input,
            final int[] dim,
            final byte failVal ) {
        // assume number of successes >> number of failures => only store failures and set image to
        List<Tuple2<Integer, Integer>> failures = input
                .filter(new OnlyFailures< Tuple2<Integer,Integer>, double[] >() )
                .map(new DropValue<Tuple2<Integer, Integer>, double[]>())
                .collect()
                ;

        ByteProcessor ip = new ByteProcessor(dim[0], dim[1]);
        for( Tuple2<Integer, Integer> xy : failures )
            ip.set( xy._1(), xy._2(), failVal );

        return ip;
    }

    public static class OnlyFailures<K,V> implements Function<Tuple2<K, V>, Boolean> {
        @Override
        public Boolean call(Tuple2<K, V> t) throws Exception {
            return t._2() == null;
        }
    }

    public static class ToBoolean<K, V> implements PairFunction<Tuple2<K, V>, K, Boolean> {
        @Override
        public Tuple2<K, Boolean> call(Tuple2<K, V> t) throws Exception {
            return Utility.tuple2(t._1(), true);
        }
    }

    public static class DropValue<K, V> implements Function<Tuple2<K, V>, K>
    {

        @Override
        public K call(Tuple2<K, V> t) throws Exception {
            return t._1();
        }
    }

    public static void main(String[] args) {

    }

}
