package org.janelia.thickness;

import ij.process.FloatProcessor;

import java.util.ArrayList;

import org.apache.spark.api.java.JavaPairRDD;
import org.apache.spark.api.java.function.Function2;
import org.apache.spark.api.java.function.PairFlatMapFunction;
import org.apache.spark.api.java.function.PairFunction;

import org.janelia.thickness.utility.Utility;
import scala.Tuple2;
import scala.Tuple3;

public class SparkTransformationToImages {

    public static JavaPairRDD< Integer, FloatProcessor> toImages(
            final JavaPairRDD< Tuple2< Integer, Integer >, double[] > input,
            final int[] dim,
            final int[] radius,
            final int[] step )
    {
        JavaPairRDD<Integer, FloatProcessor> output = input // (x, y) -> double[]
                .flatMapToPair( new FromArrayToIndexValuePairs() ) // (x, y) -> (z, value)
                .mapToPair(new FromXYKeyToZKey()) // z -> (x, y, value)
                .aggregateByKey( // z -> FPTuple
                        new FloatProcessor( (int)dim[0], (int)dim[1] ),
                        new WriteToFloatProcessor( radius, step ),
                        new MergeDisjointFloatProcessors() )
                ;

        return output;
    }


    public static class FromArrayToIndexValuePairs implements PairFlatMapFunction<Tuple2<Tuple2<Integer,Integer>,double[]>, Tuple2<Integer,Integer>, Tuple2< Integer, Double > >
    {
        private static final long serialVersionUID = 6343315847242166966L;

        public Iterable<Tuple2<Tuple2<Integer, Integer>, Tuple2<Integer, Double>>> call(
                Tuple2<Tuple2<Integer, Integer>, double[]> t) throws Exception {
            ArrayList<Tuple2<Tuple2<Integer, Integer>, Tuple2<Integer, Double>>> res = new ArrayList<Tuple2<Tuple2<Integer, Integer>, Tuple2<Integer, Double>>>();
            Tuple2<Integer, Integer> xy = t._1();
            double[] coord = t._2();
            for (int j = 0; j < coord.length; j++) {
                res.add( Utility.tuple2( xy, Utility.tuple2( new Integer(j), coord[j] ) ) );
            }
            return res;
        }
    }


    public static class FromXYKeyToZKey implements PairFunction<Tuple2<Tuple2<Integer,Integer>,Tuple2<Integer,Double>>, Integer, Tuple3< Integer, Integer, Double > >
    {

        private static final long serialVersionUID = 4039365688638871552L;

        public Tuple2<Integer, Tuple3<Integer, Integer, Double>> call(
                Tuple2<Tuple2<Integer, Integer>, Tuple2<Integer, Double>> t)
                throws Exception {
            Tuple2<Integer, Integer> xy = t._1();
            Tuple2<Integer, Double> t2 = t._2();
            return Utility.tuple2( t2._1(), Utility.tuple3( xy._1(), xy._2(), t2._2() ) );
        }
    }


    public static class WriteToFloatProcessor implements Function2< FloatProcessor, Tuple3<Integer, Integer, Double>, FloatProcessor >
    {
        private static final long serialVersionUID = 426010164977854733L;
        private final int[] radius;
        private final int[] step;

        public WriteToFloatProcessor( int[] radius, int[] step ) {
            super();
            this.radius = radius;
            this.step = step;
        }

        public FloatProcessor call( FloatProcessor fp, Tuple3<Integer, Integer, Double> t )
                throws Exception {
            int x = t._1();
            int y = t._2();
            FloatProcessor proc = fp;
            if( x >= 0 && x < proc.getWidth() && y >= 0 && y < proc.getHeight() ) // TODO pass correct dims, so no data is lost
                proc.setf( t._1(), t._2(), t._3().floatValue() );
            return fp;
        }
    }


    public static class MergeDisjointFloatProcessors implements Function2< FloatProcessor, FloatProcessor, FloatProcessor >
    {
        private static final long serialVersionUID = 1608128472889969913L;

        public FloatProcessor call( FloatProcessor fp1, FloatProcessor fp2 ) throws Exception {
            float[] p1 = (float[])fp1.getPixels();
            float[] p2 = (float[])fp2.getPixels();
            for (int j = 0; j < p1.length; j++) {
                p1[j] += p2[j];
            }
            return fp1;
        }
    }

}

