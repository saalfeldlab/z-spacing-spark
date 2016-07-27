package org.janelia.thickness.similarity;

import ij.ImageJ;
import ij.ImagePlus;
import ij.process.FloatProcessor;
import net.imglib2.type.numeric.integer.LongType;
import net.imglib2.util.RealSum;
import org.apache.spark.SparkConf;
import org.apache.spark.api.java.JavaPairRDD;
import org.apache.spark.api.java.JavaSparkContext;
import org.apache.spark.api.java.function.Function;
import org.apache.spark.api.java.function.PairFunction;
import org.janelia.thickness.utility.Utility;
import scala.Tuple2;
import scala.Tuple6;

import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

/**
 * Created by hanslovskyp on 9/18/15.
 */
public class Correlations {

    public static double calculate( final FloatProcessor img1, final FloatProcessor img2 )
    {
        final RealSum sumA = new RealSum();
        final RealSum sumAA = new RealSum();
        final RealSum sumB = new RealSum();
        final RealSum sumBB = new RealSum();
        final RealSum sumAB = new RealSum();
        int n = 0;
        float[] d1 = (float[])img1.getPixels();
        float[] d2 = (float[])img2.getPixels();
        for ( int i = 0; i < d1.length; ++i )
        {
            final float va = d1[i];
            final float vb = d2[i];

            if ( Float.isNaN( va ) || Float.isNaN( vb ) )
                continue;
            ++n;
            sumA.add( va );
            sumAA.add( va * va );
            sumB.add( vb );
            sumBB.add( vb * vb );
            sumAB.add( va * vb );
        }
        final double suma = sumA.getSum();
        final double sumaa = sumAA.getSum();
        final double sumb = sumB.getSum();
        final double sumbb = sumBB.getSum();
        final double sumab = sumAB.getSum();

        return ( n * sumab - suma * sumb ) / Math.sqrt( n * sumaa - suma * suma ) / Math.sqrt( n * sumbb - sumb * sumb );
    }

    public static void calculate(
            final FloatProcessor img1,
            final FloatProcessor img2,
            int[] min, // in img1 coordinates
            int[] max, // in img1 coordinates
            Tuple6< RealSum, RealSum, RealSum, RealSum, RealSum, LongType > sums )
    {
        final RealSum sumA = sums._1();
        final RealSum sumAA = sums._2();
        final RealSum sumB = sums._3();
        final RealSum sumBB = sums._4();
        final RealSum sumAB = sums._5();
        long n = sums._6().get();
        int[] maxIndices = new int[]{Math.min(img1.getWidth(), max[0]), Math.min(img1.getHeight(), max[1])};
        for ( int x = Math.max( 0, min[ 0 ]); x < maxIndices[ 0]; ++x )
        {
            for ( int y = Math.max( 0, min[ 1 ] ); y < maxIndices[ 1 ]; ++y )
            {
                final double va = img1.getf( x, y );
                final double vb = img2.getf( x, y );

                if (Double.isNaN(va) || Double.isNaN(vb))
                    continue;
                ++n;
                sumA.add(va);
                sumAA.add(va * va);
                sumB.add(vb);
                sumBB.add(vb * vb);
                sumAB.add(va * vb);
            }
        }
        sums._6().set( n );
    }

    public static FloatProcessor calculate(
            HashMap<Tuple2<Integer, Integer>, FloatProcessor[]> hm, /* x, y */
            Tuple2<Integer, Integer> min,
            Tuple2<Integer, Integer> max,
            int[] blockSize,
            int range ) {

        if ( hm.values().size() == 0 )
            return null;

        int nLayers = hm.values().iterator().next().length;

        // zRef, zComp
        HashMap<Tuple2<Integer, Integer>, Tuple6<RealSum, RealSum, RealSum, RealSum, RealSum, LongType>> sums =
                new HashMap<Tuple2<Integer, Integer>, Tuple6<RealSum, RealSum, RealSum, RealSum, RealSum, LongType>>();

        for( int i = 0; i < nLayers; ++i )
        {
            for (int k = i + 1; k - i <= range && k < nLayers; ++k)
            {
                sums.put(
                        Utility.tuple2( i, k ),
                        Utility.tuple6(new RealSum(), new RealSum(), new RealSum(), new RealSum(), new RealSum(), new LongType(0))
                );
            }
        }
        for( Map.Entry< Tuple2< Integer, Integer >, FloatProcessor[] > e : hm.entrySet() )
        {
        	FloatProcessor[] images = e.getValue();
            Tuple2<Integer, Integer> positionInBlockCoordinates = e.getKey();
            Tuple2<Integer, Integer> positionsInWorldCoordinates = Utility.tuple2(
                    positionInBlockCoordinates._1() * blockSize[0],
                    positionInBlockCoordinates._2() * blockSize[1]
            );
            int[] minimumRelativeToBlock = new int[]{
                    min._1() - positionsInWorldCoordinates._1(),
                    min._2() - positionsInWorldCoordinates._2()
            };
            int[] maximumRelativeToBlock = new int[]{
                    max._1() - positionsInWorldCoordinates._1(),
                    max._2() - positionsInWorldCoordinates._2()
            };
            for( int i = 0; i < nLayers; ++i )
            {
                FloatProcessor fp1 = images[i];
                for ( int k = i + 1; k - i <= range && k < nLayers; ++k )
                {
                    FloatProcessor fp2 = images[k];
                    Tuple6<RealSum, RealSum, RealSum, RealSum, RealSum,LongType> currentSums = sums.get(Utility.tuple2(i, k));
                    calculate( fp1, fp2, minimumRelativeToBlock, maximumRelativeToBlock, currentSums );
                }
            }
        }

        FloatProcessor matrix = new FloatProcessor(nLayers, nLayers);
        matrix.add( Double.NaN );
        for ( int z = 0; z < nLayers; ++z )
            matrix.setf( z, z, 1.0f );

        for ( Map.Entry< Tuple2< Integer, Integer >, Tuple6<RealSum, RealSum, RealSum, RealSum, RealSum, LongType> > e : sums.entrySet() )
        {
            Tuple2<Integer, Integer> indices = e.getKey();
            int x = indices._1();
            int y = indices._2();
            Tuple6<RealSum, RealSum, RealSum, RealSum, RealSum, LongType> s = e.getValue();
            final double suma = s._1().getSum();
            final double sumaa = s._2().getSum();
            final double sumb = s._3().getSum();
            final double sumbb = s._4().getSum();
            final double sumab = s._5().getSum();
            long n = s._6().get();

            float correlation = (float) (( n * sumab - suma * sumb ) / Math.sqrt( n * sumaa - suma * suma ) / Math.sqrt( n * sumbb - sumb * sumb ));

            matrix.setf( x, y, correlation );
            matrix.setf( y, x, correlation );
        }
        return matrix;
    }

    public static class Calculate< K > implements PairFunction< Tuple2< K, Tuple2< FloatProcessor, FloatProcessor > >, K, Double >
    {

        /**
		 * 
		 */
		private static final long serialVersionUID = -2100509719455523081L;

		@Override
        public Tuple2<K, Double> call(Tuple2<K, Tuple2<FloatProcessor, FloatProcessor>> t) throws Exception {
            Tuple2<FloatProcessor, FloatProcessor> fps = t._2();
            return Utility.tuple2(
                    t._1(),
                    calculate( fps._1(), fps._2() )
            );
        }
    }

    public static void main(String[] args) {
        String input = "/nobackup/saalfeld/hanslovskyp/Chlamy/428x272x1414+20+20+0/data/%04d.tif";
        int start = 0;
        int stop = 1414;
        final int range = 50;
        ArrayList<Integer> indices = Utility.arange(start, stop);
        SparkConf conf = new SparkConf().setAppName("Correlations").setMaster("local[*]");
        JavaSparkContext sc = new JavaSparkContext(conf);
        JavaPairRDD<Integer, FloatProcessor> files = sc
                .parallelize(indices)
                .mapToPair(new Utility.Format<Integer>(input))
                .mapToPair(new Utility.LoadFile())
                .mapToPair(new Utility.ReplaceValue( 0.0f, Float.NaN ) )
                .cache();

        @SuppressWarnings("serial")
		List<Tuple2<Tuple2<Integer, Integer>, Double>> correlations = files
                .cartesian(files)
                .filter(new Function<Tuple2<Tuple2<Integer, FloatProcessor>, Tuple2<Integer, FloatProcessor>>, Boolean>() {
                    @Override
                    public Boolean call(Tuple2<Tuple2<Integer, FloatProcessor>, Tuple2<Integer, FloatProcessor>> t) throws Exception {
                        int diff = t._1()._1().intValue() - t._2()._1().intValue();
                        return diff > 0 && diff <= range;
                    }
                })
                .mapToPair(new PairFunction<Tuple2<Tuple2<Integer, FloatProcessor>, Tuple2<Integer, FloatProcessor>>, Tuple2<Integer, Integer>, Tuple2<FloatProcessor, FloatProcessor>>() {
                    @Override
                    public Tuple2<Tuple2<Integer, Integer>, Tuple2<FloatProcessor, FloatProcessor>>
                    call(Tuple2<Tuple2<Integer, FloatProcessor>, Tuple2<Integer, FloatProcessor>> t) throws Exception {
                        return Utility.tuple2(
                                Utility.tuple2(t._1()._1(), t._2()._1()),
                                Utility.tuple2(t._1()._2(), t._2()._2())
                        );
                    }
                })
                .mapToPair(new Calculate<Tuple2<Integer, Integer>>())
                .collect()
                ;
        
        sc.close();

        FloatProcessor matrix = new FloatProcessor(stop - start, stop - start);
        matrix.add( Double.NaN );

        for ( int z = start; z < stop; ++z )
        {
            matrix.setf( z-start,z-start, 1.0f );
        }

        for( Tuple2<Tuple2<Integer, Integer>, Double> entry : correlations )
        {
            int x = entry._1()._1();
            int y = entry._1()._2();
            float val = entry._2().floatValue();
            matrix.setf( x,y,val );
            matrix.setf( y,x,val );
        }
        new ImageJ();
        new ImagePlus( "matrix", matrix ).show();
    }
}
