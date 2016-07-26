package org.janelia.thickness;

import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;

import org.apache.spark.SparkConf;
import org.apache.spark.api.java.JavaPairRDD;
import org.apache.spark.api.java.JavaSparkContext;
import org.apache.spark.api.java.function.Function;
import org.apache.spark.api.java.function.PairFunction;
import org.janelia.thickness.utility.Utility;

import ij.ImageJ;
import ij.ImagePlus;
import ij.process.FloatProcessor;
import scala.Tuple2;
import scala.Tuple3;

/**
 * Created by hanslovskyp on 6/10/16.
 */
public class SmallerJoinTest {

    private final JavaSparkContext sc;
    private final JavaPairRDD< Integer, FloatProcessor > files;
    private final ArrayList< MatrixGenerationFromImagePairs > generators;
    private final ArrayList<Tuple3<Integer,Integer,Integer>> bounds;
    private final int stepSize;
    private final int maxRange;
    private final int[] dim;

    public SmallerJoinTest( JavaSparkContext sc, JavaPairRDD< Integer, FloatProcessor > files, int stepSize, int maxRange, int[] dim, boolean ensurePersistence )
    {
        this.sc = sc;
        this.files = files;
        this.stepSize = stepSize;
        this.maxRange = maxRange;

        this.generators = new ArrayList<>();
        this.bounds = new ArrayList<>();
        this.dim = dim;

        int stop = (int) files.count();
        for ( int z = 0; z < stop; z += this.stepSize )
        {
            final int lower = Math.max(z - this.maxRange, 0);
            final int upper = Math.min(z + this.maxRange + stepSize, stop);
            final int size  = upper - lower;
            JavaPairRDD<Integer, FloatProcessor> rdd = files.filter(new FilterRange(lower, upper)).cache();
            final HashMap<Integer, ArrayList<Integer>> keyPairList = new HashMap<Integer, ArrayList<Integer>>();
            for ( int i = lower; i < upper; ++i )
            {
                ArrayList<Integer> al = new ArrayList<Integer>();
                for ( int k = i + 1; k < upper && k - i <= maxRange; ++k )
                {
                    al.add( k );
                }
                keyPairList.put( i, al );
            }
            JavaPairRDD<Tuple2<Integer, Integer>, Tuple2<FloatProcessor, FloatProcessor>> pairs =
                    JoinFromList.projectOntoSelf(rdd, sc.broadcast(keyPairList));
            MatrixGenerationFromImagePairs matrixGenerator = new MatrixGenerationFromImagePairs(sc, pairs, this.dim, size, lower);
            if ( ensurePersistence )
                matrixGenerator.ensurePersistence();
            this.generators.add( matrixGenerator );
            this.bounds.add( Utility.tuple3( z, Math.min( z + stepSize, stop ), z - lower ) );
        }



    }

    public JavaPairRDD<Tuple2<Integer, Integer>, FloatProcessor> run(
            int range,
            int stride[],
            int[] correlationBlockRadius )
    {
        ArrayList<JavaPairRDD<Tuple2<Integer, Integer>,FloatProcessor>> rdds = new ArrayList<>();
        final int maxIndex = (int) (files.count() - 1);
        final int stop = maxIndex + 1;
        for ( int i = 0; i < generators.size(); ++i ) {
//            final int lower = Math.max(z - range, 0);
//            final int upper = Math.min(z + range + stepSize, stop);
//            final int size = upper - lower;
//            JavaPairRDD<Integer, FPTuple> rdd = files.filter(new FilterRange(lower, upper)).cache();
//            final HashMap<Integer, ArrayList<Integer>> keyPairList = new HashMap<Integer, ArrayList<Integer>>();
//            for ( int i = lower; i < upper; ++i )
//            {
//                ArrayList<Integer> al = new ArrayList<Integer>();
//                for ( int k = i + 1; k < upper && k - i <= range; ++k )
//                {
//                    al.add( k );
//                }
//                keyPairList.put( i, al );
//            }
//            JavaPairRDD<Tuple2<Integer, Integer>, Tuple2<FPTuple, FPTuple>> pairs =
//                    JoinFromList.projectOntoSelf(rdd, sc.broadcast(keyPairList));
//            MatrixGenerationFromImagePairs matrixGenerator =
//                    new MatrixGenerationFromImagePairs( sc, pairs, dim, size, lower );
//            matrixGenerator.ensurePersistence();
            MatrixGenerationFromImagePairs matrixGenerator = this.generators.get( i );
            Tuple3<Integer, Integer, Integer> bound = this.bounds.get(i);
            JavaPairRDD<Tuple2<Integer, Integer>, FloatProcessor> matrices =
                    matrixGenerator.generateMatrices(stride, correlationBlockRadius, range).cache();
            System.out.println( "SmallerJoinTest: " + matrices.count() + " matrices."  );
            Tuple2<Tuple2<Integer, Integer>, FloatProcessor> m = matrices.first();
            JavaPairRDD<Tuple2<Integer, Integer>, FloatProcessor> strip =
                    matrices.mapToPair(
                            new MatrixToStrip<Tuple2<Integer, Integer>>( bound._3(), Math.min( stepSize, stop - bound._1() ), range, stop ) );
            Tuple2<Tuple2<Integer, Integer>, FloatProcessor> s = strip.first();
            //            JavaPairRDD<Tuple2<Integer, Integer>, Tuple2<FPTuple, Tuple2<Integer, Integer>>> annotatedMatrices =
//                    matrices.mapToPair(new AnnotateConstant<Tuple2<Integer, Integer>, FPTuple, Tuple2<Integer, Integer>>(Utility.tuple2(lower, upper))).cache();
            rdds.add( strip );
//            bounds.add( Utility.tuple3( z, Math.min( z + stepSize, stop ), z - lower ) );
        }

        JavaPairRDD<Tuple2<Integer, Integer>, FloatProcessor> result = rdds.get(0)
                .mapToPair( new PutIntoGlobalContext<Tuple2<Integer, Integer>>( stop, bounds.get(0)._1(), bounds.get(0)._2()) );
        Tuple2<Tuple2<Integer, Integer>, FloatProcessor> r = result.first();

        for ( int i = 1; i < rdds.size(); ++i )
        {
            JavaPairRDD<Tuple2<Integer, Integer>, FloatProcessor> rdd = rdds.get(i);
            Tuple3<Integer, Integer, Integer> bound = bounds.get(i);

            final int offset = bound._1();

            result = result.join( rdd )
                    .mapToPair( new JoinStrips( offset ) );
        }

        return result;


    }


    public static void main(String[] args) {


        SparkConf conf = new SparkConf()
        		.setAppName("Smaller Joins!")
        		.setMaster("local[*]")
        	    .set("spark.serializer", "org.apache.spark.serializer.KryoSerializer")
        	    .set("spark.kryo.registrator", KryoSerialization.Registrator.class.getName())
        		;
        JavaSparkContext sc = new JavaSparkContext(conf);
        final String pattern = "/groups/saalfeld/saalfeldlab/FROM_TIER2/hanslovskyp/flyem/data/Z0115-22_Sec27/align1/Thick/image.%05d.png";
        final int start = 3750;
        final int stop = start + 500;
        int scaleLevel = 2;
        int stepSize = 50;
        int maxRange = 20;
        int range = 15;
        int[] stride = new int[] { 256, 256 };
        int[] correlationBlockRadius = {128, 128};
        int[] dim = {2048 / 4, 2048 / 4};
        ArrayList<Integer> r = Utility.arange(start, stop);
        @SuppressWarnings("serial")
		JavaPairRDD<Integer, FloatProcessor> files = sc.parallelize(r)
                .mapToPair(new Utility.LoadFileFromPattern(pattern))
                .mapToPair(new PairFunction<Tuple2<Integer, FloatProcessor>, Integer, FloatProcessor>() {
                    @Override
                    public Tuple2<Integer, FloatProcessor> call(Tuple2<Integer, FloatProcessor> t) throws Exception {
                        return Utility.tuple2(t._1().intValue() - start, t._2());
                    }
                })
                .mapToPair(new Utility.DownSample<Integer>(scaleLevel))
                .cache()
                ;

        System.out.println( files.count() + " files" );
//        List<Integer> keys = files.keys().collect();
//        sc.close();
//        for ( int k : keys )
//            System.out.println( k );
//        System.exit(3);

        SmallerJoinTest test = new SmallerJoinTest(sc, files, stepSize, maxRange, dim, true);

//        List<Tuple2<Tuple2<Integer, Integer>, FPTuple>> strips = run(sc, files, stepSize, range, stride, correlationBlockRadius, dim).collect();
        List<Tuple2<Tuple2<Integer, Integer>, FloatProcessor>> strips = test.run(range, stride, correlationBlockRadius).collect();
        sc.close();
        new ImageJ();
        for ( Tuple2<Tuple2<Integer, Integer>, FloatProcessor> s : strips ) {
            System.out.println( s );
            new ImagePlus( s._1().toString(), s._2() ).show();
        }


    }


    public static class FilterRange implements Function<Tuple2< Integer, FloatProcessor>, Boolean >
    {

        /**
		 * 
		 */
		private static final long serialVersionUID = -2112302031715609728L;
		private final long start;
        private final long stop;

        public FilterRange(long start, long stop) {
            this.start = start;
            this.stop = stop;
        }

        @Override
        public Boolean call(Tuple2< Integer, FloatProcessor > t) throws Exception {
            int unboxed = t._1().intValue();
            return unboxed >= start && unboxed < stop;
        }
    }

    public static class AnnotateConstant< K, V, A > implements PairFunction< Tuple2< K, V >, K, Tuple2< V, A > >
    {

        /**
		 * 
		 */
		private static final long serialVersionUID = -1024830680710579984L;
		private final A annotation;

        public AnnotateConstant(A annotation) {
            this.annotation = annotation;
        }

        @Override
        public Tuple2<K, Tuple2<V, A>> call(Tuple2<K, V> t) throws Exception {
            return Utility.tuple2( t._1(), Utility.tuple2( t._2(), annotation ) );
        }
    }


    public static class MatrixToStrip< K > implements PairFunction< Tuple2< K, FloatProcessor >, K, FloatProcessor >
    {
        /**
		 * 
		 */
		private static final long serialVersionUID = -6780567502737253053L;
		private final int offset;
        private final int size;
        private final int range;
        private final int stop;

        public MatrixToStrip(int offset, int size, int range, int stop) {
            this.offset = offset;
            this.size = size;
            this.range= range;
            this.stop = stop;
        }

        @Override
        public Tuple2< K, FloatProcessor > call(Tuple2< K, FloatProcessor > t) throws Exception {

            FloatProcessor matrix = t._2();
            FloatProcessor strip = new FloatProcessor(2 * range + 1, this.size);
            int w = matrix.getWidth();

            System.out.println( matrix.getWidth() + " " + matrix.getHeight() + " " + strip.getWidth() + " " + strip.getHeight() + " " + offset );

            for ( int z = offset, stripZ = 0; stripZ < size; ++z, ++stripZ )
            {
                for ( int r = -range; r <= range; ++r )
                {
                    int k = r + z;
                    if ( k < 0 || k >= w )
                        continue;
                    strip.setf( range + r, stripZ, matrix.getf( z, k ) );
                }
            }

            return Utility.tuple2( t._1(), strip );
        }
    }


    public static class PutIntoGlobalContext< K > implements PairFunction< Tuple2< K , FloatProcessor >, K, FloatProcessor >
    {

        /**
		 * 
		 */
		private static final long serialVersionUID = -126790284079446439L;
		private final int size;
        private final int lower;
        private final int upper;

        public PutIntoGlobalContext(int size, int lower, int upper) {
            this.size = size;
            this.lower = lower;
            this.upper = upper;
        }

        @Override
        public Tuple2<K, FloatProcessor> call(Tuple2<K, FloatProcessor> t) throws Exception {
            FloatProcessor source = t._2();
            int w = source.getWidth();
            FloatProcessor result = new FloatProcessor(w, this.size);
            result.add( Double.NaN );
            for ( int y = this.lower, sourceY = 0;  y < this.upper; ++y, ++sourceY )
                for ( int x = 0; x < w; ++x )
                    result.setf( x, y, source.getf( x, sourceY ) );
            return Utility.tuple2( t._1(), result );
        }
    }


    public static class JoinStrips 
    implements PairFunction<Tuple2<Tuple2<Integer, Integer>, Tuple2<FloatProcessor, FloatProcessor>>, Tuple2<Integer, Integer>, FloatProcessor> {

        /**
		 * 
		 */
		private static final long serialVersionUID = -7559889575641624904L;
		private final int offset;

        public JoinStrips(int offset) {
            this.offset = offset;
        }

        @Override
        public Tuple2<Tuple2<Integer, Integer>, FloatProcessor> call(Tuple2<Tuple2<Integer, Integer>, Tuple2<FloatProcessor, FloatProcessor>> t) throws Exception {
            Tuple2<FloatProcessor, FloatProcessor> fps = t._2();
            FloatProcessor source = fps._2();
            FloatProcessor target = fps._1();
            int sourceH = source.getHeight();
            int sourceW = source.getWidth();
            for ( int sourceY = 0, y = offset; sourceY < sourceH; ++sourceY, ++y )
            {
                for ( int x = 0; x < sourceW; ++x )
                {
                    target.setf( x, y, source.getf( x, sourceY ) );
                }
            }

            return Utility.tuple2( t._1(), target );
        }
    }

}
