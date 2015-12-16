package org.janelia.thickness.utility;

import ij.ImagePlus;
import ij.io.FileSaver;
import ij.process.FloatProcessor;
import ij.process.ImageProcessor;
import loci.formats.FileStitcher;
import loci.plugins.util.ImageProcessorReader;
import mpicbg.trakem2.util.Downsampler;
import net.imglib2.*;
import net.imglib2.converter.RealFloatConverter;
import net.imglib2.converter.read.ConvertedRandomAccessibleInterval;
import net.imglib2.img.Img;
import net.imglib2.img.array.ArrayCursor;
import net.imglib2.img.array.ArrayImg;
import net.imglib2.img.array.ArrayImgs;
import net.imglib2.img.basictypeaccess.array.DoubleArray;
import net.imglib2.img.basictypeaccess.array.FloatArray;
import net.imglib2.img.display.imagej.ImageJFunctions;
import net.imglib2.interpolation.randomaccess.NLinearInterpolatorFactory;
import net.imglib2.realtransform.RealTransformRealRandomAccessible;
import net.imglib2.type.numeric.RealType;
import net.imglib2.type.numeric.real.DoubleType;
import net.imglib2.type.numeric.real.FloatType;
import net.imglib2.view.IntervalView;
import net.imglib2.view.Views;
import net.imglib2.view.composite.RealComposite;
import org.apache.spark.SparkConf;
import org.apache.spark.api.java.JavaSparkContext;
import org.apache.spark.api.java.function.Function2;
import org.apache.spark.api.java.function.PairFlatMapFunction;
import org.apache.spark.api.java.function.PairFunction;
import org.janelia.thickness.lut.LUTRealTransform;
import org.janelia.thickness.lut.SingleDimensionLUTRealTransformField;
import org.janelia.utility.io.IO;
import scala.*;

import java.io.Serializable;
import java.lang.Boolean;
import java.lang.Float;
import java.lang.Long;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.Iterator;
import java.util.Map;
import java.util.concurrent.Callable;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;

/**
 * Created by hanslovskyp on 9/18/15.
 */
public class Utility {

    public static < U, V > Tuple2< U, V > tuple2( U u, V v )
    {
        return new Tuple2< U, V >( u, v );
    }


    public static < U, V, W > Tuple3< U, V, W > tuple3( U u, V v, W w )
    {
        return new Tuple3< U, V, W >( u, v, w );

    }

    public static < U, V, W, X > Tuple4< U, V, W, X > tuple4( U u, V v, W w, X x )
    {
        return new Tuple4< U, V, W, X >( u, v, w, x );
    }

    public static < U, V, W, X, Y > Tuple5< U, V, W, X, Y > tuple5( U u, V v, W w, X x, Y y )
    {
        return new Tuple5< U, V, W, X, Y >( u, v, w, x, y );
    }

    public static < U, V, W, X, Y, Z > Tuple6< U, V, W, X, Y, Z > tuple6( U u, V v, W w, X x, Y y, Z z )
    {
        return new Tuple6< U, V, W, X, Y, Z >( u, v, w, x, y, z );
    }


    public static ArrayList< Integer > arange( int stop )
    {
        return( arange( 0, stop ) );
    }

    public static ArrayList< Integer > arange( int start, int stop )
    {
        ArrayList< Integer > numbers = new ArrayList< Integer >();
        for ( int i = start; i < stop; ++i )
            numbers.add( i );
        return numbers;
    }

    public static ArrayList<Long> arange(long stop )
    {
        return arange( 0l, stop );
    }


    public static ArrayList< Long > arange( long start, long stop )
    {
        ArrayList< Long > numbers = new ArrayList< Long >();
        for ( long i = start; i < stop; ++i )
            numbers.add( i );
        return numbers;
    }

    /**
     *
     * rdd in:  ( K -> V )
     * rdd out: ( V -> K )
     *
     * @param <K>
     * @param <V>
     */
    public static class SwapKeyValue< K, V> implements PairFunction<Tuple2<K, V>, V, K >
    {
        private static final long serialVersionUID = -4173593608656179460L;

        public Tuple2<V, K> call(Tuple2<K, V> t) throws Exception {
            return tuple2( t._2(), t._1() );
        }
    }

    /**
     * rdd in:  ( K -> G,V )
     * rdd out: ( G -> K,V )
     * @param <K>
     * @param <G>
     * @param <V>
     */
    public static class Swap< K, G, V> implements PairFunction<Tuple2<K,Tuple2<G,V>>, G, Tuple2< K, V > >
    {
        private static final long serialVersionUID = -4848162685420711301L;

        public Tuple2<G, Tuple2<K, V>> call(Tuple2<K, Tuple2<G, V>> t)
                throws Exception {
            Tuple2<G, V> t2 = t._2();
            return Utility.tuple2(t2._1(), Utility.tuple2(t._1(), t2._2()));
        }
    }

    public static class StackOpener implements PairFunction<Integer, Integer, FPTuple>
    {

        private static final long serialVersionUID = 6238343817172855495L;
        private final String fileName;
        private final boolean isPattern;

        public StackOpener( String fileName, boolean isPattern ) {
            super();
            this.fileName = fileName;
            this.isPattern = isPattern;
        }

        public Tuple2<Integer, FPTuple> call( Integer t )
                throws Exception {
            ImageProcessorReader reader = new ImageProcessorReader();
            final ImageProcessor ip;
            if ( isPattern )
            {
                FileStitcher stitcher = new FileStitcher( isPattern );
                stitcher.setId( fileName );
                reader.setId( stitcher.getFilePattern().getFiles()[ t ] );
                ip = reader.openProcessors( 0 )[ 0 ];
                stitcher.close();
            }
            else
            {
                reader.setId( fileName );
                ip = reader.openProcessors( t )[ 0 ];
            }
            // assume only one channel right now
            FPTuple fp = new FPTuple( ip.convertToFloatProcessor() );
            reader.close();
            return Utility.tuple2( t, fp );
        }
    }

    public static class FileListOpener implements PairFunction< Integer, Integer, FPTuple[] >
    {
        private final String[] formats;

        public FileListOpener(String[] formats) {
            this.formats = formats;
        }

        @Override
        public Tuple2<Integer, FPTuple[]> call(Integer index) throws Exception {
            FPTuple[] images = new FPTuple[formats.length];
            for( int i = 0; i < formats.length; ++i )
            {
                images[i] = new FPTuple( new ImagePlus( String.format(formats[i], index ) ).getProcessor().convertToFloatProcessor() );
            }
            return Utility.tuple2( index, images );
        }
    }

    public static class WriteToFormatString< K > implements PairFunction<Tuple2<K,FPTuple>, K, Boolean>
    {
        private static final long serialVersionUID = -4095400790949311166L;
        private final String outputFormat;

        public WriteToFormatString(String outputFormat) {
            super();
            this.outputFormat = outputFormat;
        }

        public Tuple2<K, Boolean> call(Tuple2<K, FPTuple> t)
                throws Exception {
            K index = t._1();
            ImagePlus imp = new ImagePlus( "", t._2().rebuild() );
            String path = String.format( outputFormat, index );
            IO.createDirectoryForFile(path);
            boolean success = new FileSaver( imp ).saveAsTiff( path );
            return Utility.tuple2( index, success );
        }
    }

    public static class WriteToFormatStringDouble<K> implements PairFunction<Tuple2<K,DPTuple>, K, Boolean>
    {
        private static final long serialVersionUID = -4095400790949311166L;
        private final String outputFormat;

        public WriteToFormatStringDouble(String outputFormat) {
            super();
            this.outputFormat = outputFormat;
        }

        public Tuple2<K, Boolean> call(Tuple2<K, DPTuple> t)
                throws Exception {
            K index = t._1();
            DPTuple dp = t._2();
            ArrayImg<DoubleType, DoubleArray> img = ArrayImgs.doubles(dp.pixels, dp.width, dp.height);
            ConvertedRandomAccessibleInterval<DoubleType, FloatType> floats =
                    new ConvertedRandomAccessibleInterval<DoubleType, FloatType>(img, new RealFloatConverter<DoubleType>(), new FloatType());
            ImagePlus imp = ImageJFunctions.wrap( floats, "" );
            String path = String.format( outputFormat, index );
            IO.createDirectoryForFile(path);
            boolean success = new FileSaver( imp ).saveAsTiff( path );
            return Utility.tuple2( index, success );
        }
    }

    public static class DownSample< K > implements PairFunction< Tuple2< K, FPTuple >, K, FPTuple > {

        private final int sampleScale;

        /**
         * @param sampleScale
         */
        public DownSample(int sampleScale) {
            super();
            this.sampleScale = sampleScale;
        }

        private static final long serialVersionUID = -8634964011671381854L;

        @Override
        public Tuple2<K, FPTuple> call(
                Tuple2<K, FPTuple> indexedFloatProcessor ) throws Exception {
            FloatProcessor downsampled = (FloatProcessor) Downsampler.downsampleImageProcessor(indexedFloatProcessor._2().rebuild(), sampleScale);
            return Utility.tuple2( indexedFloatProcessor._1(), new FPTuple( downsampled ) );
        }

    }

    public static class GaussianBlur< K > implements  PairFunction< Tuple2< K, FPTuple >, K, FPTuple > {

        private final double[] sigma;

        public GaussianBlur(double[] sigma) {
            this.sigma = sigma;
        }

        public GaussianBlur( double sigmaX, double sigmaY )
        {
            this( new double[] { sigmaX, sigmaY } );
        }

        public GaussianBlur( double sigma )
        {
            this( sigma, sigma );
        }

        @Override
        public Tuple2<K, FPTuple> call(Tuple2<K, FPTuple> t) throws Exception {
            FloatProcessor fp = (FloatProcessor) t._2().rebuild().duplicate();
            new ij.plugin.filter.GaussianBlur().blurFloat( fp, sigma[0], sigma[1], 1e-4 );
            return Utility.tuple2( t._1(), new FPTuple( fp ) );
        }
    }

    public static int xyToLinear( int width, int x, int y )
    {
        return width*y + x;
    }

    public static int[] linearToXY( int width, int i, int[] xy )
    {
        xy[1] = i / width;
        xy[0] = i - ( xy[1] * width );
        return xy;
    }

    // takes backward transform from source to target
    public static class Transform<K> implements PairFunction<Tuple2<K, Tuple2<FPTuple, double[]>>, K, FPTuple> {
        @Override
        public Tuple2<K, FPTuple> call(Tuple2<K, Tuple2<FPTuple, double[]>> t) throws Exception {
            Tuple2<FPTuple, double[]> t2 = t._2();
            int width = t2._1().width;
            int height = t2._1().height;
            Img<FloatType> matrix = ImageJFunctions.wrapFloat(new ImagePlus("", t2._1().rebuild()));
            float[] data = new float[width * height];
            ArrayImg<FloatType, FloatArray> transformed = ArrayImgs.floats(data, width, height);
            RealRandomAccessible<FloatType> source = Views.interpolate(Views.extendValue(matrix, new FloatType(Float.NaN)), new NLinearInterpolatorFactory<FloatType>());
            LUTRealTransform transform = new LUTRealTransform(t2._2(), 2, 2);
            Cursor<FloatType> s =
                    Views.flatIterable(
                            Views.interval(
                                    Views.raster(
                                            new RealTransformRealRandomAccessible<FloatType, LUTRealTransform>(source, transform)
                                    ), matrix)
                    ).cursor();
            ArrayCursor<FloatType> tc = transformed.cursor();
            while( tc.hasNext() )
                tc.next().set( s.next() );

            return Utility.tuple2( t._1(), new FPTuple(data, width, height ) );
        }
    }

    public static class FlatmapMap<K1, K2, V, M extends Map< K2, V > > implements PairFlatMapFunction<Tuple2<K1,M>,K1,Tuple2<K2,V>>
    {
        @Override
        public Iterable<Tuple2<K1, Tuple2<K2, V>>> call(Tuple2<K1, M> t) throws Exception {
            return new IterableWithConstKeyFromMap<K1,K2,V>( t._1(), t._2() );
        }
    }

    public static class EntryToTuple<K,V1,V2,E extends Map.Entry<V1,V2>> implements PairFunction<Tuple2<K,E>,K,Tuple2<V1,V2>>
    {

        @Override
        public Tuple2<K, Tuple2<V1, V2>> call(Tuple2<K, E> t) throws Exception {
            return Utility.tuple2( t._1(), Utility.tuple2( t._2().getKey(), t._2().getValue() ) );
        }
    }

    public static class IterableWithConstKeyFromMap<K1,K2,V> implements Iterable< Tuple2<K1,Tuple2<K2,V> > >, Serializable
    {

        private final K1 k;
        private final Map<K2,V> m;

        public IterableWithConstKeyFromMap(K1 k, Map<K2, V> m) {
            this.k = k;
            this.m = m;
        }

        @Override
        public Iterator<Tuple2<K1, Tuple2<K2, V>>> iterator() {
            final Iterator<Map.Entry<K2, V>> it = m.entrySet().iterator();
            return new Iterator<Tuple2<K1, Tuple2<K2, V>>>() {
                @Override
                public boolean hasNext() {
                    return it.hasNext();
                }

                @Override
                public Tuple2<K1, Tuple2<K2, V>> next() {
                    Map.Entry<K2, V> entry = it.next();
                    return Utility.tuple2( k, Utility.tuple2( entry.getKey(), entry.getValue() ) );
                }

                @Override
                public void remove() {
                    throw new UnsupportedOperationException();
                }
            };
        }
    }

    public static class IterableWithConstKey<K,V> implements Iterable<Tuple2<K,V>>, Serializable
    {

        private final K key;
        private final Iterable< V > iterable;

        public IterableWithConstKey(K key, Iterable<V> iterable) {
            this.key = key;
            this.iterable = iterable;
        }

        @Override
        public Iterator<Tuple2<K, V>> iterator() {
            return new Iterator<Tuple2<K, V>>() {
                final Iterator<V> it = iterable.iterator();
                @Override
                public boolean hasNext() {
                    return it.hasNext();
                }

                @Override
                public Tuple2<K, V> next() {
                    return Utility.tuple2( key, it.next() );
                }

                @Override
                public void remove() {
                    throw new UnsupportedOperationException();
                }
            };
        }
    }

    public static class ValueAsMap<K1,K2,V> implements PairFunction< Tuple2<K1,Tuple2<K2,V>>,K1,HashMap<K2,V>>
    {

        @Override
        public Tuple2<K1, HashMap<K2, V>> call(Tuple2<K1, Tuple2<K2, V>> t) throws Exception {
            HashMap<K2, V> hm = new HashMap<K2, V>();
            hm.put( t._2()._1(), t._2()._2() );
            return Utility.tuple2( t._1(), hm );
        }
    }

    public static class ReduceMapsByUnion<K,V,M extends Map<K,V>> implements Function2<M,M,M>
    {

        @Override
        public M call(M m1, M m2) throws Exception {
            m1.putAll( m2 );
            return m1;
        }
    }


//    public static class SwapMultiKey<K1,K2,V> implements PairFunction<Tuple2<K1,Tuple2<K2,V>>,K2,Tuple2<K1,V>>
//    {
//
//        @Override
//        public Tuple2<K2, Tuple2<K1, V>> call(Tuple2<K1, Tuple2<K2, V>> t) throws Exception {
//            return Utility.tuple2( t._2()._1(), Utility.tuple2( t._1(), t._2()._2() ) );
//        }
//    }

    public static void main(String[] args) {
        SparkConf conf = new SparkConf().setAppName("BLA").setMaster("local");
        JavaSparkContext sc = new JavaSparkContext(conf);
    }

    public static class Format<K> implements PairFunction< K, K, String >
    {

        private final String pattern;

        public Format(String pattern) {
            this.pattern = pattern;
        }

        @Override
        public Tuple2<K, String> call(K k) throws Exception {
            return Utility.tuple2( k, String.format( pattern, k ) );
        }
    }

    public static class LoadFile implements PairFunction< Tuple2< Integer, String >, Integer, FPTuple > {

        private static final long serialVersionUID = -5220501390575963707L;

        @Override
        public Tuple2< Integer, FPTuple > call( Tuple2< Integer, String > tuple ) throws Exception {
            Integer index     = tuple._1();
            String filename   = tuple._2();
            FloatProcessor fp = new ImagePlus( filename ).getProcessor().convertToFloatProcessor();
            return Utility.tuple2( index, new FPTuple( fp ) );
        }
    }
    public static class ReplaceValue implements PairFunction< Tuple2< Integer, FPTuple >, Integer, FPTuple > {

        private final float value;
        private final float replacement;

        /**
         * @param value
         * @param replacement
         */
        public ReplaceValue(float value, float replacement) {
            super();
            this.value = value;
            this.replacement = replacement;
        }

        private static final long serialVersionUID = 5642948550521110112L;

        @Override
        public Tuple2<Integer, FPTuple> call(
                Tuple2<Integer, FPTuple> indexedFloatProcessor ) throws Exception {
            FloatProcessor fp = (FloatProcessor)indexedFloatProcessor._2().rebuild().duplicate();
            float[] pixels = (float[])fp.getPixels();
            for ( int i = 0; i < pixels.length; ++i )
                if ( pixels[i] == this.value )
                    pixels[i] = this.replacement;
            return new Tuple2< Integer, FPTuple >( indexedFloatProcessor._1(), new FPTuple( fp ) );
        }

    }

    public static < T extends RealType<T>> RandomAccessibleInterval<T> transform(
            RandomAccessible<T> source,
            RandomAccessibleInterval< T > target,
            RandomAccessibleInterval< DoubleType > lut) throws InterruptedException {
        Cursor<RealComposite<DoubleType>> lutCursor = Views.flatIterable( Views.collapseReal(lut) ).cursor();
        ArrayList<Callable<Void>> callables = new ArrayList<Callable<Void>>();
        ExecutorService es = Executors.newFixedThreadPool(Runtime.getRuntime().availableProcessors());
        long size = lut.dimension(2);
        while( lutCursor.hasNext() )
        {
            lutCursor.fwd();
            long x = lutCursor.getLongPosition(0);
            long y = lutCursor.getLongPosition(1);
            final RealRandomAccessible<T> sourceColumn =
                    Views.interpolate(Views.hyperSlice(Views.hyperSlice(source, 1, y), 0, x), new NLinearInterpolatorFactory<T>());
            final IntervalView<T> targetColumn = Views.hyperSlice(Views.hyperSlice(target, 1, y), 0, x);
            final IntervalView<DoubleType> lutColumn = Views.hyperSlice(Views.hyperSlice(lut, 1, y), 0, x);

            final IntervalView<T> targetColumn3D = Views.offsetInterval(target, new long[]{x, y, 0}, new long[]{1, 1, size});
            final IntervalView<DoubleType> lutColumn3D = Views.offsetInterval(lut, new long[]{x, y, 0}, new long[]{1, 1, size});
            final RealPoint p = new RealPoint( targetColumn3D.numDimensions() );

            final SingleDimensionLUTRealTransformField transform = new SingleDimensionLUTRealTransformField(3, 3, lutColumn3D);
            callables.add(new Callable<Void>() {
                @Override
                public Void call() throws Exception {
                    RealRandomAccess<T> ra = sourceColumn.realRandomAccess();
                    Cursor<T> c = Views.flatIterable(targetColumn3D).cursor();
                    while (c.hasNext()) {
                        T t = c.next();
                        transform.apply(c, p);
                        ra.setPosition( p.getDoublePosition( 2 ), 0 );
                        t.set(ra.get());
                    }
                    return null;
                }
            });
        }
        System.out.println( "Invoking all column jobs." );
        System.out.flush();
        es.invokeAll( callables );
        return target;
    }


//    public static class ColumnsToSections implements PairFunction< Tuple2< Tuple2< Integer, Integer >, double[] >, Integer, DPTuple >
//    {
//
//        private final int[] dim;
//
//        @Override
//        public Tuple2<Integer, DPTuple> call(Tuple2<Tuple2<Integer, Integer>, double[]> tuple2Tuple2) throws Exception {
//            return null;
//        }
//    }



}
