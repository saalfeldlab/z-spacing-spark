package org.janelia.thickness;

import ij.IJ;
import ij.ImageJ;
import ij.ImagePlus;
import ij.plugin.FolderOpener;
import ij.process.ImageConverter;
import net.imglib2.*;
import net.imglib2.algorithm.gauss3.Gauss3;
import net.imglib2.exception.IncompatibleTypeException;
import net.imglib2.img.Img;
import net.imglib2.img.array.ArrayCursor;
import net.imglib2.img.array.ArrayImg;
import net.imglib2.img.array.ArrayImgs;
import net.imglib2.img.array.ArrayRandomAccess;
import net.imglib2.img.basictypeaccess.array.DoubleArray;
import net.imglib2.img.basictypeaccess.array.FloatArray;
import net.imglib2.img.display.imagej.ImageJFunctions;
import net.imglib2.interpolation.randomaccess.NLinearInterpolatorFactory;
import net.imglib2.realtransform.RealTransformRealRandomAccessible;
import net.imglib2.realtransform.RealViews;
import net.imglib2.type.numeric.RealType;
import net.imglib2.type.numeric.real.DoubleType;
import net.imglib2.type.numeric.real.FloatType;
import net.imglib2.view.IntervalView;
import net.imglib2.view.MixedTransformView;
import net.imglib2.view.Views;
import net.imglib2.view.composite.CompositeIntervalView;
import net.imglib2.view.composite.RealComposite;
import org.janelia.thickness.lut.SingleDimensionLUTGrid;
import org.janelia.thickness.lut.SingleDimensionLUTRealTransformField;
import scala.Tuple2;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.Random;
import java.util.concurrent.Callable;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;

/**
 * Created by hanslovskyp on 10/14/15.
 */
public class GenerateWave {

    public interface Shifter
    {
        double call( double x, double y, double z, double curr, double prev );
        void forwardZ() throws Exception;
    }

//    public static class ChangeOffsetByRandomSpeedsDifferenceShifter implements Shifter{
//
//        private final double[][] offsets;
//        private final double[][] speed;
//        private final double[] add;
//
//        public ChangeOffsetByRandomSpeedsDifferenceShifter(double[][] offsets, double[][] speed, double[] add) {
//            this.offsets = offsets;
//            this.speed = speed;
//            this.add = add;
//        }
//
//        @Override
//        public double call(double x, double y, double z, double curr, double prev) {
//            double offset = offsets[(int)x]; // ( z - start ) * reciprocal * dim[1];
//            double result;
//            double diff = Math.abs( y - offset );
//            // if ( Math.abs( diff ) > 20 )
//            if ( Math.abs( diff ) > 20.0 )
//                result = prev + 1;
//            else {
//                double weight = (20.0 - diff) / 20;
//                result = prev + weight * add + (1 - weight);
//            }
//            return result;
//        }
//
//        @Override
//        public void forwardZ() throws IncompatibleTypeException {
//            for ( offset : offsets )
//            for( int i = 0; i < offsets.length; ++i )
//            {
//                offsets[i] += speed[i];//*( rng.nextGaussian() + 1 );
//                Gauss3.gauss( 0.35, Views.extendBorder( ArrayImgs.doubles( offsets.clone(), offsets.length ) ), ArrayImgs.doubles( offsets, offsets.length ) );
//            }
//        }
//    }

    public static class ChangeOffsetByRandomSpeeds implements Shifter{

        private final double[] offsets;
        private final double[] speed;
        private final double add;

        public ChangeOffsetByRandomSpeeds(double[] offsets, double[] speed, double add) {
            this.offsets = offsets;
            this.speed = speed;
            this.add = add;
        }

        @Override
        public double call(double x, double y, double z, double curr, double prev) {
            double offset = offsets[(int)x]; // ( z - start ) * reciprocal * dim[1];
            double result;
            double diff = Math.abs( y - offset );
            // if ( Math.abs( diff ) > 20 )
            if ( Math.abs( diff ) > 39.5 )
                result = prev + 1;
            else {
//                double weight = (20.0 - diff) / 20;
//                result = prev + weight * add + (1 - weight);
                result = prev + add;
            }
            return result;
        }

        @Override
        public void forwardZ() throws IncompatibleTypeException {
            for( int i = 0; i < offsets.length; ++i )
            {
                offsets[i] += speed[i];//*( rng.nextGaussian() + 1 );
                Gauss3.gauss( 0.35, Views.extendBorder( ArrayImgs.doubles( offsets.clone(), offsets.length ) ), ArrayImgs.doubles( offsets, offsets.length ) );
            }
        }
    }

    public static <T extends RealType<T>> void generate( RandomAccessibleInterval<T> lut, Shifter s, long zStart, long zEnd ) throws Exception {
        for ( long z = zStart+1; z < zEnd; ++z, s.forwardZ() ) {
            IntervalView<T> previousHs = Views.hyperSlice(lut, 2, z - 1);
            IntervalView<T> hs = Views.hyperSlice(lut, 2, z);
            for (Cursor<T> c = Views.flatIterable(hs).cursor(), p = Views.flatIterable(previousHs).cursor(); c.hasNext(); ) {
                T curr = c.next();
                double prev = p.next().getRealDouble();
                double lutVal = s.call(c.getDoublePosition(0), c.getDoublePosition(1), z, curr.getRealDouble(), prev );
                curr.setReal(lutVal);
            }
        }
    }

    public static <T extends RealType<T>> void generate( RandomAccessibleInterval<T> lut, Shifter s, long zStart ) throws Exception {
        generate( lut, s, zStart, lut.dimension( 2 ) );
    }

    public static <T extends RealType<T>> void generate( RandomAccessibleInterval<T> lut, Shifter s ) throws Exception {
        generate( lut, s, 0 );
    }

    public static void main(String[] args) throws Exception {
        // String fn = "/nobackup/saalfeld/philipp/AL-Z0613-14/analysis/z-spacing/35/out/spark-test-2/04/subset.tif";
        // String fn = "/home/hanslovskyp/workspace-idea/z_spacing-spark-scala/synthetic-distortion/noise-sigma-20/raw";
//        String fn = "/nobackup/saalfeld/hanslovskyp/CutOn4-15-2013_ImagedOn1-27-2014/export/crop/1349-3399/experiments/20151021_002011/out/spark-test-2/04/render-xy-view/";
//        ImagePlus imp = new FolderOpener().openFolder( fn );
//        new ImageConverter( imp ).convertToGray32();
//        Img<FloatType> img = ImageJFunctions.wrapFloat(imp);

//        final long[] dim = new long[ img.numDimensions() ];
//        img.dimensions( dim );
        long[] dim = new long[] { 40, 25, 400 };
        System.out.println( Arrays.toString( dim ) );
        // try f(x) = a(x - width/2)^2
        final double a = -0.00000025;
        final double b = 0.0025;
        final long off1 = dim[0] / 2;
        final long off2 = dim[0] / 2 - dim[0] / 16;
        final double add = 10.0; // 0.001;
        ArrayImg<DoubleType, DoubleArray> lut = ArrayImgs.doubles( dim );

        final double[] offsets = new double[(int) dim[0]];
        final double[] speed = offsets.clone();
        Random rng = new Random(100);

        for( int i = 0; i < speed.length; ++i )
        {
            speed[i] = Math.abs( rng.nextGaussian()*4.5 + 16 );
            offsets[i] = -200;// + a*Math.pow(i-off1,4) + b*Math.pow(i-off2,2);
        }

        System.out.println( Arrays.toString(speed) );
        Gauss3.gauss( 0.8, Views.extendBorder( ArrayImgs.doubles( speed.clone(), speed.length ) ), ArrayImgs.doubles( speed, speed.length ) );
        System.out.println( Arrays.toString(speed) );

//        generate(lut, new Shifter() {
//            @Override
//            public double call(double x, double y, double z, double val) {
//                return z;
//            }
//
//            @Override
//            public void forwardZ() {
//            }
//        });

        generate( lut, new ChangeOffsetByRandomSpeeds( offsets, speed, add ), 0 );
//        generate( lut, new ChangeOffsetByRandomSpeeds( offsets, speed, add ) );


        ArrayImg<DoubleType, DoubleArray> smoothed = ArrayImgs.doubles(dim);
        double sigma = 1.75;
        Gauss3.gauss( sigma, Views.extendBorder( lut ), smoothed );




        SingleDimensionLUTGrid transform = new SingleDimensionLUTGrid(3, 3, smoothed, 2);
//        ArrayImg<DoubleType, DoubleArray> inverseLut = ArrayImgs.doubles(dim);
//        CompositeIntervalView<DoubleType, RealComposite<DoubleType>> collapsedInverseLut = Views.collapseReal(inverseLut);
//        CompositeIntervalView<DoubleType, RealComposite<DoubleType>> collapsedSmoothed = Views.collapseReal(smoothed);
//        long[] offset = new long[3];
//        long[] size = new long[] { 1, 1, dim[2] };
//        for( Cursor<RealComposite<DoubleType>> c = Views.flatIterable( collapsedSmoothed ).cursor(); c.hasNext(); )
//        {
//            c.fwd();
//            long x = c.getLongPosition(0);
//            long y = c.getLongPosition(1);
//            offset[0] = 0;
//            offset[1] = 1;
//            IntervalView<DoubleType> hsSource =
//                    Views.offsetInterval( smoothed, offset, size );
////                    Views.interval(
////                            Views.addDimension(Views.addDimension(Views.hyperSlice(Views.hyperSlice(smoothed, 1, y), 0, x))),
////                            new FinalInterval(1, 1, dim[2])
////                    );
//            IntervalView<DoubleType> hsTarget =
//                    Views.hyperSlice( Views.hyperSlice( inverseLut, 1, y ), 0, x );
////                    Views.offsetInterval( inverseLut, offset, size );
////                    Views.interval(
////                    Views.addDimension(Views.addDimension(Views.hyperSlice(Views.hyperSlice(inverseLut, 1, y), 0, x))),
////                    new FinalInterval(1, 1, dim[2])
////            );
//            SingleDimensionLUTGrid tf = new SingleDimensionLUTGrid(3, 3, hsSource, 2);
//            SingleDimensionLUTRealTransformField tff = new SingleDimensionLUTRealTransformField(3, 3, hsSource);
//            for( Cursor<DoubleType> targ = Views.flatIterable( hsTarget ).cursor(); targ.hasNext(); )
//            {
//                targ.fwd();
//                RealPoint point = new RealPoint(3);
//                point.setPosition( targ.getDoublePosition( 0 ), 2 );
//                tf.applyInverse(point, point);
////                System.out.println( point );
//                targ.get().set( point.getDoublePosition(  2 ) );
//            }
////            transform.applyInverse( p, t );
////            t.get().set( p.getDoublePosition( 2 ) );
//        }

//        ArrayImg<DoubleType, DoubleArray> inverseLut = ArrayImgs.doubles(dim);
//        for( ArrayCursor<DoubleType> c = inverseLut.cursor(); c.hasNext(); )
//        {
//            RealPoint p = new RealPoint(c.numDimensions());
//            DoubleType curr = c.next();
//            transform.applyInverse( p, c );
//            curr.set( p.getDoublePosition( 2 ) );
//            System.out.println( p );
//        }
//        System.out.println( "Created inverse transform" );
//

//        RealRandomAccessible<FloatType> interpolatedTarget = Views.interpolate(Views.extendBorder(img), new NLinearInterpolatorFactory<FloatType>());
//
//        IntervalView<FloatType> transformed =
////                Views.interval(
////                        Views.raster(
////                                new RealTransformRealRandomAccessible<>(
////                                        interpolatedTarget,
////                                        transform
////                                )
////                        ),
////                        img
////                );
//                Views.interval(
//                Views.raster(RealViews.transformReal(
//                        Views.interpolate(Views.extendBorder(img), new NLinearInterpolatorFactory<FloatType>()),
//                        transform)),
//                img);
//        System.out.println("Created transform-view");
//        ArrayImg<FloatType, FloatArray> target = ArrayImgs.floats(dim);
//        Cursor<FloatType> tf = Views.flatIterable(transformed).cursor();
//        ArrayCursor<FloatType> tg = target.cursor();
//        System.out.println( "Wrote transform" );
//        RealRandomAccess<FloatType> ra = interpolatedTarget.realRandomAccess();
//        ExecutorService es = Executors.newFixedThreadPool(Runtime.getRuntime().availableProcessors());
//        ArrayList<Callable<Void>> callables = new ArrayList<Callable<Void>>();
//        long count = 0;
//        long nElements = dim[0] * dim[1] * dim[2];
//        long nElementsPerBatch = nElements / Runtime.getRuntime().availableProcessors();
//        ArrayList<Tuple2<RealRandomAccess<FloatType>, FloatType>> sourcesAndTargets = new ArrayList<>();
//        while( tg.hasNext() )
//        {
//            final FloatType t = tg.next();
//            RealRandomAccess<FloatType> s = interpolatedTarget.realRandomAccess();
//            transform.apply( tg, ra );
//            ++count;
//            sourcesAndTargets.add( Utility.tuple2( s, t ) );
//            if ( count >= nElements ) {
//                final ArrayList<Tuple2<RealRandomAccess<FloatType>, FloatType>> sat = sourcesAndTargets;
//                callables.add(new Callable<Void>() {
//                    @Override
//                    public Void call() throws Exception {
//                        for( Tuple2<RealRandomAccess<FloatType>, FloatType> st : sat )
//                            st._2().set( st._1().get() );
//                        return null;
//                    }
//                });
//                count = 0;
//                sourcesAndTargets = new ArrayList<>();
//            }
////            t.set(s);
//        }
//        final ArrayList<Tuple2<RealRandomAccess<FloatType>, FloatType>> sat = sourcesAndTargets;
//        callables.add(new Callable<Void>() {
//            @Override
//            public Void call() throws Exception {
//                for( Tuple2<RealRandomAccess<FloatType>, FloatType> st : sat )
//                    st._2().set( st._1().get() );
//                return null;
//            }
//        });
//        System.out.println( "Invoking jobs." );
//        System.out.flush();
//        es.invokeAll( callables );
//        transform( Views.extendBorder( img ), target, smoothed );
        new ImageJ();
        // ImageJFunctions.show( smoothed );
        ImagePlus smoothedImagePlus = ImageJFunctions.wrapFloat(smoothed, "transform");
        smoothedImagePlus.setDimensions( 1, (int)dim[2], 1);
        smoothedImagePlus.show();
        IJ.run("Reslice [/]...", "output=1.000 start=Top avoid");
        IJ.run("Convolve...", "text1=-0.5\n0\n0.5\n stack");
//        ImageJFunctions.show( target );
        System.out.println("Showed stuff!") ;

    }

    public static < T extends RealType<T> > RandomAccessibleInterval<T> transform(
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

}
