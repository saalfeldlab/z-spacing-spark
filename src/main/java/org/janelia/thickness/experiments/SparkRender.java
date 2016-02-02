/**/package org.janelia.thickness.experiments;

import ij.ImagePlus;
import ij.process.FloatProcessor;
import loci.formats.FileStitcher;
import loci.formats.FormatException;
import net.imglib2.Cursor;
import net.imglib2.FinalInterval;
import net.imglib2.RealRandomAccess;
import net.imglib2.RealRandomAccessible;
import net.imglib2.img.array.ArrayCursor;
import net.imglib2.img.array.ArrayImg;
import net.imglib2.img.array.ArrayImgs;
import net.imglib2.img.basictypeaccess.array.FloatArray;
import net.imglib2.interpolation.randomaccess.NLinearInterpolatorFactory;
import net.imglib2.realtransform.InverseRealTransform;
import net.imglib2.realtransform.RealTransformRandomAccessible;
import net.imglib2.realtransform.RealViews;
import net.imglib2.realtransform.Scale2D;
import net.imglib2.type.numeric.real.FloatType;
import net.imglib2.view.Views;
import org.apache.spark.SparkConf;
import org.apache.spark.api.java.JavaPairRDD;
import org.apache.spark.api.java.JavaSparkContext;
import org.apache.spark.api.java.function.Function;
import org.apache.spark.api.java.function.Function2;
import org.apache.spark.api.java.function.PairFlatMapFunction;
import org.apache.spark.api.java.function.PairFunction;
import org.janelia.thickness.ScaleOptions;
import org.janelia.thickness.utility.FPTuple;
import org.janelia.thickness.utility.Utility;
import org.janelia.utility.realtransform.ScaleAndShift;
import scala.Tuple2;

import java.io.IOException;
import java.util.*;
import java.util.regex.Pattern;

/**
 * Created by hanslovskyp on 10/22/15.
 */
public class SparkRender {

    public static void main(String[] args) throws IOException, FormatException {

        SparkConf sparkConf = new SparkConf().setAppName("Render");
        JavaSparkContext sc = new JavaSparkContext(sparkConf);

//        final String sourceFormat = args[0];
//        final String transformFormat = args[1];
        final String configPath = args[0];
        final int iteration = Integer.parseInt(args[1]);
        final String outputFormat = args[2];
        final ScaleOptions options = ScaleOptions.createFromFile(configPath);
        final int[] radii = options.radii[iteration];
        final int[] steps = options.steps[iteration];
        final double scale = Math.pow( 2, options.scale );
        final int start = options.start;
        final int stop = options.stop;
        final String sourceFormat = options.source;
        final String transformFormat = options.target + String.format( "/%02d/backward/", iteration ) + "%04d.tif";

//        final String outputFormat = args[4];

        ImagePlus img0 = new ImagePlus(String.format(sourceFormat, start));

        final int width = img0.getWidth();
        final int height = img0.getHeight();
        final int size = stop - start;

        final long[] dim = new long[]{width, height, size};



        final double[] radiiDouble = new double[]{radii[0], radii[1]};
        final double[] stepsDouble = new double[]{steps[0], steps[1]};

        ArrayList<Integer> indices = Utility.arange(size);
        JavaPairRDD<Integer, FPTuple> sections = sc
                .parallelize(indices)
                .mapToPair(new PairFunction<Integer, Integer, Integer>() {

                    public Tuple2<Integer, Integer> call(Integer arg0) throws Exception {
                        return Utility.tuple2(arg0, arg0);
                    }
                })
                .sortByKey()
                .map(new Function<Tuple2<Integer, Integer>, Integer>() {

                    public Integer call(Tuple2<Integer, Integer> arg0) throws Exception {
                        return arg0._1();
                    }
                })
                .mapToPair(new Utility.LoadFileFromPattern(sourceFormat))
                .cache();

        JavaPairRDD<Integer, FPTuple> transforms = sc
                .parallelize(indices)
                .mapToPair(new PairFunction<Integer, Integer, Integer>() {

                    public Tuple2<Integer, Integer> call(Integer arg0) throws Exception {
                        return Utility.tuple2(arg0, arg0);
                    }
                })
                .sortByKey()
                .map(new Function<Tuple2<Integer, Integer>, Integer>() {

                    public Integer call(Tuple2<Integer, Integer> arg0) throws Exception {
                        return arg0._1();
                    }
                })
                .mapToPair(new PairFunction<Integer, Integer, FPTuple>() {
                    @Override
                    public Tuple2<Integer, FPTuple> call(Integer integer) throws Exception {
                        ImagePlus imp = new ImagePlus(String.format(transformFormat, integer.intValue()));
                        FloatProcessor fp = imp.getProcessor().convertToFloatProcessor();
                        return Utility.tuple2(integer, new FPTuple(fp));
                    }
                })
                .cache();

        JavaPairRDD<Integer, FPTuple> transformed = render(sc, sections, transforms, stepsDouble, radiiDouble, dim, scale);

        List<Tuple2<Integer, Boolean>> successOnWrite = transformed
                .mapToPair(new Utility.WriteToFormatString<Integer>(outputFormat))
                .collect();
        int count = 0;
        for ( Tuple2<Integer, Boolean> s : successOnWrite )
        {
            if ( s._2().booleanValue() )
                continue;
            ++count;
            System.out.println( "Failed to write forward image " + s._1().intValue() );
        }
        System.out.println( "Successfully transformed and wrote " + (size-count) + "/" + size + " images." );

    }

    public static JavaPairRDD<Integer, FPTuple> render(
            final JavaSparkContext sc,
            final JavaPairRDD<Integer, FPTuple> sections,
            final JavaPairRDD<Integer, FPTuple> transforms,
            final double[] stepsDouble,
            final double[] radiiDouble,
            final long[] dim,
            final double scale )
    {

        final int width = (int) dim[0];
        final int height = (int) dim[1];

        final Scale2D scaleToOriginalTransform = new Scale2D(scale, scale);

        JavaPairRDD<Integer, Tuple2<FPTuple, Integer>> transformsToRequiredSectionsMapping = transforms
                .mapToPair(new PairFunction<Tuple2<Integer, FPTuple>, Integer, Tuple2<FPTuple, HashSet<Integer>>>() {
                    @Override
                    public Tuple2<Integer, Tuple2<FPTuple, HashSet<Integer>>> call(Tuple2<Integer, FPTuple> t) throws Exception {
                        HashSet<Integer> s = new HashSet<Integer>();
                        // why need to generate high res image?
                        FPTuple transformFP = t._2();
                        ArrayImg<FloatType, FloatArray> transformImg
                                = ArrayImgs.floats(transformFP.pixels, transformFP.width, transformFP.height);
                        RealRandomAccessible<FloatType> extendedAndInterpolatedTransform
                                = Views.interpolate(Views.extendBorder(transformImg), new NLinearInterpolatorFactory<FloatType>());
                        RealTransformRandomAccessible<FloatType, InverseRealTransform> scaledTransformImg
                                = RealViews.transform(extendedAndInterpolatedTransform, new ScaleAndShift(stepsDouble, radiiDouble));
                        RealTransformRandomAccessible<FloatType, InverseRealTransform> scaledToOriginalResolutionImg
                                = RealViews.transform(scaledTransformImg, new Scale2D(scale, scale) );
                        Cursor<FloatType> c =
                                Views.flatIterable(Views.interval(Views.raster(scaledToOriginalResolutionImg), new FinalInterval(width, height))).cursor();
//                        for (float p : t._2().pixels) {
                        while( c.hasNext() ) {
                            float p = c.next().get();
                            if (!Float.isNaN(p) && !Float.isInfinite(p)) {
                                s.add((int) Math.floor(p));
                                s.add((int) Math.ceil(p));
                            }
                        }
                        return Utility.tuple2(t._1(), Utility.tuple2(t._2(), s));
                    }
                })
                .flatMapToPair(new PairFlatMapFunction<Tuple2<Integer, Tuple2<FPTuple, HashSet<Integer>>>, Integer, Tuple2<FPTuple, Integer>>() {
                    @Override
                    public Iterable<Tuple2<Integer, Tuple2<FPTuple, Integer>>>
                    call(Tuple2<Integer, Tuple2<FPTuple, HashSet<Integer>>> t) throws Exception {
                        final Integer idx = t._1();
                        final FPTuple data = t._2()._1();
                        final HashSet<Integer> s = t._2()._2();
                        return new Iterable<Tuple2<Integer, Tuple2<FPTuple, Integer>>>() {
                            @Override
                            public Iterator<Tuple2<Integer, Tuple2<FPTuple, Integer>>> iterator() {
                                return new Iterator<Tuple2<Integer, Tuple2<FPTuple, Integer>>>() {
                                    final Iterator<Integer> it = s.iterator();

                                    @Override
                                    public boolean hasNext() {
                                        return it.hasNext();
                                    }

                                    @Override
                                    public Tuple2<Integer, Tuple2<FPTuple, Integer>> next() {
                                        return Utility.tuple2(idx, Utility.tuple2(data, it.next()));
                                    }

                                    @Override
                                    public void remove() {

                                    }
                                };
                            }
                        };
                    }
                })
                        // ( target-index -> ... ==> source-index -> ... )
                .mapToPair(new PairFunction<Tuple2<Integer, Tuple2<FPTuple, Integer>>, Integer, Tuple2<FPTuple, Integer>>() {
                    @Override
                    public Tuple2<Integer, Tuple2<FPTuple, Integer>> call(Tuple2<Integer, Tuple2<FPTuple, Integer>> t) throws Exception {
                        return Utility.tuple2(t._2()._2(), Utility.tuple2(t._2()._1(), t._1()));
                    }
                })
                ;

        JavaPairRDD<Integer, Tuple2<FPTuple, HashMap<Integer, FPTuple>>> targetsAndSections = sections
                .join(transformsToRequiredSectionsMapping)
                .mapToPair(new PairFunction<Tuple2<Integer, Tuple2<FPTuple, Tuple2<FPTuple, Integer>>>, Integer, Tuple2<FPTuple, HashMap<Integer, FPTuple>>>() {
                    @Override
                    public Tuple2<Integer, Tuple2<FPTuple, HashMap<Integer, FPTuple>>>
                    call(Tuple2<Integer, Tuple2<FPTuple, Tuple2<FPTuple, Integer>>> t) throws Exception {
                        Integer sectionIndex = t._1();
                        Integer targetIndex = t._2()._2()._2();
                        FPTuple transform = t._2()._2()._1();
                        FPTuple section = t._2()._1();
                        HashMap<Integer, FPTuple> m = new HashMap<Integer, FPTuple>();
                        m.put(sectionIndex, section);
                        return Utility.tuple2(targetIndex, Utility.tuple2(transform, m));
                    }
                })
                .reduceByKey(new Function2<Tuple2<FPTuple, HashMap<Integer, FPTuple>>, Tuple2<FPTuple, HashMap<Integer, FPTuple>>, Tuple2<FPTuple, HashMap<Integer, FPTuple>>>() {
                    @Override
                    public Tuple2<FPTuple, HashMap<Integer, FPTuple>>
                    call(Tuple2<FPTuple, HashMap<Integer, FPTuple>> t1, Tuple2<FPTuple, HashMap<Integer, FPTuple>> t2) throws Exception {
                        HashMap<Integer, FPTuple> m = t1._2();
                        m.putAll(t2._2());
                        return Utility.tuple2(t1._1(), m);
                    }
                })
                .cache()
                ;

        JavaPairRDD<Integer, FPTuple> transformed = targetsAndSections
                .mapToPair(new PairFunction<Tuple2<Integer, Tuple2<FPTuple, HashMap<Integer, FPTuple>>>, Integer, FPTuple>() {
                    @Override
                    public Tuple2<Integer, FPTuple> call(Tuple2<Integer, Tuple2<FPTuple, HashMap<Integer, FPTuple>>> t) throws Exception {
                        Integer index = t._1();
                        FPTuple transformFP = t._2()._1();
                        HashMap<Integer, FPTuple> m = t._2()._2();
                        ArrayImg<FloatType, FloatArray> transformImg = ArrayImgs.floats(transformFP.pixels, transformFP.width, transformFP.height);
                        RealRandomAccessible<FloatType> extendedAndInterpolatedTransform
                                = Views.interpolate(Views.extendBorder(transformImg), new NLinearInterpolatorFactory<FloatType>());
                        RealTransformRandomAccessible<FloatType, InverseRealTransform> scaledTransformImg
                                = RealViews.transform(extendedAndInterpolatedTransform, new ScaleAndShift(stepsDouble, radiiDouble));
                        RealTransformRandomAccessible<FloatType, InverseRealTransform> scaledToOriginalResolutionImg
                                = RealViews.transform(scaledTransformImg, new Scale2D(scale, scale) );
                        RealRandomAccess<FloatType> transformRA = scaledToOriginalResolutionImg.realRandomAccess();
                        float[] targetPixels = new float[(int) dim[0] * (int) dim[1]];
                        new FloatProcessor( (int)dim[0], (int)dim[1], targetPixels ).add(Double.NaN);
                        for (ArrayCursor<FloatType> c = ArrayImgs.floats(targetPixels, dim[0], dim[1]).cursor(); c.hasNext(); ) {
                            FloatType fv = c.next();
                            int x = c.getIntPosition(0);
                            int y = c.getIntPosition(1);
                            transformRA.setPosition( x, 0 );
                            transformRA.setPosition( y ,1 );

                            float pos = transformRA.get().get();

                            int lowerPos = (int) pos;
                            int upperPos = lowerPos + 1;
                            float lowerWeight = upperPos - pos;
                            float upperWeight = 1 - lowerWeight;

                            float weightSum = 0.0f;
                            float val = 0.0f;

                            FPTuple lower = m.get(lowerPos);
                            if ( lower != null )
                            {
                                weightSum += lowerWeight;
                                val += lower.rebuild().getf( x, y )*lowerWeight;
                            }

                            FPTuple upper = m.get(upperPos);
                            if( upper != null )
                            {
                                weightSum += upperWeight;
                                val += upper.rebuild().getf( x, y )*upperWeight;
                            }


//                            if( weightSum != 0.0 )
                            fv.setReal( val / weightSum );
//                            if ( index.intValue() > 40 && Float.isNaN( fv.get() ) )
//                            {
//                                String error = String.format(
//                                        "NaN at pos=%f,x=%d,y=%d,z=%d,lower=%s,upper=%s,val=%f,weightSum=%f,lowerWeight=%f,upperWeight=%f,lowergetf=%f,uppergetf=%f",
//                                        pos,
//                                        x,
//                                        y,
//                                        index,
//                                        lower == null? null : lower.toString(),
//                                        upper == null? null : upper.toString(),
//                                        val,
//                                        weightSum,
//                                        lowerWeight,
//                                        upperWeight,
//                                        lower == null? Float.NaN : lower.rebuild().getf(x,y),
//                                        upper == null? Float.NaN : upper.rebuild().getf(x,y)
//                                );
//                                throw new RuntimeException( error );
//                            }

                        }
                        return Utility.tuple2( index, new FPTuple( targetPixels, width, height ) );
                    }
                });

        return transformed;


    }

}
