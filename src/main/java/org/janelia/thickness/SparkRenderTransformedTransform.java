/**/package org.janelia.thickness;

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
import net.imglib2.realtransform.ScaleAndTranslation;
import net.imglib2.type.numeric.real.FloatType;
import net.imglib2.view.Views;
import org.apache.spark.SparkConf;
import org.apache.spark.api.java.JavaPairRDD;
import org.apache.spark.api.java.JavaSparkContext;
import org.apache.spark.api.java.function.Function;
import org.apache.spark.api.java.function.Function2;
import org.apache.spark.api.java.function.PairFlatMapFunction;
import org.apache.spark.api.java.function.PairFunction;
import org.janelia.thickness.utility.Utility;
import scala.Tuple2;

import java.io.IOException;
import java.util.*;
import java.util.regex.Pattern;

/**
 * @author Philipp Hanslovsky &lt;hanslovskyp@janelia.hhmi.org&gt;
 */
public class SparkRenderTransformedTransform {

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
        final String sourceFormat = options.target + String.format( "/%02d/forward/", iteration ) + "<0000-2100>.tif";
        final String transformFormat = options.target + String.format( "/%02d/backward/", iteration ) + "%04d.tif";

//        final String outputFormat = args[4];

        boolean isPattern = Pattern.compile(".*<\\d+-\\d+>.*").matcher(sourceFormat).matches(); // fileName.indexOf( "%" ) > -1;
        FileStitcher reader = new FileStitcher(isPattern);
        reader.setId(sourceFormat);

        final int width = reader.getSizeX();
        final int height = reader.getSizeY();
        final int size = reader.getSizeZ(); // 201;//reader.getSizeZ();

        final long[] dim = new long[]{width, height, size};

        reader.close();



        final double[] radiiDouble = new double[]{radii[0], radii[1]};
        final double[] stepsDouble = new double[]{steps[0], steps[1]};

        ArrayList<Integer> indices = Utility.arange(size);
        @SuppressWarnings("serial")
		JavaPairRDD<Integer, FloatProcessor> sections = sc
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
                .mapToPair( new Utility.LoadFileFromPattern( sourceFormat ) )
                .cache()
                ;

        @SuppressWarnings("serial")
		JavaPairRDD<Integer, FloatProcessor> transforms = sc
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
                .mapToPair(new PairFunction<Integer, Integer, FloatProcessor>() {
                    @Override
                    public Tuple2<Integer, FloatProcessor> call(Integer integer) throws Exception {
                        ImagePlus imp = new ImagePlus(String.format(transformFormat, integer.intValue()));
                        FloatProcessor fp = imp.getProcessor().convertToFloatProcessor();
                        return Utility.tuple2(integer, fp);
                    }
                })
                .cache();

        JavaPairRDD<Integer, FloatProcessor> transformed = render(sc, sections, transforms, stepsDouble, radiiDouble, dim);

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

    public static JavaPairRDD<Integer, FloatProcessor> render(
            final JavaSparkContext sc,
            final JavaPairRDD<Integer, FloatProcessor> sections,
            final JavaPairRDD<Integer, FloatProcessor> transforms,
            final double[] stepsDouble,
            final double[] radiiDouble,
            final long[] dim )
    {

        final int width = (int) dim[0];
        final int height = (int) dim[1];

        @SuppressWarnings("serial")
		JavaPairRDD<Integer, Tuple2<FloatProcessor, Integer>> transformsToRequiredSectionsMapping = transforms
                .mapToPair(new PairFunction<Tuple2<Integer, FloatProcessor>, Integer, Tuple2<FloatProcessor, HashSet<Integer>>>() {
                    @Override
                    public Tuple2<Integer, Tuple2<FloatProcessor, HashSet<Integer>>> call(Tuple2<Integer, FloatProcessor> t) throws Exception {
                        HashSet<Integer> s = new HashSet<Integer>();
                        // why need to generate high res image?
                        FloatProcessor transformFP = t._2();
                        ArrayImg<FloatType, FloatArray> transformImg = ArrayImgs.floats((float[])transformFP.getPixels(), transformFP.getWidth(), transformFP.getHeight());
                        RealRandomAccessible<FloatType> extendedAndInterpolatedTransform
                                = Views.interpolate(Views.extendBorder(transformImg), new NLinearInterpolatorFactory<FloatType>());
                        RealTransformRandomAccessible<FloatType, InverseRealTransform> scaledTransformImg
                                = RealViews.transform(extendedAndInterpolatedTransform, new ScaleAndTranslation(stepsDouble, radiiDouble));
                        Cursor<FloatType> c =
                                Views.flatIterable(Views.interval(Views.raster(scaledTransformImg), new FinalInterval(width, height))).cursor();
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
                .flatMapToPair(new PairFlatMapFunction<Tuple2<Integer, Tuple2<FloatProcessor, HashSet<Integer>>>, Integer, Tuple2<FloatProcessor, Integer>>() {
                    @Override
                    public Iterable<Tuple2<Integer, Tuple2<FloatProcessor, Integer>>>
                    call(Tuple2<Integer, Tuple2<FloatProcessor, HashSet<Integer>>> t) throws Exception {
                        final Integer idx = t._1();
                        final FloatProcessor data = t._2()._1();
                        final HashSet<Integer> s = t._2()._2();
                        return new Iterable<Tuple2<Integer, Tuple2<FloatProcessor, Integer>>>() {
                            @Override
                            public Iterator<Tuple2<Integer, Tuple2<FloatProcessor, Integer>>> iterator() {
                                return new Iterator<Tuple2<Integer, Tuple2<FloatProcessor, Integer>>>() {
                                    final Iterator<Integer> it = s.iterator();

                                    @Override
                                    public boolean hasNext() {
                                        return it.hasNext();
                                    }

                                    @Override
                                    public Tuple2<Integer, Tuple2<FloatProcessor, Integer>> next() {
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
                .mapToPair(new PairFunction<Tuple2<Integer, Tuple2<FloatProcessor, Integer>>, Integer, Tuple2<FloatProcessor, Integer>>() {
                    @Override
                    public Tuple2<Integer, Tuple2<FloatProcessor, Integer>> call(Tuple2<Integer, Tuple2<FloatProcessor, Integer>> t) throws Exception {
                        return Utility.tuple2(t._2()._2(), Utility.tuple2(t._2()._1(), t._1()));
                    }
                })
                ;

        @SuppressWarnings("serial")
		JavaPairRDD<Integer, Tuple2<FloatProcessor, HashMap<Integer, FloatProcessor>>> targetsAndSections = sections
                .join(transformsToRequiredSectionsMapping)
                .mapToPair(new PairFunction<Tuple2<Integer, Tuple2<FloatProcessor, Tuple2<FloatProcessor, Integer>>>, Integer, Tuple2<FloatProcessor, HashMap<Integer, FloatProcessor>>>() {
                    @Override
                    public Tuple2<Integer, Tuple2<FloatProcessor, HashMap<Integer, FloatProcessor>>>
                    call(Tuple2<Integer, Tuple2<FloatProcessor, Tuple2<FloatProcessor, Integer>>> t) throws Exception {
                        Integer sectionIndex = t._1();
                        Integer targetIndex = t._2()._2()._2();
                        FloatProcessor transform = t._2()._2()._1();
                        FloatProcessor section = t._2()._1();
                        HashMap<Integer, FloatProcessor> m = new HashMap<Integer, FloatProcessor>();
                        m.put(sectionIndex, section);
                        return Utility.tuple2(targetIndex, Utility.tuple2(transform, m));
                    }
                })
                .reduceByKey(new Function2<Tuple2<FloatProcessor, HashMap<Integer, FloatProcessor>>, Tuple2<FloatProcessor, HashMap<Integer, FloatProcessor>>, Tuple2<FloatProcessor, HashMap<Integer, FloatProcessor>>>() {
                    @Override
                    public Tuple2<FloatProcessor, HashMap<Integer, FloatProcessor>>
                    call(Tuple2<FloatProcessor, HashMap<Integer, FloatProcessor>> t1, Tuple2<FloatProcessor, HashMap<Integer, FloatProcessor>> t2) throws Exception {
                        HashMap<Integer, FloatProcessor> m = t1._2();
                        m.putAll(t2._2());
                        return Utility.tuple2(t1._1(), m);
                    }
                })
                .cache()
                ;

        @SuppressWarnings("serial")
		JavaPairRDD<Integer, FloatProcessor> transformed = targetsAndSections
                .mapToPair(new PairFunction<Tuple2<Integer, Tuple2<FloatProcessor, HashMap<Integer, FloatProcessor>>>, Integer, FloatProcessor>() {
                    @Override
                    public Tuple2<Integer, FloatProcessor> call(Tuple2<Integer, Tuple2<FloatProcessor, HashMap<Integer, FloatProcessor>>> t) throws Exception {
                        Integer index = t._1();
                        FloatProcessor transformFP = t._2()._1();
                        HashMap<Integer, FloatProcessor> m = t._2()._2();
                        ArrayImg<FloatType, FloatArray> transformImg = ArrayImgs.floats((float[])transformFP.getPixels(), transformFP.getWidth(), transformFP.getHeight());
                        RealRandomAccessible<FloatType> extendedAndInterpolatedTransform
                                = Views.interpolate(Views.extendBorder(transformImg), new NLinearInterpolatorFactory<FloatType>());
                        RealTransformRandomAccessible<FloatType, InverseRealTransform> scaledTransformImg
                                = RealViews.transform(extendedAndInterpolatedTransform, new ScaleAndTranslation(stepsDouble, radiiDouble));
                        RealRandomAccess<FloatType> transformRA = scaledTransformImg.realRandomAccess();
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

                            FloatProcessor lower = m.get(lowerPos);
                            if ( lower != null )
                            {
                                weightSum += lowerWeight;
                                val += lower.getf( x, y )*lowerWeight;
                            }

                            FloatProcessor upper = m.get(upperPos);
                            if( upper != null )
                            {
                                weightSum += upperWeight;
                                val += upper.getf( x, y )*upperWeight;
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
                        return Utility.tuple2( index, new FloatProcessor( width, height, targetPixels ) );
                    }
                });

        return transformed;


    }

}
