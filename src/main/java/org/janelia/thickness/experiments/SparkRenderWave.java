package org.janelia.thickness.experiments;

import ij.ImagePlus;
import ij.process.FloatProcessor;
import loci.formats.FileStitcher;
import loci.formats.FormatException;
import org.apache.spark.SparkConf;
import org.apache.spark.api.java.JavaPairRDD;
import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.api.java.JavaSparkContext;
import org.apache.spark.api.java.function.Function;
import org.apache.spark.api.java.function.PairFunction;
import org.janelia.thickness.utility.Utility;
import scala.Tuple2;

import java.io.IOException;
import java.util.List;
import java.util.regex.Pattern;

/**
 * Created by hanslovskyp on 10/23/15.
 */
public class SparkRenderWave {

    public static void main(String[] args) throws IOException, FormatException {

        SparkConf sparkConf = new SparkConf().setAppName("Render");
        JavaSparkContext sc = new JavaSparkContext(sparkConf);

//        final String sourceFormat = args[0];
//        final String transformFormat = args[1];
        final String sourceFormat = args[0];
        final String transformFormat = args[1];
        final String outputFormat = args[2];
        int radius = Integer.parseInt(args[3]);
        int step = Integer.parseInt(args[4]);
        int start = Integer.parseInt(args[5]);
        int stop = Integer.parseInt(args[6]);


//        final String outputFormat = args[4];

        ImagePlus img0 = new ImagePlus(String.format(sourceFormat, start));

        final int width = img0.getWidth();
        final int height = img0.getHeight();
        final int size = stop - start; // 201;//reader.getSizeZ();

        System.out.println( sourceFormat );
        System.out.println( transformFormat );
        System.out.println( outputFormat );
        System.out.println( String.format( "width=%d, height=%d, size=%d", width, height, size ) );

        final long[] dim = new long[]{width, height, size};

        ImagePlus firstTransform = new ImagePlus(String.format(transformFormat, 0) );
        final int transformWidth = firstTransform.getWidth();
        final int transformHeight = firstTransform.getHeight();
        System.out.println( String.format( "transform width=%d, transform height=%d", transformWidth, transformHeight ) );






        final double[] radiiDouble = new double[]{ radius, radius };
        final double[] stepsDouble = new double[]{ step, step };

        JavaRDD<Integer> indices = sc.parallelize(Utility.arange(size));
        JavaPairRDD<Integer, FloatProcessor> sections = indices
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
        System.out.println( "sections: " + sections.count() );

        JavaPairRDD<Integer, FloatProcessor> transforms = indices
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

        System.out.println( "transforms: " + transforms.count() );

        JavaPairRDD<Integer, FloatProcessor> transformed = SparkRender.render(sc, sections, transforms, stepsDouble, radiiDouble, dim, 1.0 /* fix this */ ).cache();
        System.out.println( "transformed: " + transformed.count() );


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

}
