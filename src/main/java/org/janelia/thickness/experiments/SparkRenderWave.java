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
import org.janelia.thickness.utility.FPTuple;
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
        final String configPath = args[3];
        int radius = Integer.parseInt(args[3]);
        int step = Integer.parseInt(args[4]);


//        final String outputFormat = args[4];

        boolean isPattern = Pattern.compile(".*<\\d+-\\d+>.*").matcher(sourceFormat).matches(); // fileName.indexOf( "%" ) > -1;
        FileStitcher reader = new FileStitcher(isPattern);
        reader.setId(sourceFormat);

        final int width = reader.getSizeX();
        final int height = reader.getSizeY();
        final int size = reader.getSizeZ(); // 201;//reader.getSizeZ();

        final long[] dim = new long[]{width, height, size};

        reader.close();

        ImagePlus firstTransform = new ImagePlus(String.format(transformFormat, 0) );
        final int transformWidth = firstTransform.getWidth();
        final int transformHeight = firstTransform.getHeight();






        final double[] radiiDouble = new double[]{ radius, radius };
        final double[] stepsDouble = new double[]{ step, step };

        JavaRDD<Integer> indices = sc.parallelize(Utility.arange(size));
        JavaPairRDD<Integer, FPTuple> sections = indices
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
                .mapToPair(new Utility.StackOpener(sourceFormat, isPattern))
                .cache();
        sections.count();

        JavaPairRDD<Integer, FPTuple> transforms = indices
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

        transforms.count();

        JavaPairRDD<Integer, FPTuple> transformed = SparkRender.render(sc, sections, transforms, stepsDouble, radiiDouble, dim, 0 /* fix this */ ).cache();
        transformed.count();


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
