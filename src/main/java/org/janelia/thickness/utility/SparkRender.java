/**/package org.janelia.thickness.utility;

import java.io.IOException;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.HashMap;
import java.util.HashSet;
import java.util.Iterator;
import java.util.List;

import org.apache.spark.SparkConf;
import org.apache.spark.api.java.JavaPairRDD;
import org.apache.spark.api.java.JavaSparkContext;
import org.apache.spark.api.java.function.Function2;
import org.apache.spark.api.java.function.PairFlatMapFunction;
import org.apache.spark.api.java.function.PairFunction;
import org.janelia.thickness.ScaleOptions;
import org.janelia.thickness.utility.Utility;
import org.kohsuke.args4j.Argument;
import org.kohsuke.args4j.CmdLineException;
import org.kohsuke.args4j.CmdLineParser;
import org.kohsuke.args4j.Option;

import ij.ImagePlus;
import ij.process.FloatProcessor;
import loci.formats.FormatException;
import net.imglib2.Cursor;
import net.imglib2.FinalInterval;
import net.imglib2.RealRandomAccess;
import net.imglib2.RealRandomAccessible;
import net.imglib2.img.array.ArrayCursor;
import net.imglib2.img.array.ArrayImg;
import net.imglib2.img.array.ArrayImgs;
import net.imglib2.img.basictypeaccess.array.FloatArray;
import net.imglib2.interpolation.randomaccess.NearestNeighborInterpolatorFactory;
import net.imglib2.realtransform.InverseRealTransform;
import net.imglib2.realtransform.RealTransformRandomAccessible;
import net.imglib2.realtransform.RealViews;
import net.imglib2.realtransform.Scale2D;
import net.imglib2.realtransform.ScaleAndTranslation;
import net.imglib2.type.numeric.real.FloatType;
import net.imglib2.view.Views;
import scala.Tuple2;

/**
 * @author Philipp Hanslovsky &lt;hanslovskyp@janelia.hhmi.org&gt;
 */
public class SparkRender {
	
	public static class Options {
		
		@Argument( metaVar = "CONFIG_PATH", index = 0 )
		private String configPath;
		
		@Argument( metaVar = "ITERATION", index = 1 )
		private Integer iteration;
		
		@Argument( metaVar = "TARGET_PATTERN", index = 2 )
		private String targetPattern;
		
		@Option( name="--pattern", aliases = {"-p"}, metaVar="SOURCE_PATTERN", required=false, depends={ "--scale"}  )
		private String sourcePattern;
		
		@Option( name="--scale", aliases = {"-s"}, metaVar="SCALE" )
		private Double scale;
		
		private boolean parsedSuccessfully = false;
		
	}

    public static void main(String[] args) throws IOException, FormatException {
    	
    	Options o = new Options();
    	CmdLineParser parser = new CmdLineParser( o );
    	args = new String[] {
    			"123", "4", "-s", "0.1", "343"
    	};
    	System.out.println( Arrays.toString( args ) );
    	try {
			parser.parseArgument(args);
			o.scale = o.sourcePattern == null ? 1.0 : o.scale;
			o.parsedSuccessfully = true;
		} catch (CmdLineException e) {
            // handling of wrong arguments
			System.err.println(e.getMessage());
			parser.printUsage(System.err);
			o.parsedSuccessfully = false;
		}
    	System.out.println( o.configPath );
    	System.out.println( o.iteration );
    	System.out.println( o.targetPattern );
    	System.out.println( o.sourcePattern );
    	System.out.println( o.scale );
    	System.out.println( o.parsedSuccessfully );
    	System.exit( 512 );

        SparkConf sparkConf = new SparkConf().setAppName("Render");
        JavaSparkContext sc = new JavaSparkContext(sparkConf);
        if ( o.parsedSuccessfully )
        {
	        final String configPath = o.configPath;
	        final int iteration = o.iteration;
	        final String outputFormat = o.targetPattern;
	        final ScaleOptions options = ScaleOptions.createFromFile(configPath);
	        final int[] radii = options.radii[iteration];
	        final int[] steps = options.steps[iteration];
	        final double scale = Math.pow( 2, options.scale ) * o.scale;
	        final int start = options.start;
	        final int stop = options.stop;
	        final String sourceFormat = options.source;
	        final String transformFormat = options.target + String.format( "/%02d/backward/", iteration ) + "%04d.tif";
	
	        ImagePlus img0 = new ImagePlus(String.format(sourceFormat, start));
	
	        final int width =  img0.getWidth();
	        final int height = img0.getHeight();
	        final int size = stop - start;
	
	        final long[] dim = new long[]{width, height, size};
	
	
	
	        final double[] radiiDouble = new double[]{radii[0], radii[1]};
	        final double[] stepsDouble = new double[]{steps[0], steps[1]};
	
	        ArrayList<Integer> indices = Utility.arange(start, stop);
	        JavaPairRDD<Integer, FloatProcessor> sections = sc
	                .parallelize(indices)
	                .mapToPair( new Utility.Duplicate<>() )
	                .sortByKey()
	                .map( new Utility.DropValue<>() )
	                .mapToPair(new Utility.LoadFileFromPattern(sourceFormat) )
	                .cache();
	
	        JavaPairRDD<Integer, FloatProcessor> transforms = sc
	                .parallelize(indices)
	                .mapToPair( new Utility.Duplicate<>() )
	                .sortByKey()
	                .map( new Utility.DropValue<>() )
	                .mapToPair(new Utility.LoadFileFromPattern( transformFormat ) )
	                .cache();
	
	        JavaPairRDD<Integer, FloatProcessor> transformed = render(sc, sections, transforms, stepsDouble, radiiDouble, dim, scale, start);
	
	        write( sc, transformed, outputFormat, size, Utility.ConvertImageProcessor.getType( img0.getProcessor() ) );
        }

    }

    public static void write(
            JavaSparkContext sc,
            JavaPairRDD<Integer, FloatProcessor> transformed,
            String outputFormat,
            int size,
            Utility.ConvertImageProcessor.TYPE type )
    {
        List<Tuple2<Integer, Boolean>> successOnWrite = transformed
                .mapToPair(new Utility.WriteToFormatString<Integer>(outputFormat, type))
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
            final long[] dim,
            final double scale,
            final int offset )
    {

        final int width = (int) dim[0];
        final int height = (int) dim[1];

        JavaPairRDD<Integer, Tuple2<FloatProcessor, Integer>> transformsToRequiredSectionsMapping = transforms
        		.mapToPair( new Utility.AddOffsetToKey<>( -offset ) )
                .mapToPair(new FindRequiredSectionIndices(stepsDouble, radiiDouble, scale, width, height) )
                .flatMapToPair(new FlattenRequiredSectionIndices() )
                        // ( target-index -> ... ==> source-index -> ... )
                .mapToPair(new SwapIndex<>() )
                ;

        JavaPairRDD<Integer, Tuple2<FloatProcessor, HashMap<Integer, FloatProcessor>>> targetsAndSections = sections
                .join(transformsToRequiredSectionsMapping)
                .mapToPair(new MapSectionToTarget<>() )
                .reduceByKey(new ReduceMap<>() )
                .cache()
                ;

        JavaPairRDD<Integer, FloatProcessor> transformed = targetsAndSections
                .mapToPair(new InterpolateInZ( stepsDouble, radiiDouble, scale, dim, width, height ) )
                .mapToPair( new Utility.AddOffsetToKey<>( offset ) )
                ;

        return transformed;


    }
    
    
    public static class FindRequiredSectionIndices implements 
    PairFunction<Tuple2<Integer, FloatProcessor>, Integer, Tuple2<FloatProcessor, HashSet<Integer>>> {
    	
    	/**
		 * 
		 */
		private static final long serialVersionUID = 8632136393182244948L;
		
		private final double[] stepsDouble;
    	private final double[] radiiDouble;
    	private final double scale;
    	private final int width;
    	private final int height;
    	
        public FindRequiredSectionIndices(double[] stepsDouble, double[] radiiDouble, double scale, int width, int height) {
			super();
			this.stepsDouble = stepsDouble;
			this.radiiDouble = radiiDouble;
			this.scale = scale;
			this.width = width;
			this.height = height;
		}

		@Override
        public Tuple2<Integer, Tuple2<FloatProcessor, HashSet<Integer>>> call(Tuple2<Integer, FloatProcessor> t) throws Exception {
            HashSet<Integer> s = new HashSet<Integer>();
            // Need to generate high-res images because some require sections might be missed otherwise.
            FloatProcessor transformFP = t._2();
            ArrayImg<FloatType, FloatArray> transformImg
                    = ArrayImgs.floats((float[])transformFP.getPixels(), transformFP.getWidth(), transformFP.getHeight() );
            RealRandomAccessible<FloatType> extendedAndInterpolatedTransform
                    = Views.interpolate(Views.extendBorder(transformImg), new NearestNeighborInterpolatorFactory<FloatType>());
            RealTransformRandomAccessible<FloatType, InverseRealTransform> scaledTransformImg
                    = RealViews.transform(extendedAndInterpolatedTransform, new ScaleAndTranslation(stepsDouble, radiiDouble));
            RealTransformRandomAccessible<FloatType, InverseRealTransform> scaledToOriginalResolutionImg
                    = RealViews.transform(scaledTransformImg, new Scale2D(scale, scale) );
            Cursor<FloatType> c =
                    Views.flatIterable(Views.interval(Views.raster(scaledToOriginalResolutionImg), new FinalInterval(width, height))).cursor();
//            for (float p : t._2().pixels) {
            while( c.hasNext() ) {
                float p = c.next().get();
                if (!Float.isNaN(p) && !Float.isInfinite(p)) {
                    s.add((int) Math.floor(p));
                    s.add((int) Math.ceil(p));
                }
            }
            return Utility.tuple2(t._1(), Utility.tuple2(t._2(), s));
        }
    }
    
    
    public static class FlattenRequiredSectionIndices implements
    PairFlatMapFunction<Tuple2<Integer, Tuple2<FloatProcessor, HashSet<Integer>>>, Integer, Tuple2<FloatProcessor, Integer>> {
        /**
		 * 
		 */
		private static final long serialVersionUID = -1019623242143839023L;

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
    }
    
    
    public static class SwapIndex< K1, K2 > implements PairFunction<Tuple2<K1, Tuple2<FloatProcessor, K2>>, K2, Tuple2<FloatProcessor, K1>> {
        /**
		 * 
		 */
		private static final long serialVersionUID = -350190126402273055L;

		@Override
        public Tuple2<K2, Tuple2<FloatProcessor, K1>> call(Tuple2<K1, Tuple2<FloatProcessor, K2>> t) throws Exception {
            return Utility.tuple2(t._2()._2(), Utility.tuple2(t._2()._1(), t._1()));
        }
    }
    
    
    public static class MapSectionToTarget<SECTION_IDX, TARGET_IDX, SECTION, TARGET> implements
    PairFunction<Tuple2<SECTION_IDX, Tuple2<SECTION, Tuple2<TARGET, TARGET_IDX>>>, TARGET_IDX, Tuple2<TARGET, HashMap<SECTION_IDX, SECTION>>> {
        /**
		 * 
		 */
		private static final long serialVersionUID = -1239436958339099544L;

		@Override
        public Tuple2<TARGET_IDX, Tuple2<TARGET, HashMap<SECTION_IDX, SECTION>>>
        call(Tuple2<SECTION_IDX, Tuple2<SECTION, Tuple2<TARGET, TARGET_IDX>>> t) throws Exception {
            SECTION_IDX sectionIndex = t._1();
            TARGET_IDX targetIndex = t._2()._2()._2();
            TARGET transform = t._2()._2()._1();
            SECTION section = t._2()._1();
            HashMap<SECTION_IDX, SECTION> m = new HashMap<>();
            m.put(sectionIndex, section);
            return Utility.tuple2(targetIndex, Utility.tuple2(transform, m));
        }
    }
    
    
    public static class ReduceMap<K, V1, V2> implements 
    Function2<Tuple2<V1, HashMap<K, V2>>, Tuple2<V1, HashMap<K, V2>>, Tuple2<V1, HashMap<K, V2>>> {
        /**
		 * 
		 */
		private static final long serialVersionUID = -4858382224061550724L;

		@Override
        public Tuple2<V1, HashMap<K, V2>>
        call(Tuple2<V1, HashMap<K, V2>> t1, Tuple2<V1, HashMap<K, V2>> t2) throws Exception {
            HashMap<K, V2> m = t1._2();
            m.putAll(t2._2());
            return Utility.tuple2(t1._1(), m);
        }
    }
    
    public static class InterpolateInZ implements
    PairFunction<Tuple2<Integer, Tuple2<FloatProcessor, HashMap<Integer, FloatProcessor>>>, Integer, FloatProcessor> {
    	
    	/**
		 * 
		 */
		private static final long serialVersionUID = -166106204082008538L;
		
		private final double[] stepsDouble;
    	private final double[] radiiDouble;
    	private final double scale;
    	private final long[] dim;
    	private final int width;
    	private final int height;
    	
        public InterpolateInZ(double[] stepsDouble, double[] radiiDouble, double scale, long[] dim, int width, int height) {
			super();
			this.stepsDouble = stepsDouble;
			this.radiiDouble = radiiDouble;
			this.scale = scale;
			this.dim = dim;
			this.width = width;
			this.height = height;
		}

		@Override
        public Tuple2<Integer, FloatProcessor> call(Tuple2<Integer, Tuple2<FloatProcessor, HashMap<Integer, FloatProcessor>>> t) throws Exception {
            Integer index = t._1();
            FloatProcessor transformFP = t._2()._1();
            HashMap<Integer, FloatProcessor> m = t._2()._2();
            ArrayImg<FloatType, FloatArray> transformImg = ArrayImgs.floats((float[])transformFP.getPixels(), transformFP.getWidth(), transformFP.getHeight());
            RealRandomAccessible<FloatType> extendedAndInterpolatedTransform
                    = Views.interpolate(Views.extendBorder(transformImg), new NearestNeighborInterpolatorFactory<FloatType>());
            RealTransformRandomAccessible<FloatType, InverseRealTransform> scaledTransformImg
                    = RealViews.transform(extendedAndInterpolatedTransform, new ScaleAndTranslation(stepsDouble, radiiDouble));
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
                fv.setReal( val / weightSum );
            }
            return Utility.tuple2( index, new FloatProcessor( width, height, targetPixels ) );
        }
    }
    

}
