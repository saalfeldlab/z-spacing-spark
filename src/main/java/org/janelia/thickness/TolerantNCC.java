package org.janelia.thickness;

import ij.ImageJ;
import ij.ImagePlus;
import ij.io.FileSaver;
import ij.process.ByteProcessor;
import ij.process.FloatProcessor;
import mpicbg.ij.integral.BlockPMCC;
import mpicbg.ij.integral.DoubleIntegralImage;
import mpicbg.ij.integral.MaskedBlockPMCC;
import mpicbg.ij.integral.WeightedBlockPMCC;
import org.apache.commons.lang3.StringUtils;
import org.apache.spark.SparkConf;
import org.apache.spark.api.java.JavaPairRDD;
import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.api.java.JavaSparkContext;
import org.apache.spark.api.java.function.Function;
import org.apache.spark.api.java.function.PairFlatMapFunction;
import org.apache.spark.api.java.function.PairFunction;
import org.janelia.similarities.NCC;
import org.janelia.utility.io.IO;
import scala.Tuple2;

import java.util.*;

/**
 * Created by hanslovskyp on 11/19/15.
 */
public class TolerantNCC {

    private final JavaPairRDD<Tuple2< Integer, Integer >, Tuple2< FPTuple, FPTuple >> overcompleteSections;

    public TolerantNCC(JavaPairRDD<Tuple2<Integer, Integer>, Tuple2<FPTuple, FPTuple>> overcompleteSections) {
        this.overcompleteSections = overcompleteSections;
    }

    public void ensurePersistence()
    {
        overcompleteSections.cache();
        overcompleteSections.count();
    }


//    public static void main(String[] args) {
//
//        SparkConf conf = new SparkConf().setAppName("TolerantNCC");
//        JavaSparkContext sc = new JavaSparkContext(conf);
//
//    }

    public JavaPairRDD<Tuple2<Integer, Integer>, FPTuple> calculate(
            JavaSparkContext sc,
            final int[] blockRadius,
            final int[] stepSize,
            final int[] correlationBlockRadius,
            final int[] maxOffset,
            final int size,
            final int range
    )
    {

        JavaPairRDD<Tuple2<Integer, Integer>, Tuple2<FPTuple, FPTuple>> sections = overcompleteSections
                .filter(new MatrixGenerationFromImagePairs.SelectInRange<Tuple2<FPTuple, FPTuple>>(range))
                ;

        System.out.println( "sections: " + sections.count() );

        JavaPairRDD<Tuple2<Integer, Integer>, Tuple2<FPTuple, FPTuple>> maxProjections = sections
                .mapToPair(new FPToSimilarities<Tuple2<Integer, Integer>>(
                        maxOffset,
                        correlationBlockRadius
                ))
                .cache();

        System.out.println( "maxOffset=" + Arrays.toString( maxOffset ) );
        System.out.println( "correlationBlockRadius=" + Arrays.toString( correlationBlockRadius ) );

//        List<Tuple2<Tuple2<Integer, Integer>, FPTuple>> mps = maxProjections.take( 100 );
//        for( Tuple2<Tuple2<Integer, Integer>, FPTuple> m : mps )
//        {
//            String path = "/groups/saalfeld/home/hanslovskyp/local/tmp/maxprojections/" + m._1() + ".tif";
//            IO.createDirectoryForFile( path );
//            new FileSaver( new ImagePlus( "", m._2().rebuild() ) ).saveAsTiff( path );
//        }

        System.out.println( "maxProjections: " + maxProjections.count() );

        JavaPairRDD<Tuple2<Integer, Integer>, HashMap<Tuple2<Integer, Integer>, Double>> averages = maxProjections
                .mapToPair(new AverageBlocks<Tuple2<Integer, Integer>>(blockRadius, stepSize))
                .cache()
                ;

        System.out.println( "averages: " + averages.count() );

        JavaPairRDD<Tuple2<Integer, Integer>, Tuple2<Tuple2<Integer, Integer>, Double>> flatAverages = averages
                .flatMapToPair(
                        new Utility.FlatmapMap<Tuple2<Integer, Integer>, Tuple2<Integer, Integer>, Double, HashMap<Tuple2<Integer, Integer>, Double>>()
                )
                .cache();

        System.out.println( "flatAverages: " + flatAverages.count() );

//        JavaPairRDD<Tuple2<Integer, Integer>, Tuple2<Tuple2<Integer, Integer>, Double>> averagesIndexedBySectionIndexPairs = flatAverages
//                .mapToPair(
//                        new Utility.EntryToTuple<Tuple2<Integer, Integer>, Tuple2<Integer, Integer>, Double, Map.Entry<Tuple2<Integer, Integer>, Double>>()
//                )
//                .cache();
//
//        averagesIndexedBySectionIndexPairs.count();

        JavaPairRDD<Tuple2<Integer, Integer>, HashMap<Tuple2<Integer, Integer>, Double>> averagesIndexedByXYTuples = flatAverages
                .mapToPair(new Utility.Swap<Tuple2<Integer, Integer>, Tuple2<Integer, Integer>, Double>())
                .mapToPair( new Utility.ValueAsMap<Tuple2<Integer,Integer>,Tuple2<Integer,Integer>,Double>())
                .cache()
                ;

        averagesIndexedByXYTuples.count();

        JavaPairRDD<Tuple2<Integer, Integer>, FPTuple> matrices = averagesIndexedByXYTuples
                .reduceByKey(new Utility.ReduceMapsByUnion<Tuple2<Integer, Integer>, Double, HashMap<Tuple2<Integer, Integer>, Double>>())
                .mapToPair(new MatrixGenerationFromImagePairs.MapToFPTuple(size))
                .cache()
                ;

        matrices.count();

        return matrices;


//        JavaPairRDD<Tuple2<Integer, Integer>, FPTuple> pairWiseSimilarities = sections
//                .mapToPair(new FPToSimilarities<Tuple2<Integer, Integer>>(maxOffset, blockRadius, step));

//        JavaPairRDD<Tuple2<Integer, Integer>, Tuple2<Tuple2<Integer, Integer>, Double>> flatSimilarities = pairWiseSimilarities
//                .flatMapToPair(new PairFlatMapFunction<Tuple2<Tuple2<Integer, Integer>, HashMap<Tuple2<Integer, Integer>, Double>>, Tuple2<Integer, Integer>, Tuple2<Tuple2<Integer, Integer>, Double>>() {
//                    @Override
//                    public Iterable<Tuple2<Tuple2<Integer, Integer>, Tuple2<Tuple2<Integer, Integer>, Double>>>
//                    call(final Tuple2<Tuple2<Integer, Integer>, HashMap<Tuple2<Integer, Integer>, Double>> t) throws Exception {
//                        final Tuple2<Integer, Integer> key = t._1();
//                        final Set<Map.Entry<Tuple2<Integer, Integer>, Double>> es = t._2().entrySet();
//                        final Iterator<Map.Entry<Tuple2<Integer, Integer>, Double>> it = es.iterator();
//                        return new Iterable<Tuple2<Tuple2<Integer, Integer>, Tuple2<Tuple2<Integer, Integer>, Double>>>() {
//                            @Override
//                            public Iterator<Tuple2<Tuple2<Integer, Integer>, Tuple2<Tuple2<Integer, Integer>, Double>>> iterator() {
//                                return new Iterator<Tuple2<Tuple2<Integer, Integer>, Tuple2<Tuple2<Integer, Integer>, Double>>>() {
//                                    @Override
//                                    public boolean hasNext() {
//                                        return it.hasNext();
//                                    }
//
//                                    @Override
//                                    public Tuple2<Tuple2<Integer, Integer>, Tuple2<Tuple2<Integer, Integer>, Double>> next() {
//                                        Map.Entry<Tuple2<Integer, Integer>, Double> entry = it.next();
//                                        return Utility.tuple2(key, Utility.tuple2(entry.getKey(), entry.getValue()));
//                                    }
//
//                                    @Override
//                                    public void remove() {
//
//                                    }
//                                };
//                            }
//                        };
//                    }
//                });
//        JavaPairRDD<Tuple2<Integer, Integer>, HashMap<Tuple2<Integer, Integer>, Double>> similaritiesWithXYKeys = flatSimilarities
//                .mapToPair(new PairFunction<Tuple2<Tuple2<Integer, Integer>, Tuple2<Tuple2<Integer, Integer>, Double>>, Tuple2<Integer, Integer>, HashMap<Tuple2<Integer, Integer>, Double>>() {
//                    @Override
//                    public Tuple2<Tuple2<Integer, Integer>, HashMap<Tuple2<Integer, Integer>, Double>>
//                    call(Tuple2<Tuple2<Integer, Integer>, Tuple2<Tuple2<Integer, Integer>, Double>> t) throws Exception {
//                        Tuple2<Integer, Integer> pairwiseZKey = t._1();
//                        Tuple2<Tuple2<Integer, Integer>, Double> entry = t._2();
//                        HashMap<Tuple2<Integer, Integer>, Double> hm = new HashMap<Tuple2<Integer, Integer>, Double>();
//                        hm.put(pairwiseZKey, entry._2());
//                        return Utility.tuple2(entry._1(), hm);
//                    }
//                });
//        JavaPairRDD<Tuple2<Integer, Integer>, HashMap<Tuple2<Integer, Integer>, Double>> similarities = similaritiesWithXYKeys
//                .reduceByKey(new Function2<HashMap<Tuple2<Integer, Integer>, Double>, HashMap<Tuple2<Integer, Integer>, Double>, HashMap<Tuple2<Integer, Integer>, Double>>() {
//                    @Override
//                    public HashMap<Tuple2<Integer, Integer>, Double> call(HashMap<Tuple2<Integer, Integer>, Double> hm1, HashMap<Tuple2<Integer, Integer>, Double> hm2) throws Exception {
//                        HashMap<Tuple2<Integer, Integer>, Double> result = new HashMap<Tuple2<Integer, Integer>, Double>();
//                        result.putAll(hm1);
//                        result.putAll(hm2);
//                        return result;
//                    }
//                });
//        JavaPairRDD<Tuple2<Integer, Integer>, FPTuple> result = similarities
//                .mapToPair(new PairFunction<Tuple2<Tuple2<Integer, Integer>, HashMap<Tuple2<Integer, Integer>, Double>>, Tuple2<Integer, Integer>, FPTuple>() {
//                    @Override
//                    public Tuple2<Tuple2<Integer, Integer>, FPTuple> call(Tuple2<Tuple2<Integer, Integer>, HashMap<Tuple2<Integer, Integer>, Double>> t) throws Exception {
//                        FloatProcessor result = new FloatProcessor( size, size );
//                        result.add( Double.NaN );
//                        for ( int z = 0; z < size; ++z )
//                            result.setf( z, z, 1.0f );
//                        for( Map.Entry<Tuple2<Integer, Integer>, Double> e : t._2().entrySet() )
//                        {
//                            Tuple2<Integer, Integer> xy = e.getKey();
//                            float val = e.getValue().floatValue();
//                            int x = xy._1();
//                            int y = xy._2();
//                            result.setf( x, y, val );
//                            result.setf( y, x, val );
//                        }
//
//                        return Utility.tuple2( t._1(), new FPTuple( result ) );
//                    }
//                });
    }

    public static class FPToSimilarities<K>
    implements PairFunction<Tuple2<K,Tuple2<FPTuple,FPTuple>>, K, Tuple2<FPTuple,FPTuple>> {

        private final int[] maxOffsets;
        private final int[] blockRadius;

        public FPToSimilarities(int[] maxOffsets, int[] blockRadius) {
            this.maxOffsets = maxOffsets;
            this.blockRadius = blockRadius;
        }

        @Override
        public Tuple2<K, Tuple2<FPTuple,FPTuple>> call(Tuple2<K, Tuple2<FPTuple, FPTuple>> t) throws Exception {
            FloatProcessor fixed = t._2()._1().rebuild();
            FloatProcessor moving = t._2()._2().rebuild();

            Tuple2<FloatProcessor, FloatProcessor> ccs = tolerantNCC(fixed, moving, maxOffsets, blockRadius);

            // remove
//
//            FloatProcessor weights = ccs._2();
//            if( t._1() instanceof  Tuple2 ) {
//                Object k1 = ((Tuple2) t._1())._1();
//                Object k2 = ((Tuple2) t._1())._2();
//                if( k1 instanceof Integer && k2 instanceof Integer &&
//                        ( ((Integer) k2).intValue() == ((Integer) k1).intValue()+1 ||
//                        ((Integer)k1).intValue() == ((Integer) k2).intValue() + 1 ) ) {
//                    String path = "/groups/saalfeld/home/hanslovskyp/local/tmp/weights/" +
//                            ((Integer) k1).intValue() + ".tif";
//                    IO.createDirectoryForFile(path);
//                    new FileSaver(new ImagePlus("", weights)).saveAsTiff(path);
//                }
//            }
            // end remove
            return Utility.tuple2( t._1(), Utility.tuple2( new FPTuple( ccs._1()), new FPTuple( ccs._2()) ) );
        }
    }

    public static class AverageBlocks<K>
    implements PairFunction<Tuple2<K,Tuple2<FPTuple,FPTuple>>,K,HashMap<Tuple2<Integer,Integer>,Double>>
    {

        private final int[] blockRadius;
        private final int[] stepSize;

        public AverageBlocks(int[] blockRadius, int[] stepSize) {
            this.blockRadius = blockRadius;
            this.stepSize = stepSize;
        }

        @Override
        public Tuple2<K, HashMap<Tuple2<Integer, Integer>, Double>> call(Tuple2<K, Tuple2<FPTuple,FPTuple>> t) throws Exception {
            return Utility.tuple2( t._1(), average( t._2()._1().rebuild(), t._2()._2().rebuild(), blockRadius, stepSize ) );
        }
    }

    public static FloatProcessor generateMask( FloatProcessor img, HashSet< Float > values )
    {
        FloatProcessor mask = new FloatProcessor(img.getWidth(), img.getHeight());
        float[] i = (float[]) img.getPixels();
        float[] m = (float[]) mask.getPixels();
        for( int k = 0; k < i.length; ++k )
            m[k] = values.contains( i[k] ) ? 0.0f : 1.0f;
        return mask;
    }

    public static Tuple2< FloatProcessor, FloatProcessor > tolerantNCC(
            FloatProcessor fixed,
            FloatProcessor moving,
            final int[] maxOffsets,
            final int[] blockRadiusInput
    )
    {
        int width = moving.getWidth();
        int height = moving.getHeight();

        int[] blockRadius = new int[]{
                Math.min( blockRadiusInput[0], width / 2 ),
                Math.min( blockRadiusInput[1], height / 2 )
        };


        // pad image?
//        int paddedWidth = width + 2*maxOffsets[0];
//        int paddedHeight = height + 2*maxOffsets[1];
//
//        FloatProcessor fixedPadded = new FloatProcessor( paddedWidth, paddedHeight );
//        Random rng = new Random();
//
//        int innerBoundaryLowerX = maxOffsets[0];
//        int innerBoundaryLowerY = maxOffsets[1];
//
//        int innerBoundaryUpperX = innerBoundaryLowerX + width;
//        int innerBoundaryUpperY = innerBoundaryLowerY + height;
//
//        for ( int y = 0; y < paddedHeight; ++y )
//        {
//            for ( int x = 0; x < paddedWidth; ++x )
//            {
//                if( x < innerBoundaryLowerX || x >= innerBoundaryUpperX || y < innerBoundaryLowerY || y >= innerBoundaryUpperY )
//                    fixedPadded.setf( x, y, rng.nextFloat() );
//                else
//                    fixedPadded.setf( x, y, fixed.getf( x-innerBoundaryLowerX, y-innerBoundaryLowerY ) );
//            }
//        }


//        BlockPMCC pmcc = new BlockPMCC( fixed, moving );
        HashSet<Float> maskedValues = new HashSet<Float>();
        maskedValues.add( 0.0f );
        WeightedBlockPMCC pmcc = new WeightedBlockPMCC(fixed, moving, generateMask( fixed, maskedValues ), generateMask( moving, maskedValues) );
        FloatProcessor tp = pmcc.getTargetProcessor();
//            pmcc.rSignedSquare( radius[0], radius[1] );

        FloatProcessor maxCorrelations = new FloatProcessor(width, height);

        final int xStart = -1 * maxOffsets[0];
        final int yStart = -1 * maxOffsets[1];

        final int xStop = 1 * maxOffsets[0]; // inclusive
        final int yStop = 1 * maxOffsets[1]; // inclusive

        FloatProcessor weights = new FloatProcessor( width, height );
//        weights.add( 1.0 );

//        DoubleIntegralImage sumsXX = pmcc.getSumsXX();
//        DoubleIntegralImage sumsYY = pmcc.getSumsYY();
//        DoubleIntegralImage sumsX  = pmcc.getSumsX();
//        DoubleIntegralImage sumsY  = pmcc.getSumsY();

        for( int yOff = yStart; yOff <= yStop; ++yOff )
        {
            for ( int xOff = xStart; xOff <= xStop; ++ xOff )
            {
                pmcc.setOffset( xOff, yOff );
                pmcc.r( blockRadius[0], blockRadius[1] );

                for ( int y = 0; y < height; ++y )
                {
                    for ( int x = 0; x < width; ++x )
                    {
                        // if full correlation block is not contained within image, ignore it!
                        if(
                                x + xOff - blockRadius[0] < -1 || y + yOff - blockRadius[1] < -1 ||
                                        x + xOff + blockRadius[0] > width || y + yOff + blockRadius[1] > height )
                            continue;

                        // if full correlation block is not contained within moving image, ignore it!
                        if(
                                x - blockRadius[0] < -1 || y - blockRadius[1] < -1 ||
                                        x + blockRadius[0] > width || y + blockRadius[1] > height )
                            continue;

                        float val = tp.getf(x, y);
                        if ( val > maxCorrelations.getf( x, y ) )
                            maxCorrelations.setf( x, y, val );
                    }
                }

            }
        }

        double varSum = 0.0;
        FloatProcessor variances = new FloatProcessor(width, height);
        int varianceRadius = 40;

        for ( int y = 0; y < height; ++y ) {
            final int yMin = Math.max(-1, y - varianceRadius - 1);
            final int yMax = Math.min(height - 1, y + varianceRadius);
            for (int x = 0; x < width; ++x) {
                final int xMin = Math.max(-1, x - varianceRadius - 1);
                final int xMax = Math.min(width - 1, x + varianceRadius);
                int n = (xMax - xMin) * (yMax - yMin);

                final double sumX = pmcc.getSumX(xMin, yMin, xMax, yMax);
//                final double sumY  = sumsY.getDoubleSum( xMin, yMin, xMax, yMax );
                final double sumXX = pmcc.getSumXX(xMin, yMin, xMax, yMax);
                double variance = (sumXX - sumX * sumX / n) / n;
                variances.setf( x,y, (float) variance );
                varSum += variance;
            }
        }

        double varMean = varSum / ( width * height );
        float[] varPixels = (float[]) variances.getPixels();
        ByteProcessor binaryMask = new ByteProcessor(width, height);
        byte[] maskPixels = (byte[]) binaryMask.getPixels();
        for ( int i = 0; i < varPixels.length; ++i )
            maskPixels[i] = varPixels[i] > varMean ? (byte) 0 : (byte) 1;

        // imagej api ImageProcessor.dilate():
        // Dilates the image or ROI using a 3x3 minimum filter. Requires 8-bit or RGB image.
        // That sounds like erosion rather than dilation?
        binaryMask.dilate();
        binaryMask.dilate();





        for ( int y = 0; y < height; ++y )
        {
            final int yMin = Math.max( -1, y - blockRadius[1] - 1 );
            final int yMax = Math.min( height-1, y + blockRadius[1] );
            for ( int x = 0; x < width; ++x )
            {
                final int xMin = Math.max( -1, x - blockRadius[0] - 1 );
                final int xMax = Math.min( width-1, x + blockRadius[0] );
//                int n = (xMax - xMin) * (yMax - yMin);

//                final double sumX  = pmcc.getSumX( xMin, yMin, xMax, yMax );
//                final double sumY  = sumsY.getDoubleSum( xMin, yMin, xMax, yMax );
//                final double sumXX = pmcc.getSumXX( xMin, yMin, xMax, yMax );
//                final double sumYY = sumsYY.getDoubleSum(xMin, yMin, xMax, yMax);
//                final double Z = pmcc.getZ( xMin, yMin, xMax, yMax );


                // if blocks would reach out of bounds, ignore them (NaN)
//                double weight = (
//                        (x < blockRadius[0]) || (x > (width - blockRadius[0])) ||
//                                (y < blockRadius[1]) || (y > (height - blockRadius[1]))
//                ) ?
//                        Double.NaN :
//                        Math.sqrt( sumXX - sumX * sumX / n ) / n;
                // 0.5* ( n * sumXX - sumX * sumX + n * sumYY - sumY * sumY );

                float weight = binaryMask.getf( x, y );
                weights.setf( x, y, weight );
            }
        }

        return Utility.tuple2( maxCorrelations, weights );
    }

    public static HashMap<Tuple2< Integer, Integer >, Double> average(
            FloatProcessor maxCorrelations,
            FloatProcessor weights,
            int[] blockSizeInput,
            int[] stepSize
    )
    {
        HashMap<Tuple2<Integer, Integer>, Double> hm = new HashMap<Tuple2<Integer, Integer>, Double>();
        int width = maxCorrelations.getWidth();
        int height = maxCorrelations.getHeight();

//        int[] blockSize = new int[] {
//                Math.min( blockSizeInput[0], width / 2 ),
//                Math.min( blockSizeInput[1], height / 2 )
//        };

        int[] blockSize = blockSizeInput;

        int maxX = width - 1;
        int maxY = height - 1;

        for( int  y = blockSize[1], yIndex = 0; y < height; y += stepSize[1], ++yIndex )
        {
            int lowerY = y - blockSize[1];
            int upperY = Math.min(y + blockSize[1], maxY);
            for ( int x = blockSize[0], xIndex = 0; x < width; x += stepSize[0], ++xIndex )
            {
                int lowerX = x - blockSize[0];
                int upperX = Math.min(x + blockSize[0], maxX);
                double sum = 0.0;
                double weightSum = 0.0;
                for ( int yLocal = lowerY; yLocal < upperY; ++yLocal )
                {
                    for ( int xLocal = lowerX; xLocal < upperX; ++xLocal )
                    {
                        float weight = weights.getf(xLocal, yLocal);
                        if ( Double.isNaN( weight ) )
                            continue;
                        sum += weight*maxCorrelations.getf( xLocal, yLocal );
                        weightSum += weight;
                    }
                }
                sum /= weightSum; // ( upperY - lowerY ) * ( upperX - lowerX );
                hm.put( Utility.tuple2( xIndex, yIndex ), sum );
            }
        }
        return hm;
    }

    public static void main(String[] args) {

//        SparkConf conf = new SparkConf().setAppName("BLA").setMaster("local");
//        JavaSparkContext sc = new JavaSparkContext(conf);

//         String pathFixed = "/home/hanslovskyp/local/tmp/volcano.png";
//         String pathMoving = "/home/hanslovskyp/local/tmp/volcano-shifted.png";
        String base = "/nobackup/saalfeld/hanslovskyp/CutOn4-15-2013_ImagedOn1-27-2014/aligned/substacks/1300-3449/4000x2500+5172+1416/downscale-by-2/1000x600x800+500+312+0";
        String pathFixed = base + "/data/0000.tif";
        String pathMoving = base + "/0001-warped.tif";

        FloatProcessor fixed = new ImagePlus(pathFixed).getProcessor().convertToFloatProcessor();
        FloatProcessor moving = new ImagePlus(pathMoving).getProcessor().convertToFloatProcessor();

        new ImagePlus( "fixed", fixed ).show();
        new ImagePlus( "moviong", moving ).show();

        int[] maxDistance = new int[] { 0, 0 };
        int[] blockRadius = new int[] {  fixed.getWidth()/2, fixed.getHeight()/2 };

        Tuple2<FloatProcessor, FloatProcessor> maxCorrs = tolerantNCC(fixed, moving, maxDistance, blockRadius);

        HashMap<Tuple2<Integer, Integer>, Double> hm = average(maxCorrs._1(), maxCorrs._2(), new int[]{5, 5}, new int[]{5, 5});

        new ImageJ();

        new ImagePlus( "maxCorrs", maxCorrs._1() ).show();

        BlockPMCC pmcc = new BlockPMCC(fixed, moving);
        pmcc.setOffset( 0, 0 );
        pmcc.r( blockRadius[0], blockRadius[1] );
        new ImagePlus( "pmcc", pmcc.getTargetProcessor() ).show();

        System.out.println( StringUtils.join( hm, "," ) );

        System.out.println( (float)new NCC().calculate( fixed, moving ) + " " + (float)Correlations.calculate( fixed, moving ) + " " + maxCorrs._1().getf(fixed.getWidth()/2,fixed.getHeight()/2) );

        new ImagePlus( "weights", maxCorrs._2() ).show();

//        String pattern = "/nobackup/saalfeld/hanslovskyp/Chlamy/428x272x1414+20+20+0/data/%04d.tif";
//        int start = 0;
//        int stop  = 200;
//
//        final int range = 50;
//
//        JavaRDD<Integer> indices = sc.parallelize( Utility.arange(start, stop));
//
//        JavaPairRDD<Integer, FPTuple> indexedImages = indices
//                .mapToPair(new Utility.Format<Integer>(pattern))
//                .mapToPair(new Utility.LoadFile())
//                .cache()
//                ;
//
//        JavaPairRDD<Tuple2<Integer, Integer>, Tuple2<FPTuple, FPTuple>> imagePairs = indexedImages
//                .cartesian(indexedImages)
//                .filter(new Function<Tuple2<Tuple2<Integer, FPTuple>, Tuple2<Integer, FPTuple>>, Boolean>() {
//                    @Override
//                    public Boolean call(Tuple2<Tuple2<Integer, FPTuple>, Tuple2<Integer, FPTuple>> t) throws Exception {
//                        int v1 = t._1()._1();
//                        int v2 = t._2()._1();
//                        int diff = v2 - v1;
//                        return diff > 0 && diff <= range;
//                    }
//                })
//                .mapToPair(new PairFunction<Tuple2<Tuple2<Integer, FPTuple>, Tuple2<Integer, FPTuple>>, Tuple2<Integer, Integer>, Tuple2<FPTuple, FPTuple>>() {
//                    @Override
//                    public Tuple2<Tuple2<Integer, Integer>, Tuple2<FPTuple, FPTuple>>
//                    call(Tuple2<Tuple2<Integer, FPTuple>, Tuple2<Integer, FPTuple>> t) throws Exception {
//                        return Utility.tuple2(
//                                Utility.tuple2(t._1()._1(), t._2()._1()),
//                                Utility.tuple2(t._1()._2(), t._2()._2())
//                        );
//                    }
//                })
//                ;
//
//        TolerantNCC tolerantNCC = new TolerantNCC( imagePairs );
//        tolerantNCC.ensurePersistence();
//        List<Tuple2<Tuple2<Integer, Integer>, FPTuple>> tolerantMatrices =
//                tolerantNCC.calculate(sc, blockRadius, new int[]{400, 400}, blockRadius, maxDistance, stop - start, range).collect();
//
//        sc.close();
//
//        System.out.println( tolerantMatrices.size() );



    }

//    public static class CellsToSimilarities< K, M1 extends Map<Tuple2<Integer,Integer>, FPTuple>, M2 extends Map<Tuple2<Integer,Integer>,FPTuple>>
//            implements PairFunction<Tuple2<K,Tuple2<M1,M2>>, K, HashMap< Tuple2<Integer,Integer>,Double>> {
//
//        private final long[] dims;
//        private final long[] cellDims;
//
//        public CellsToSimilarities(long[] dims, long[] cellDims) {
//            this.dims = dims;
//            this.cellDims = cellDims;
//        }
//
//        @Override
//        public Tuple2<K, HashMap<Tuple2<Integer, Integer>, Double>> call(Tuple2<K, Tuple2<M1, M2>> t) throws Exception {
//            // assume first image rests, second image is moved
//            K key = t._1();
//            Tuple2<M1, M2> pairedCells = t._2();
//            M1 cells1 = pairedCells._1();
//            M2 cells2 = pairedCells._2();
//
//            CellImgFactory<FloatType> factory = new CellImgFactory<FloatType>();
//
//            CellImg<FloatType, ?, ?> reference = factory.create(dims, new FloatType());
//            CellImg<FloatType, ?, ?> moving = factory.create(dims, new FloatType());
//
//            copyToInterval( cells1, reference );
//            copyToInterval( cells2, moving );
//
//            return null;
//        }
//
//        public <M extends Map<Tuple2<Integer,Integer>, FPTuple > > void copyToInterval(M m, RandomAccessibleInterval< FloatType > img)
//        {
//            long[] currentMin = new long[2];
//            RandomAccess<FloatType> ra = img.randomAccess();
//            for ( Map.Entry<Tuple2<Integer, Integer>, FPTuple> entry : m.entrySet() )
//            {
//                Tuple2<Integer, Integer> pos = entry.getKey();
//                currentMin[0] = pos._1() * cellDims[0];
//                currentMin[1] = pos._2() * cellDims[1];
//                FloatProcessor fp = entry.getValue().rebuild();
//                int width = fp.getWidth();
//                int height = fp.getHeight();
//                for ( int y = 0; y < height; ++y ) {
//                    ra.setPosition( currentMin[1] + y, 1 );
//                    for (int x = 0; x < width; ++x) {
//                        ra.setPosition( currentMin[0] + x, 0 );
//                        ra.get().set( fp.getf( x, y ) );
//                    }
//                }
//            }
//        }
//    }

}
