package org.janelia.thickness;

import ij.ImageJ;
import ij.ImagePlus;
import ij.io.FileSaver;
import ij.process.FloatProcessor;
import mpicbg.ij.integral.BlockPMCC;
import mpicbg.ij.integral.WeightedBlockPMCC;
import org.apache.commons.lang3.StringUtils;
import org.apache.spark.api.java.JavaPairRDD;
import org.apache.spark.api.java.JavaSparkContext;
import org.apache.spark.api.java.function.PairFunction;
import org.janelia.similarities.NCC;
import org.janelia.thickness.utility.FPTuple;
import org.janelia.thickness.utility.Utility;
import scala.Tuple2;

import java.util.Arrays;
import java.util.HashMap;
import java.util.HashSet;

/**
 * Created by hanslovskyp on 11/19/15.
 */
public class TolerantNCC {

    private final JavaPairRDD<Tuple2< Integer, Integer >, Tuple2<Tuple2<FPTuple, FPTuple >,Tuple2<FPTuple,FPTuple>>> overcompleteSections;

    public TolerantNCC(JavaPairRDD<Tuple2< Integer, Integer >, Tuple2<Tuple2<FPTuple, FPTuple >,Tuple2<FPTuple,FPTuple>>> overcompleteSections) {
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

        JavaPairRDD<Tuple2<Integer, Integer>, Tuple2<Tuple2<FPTuple, FPTuple>, Tuple2<FPTuple, FPTuple>>> sections = overcompleteSections
                .filter(new MatrixGenerationFromImagePairs.SelectInRange<Tuple2<Tuple2<FPTuple, FPTuple>,Tuple2<FPTuple,FPTuple>>>(range))
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
                .mapToPair(new MatrixGenerationFromImagePairs.MapToFPTuple(size, 0))
                .cache()
                ;

        matrices.count();

        return matrices;


    }

    public static class FPToSimilarities<K>
    implements PairFunction<Tuple2<K,Tuple2<Tuple2<FPTuple,FPTuple>,Tuple2<FPTuple,FPTuple>>>, K, Tuple2<FPTuple,FPTuple>> {

        private final int[] maxOffsets;
        private final int[] blockRadius;

        public FPToSimilarities(int[] maxOffsets, int[] blockRadius) {
            this.maxOffsets = maxOffsets;
            this.blockRadius = blockRadius;
        }

        @Override
        public Tuple2<K, Tuple2<FPTuple,FPTuple>> call(Tuple2<K, Tuple2<Tuple2<FPTuple,FPTuple>,Tuple2<FPTuple,FPTuple>>> t) throws Exception {
            FloatProcessor fixed = t._2()._1()._1().rebuild();
            FloatProcessor moving = t._2()._2()._1().rebuild();
            FloatProcessor fixedMask = t._2()._1()._2().rebuild();
            FloatProcessor movingMask = t._2()._2()._2().rebuild();

            K k = t._1();

            int x =0, y = 0;
            if ( k instanceof Tuple2 )
                if ( ((Tuple2) k)._1() instanceof Integer )
                    x = ((Integer) ((Tuple2) k)._1()).intValue();
            if ( ((Tuple2) k)._2() instanceof Integer )
                y = ((Integer) ((Tuple2) k)._2()).intValue();

            Tuple2<FloatProcessor, FloatProcessor> ccs = tolerantNCC(
                    (FloatProcessor) fixed.duplicate(),
                    (FloatProcessor) moving.duplicate(),
                    fixedMask,
                    movingMask,
                    maxOffsets,
                    blockRadius,
                    x ,
                    y );
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

    public static FloatProcessor generateMask( FloatProcessor fp )
    {
        FloatProcessor weights = new FloatProcessor(fp.getWidth(), fp.getHeight());
        float[] weightsPixels = (float[]) weights.getPixels();
        float[] fpPixels = (float[]) fp.getPixels();
        for( int i = 0; i < fpPixels.length; ++i )
        {
            boolean isNaN = Float.isNaN(fpPixels[i]);
            // ignore NaNs (leave them 0.0f in mask)
            // still need to replace NaNs in image because 0.0 * NaN = NaN
            if ( isNaN )
                fpPixels[i] = 0.0f;
            else
                weightsPixels[i] = 1.0f;
        }
        return weights;
    }

    public static Tuple2< FloatProcessor, FloatProcessor > tolerantNCC(
            FloatProcessor fixed,
            FloatProcessor moving,
            FloatProcessor fixedMask,
            FloatProcessor movingMask,
            final int[] maxOffsets,
            final int[] blockRadiusInput,
            int z1,
            int z2
    ) {
        int width = moving.getWidth();
        int height = moving.getHeight();

        int[] blockRadius = new int[]{
                Math.min(blockRadiusInput[0], width / 2),
                Math.min(blockRadiusInput[1], height / 2)
        };


//        BlockPMCC pmcc = new BlockPMCC( fixed, moving );
//        HashSet<Float> maskedValues = new HashSet<Float>();
//        maskedValues.add(0.0f);
//        for ( int i = 1; i <= 20; ++i )
//            maskedValues.add((float)i);
//        maskedValues.add(255f);
//        FloatProcessor weightsX = generateMask(fixed, maskedValues);
//        FloatProcessor weightsY = generateMask(moving, maskedValues);
//        WeightedBlockPMCC pmcc = new WeightedBlockPMCC(fixed, moving, weightsX, weightsY);
////            pmcc.rSignedSquare( radius[0], radius[1] );
//
//        pmcc.setOffset(0, 0);
//        pmcc.r(width, height);
//
        FloatProcessor maxCorrelations = new FloatProcessor(width, height);

        final int xStart = -1 * maxOffsets[0];
        final int yStart = -1 * maxOffsets[1];

        final int xStop = 1 * maxOffsets[0]; // inclusive
        final int yStop = 1 * maxOffsets[1]; // inclusive

        // variances
//        FloatProcessor variancesX = (FloatProcessor) fixed.duplicate();
//        FloatProcessor variancesY = (FloatProcessor) moving.duplicate();
//
//        BlockStatistics bssX = new BlockStatistics(variancesX);
//        BlockStatistics bssY = new BlockStatistics(variancesY);
//
//        int varianceRadius = 15;
//
//        bssX.sampleVariance( varianceRadius );
//        bssY.sampleVariance( varianceRadius );
//
//        double varMaxX = 0.0;
//        double varMaxY = 0.0;
//        double varMeanX = 0.0;
//        double varMeanY = 0.0;
//
//        float[] varPixelsX = (float[]) variancesX.getPixels();
//        float[] varPixelsY = (float[]) variancesY.getPixels();
//
//        for( int i = 0; i < varPixelsY.length; ++i )
//        {
//            double valX = varPixelsX[i];
//            double valY = varPixelsY[i];
//            varMaxX = Math.max( valX, varMaxX );
//            varMaxY = Math.max( valY, varMaxY );
//            varMeanX += valX;
//            varMeanY += valY;
//        }
//
//        varMeanX /= varPixelsX.length;
//        varMeanY /= varPixelsY.length;
//
//        ByteProcessor binaryMaskX = new ByteProcessor(variancesX.getWidth(), variancesX.getHeight());
//        ByteProcessor binaryMaskY = new ByteProcessor(variancesY.getWidth(), variancesY.getHeight());
//
//        byte[] binaryPixelsX = (byte[]) binaryMaskX.getPixels();
//        byte[] binaryPixelsY = (byte[]) binaryMaskY.getPixels();
//
//        for ( int i = 0; i < varPixelsY.length; ++i )
//        {
//            binaryPixelsX[i] = (byte) (varPixelsX[i] > 0.1 * varMeanX ? 1 : 0);
//            binaryPixelsY[i] = (byte) (varPixelsY[i] > 0.1 * varMeanY ? 1 : 0);
//        }
//
//
//
//        int nErode = 2;
//        int nDilate = nErode + 2;
//
//        for( int i = 0; i < nErode; ++i )
//        {
//            binaryMaskX.erode();
//            binaryMaskY.erode();
//        }
//
//        for ( int i = 0; i < nDilate; ++i )
//        {
//            binaryMaskX.dilate();
//            binaryMaskY.dilate();
//        }
//
//        float[] weightPixelsX = (float[]) weightsX.getPixels();
//        float[] weightPixelsY = (float[]) weightsY.getPixels();
//
//        for ( int i = 0; i < binaryPixelsY.length; ++i )
//        {
//            weightPixelsX[i] *= binaryPixelsX[i];
//            weightPixelsY[i] *= binaryPixelsY[i];
//        }



        // old stuff
//
//        {
//            double varSumX = 0.0;
//            double varMaxX = 0.0;
//            FloatProcessor variancesX = new FloatProcessor(width, height);
//            int varianceRadiusX = varianceRadius;
//
//            for (int y = 0; y < height; ++y) {
//                final int yMin = Math.max(-1, y - varianceRadiusX - 1);
//                final int yMax = Math.min(height - 1, y + varianceRadiusX);
//                for (int x = 0; x < width; ++x) {
//                    final int xMin = Math.max(-1, x - varianceRadiusX - 1);
//                    final int xMax = Math.min(width - 1, x + varianceRadiusX);
//                    int n = (xMax - xMin) * (yMax - yMin);
//
//                    final double sumX = pmcc.getSumX(xMin, yMin, xMax, yMax);
//                    //                final double sumY  = sumsY.getDoubleSum( xMin, yMin, xMax, yMax );
//                    final double sumXX = pmcc.getSumXX(xMin, yMin, xMax, yMax);
//                    double variance = (sumXX - sumX * sumX / n) / n;
//                    variancesX.setf(x, y, (float) variance);
//                    varSumX += variance;
//                    varMaxX = Math.max( variance, varMaxX );
//                }
//            }
//
//            double varMeanX = varSumX / (width * height);
//            float[] varPixelsX = (float[]) variancesX.getPixels();
//            ByteProcessor binaryMaskX = new ByteProcessor(width, height);
//            byte[] maskPixelsX = (byte[]) binaryMaskX.getPixels();
//            float[] weightsPixelsX = (float[]) weightsX.getPixels();
//            for (int i = 0; i < varPixelsX.length; ++i)
//                maskPixelsX[i] = varPixelsX[i] > varMeanX ? (byte) 1 : (byte) 0;
//
//
//            // imagej api ImageProcessor.dilate():
//            // Dilates the image or ROI using a 3x3 minimum filter. Requires 8-bit or RGB image.
//            // That sounds like erosion rather than dilation?
//            binaryMaskX.dilate();
//            binaryMaskX.dilate();
//            //        binaryMaskX.dilate();
//            //        binaryMaskX.dilate();
//
//            variancesX.erode();
//            variancesX.erode();
//            variancesX.erode();
//            variancesX.erode();
//
//            variancesX.dilate();
//            variancesX.dilate();
//            variancesX.dilate();
//            variancesX.dilate();
//            variancesX.dilate();
//            variancesX.dilate();
//            variancesX.dilate();
//            variancesX.dilate();
////            variancesX.dilate();
////            variancesX.dilate();
////            variancesX.dilate();
////            variancesX.dilate();
//            for (int i = 0; i < varPixelsX.length; ++i)
//                weightsPixelsX[i] *= varPixelsX[i] / varMaxX;
////                weightsPixelsX[i] *= maskPixelsX[i];
//        }
//        {
//
//            double varSumY = 0.0;
//            double varMaxY = 0.0;
//            FloatProcessor variancesY = new FloatProcessor(width, height);
//            int varianceRadiusY = varianceRadius;
//
//            for (int y = 0; y < height; ++y) {
//                final int yMin = Math.max(-1, y - varianceRadiusY - 1);
//                final int yMax = Math.min(height - 1, y + varianceRadiusY);
//                for (int x = 0; x < width; ++x) {
//                    final int xMin = Math.max(-1, x - varianceRadiusY - 1);
//                    final int xMax = Math.min(width - 1, x + varianceRadiusY);
//                    int n = (xMax - xMin) * (yMax - yMin);
//
//                    final double sumY = pmcc.getSumY(xMin, yMin, xMax, yMax);
////                final double sumY  = sumsY.getDoubleSum( xMin, yMin, xMax, yMax );
//                    final double sumYY = pmcc.getSumYY(xMin, yMin, xMax, yMax);
//                    double variance = (sumYY - sumY * sumY / n) / n;
//                    variancesY.setf(x, y, (float) variance);
//                    varSumY += variance;
//                    varMaxY = Math.max( variance, varMaxY );
//                }
//            }
//
//            double varMeanY = varSumY / (width * height);
//            float[] varPixelsY = (float[]) variancesY.getPixels();
//            ByteProcessor binaryMaskY = new ByteProcessor(width, height);
//            byte[] maskPixelsY = (byte[]) binaryMaskY.getPixels();
//            float[] weightsPixelsY = (float[]) weightsY.getPixels();
//            for (int i = 0; i < varPixelsY.length; ++i) {
//                maskPixelsY[i] = varPixelsY[i] > varMeanY ? (byte) 1 : (byte) 0;
//            }
//
//            // imagej api ImageProcessor.dilate():
//            // Dilates the image or ROI using a 3x3 minimum filter. Requires 8-bit or RGB image.
//            // That sounds like erosion rather than dilation?
//            binaryMaskY.dilate();
//            binaryMaskY.dilate();
////        binaryMaskY.dilate();
////        binaryMaskY.dilate();
//
//            variancesY.erode();
//            variancesY.erode();
//            variancesY.erode();
//            variancesY.erode();
//
//            variancesY.dilate();
//            variancesY.dilate();
//            variancesY.dilate();
//            variancesY.dilate();
//            variancesY.dilate();
//            variancesY.dilate();
//            variancesY.dilate();
//            variancesY.dilate();
////            variancesY.dilate();
////            variancesY.dilate();
////            variancesY.dilate();
////            variancesY.dilate();
//            for (int i = 0; i < varPixelsY.length; ++i)
//                weightsPixelsY[i] *= varPixelsY[i] / varMaxY;
////                weightsPixelsY[i] *= maskPixelsY[i];
//        }


        // actual ncc calculation starts here!
        FloatProcessor weightsX = generateMask( fixed );
        FloatProcessor weightsY = generateMask( moving );


        // structure tensor
//        FloatImage fImgX = new FloatImage(new ImagePlus("", fixed));
//        FloatImage fImgY = new FloatImage(new ImagePlus("", moving));
//
//        final Aspects aspects = fImgX.aspects();
////        if (!FJ_Options.isotropic) fImgX.aspects(new Aspects());
//
//        Structure structure = new Structure();
//        structure.messenger.log(false);
//        structure.progressor.display(false);
//
//        double sscaleval = 0.3;
//        double iscaleval   = 5.0;
//        Vector<Image> eigenimagesX = structure.run(fImgX, sscaleval, iscaleval);
//        Vector<Image> eigenimagesY = structure.run(fImgY, sscaleval, iscaleval);
//
//        FloatProcessor evX = eigenimagesX.get(1).imageplus().getProcessor().convertToFloatProcessor();
//        FloatProcessor evY = eigenimagesY.get(1).imageplus().getProcessor().convertToFloatProcessor();
//
//        float[] evPixelsX = (float[]) evX.getPixels();
//        float[] evPixelsY = (float[]) evY.getPixels();
//
//        float[] weightPixelsX = (float[]) weightsX.getPixels();
//        float[] weightPixelsY = (float[]) weightsY.getPixels();
//
//        for ( int i = 0; i < evPixelsX.length; ++i )
//        {
//            float multX = 1.0f/evPixelsX[i];
//            float multY = 1.0f/evPixelsY[i];
//            weightPixelsX[i] *= Float.isNaN( multX ) ? 0.0 : multX;
//            weightPixelsY[i] *= Float.isNaN( multY ) ? 0.0 : multY;
//        }

        float[] weightsXPixels = (float[]) weightsX.getPixels();
        float[] weightsYPixels = (float[]) weightsY.getPixels();

        float[] fixedMaskPixels = (float[]) fixedMask.getPixels();
        float[] movingMaskPxiels = (float[]) movingMask.getPixels();

        for( int i = 0; i < weightsXPixels.length; ++i )
        {
            weightsXPixels[i] *= fixedMaskPixels[i];
            weightsYPixels[i] *= movingMaskPxiels[i];
        }

        String pattern = "/nobackup/saalfeld/hanslovskyp/weights-dump/%04d.tif";
        new FileSaver( new ImagePlus("", weightsX) ).saveAsTiff( String.format( pattern, z1 ) );
//        new FileSaver( new ImagePlus("", weightsY) ).saveAsTiff( String.format( pattern, z2 ) );
        WeightedBlockPMCC pmcc = new WeightedBlockPMCC(fixed, moving, weightsX, weightsY);
        FloatProcessor tp = pmcc.getTargetProcessor();

        FloatProcessor weights = new FloatProcessor( width, height );


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
                        if ( !Double.isNaN( val ) && val > maxCorrelations.getf( x, y ) )
                            maxCorrelations.setf( x, y, val );
                    }
                }

            }
        }





        for ( int y = 0; y < height; ++y )
        {
            for ( int x = 0; x < width; ++x )
            {
                float weight = (
                        (x < blockRadius[0]) || (x > (width - blockRadius[0])) ||
                                (y < blockRadius[1]) || (y > (height - blockRadius[1]))
                ) ?
                Float.NaN : 1;//binaryMaskX.getf( x, y );
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
                        double weight = weights.getf(xLocal, yLocal);
                        double corr   = maxCorrelations.getf( xLocal, yLocal );
                        if ( Double.isNaN( weight ) || Double.isNaN( corr ))
                            continue;
                        sum += weight*corr;
                        weightSum += weight;
                    }
                }
                if ( weightSum == 0.0 )
                    throw new RuntimeException( "weightSum == 0"  + x + " " + y );
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

        FloatProcessor maskX = new FloatProcessor(fixed.getWidth(), fixed.getHeight());
        FloatProcessor maskY = new FloatProcessor(moving.getWidth(), moving.getHeight());

        maskX.add( 1 );
        maskY.add( 1 );

        Tuple2<FloatProcessor, FloatProcessor> maxCorrs = tolerantNCC(fixed, moving, maskX, maskY, maxDistance, blockRadius, 0, 0);

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

    }

}
