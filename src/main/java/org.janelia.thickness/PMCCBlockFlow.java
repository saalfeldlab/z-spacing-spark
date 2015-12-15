package org.janelia.thickness;

import ij.ImageJ;
import ij.ImagePlus;
import ij.process.FloatProcessor;
import mpicbg.ij.integral.BlockPMCC;
import net.imglib2.util.RealSum;
import org.apache.spark.api.java.function.PairFunction;
import scala.Tuple2;

import java.util.Arrays;
import java.util.Random;

/**
 * Created by hanslovskyp on 11/23/15.
 */
public class PMCCBlockFlow {

    public static class BlockFlow< K >implements PairFunction< Tuple2< K, Tuple2< FPTuple, FPTuple >>, K, Double >
    {

        final int[] maxOffset;
        final int[] radius;

        public BlockFlow(int[] maxOffset, int[] radius) {
            this.maxOffset = maxOffset;
            this.radius = radius;
        }

        @Override
        public Tuple2<K, Double> call(Tuple2<K, Tuple2<FPTuple, FPTuple>> t) throws Exception {
            K key = t._1();
            FloatProcessor fixedImage = t._2()._1().rebuild();
            FloatProcessor movingImage = t._2()._2().rebuild();

            double result = correlate( movingImage, fixedImage, maxOffset, radius );

            return Utility.tuple2( key, result );
        }
    }


    public static void fill( FloatProcessor source, FloatProcessor target, int offsetX, int offsetY )
    {
        int w = target.getWidth();
        int h = target.getHeight();
        int sw = source.getWidth();
        int sh = source.getHeight();

        Random rng = new Random();

        for ( int y = 0, yWorld = offsetY - w; y < h; ++y, ++yWorld ) {
            for ( int x = 0, xWorld = offsetX - w; x < w; ++x, ++xWorld ) {
                float val;
                if ( yWorld < 0 || xWorld < 0 || yWorld >= sh || xWorld >= sw )
                    val = rng.nextFloat();
                else
                    val = source.getf( xWorld, yWorld );
                target.setf( x, y, val );
            }
        }

    }

    public static double correlate( FloatProcessor movingImage, FloatProcessor fixedImage, int[] maxOffset, int[] radius )
    {
        int width = movingImage.getWidth();
        int height = movingImage.getHeight();

        int[] correctedRadius = new int[]{
                Math.min( radius[0], movingImage.getWidth() ),
                Math.min( radius[1], movingImage.getHeight() )
        };

        int movingWidth = 2*correctedRadius[0]+1;
        int movingHeight = 2*correctedRadius[1]+1;

        int fixedWidth = movingWidth + 2*maxOffset[0];
        int fixedHeight = movingHeight + 2*maxOffset[1];

        FloatProcessor m = new FloatProcessor(movingWidth, movingHeight);
        FloatProcessor f = new FloatProcessor(fixedWidth, fixedHeight);

        RealSum corrSum = new RealSum();
        int count = 0;

        for( int y = correctedRadius[1], yIndex = 0; y < height; y += movingHeight, ++yIndex )
        {
            for ( int x = correctedRadius[0], xIndex = 0; x < width; x += movingWidth, ++xIndex )
            {
                fill( fixedImage,  f, x, y );
                fill( movingImage, m, x, y );
                BlockPMCC pmcc = new BlockPMCC( fixedImage, movingImage );
                pmcc.rSignedSquare( correctedRadius[0], correctedRadius[1] );
                double maxCorrelation = -Double.MAX_VALUE;
                for ( int offY = -maxOffset[1]; offY <= maxOffset[1]; ++offY ) {
                    for (int offX = -maxOffset[0]; offX <= maxOffset[0]; ++offX) {
                        pmcc.setOffset( offX+maxOffset[0], offY+maxOffset[1] );
                        maxCorrelation = Math.max( maxCorrelation, pmcc.getTargetProcessor().getf( 0, 0 ) );
                    }
                }
                System.out.println( maxCorrelation + " " + count );
                corrSum.add( maxCorrelation );
                ++count;
            }
        }

        double result = corrSum.getSum() / count;

        return result;

    }

    public static void main(String[] args) {
        String path = "/nobackup/saalfeld/hanslovskyp/CutOn4-15-2013_ImagedOn1-27-2014/aligned/substacks/1300-3449/4000x2500+5172+1416/downscale-by-2/data/0000.tif";
        ImagePlus img = new ImagePlus(path);
        FloatProcessor proc = img.getProcessor().convertToFloatProcessor();
        FloatProcessor fixed = new FloatProcessor(30, 30);
        FloatProcessor moving = new FloatProcessor(20, 20);
        int[] maxOffset = new int[]{20, 20};
        int[] radius = new int[] { moving.getWidth() / 2 + 1, moving.getHeight() / 2 + 1 };
        for( int x = 0; x < fixed.getWidth(); ++x ) {
            for (int y = 0; y < fixed.getHeight(); ++y) {
                fixed.setf(x, y, proc.getf(x, y));
            }
        }

        for( int x = 0; x < moving.getWidth(); ++x ) {
            for (int y = 0; y < moving.getHeight(); ++y) {
                moving.setf(x, y, proc.getf(x, y));
            }
        }

        new ImageJ();
        new ImagePlus( "orig", proc ).show();
        new ImagePlus( "moving", moving ).show();
        new ImagePlus( "fixed", fixed ).show();

        BlockPMCC pmcc = new BlockPMCC(fixed, moving);
        pmcc.setOffset( -10, -10 );
        System.out.println( Arrays.toString( radius ) );
        pmcc.r( moving.getWidth(), moving.getHeight() );
        new ImagePlus( "target", pmcc.getTargetProcessor() ).show();
        System.out.println( " MIZZZGE " + pmcc.getTargetProcessor().getf( 0, 0 ) );

        double maxCorr = correlate(moving, fixed, maxOffset, radius);
        System.out.println( "maxCorr=" + maxCorr );

    }

}
