package org.janelia.thickness.experiments;

import ij.ImagePlus;
import ij.io.FileSaver;
import ij.plugin.FolderOpener;
import net.imglib2.Cursor;
import net.imglib2.RealRandomAccessible;
import net.imglib2.img.Img;
import net.imglib2.img.array.ArrayImg;
import net.imglib2.img.array.ArrayImgs;
import net.imglib2.img.basictypeaccess.array.FloatArray;
import net.imglib2.img.display.imagej.ImageJFunctions;
import net.imglib2.interpolation.randomaccess.NLinearInterpolatorFactory;
import net.imglib2.realtransform.RealViews;
import net.imglib2.realtransform.ScaleAndTranslation;
import net.imglib2.type.numeric.real.FloatType;
import net.imglib2.view.IntervalView;
import net.imglib2.view.Views;
import org.janelia.thickness.ScaleOptions;
import org.janelia.thickness.inference.Options;

import java.io.File;
import java.io.FileNotFoundException;
import java.util.Arrays;

/**
 * Created by hanslovskyp on 11/9/15.
 */
public class GenerateLutMovie {

    public static void main(String[] args) throws FileNotFoundException {
        String root = "/nobackup/saalfeld/hanslovskyp/CutOn4-15-2013_ImagedOn1-27-2014/aligned/substacks/1300-3449/4000x2500+5172+1416/downscale-by-2/20151109_185512";//args[0];
        String sliceAxis = "x";
        int sliceAxisIndex = sliceAxis.equals( "x" ) ? 0 : 1;
        int otherAxisIndex = 1 - sliceAxisIndex;
        int sliceIndex = 20;
        String configPath = root + "/config.json";
        ScaleOptions config = ScaleOptions.createFromFile(configPath);
        int start = 4; // args.length > 1 ? Integer.parseInt( args[1] ) : 0;
        int stop = config.radii.length; // args.length > 2 ? Integer.parseInt( args[2] ) : config.radii.length;
        int maximumIndex = stop - 1;

        String timlinesPattern = root + "/out/%02d/lut-timelines/%s=%d";

        String timelinesScaledPattern = root + "/out/%02d/lut-timelines-scaled-to-%02d/%s=%d/%04d.tif";

        int volumeWidth = 2000;
        int volumeHeight = 1250;
        int[] volumeDim = new int[] { volumeWidth, volumeHeight };

        ImagePlus maximumIndexStack = new FolderOpener().openFolder(String.format(timlinesPattern, maximumIndex, sliceAxis, sliceIndex));
        int width = maximumIndexStack.getWidth();
        int height = maximumIndexStack.getHeight();
        int size = maximumIndexStack.getStackSize();



        Options maxLevelOptions = config.inference[stop - 1];
        double[] maxLevelRadii = new double[] { config.radii[stop-1][0], config.radii[stop-1][1] };
        double[] maxLevelSteps = new double[] { config.steps[stop-1][0], config.steps[stop-1][1] };

        for ( int s = start; s < stop; ++s )
        {
            double[] currentLevelRadii = new double[]{config.radii[s][0], config.radii[s][1]};
            double[] currentLevelSteps = new double[]{config.steps[s][0], config.steps[s][1]};


            double[] radii = new double[]{
                    (currentLevelRadii[0] - maxLevelRadii[0]) * 1.0 / maxLevelRadii[0],
                    (currentLevelRadii[1] - maxLevelRadii[1]) * 1.0 / maxLevelRadii[1]
            };
            double[] steps = new double[]{
                    currentLevelSteps[0]*1.0/maxLevelSteps[0], currentLevelSteps[1]*1.0/maxLevelSteps[1]
            };

            int maxCurrentIndex =  ((int)( volumeDim[sliceAxisIndex] - currentLevelRadii[sliceAxisIndex] ) / (int)currentLevelSteps[sliceAxisIndex]) - 1;

            double imagePosition = sliceIndex*steps[sliceAxisIndex] + radii[sliceAxisIndex];
            int imagePositionFloor = (int) Math.floor(imagePosition);
            int imagePositionCeil = imagePositionFloor + 1;
            double floorWeight = imagePositionCeil - imagePosition;
            double ceilWeight = 1.0 - floorWeight;

            System.out.println(Arrays.toString( currentLevelSteps ) + " " + Arrays.toString( currentLevelRadii ) + " " + maxCurrentIndex );
            System.out.println( String.format(timlinesPattern, s, sliceAxis, Math.max( 0, Math.min( maxCurrentIndex, imagePositionFloor)) ) );
            System.out.println( String.format(timlinesPattern, s, sliceAxis, Math.max( 0, Math.min( maxCurrentIndex, imagePositionCeil)) ) );

            ImagePlus floorImagePlus = new FolderOpener().openFolder(String.format(timlinesPattern, s, sliceAxis, Math.max( 0, Math.min( maxCurrentIndex, imagePositionFloor)) ) );
            ImagePlus ceilImagePlus = new FolderOpener().openFolder(String.format(timlinesPattern, s, sliceAxis, Math.max( 0, Math.min( maxCurrentIndex, imagePositionCeil)) ) );

            Img<FloatType> floorImage = ImageJFunctions.wrapFloat(floorImagePlus);
            Img<FloatType> ceilImage = ImageJFunctions.wrapFloat(ceilImagePlus);

            ScaleAndTranslation imageTransform = new ScaleAndTranslation(new double[]{1.0, steps[otherAxisIndex], 1.0}, new double[]{0.0, radii[otherAxisIndex], 0.0});



            long[] dim = new long[]{width, height, floorImagePlus.getStackSize()};
            ArrayImg<FloatType, FloatArray> target = ArrayImgs.floats(dim);

            RealRandomAccessible<FloatType> floorInterpolated = Views.interpolate(Views.extendBorder(floorImage), new NLinearInterpolatorFactory<FloatType>());
            IntervalView<FloatType> floorTransformed = Views.interval(Views.raster(RealViews.transformReal(floorInterpolated, imageTransform)), target);

            RealRandomAccessible<FloatType> ceilInterpolated = Views.interpolate(Views.extendBorder(ceilImage), new NLinearInterpolatorFactory<FloatType>());
            IntervalView<FloatType> ceilTransformed = Views.interval(Views.raster(RealViews.transformReal(ceilInterpolated, imageTransform)), target);

            for(Cursor<FloatType> t = target.cursor(), f = Views.flatIterable(floorTransformed).cursor(), c = Views.flatIterable(ceilTransformed).cursor(); t.hasNext(); )
                t.next().setReal( f.next().get()*floorWeight + c.next().get()*ceilWeight );

            for( long z = 0; z < dim[2]; ++z )
            {
                ImagePlus targetPlus = ImageJFunctions.wrapFloat(Views.hyperSlice(target, 2, z), "");
                new File( String.format( timelinesScaledPattern, s, maximumIndex, sliceAxis, sliceIndex, z ) ).getParentFile().mkdirs();
                new FileSaver( targetPlus ).saveAsTiff( String.format( timelinesScaledPattern, s, maximumIndex, sliceAxis, sliceIndex, z ) );
            }


            System.out.println( s );


        }


    }

}
