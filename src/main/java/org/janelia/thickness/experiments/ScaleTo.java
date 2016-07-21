package org.janelia.thickness.experiments;

import ij.ImagePlus;
import ij.io.FileSaver;
import net.imglib2.RealRandomAccessible;
import net.imglib2.img.Img;
import net.imglib2.img.display.imagej.ImageJFunctions;
import net.imglib2.interpolation.randomaccess.NLinearInterpolatorFactory;
import net.imglib2.realtransform.RealViews;
import net.imglib2.realtransform.ScaleAndTranslation;
import net.imglib2.type.numeric.NumericType;
import net.imglib2.type.numeric.real.FloatType;
import net.imglib2.view.IntervalView;
import net.imglib2.view.Views;
import org.janelia.thickness.ScaleOptions;

import java.io.File;
import java.io.FileNotFoundException;

/**
 * Created by phil on 1/26/16.
 */
public class ScaleTo {

    public static void scale (
            String sourcePattern,
            String targetPattern,
            double[] toBeScaledRadius,
            double[] toBeScaledSteps,
            String referencePattern,
            ScaleOptions config,
            int refStage
    )
    {
        String toBeScaledPattern = sourcePattern;
        String run = "20160129_135000";
        String outputPattern = targetPattern;
        String referenceSizePattern = referencePattern;
        int start = config.start;
        int stop  = config.stop;

        double[] referenceRadius = new double[] { config.radii[refStage][0], config.radii[refStage][1] };
        double[] referenceSteps  = new double[] { config.steps[refStage][0], config.steps[refStage][1] };


        double[] s = new double[] {
                toBeScaledSteps[0] / referenceSteps[0],
                toBeScaledSteps[1] / referenceSteps[1]
        };
        double[] r = new double[] {
                ( toBeScaledRadius[0] - referenceRadius[0] ) / referenceSteps[0],
                ( toBeScaledRadius[1] - referenceRadius[1] ) / referenceSteps[1]
        };

        ScaleAndTranslation tf = new ScaleAndTranslation(s, r);

        Img<FloatType> referenceImage = ImageJFunctions.wrapFloat(new ImagePlus(String.format(referenceSizePattern, start)));

        for ( ; start < stop; ++start )
        {
            Img<FloatType> img = ImageJFunctions.wrapFloat(new ImagePlus(String.format(toBeScaledPattern, start)));
            RealRandomAccessible<FloatType> extendedAndInterpolated =
                    Views.interpolate(Views.extendBorder(img), new NLinearInterpolatorFactory<FloatType>());
            IntervalView<FloatType> transformed =
                    Views.interval(Views.raster(RealViews.transform(extendedAndInterpolated, tf)), referenceImage);
            ImagePlus result = ImageJFunctions.wrap(transformed, "result");
            String targetPath = String.format( outputPattern, start );
            new File( targetPath ).getParentFile().mkdirs();
            new FileSaver( result ).saveAsTiff( targetPath );
        }
    }

//    public static void scaleToImageDataNo
    public static void scaleToImageData(
            String sourcePattern,
            String targetPattern,
            double[] toBeScaledRadius,
            double[] toBeScaledSteps,
            ScaleOptions config
    )
    {
        String toBeScaledPattern = sourcePattern;
        String outputPattern     = targetPattern;
        String referenceSizePattern = config.source;
        int start = config.start;
        int stop  = config.stop;

        double[] referenceRadius = new double[] { 0, 0 };
        double[] referenceSteps  = new double[] { 1, 1 };


        double[] s = new double[] {
                toBeScaledSteps[0] / referenceSteps[0],
                toBeScaledSteps[1] / referenceSteps[1]
        };
        double[] r = new double[] {
                ( toBeScaledRadius[0] - referenceRadius[0] ) / referenceSteps[0],
                ( toBeScaledRadius[1] - referenceRadius[1] ) / referenceSteps[1]
        };

        ScaleAndTranslation tf = new ScaleAndTranslation(s, r);

        Img< ? extends NumericType> referenceImage = ImageJFunctions.wrapNumeric(new ImagePlus(String.format(referenceSizePattern, start)));
        System.out.println( referenceImage + " " + new ImagePlus( String.format( referenceSizePattern, start) ) );


        for ( ; start < stop; ++start )
        {
            Img<FloatType> img = ImageJFunctions.wrapFloat(new ImagePlus(String.format(toBeScaledPattern, start)));
            RealRandomAccessible<FloatType> extendedAndInterpolated =
                    Views.interpolate(Views.extendBorder(img), new NLinearInterpolatorFactory<FloatType>());
            IntervalView<FloatType> transformed =
                    Views.interval(Views.raster(RealViews.transform(extendedAndInterpolated, tf)), referenceImage);
            ImagePlus result = ImageJFunctions.wrap(transformed, "result");
            String targetPath = String.format( outputPattern, start );
            new File( targetPath ).getParentFile().mkdirs();
            new FileSaver( result ).saveAsTiff( targetPath );
        }
    }

    public static void main(String[] args) throws FileNotFoundException {
//        String root = "/nobackup/saalfeld/hanslovskyp/CutOn4-15-2013_ImagedOn1-27-2014/aligned/substacks/1300-3449" +
//                "/4000x2500+5172+1416/downscale-z-by-4/corrected/z=[25,499]/z=[0,99]/deformation/01";
//        String toBeScaledPattern = String.format("%s/backward/%s", root, "%04d.tif");
//        int refStage = 7;
//        String run = "20160129_135000";
//        String outputPattern = String.format("%s/%s/%s", root, String.format("backward-scaled-to-%02d", refStage), "/%04d.tif");
//        String referenceRoot = String.format("%s/%s", root, run);
//        String referenceConfigPath = referenceRoot + "/config.json";
//        String referenceSizePattern = referenceRoot + String.format("/out/%02d", refStage) + "/forward/%04d.tif";
//        ScaleOptions config = ScaleOptions.createFromFile(referenceConfigPath);
//        scale(
//                toBeScaledPattern,
//                outputPattern,
//                new double[]{10, 10},
//                new double[]{10, 10},
//                referenceSizePattern,
//                config,
//                refStage
//        );

        String root = "/nobackup/saalfeld/hanslovskyp/shan-for-local/ds2/substacks/z=[1950,2150]" +
                "/substacks/468x477+70+68/substacks/normalized-contrast/sequence/20160328_175153";
        String configPath = root + "/config.json";
        ScaleOptions config = ScaleOptions.createFromFile(configPath);
        String[] transforms = new String[] { "forward", "backward" };

        for ( String t : transforms )
        {
            for ( int i = 0; i < config.radii.length; ++i ) {
                String currStage = String.format("%02d/", i );
                String toBeScaledPattern = String.format("%s/out/" + currStage + t + "/%s", root, "%04d.tif");
                String targetPattern = String.format("%s/out/" + currStage + t + "-full-res/%s", root, "%04d.tif");

                double[] radii = {config.radii[i][0], config.radii[i][1]};
                double[] steps = {config.steps[i][0], config.steps[i][1]};

                System.out.println( toBeScaledPattern );
                System.out.println( targetPattern );

                scaleToImageData(
                        toBeScaledPattern,
                        targetPattern,
                        radii,
                        steps,
                        config
                );
            }

        }
    }

}
