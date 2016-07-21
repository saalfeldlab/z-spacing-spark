package org.janelia.thickness.experiments;

import com.google.gson.Gson;
import com.google.gson.JsonObject;
import com.google.gson.JsonParser;
import ij.ImagePlus;
import ij.io.FileSaver;
import mpicbg.models.AffineModel1D;
import mpicbg.models.IllDefinedDataPointsException;
import mpicbg.models.NotEnoughDataPointsException;
import net.imglib2.FinalInterval;
import net.imglib2.img.display.imagej.ImageJFunctions;
import net.imglib2.type.numeric.real.FloatType;
import net.imglib2.view.IntervalView;
import net.imglib2.view.RandomAccessibleOnRealRandomAccessible;
import net.imglib2.view.Views;
import org.janelia.thickness.ScaleOptions;
import org.janelia.utility.ConstantRealRandomAccesssible;
import org.janelia.utility.io.IO;

import java.io.File;
import java.io.FileNotFoundException;
import java.io.FileReader;
import java.io.IOException;
import java.util.concurrent.ExecutionException;

/**
 * Created by hanslovskyp on 2/2/16.
 */
public class Evaluation {

    public static class Experiment
    {
        public final String deformationRoot;
        public final String deformationInfoPath;
        public final String estimationRun;
        public final String estimationRoot;
        public final String estimationConfig;
        public final ScaleOptions config;
        public final int estimationReferenceStage;
        public final String deformationBackwardScaledDir;
        public final String deformationBackwardScaledPattern;
        public final String deformationBackwardGradientScaledDir;
        public final String deformationBackwardGradientScaledPattern;
        public final String estimationReferencePattern;
        public final String deformationBackwardPattern;
        public final String deformationBackwardGradientPattern;


        public Experiment(String deformationRoot, String estimationRun) throws FileNotFoundException {
            this.deformationRoot = deformationRoot;
            this.deformationInfoPath = deformationRoot + "/info.json";
            this.estimationRun = estimationRun;
            this.estimationRoot = String.format( "%s/%s", deformationRoot, estimationRun );;
            this.estimationConfig = String.format("%s/config.json", estimationRoot );
            this.config = ScaleOptions.createFromFile(estimationConfig);
            this.estimationReferenceStage = config.radii.length - 1;
            this.deformationBackwardScaledDir =
                    String.format( "%s/%s", estimationRoot, String.format( "reference-backward-scaled-to-%02d", estimationReferenceStage ) );
            this.deformationBackwardScaledPattern =
                    String.format( "%s/%s", deformationBackwardScaledDir, "%04d.tif" );

            this.deformationBackwardGradientScaledDir =
                    String.format( "%s/%s", estimationRoot, String.format( "reference-backward-gradient-scaled-to-%02d", estimationReferenceStage ) );

            this.deformationBackwardGradientScaledPattern =
                    String.format( "%s/%s", deformationBackwardGradientScaledDir, "%04d.tif" );
            this.estimationReferencePattern =
                    String.format( "%s/out/%02d/forward/%s", estimationRoot, estimationReferenceStage, "%04d.tif" );
            this.deformationBackwardPattern = String.format( "%s/%s/%s", deformationRoot, "backward", "%04d.tif" );
            this.deformationBackwardGradientPattern = String.format( "%s/%s/%s", deformationRoot, "", "%04d.tif" );
        }

    }

    public static void main(String[] args) throws IOException, InterruptedException, NotEnoughDataPointsException, IllDefinedDataPointsException, ExecutionException {

        String deformationRoot =
                "/nobackup/saalfeld/hanslovskyp/CutOn4-15-2013_ImagedOn1-27-2014/aligned/substacks/1300-3449" +
                "/4000x2500+5172+1416/downscale-z-by-4/corrected/z=[25,499]/z=[0,99]/deformation/01";
        deformationRoot =
                "/nobackup/saalfeld/hanslovskyp/CutOn4-15-2013_ImagedOn1-27-2014/aligned/substacks/1300-3449" +
                "/4000x2500+5172+1416/downscale-z-by-4/corrected/z=[25,499]/deformation/01";
        deformationRoot =
                "/nobackup/saalfeld/hanslovskyp/CutOn4-15-2013_ImagedOn1-27-2014/aligned/substacks/1300-3449" +
                "/4000x2500+5172+1416/downscale-z-by-4/corrected/xzy/z=[1000,1099]/3000x475+500+25/deformation/01";
//        String deformationRoot =
//                "/nobackup/saalfeld/hanslovskyp/CutOn4-15-2013_ImagedOn1-27-2014/aligned/substacks/1300-3449" +
//                "/4000x2500+5172+1416/downscale-z-by-4/corrected/z=[25,499]/z=[0,99]/downscale-xy-by-2/deformation/01";
//        String deformationRoot =
//        "/nobackup/saalfeld/hanslovskyp/CutOn4-15-2013_ImagedOn1-27-2014/aligned/substacks/1300-3449" +
//        "/4000x2500+5172+1416/downscale-z-by-4/corrected/z=[25,499]/deformation/01";
//        deformationRoot =
//                "/nobackup/saalfeld/hanslovskyp/CutOn4-15-2013_ImagedOn1-27-2014/aligned/substacks/1300-3449" +
//                "/4000x2500+5172+1416/downscale-z-by-4/corrected/zxy/z=[1000,1099]/475x3000+25+500/deformation/01";

//        String estimationRun = "20160303_115709"; // "20160129_135000";
//        String estimationRun = "20160202_102949";
//        String estimationRun = "20160127_113110";
        String estimationRun = "20160301_110624";
        estimationRun = "20160314_163540";
//        estimationRun = "20160323_161441";

        Experiment experiment = new Experiment(deformationRoot, estimationRun);




//        String estimationRoot   = String.format( "%s/%s", deformationRoot, estimationRun );
//        String estimationConfig = String.format("%s/config.json", estimationRoot );
//        ScaleOptions config = ScaleOptions.createFromFile(estimationConfig);
//        int estimationReferenceStage = config.radii.length - 1;

//        String deformationBackwardScaledDir =
//                String.format( "%s/%s", estimationRoot, String.format( "reference-backward-scaled-to-%02d", estimationReferenceStage ) );
//        String deformationBackwardScaledPattern =
//                String.format( "%s/%s", deformationBackwardScaledDir, "%04d.tif" );

//        String deformationBackwardGradientScaledDir =
//                String.format( "%s/%s", estimationRoot, String.format( "reference-backward-gradient-scaled-to-%02d", estimationReferenceStage ) );

//        String deformationBackwardGradientScaledPattern =
//                String.format( "%s/%s", deformationBackwardGradientScaledDir, "%04d.tif" );
//
//
//        String estimationReferencePattern =
//                String.format( "%s/out/%02d/forward/%s", estimationRoot, estimationReferenceStage, "%04d.tif" );


        // create files and directories for reference (i.e. untransformed stack)
        createBaselineDirectoryAndFiles( experiment );

        // scale all estimates to highest res
        ScaleToDimension.scale( experiment.estimationRoot, experiment.config, 0, experiment.estimationReferenceStage );

        // scale ground truth backward to highest res
        String deformationInfoPath = deformationRoot + "/info.json";
        File f = new File( experiment.deformationInfoPath );
        JsonObject deformationInfo = new JsonParser().parse( new FileReader( f ) ).getAsJsonObject();
        double[] radii = new Gson().fromJson(deformationInfo.get("radii"), double[].class);
        double[] steps = new Gson().fromJson(deformationInfo.get("steps"), double[].class);
        ScaleTo.scale(
                experiment.deformationBackwardPattern,
                experiment.deformationBackwardScaledPattern,
                radii,
                steps,
                experiment.estimationReferencePattern,
                experiment.config,
                experiment.estimationReferenceStage
        );
//
        CreateGradients.create(
                experiment.deformationBackwardScaledDir,
                experiment.deformationBackwardGradientScaledPattern,
                experiment.estimationRoot,
                experiment.config,
                0.20,
                1.79,
                20,
                30
        );
//
//
        int off = 0;
        int windowRange = 100;
        CompareDeformations.nThreads = 4;
        CompareDeformations.comparePositions(
                experiment.deformationBackwardScaledPattern,
                experiment.estimationRoot,
                experiment.config,
                off,
                windowRange );
////
////        CompareDeformations.compareGradients( deformationBackwardGradientScaledPattern, estimationRoot, config );


//        // sigma is relative to resolution of maximum stage which is only a fraction of original image!
//        for ( double sigma : new double[] { 1e7 } )//5.0 , 10.0, 15.0, 20.0 } )
//        {
//            int step = 5;
//            System.out.println( sigma );
//            FlexibleComparison.evalSeries(
//                    experiment,
//                    new AffineModel1D(),
//                    sigma,
//                    sigma,
//                    sigma,
//                    step,
//                    step,
//                    step,
//                    40 // Runtime.getRuntime().availableProcessors() / 2
//            );
//            System.out.println( sigma );
//        }

        System.out.println("Done...");

    }


    public static void createBaselineDirectoryAndFiles (
            Experiment experiment
    )
    {
        String baseRefSize = String.format( "%s/out/%02d/forward/%04d.tif", experiment.estimationRoot, experiment.config.radii.length - 1, experiment.config.start );
        String base        = String.format("%s/out/%02d", experiment.estimationRoot, -1);

        String unscaledPattern = String.format("%s/forward/%s", base, "%04d.tif");
        String scaledPattern   = String.format("%s/forward-scaled-to-%02d/%s", base, experiment.config.radii.length - 1, "%04d.tif");

        ImagePlus dummy = new ImagePlus(String.format(baseRefSize));
        int width       = dummy.getWidth();
        int height      = dummy.getHeight();

        IO.createDirectoryForFile( String.format( unscaledPattern, experiment.config.start ) );
        IO.createDirectoryForFile( String.format( scaledPattern, experiment.config.start ) );

        for ( int z = experiment.config.start; z < experiment.config.stop; ++z )
        {
            RandomAccessibleOnRealRandomAccessible<FloatType> data =
                    Views.raster( new ConstantRealRandomAccesssible<FloatType>(2, new FloatType(z)) );
            IntervalView<FloatType> unscaled = Views.interval(data, new FinalInterval(1, 1));
            IntervalView<FloatType> scaled   = Views.interval(data, new FinalInterval(width, height));

            new FileSaver(ImageJFunctions.wrapFloat( scaled, "" )).saveAsTiff( String.format( scaledPattern, z ) );
            new FileSaver(ImageJFunctions.wrapFloat( unscaled, "" )).saveAsTiff( String.format( unscaledPattern, z ) );
        }
    }

}
