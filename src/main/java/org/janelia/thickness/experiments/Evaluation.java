package org.janelia.thickness.experiments;

import com.google.gson.Gson;
import com.google.gson.JsonObject;
import com.google.gson.JsonParser;
import org.janelia.thickness.ScaleOptions;

import java.io.File;
import java.io.FileNotFoundException;
import java.io.FileReader;

/**
 * Created by hanslovskyp on 2/2/16.
 */
public class Evaluation {

    public static void main(String[] args) throws FileNotFoundException, InterruptedException {

        String deformationRoot =
                "/nobackup/saalfeld/hanslovskyp/CutOn4-15-2013_ImagedOn1-27-2014/aligned/substacks/1300-3449" +
                "/4000x2500+5172+1416/downscale-z-by-4/corrected/z=[25,499]/z=[0,99]/downscale-xy-by-2/deformation/01";

        String deformationBackwardPattern = String.format( "%s/%s/%s", deformationRoot, "backward", "%04d.tif" );
        String deformationBackwardGradientPattern = String.format( "%s/%s/%s", deformationRoot, "", "%04d.tif" );


        String estimationRun    = "20160202_102949";
        String estimationRoot   = String.format( "%s/%s", deformationRoot, estimationRun );
        String estimationConfig = String.format("%s/config.json", estimationRoot );
        ScaleOptions config = ScaleOptions.createFromFile(estimationConfig);
        int estimationReferenceStage = config.radii.length - 1;

        String deformationBackwardScaledDir =
                String.format( "%s/%s", estimationRoot, String.format( "reference-backward-scaled-to-%02d", estimationReferenceStage ) );
        String deformationBackwardScaledPattern =
                String.format( "%s/%s", deformationBackwardScaledDir, "%04d.tif" );

        String deformationBackwardGradientScaledDir =
                String.format( "%s/%s", estimationRoot, String.format( "reference-backward-gradient-scaled-to-%02d", estimationReferenceStage ) );

        String deformationBackwardGradientScaledPattern =
                String.format( "%s/%s", deformationBackwardGradientScaledDir, "%04d.tif" );


        String estimationReferencePattern =
                String.format( "%s/out/%02d/forward/%s", estimationRoot, estimationReferenceStage, "%04d.tif" );

        // scale all estimates to highest res
        ScaleToDimension.scale( estimationRoot, config, 0, estimationReferenceStage );

        // scale ground truth backward to highest res
        String deformationInfoPath = deformationRoot + "/info.json";
        File f = new File( deformationInfoPath );
        JsonObject deformationInfo = new JsonParser().parse( new FileReader( f ) ).getAsJsonObject();
        double[] radii = new Gson().fromJson(deformationInfo.get("radii"), double[].class);
        double[] steps = new Gson().fromJson(deformationInfo.get("steps"), double[].class);
        ScaleTo.scale(
                deformationBackwardPattern,
                deformationBackwardScaledPattern,
                radii,
                steps,
                estimationReferencePattern,
                config,
                estimationReferenceStage
        );

        CreateGradients.create(
                deformationBackwardScaledDir,
                deformationBackwardGradientScaledPattern,
                estimationRoot,
                config,
                0.20,
                1.79,
                30,
                30
        );


        int off = 0;
        int windowRange = 20;
        CompareDeformations.nThreads = 4;
        CompareDeformations.comparePositions( deformationBackwardScaledPattern, estimationRoot, config, off, windowRange );

        CompareDeformations.compareGradients( deformationBackwardGradientScaledPattern, estimationRoot, config );



    }

}
