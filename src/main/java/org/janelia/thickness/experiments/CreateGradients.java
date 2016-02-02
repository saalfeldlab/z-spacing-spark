package org.janelia.thickness.experiments;

import ij.IJ;
import ij.ImageJ;
import ij.ImagePlus;
import ij.WindowManager;
import ij.io.FileSaver;
import ij.plugin.FolderOpener;
import ij.process.ColorProcessor;
import ij.process.FloatProcessor;
import org.janelia.thickness.ScaleOptions;
import org.janelia.utility.io.IO;
import org.scijava.jython.shaded.com.kenai.jaffl.struct.Struct;

import java.io.FileNotFoundException;

/**
 * Created by phil on 1/26/16.
 */
public class CreateGradients {

    public static void create(
            String gt,
            String gtOut,
            String base,
            ScaleOptions config,
            double contrastMin,
            double contrastMax,
            int sectionIndex,
            int spacing
    )
    {
        int refStage = config.radii.length - 1;

//        String gtOut = String.format( "%s/backward-gradient-scaled-to-%02d/%s", root, 7, "%04d.tif" );
        String componentIn = String.format( "/forward-scaled-to-%02d", refStage );
        String componentOut = String.format( "/forward-gradient-scaled-to-%02d/%s", refStage, "%04d.tif" );

        int start = config.start;
        int stop  = config.stop;
        int width = new ImagePlus(String.format("%s/out/%02d/%s/%04d.tif", base, refStage, componentIn, start) ).getWidth();
//        double contrastMin = 0.20;
//        double contrastMax = 1.79;
        new ImageJ();
        int nImages = config.radii.length + 1;
        int[] targetDim = new int[]{(nImages + 0) * width + (nImages - 1) * spacing, (stop - start)};
        FloatProcessor targetFP = new FloatProcessor(targetDim[0], targetDim[1]);
        targetFP.add( Double.NaN );
        String collagePath = String.format( "%s/%s", base, "collage.tif" );
        String collageRGBPath = String.format( "%s/%s", base, "collage-rgb.png" );
        for ( int i = 0; i < nImages; ++i )
        {
            System.out.println( i + " " + config.radii.length );
            String currentBase = String.format( "%s/out/%02d", base, i);
            ImagePlus imp = i == config.radii.length ?
                    new FolderOpener().openFolder( gt ) :
                    new FolderOpener().openFolder(currentBase + componentIn);
            imp.show();
            IJ.run("Reslice [/]...", "output=1.000 start=Top avoid");
            ImagePlus resliced = IJ.getImage();
            IJ.run("Convolve...", "text1=-0.5\n0\n0.5\n stack");
            ImagePlus convolved = IJ.getImage();
            IJ.run("Reslice [/]...", "output=1.000 start=Top avoid");
            ImagePlus result = IJ.getImage();

            FloatProcessor selectedSlice = (FloatProcessor)convolved.getStack().getProcessor( sectionIndex + 1 );
            int currentOff = (spacing + width) * i;
            for ( int y = 0; y < selectedSlice.getHeight(); ++y )
                for ( int x = 0; x < selectedSlice.getWidth(); ++x )
                    targetFP.setf( currentOff + x, y, selectedSlice.getf(x, y) );

            for ( int z = start; z < stop; ++z )
            {
                String outputPath = String.format( i == config.radii.length ? gtOut : currentBase + componentOut, z );
                IO.createDirectoryForFile( outputPath );
                FloatProcessor fp = (FloatProcessor)result.getStack().getProcessor( z + 1 );
                fp.setMinAndMax( contrastMin, contrastMax );
                new FileSaver( new ImagePlus( "", fp ) ).saveAsTiff( outputPath );
            }

            imp.changes = false;
            imp.close();
            resliced.changes = false;
            resliced.close();
            convolved.changes = false;
            convolved.close();
            result.changes = false;
            result.close();

        }
        targetFP.setMinAndMax( contrastMin, contrastMax );
        new FileSaver( new ImagePlus( "", targetFP ) ).saveAsTiff( collagePath );
        ColorProcessor targetFPRGB = targetFP.duplicate().convertToColorProcessor();
        for ( int i = 0; i < config.radii.length; ++i )
        {
            int currentOff = width + ( spacing + width ) * i;
            for ( int y = 0; y < targetFPRGB.getHeight(); ++y )
                for ( int x = 0; x < spacing; ++x )
                    targetFPRGB.setf( currentOff + x, y, 0xff0000 );
        }
        new FileSaver( new ImagePlus( "", targetFPRGB ) ).saveAsPng( collageRGBPath );
        System.out.println("Done!");
    }

    public static void main(String[] args) throws FileNotFoundException {
        int refStage = 7;
        String root = "/nobackup/saalfeld/hanslovskyp/CutOn4-15-2013_ImagedOn1-27-2014/aligned/substacks/1300-3449" +
                "/4000x2500+5172+1416/downscale-z-by-4/corrected/z=[25,499]/z=[0,99]/deformation/01";
        String run = "20160129_135000";
        String base = String.format( "%s/%s", root, run );
        ScaleOptions config = ScaleOptions.createFromFile(base + "/config.json");
        String gt = String.format( "%s/backward-scaled-to-%02d", root, 7 );
        String gtOut = String.format( "%s/backward-gradient-scaled-to-%02d/%s", root, 7, "%04d.tif" );

        create( gt, gtOut, base, config, 0.20, 1.79, 30, 30 );
    }

}
