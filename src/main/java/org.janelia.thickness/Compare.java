package org.janelia.thickness;

import com.google.gson.Gson;
import com.google.gson.JsonArray;
import com.google.gson.JsonObject;
import com.google.gson.JsonParser;
import ij.ImageJ;
import ij.ImagePlus;
import ij.plugin.FolderOpener;
import mpicbg.models.*;
import net.imglib2.Cursor;
import net.imglib2.IterableInterval;
import net.imglib2.RealPoint;
import net.imglib2.converter.RealDoubleConverter;
import net.imglib2.converter.read.ConvertedRandomAccessibleInterval;
import net.imglib2.img.Img;
import net.imglib2.img.array.ArrayCursor;
import net.imglib2.img.array.ArrayImg;
import net.imglib2.img.array.ArrayImgs;
import net.imglib2.img.basictypeaccess.array.FloatArray;
import net.imglib2.img.display.imagej.ImageJFunctions;
import net.imglib2.interpolation.randomaccess.NLinearInterpolatorFactory;
import net.imglib2.realtransform.InverseRealTransform;
import net.imglib2.realtransform.RealTransformRealRandomAccessible;
import net.imglib2.realtransform.RealViews;
import net.imglib2.type.numeric.real.DoubleType;
import net.imglib2.type.numeric.real.FloatType;
import net.imglib2.view.IntervalView;
import net.imglib2.view.Views;
import net.imglib2.view.composite.RealComposite;
import org.janelia.thickness.lut.SingleDimensionLUTGrid;
import org.janelia.utility.realtransform.ScaleAndShift;

import java.io.FileNotFoundException;
import java.io.FileReader;
import java.util.ArrayList;
import java.util.Arrays;

/**
 * Created by hanslovskyp on 10/16/15.
 */
public class Compare {

    public static void main(String[] args) throws FileNotFoundException, NotEnoughDataPointsException, IllDefinedDataPointsException {
        new ImageJ();
        ImagePlus distortion =
                new FolderOpener().openFolder("/home/hanslovskyp/workspace-idea/z_spacing-spark-scala/synthetic-distortion/distortion");
        ImagePlus estimate =
                new FolderOpener().openFolder("/home/hanslovskyp/workspace-idea/z_spacing-spark-scala/synthetic-distortion/00/out/spark-test-2/02/forward");
        JsonObject config =
                new JsonParser().parse(new FileReader("/home/hanslovskyp/workspace-idea/z_spacing-spark-scala/synthetic-distortion/00/config.json")).getAsJsonObject();
        JsonArray options = config.get("options").getAsJsonArray();
        double[] steps = new Gson().fromJson(options.get(options.size() - 1).getAsJsonObject().get("steps"), double[].class);
        double[] radii = new Gson().fromJson(options.get(options.size() - 1).getAsJsonObject().get("radii"), double[].class);
        System.out.println( Arrays.toString( steps ) + " " + Arrays.toString( radii ) );
        ScaleAndShift transform = new ScaleAndShift(new double[] { steps[0], steps[1], 1 }, new double[] { radii[0], radii[1], 0 } );
        RealTransformRealRandomAccessible<FloatType, InverseRealTransform> transformed = RealViews.transformReal(
                Views.interpolate(
                        Views.extendBorder(ImageJFunctions.wrapFloat(estimate)),
                        new NLinearInterpolatorFactory<FloatType>()),
                transform);
        ArrayImg<FloatType, FloatArray> target = ArrayImgs.floats(distortion.getWidth(), distortion.getHeight(), distortion.getStackSize());

        for(
                Cursor<RealComposite<FloatType>> s = Views.flatIterable( Views.collapseReal( Views.interval( Views.raster( transformed ), ImageJFunctions.wrapFloat( distortion ) ) ) ).cursor(),
                t = Views.flatIterable( Views.collapseReal( target ) ).cursor();
                s.hasNext();
                ) {
            RealComposite<FloatType> src = s.next();
            RealComposite<FloatType> trg = t.next();
            for( int z = 0; z < distortion.getStackSize(); ++z )
            {
                trg.get( z ).set( src.get( z ) );
            }
        }

        Img<FloatType> distortionImg = ImageJFunctions.wrapFloat(distortion);
        double min = Double.MAX_VALUE;
        double max = -Double.MAX_VALUE;
        for( FloatType d : distortionImg )
        {
            float val = d.get();
            if( !Float.isNaN(val) && !Float.isInfinite(val))
            {
                min = Math.min( min, val );
                max = Math.max( max, val );
            }
        }

        int size = (int) Math.ceil(max - min);
        ArrayImg<FloatType, FloatArray> inverseDistortion = ArrayImgs.floats(distortion.getWidth(), distortion.getHeight(), size);

        for( Cursor<RealComposite<FloatType>> iD = Views.flatIterable(Views.collapseReal(inverseDistortion)).cursor();
        iD.hasNext(); )
        {
            RealComposite<FloatType> col = iD.next();
            long x = iD.getLongPosition(0);
            long y = iD.getLongPosition(1);
            IntervalView<FloatType> lut = Views.offsetInterval(distortionImg, new long[]{x, y, 0}, new long[]{1, 1, size});
            SingleDimensionLUTGrid tf = new SingleDimensionLUTGrid(
                    3,
                    3,
                    new ConvertedRandomAccessibleInterval<FloatType, DoubleType>(lut, new RealDoubleConverter<FloatType>(), new DoubleType()),
                    2);
            RealPoint point = new RealPoint(new double[]{x, y, 0});
            for( int z = 0; z < size; ++z )
            {
                point.setPosition( z, 2 );
                tf.applyInverse( point, point );
                col.get( z ).setReal( point.getDoublePosition( 2 ) );
            }
        }


        ImageJFunctions.show( target, "estimate" );
        ImageJFunctions.show( inverseDistortion, "inverse distortion" );
//        distortion.show();
//        estimate.show();

        ArrayList<PointMatch> matches = new ArrayList<PointMatch>();


        for (
                Cursor<FloatType> ref = Views.flatIterable( ImageJFunctions.wrapFloat( distortion ) ).cursor(),
                comp = target.cursor();
                ref.hasNext();
                )
        {
            float r = ref.next().get();
            float c = comp.next().get();
            if( Float.isNaN( c  ) )
                continue;
            matches.add( new PointMatch( new Point( new double[] { c } ), new Point( new double[] { r } ) ) );
        }

        AffineModel1D m = new AffineModel1D();
        m.fit(matches);
        double[] matrix = m.getMatrix(new double[2]);
        System.out.println( Arrays.toString ( matrix ) );

        ArrayImg<FloatType, ?> affineTransformed = target.copy();
        for(
                ArrayCursor<FloatType> c1 = target.cursor(),
                c2 = affineTransformed.cursor();
                c1.hasNext();
                )
        {
            c2.next().setReal(m.apply(new double[]{c1.next().get()})[0]);
        }

//        ImageJFunctions.show( affineTransformed, "affine 'n' shit" );

    }

}
