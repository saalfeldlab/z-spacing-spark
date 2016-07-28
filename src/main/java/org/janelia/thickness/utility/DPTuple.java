package org.janelia.thickness.utility;

import net.imglib2.img.array.ArrayImg;
import net.imglib2.img.array.ArrayImgs;
import net.imglib2.img.basictypeaccess.array.DoubleArray;
import net.imglib2.type.numeric.real.DoubleType;

import java.io.Serializable;

/**
 * @author Philipp Hanslovsky &lt;hanslovskyp@janelia.hhmi.org&gt;
 */
public class DPTuple implements Serializable {

    public final double[] pixels;
    public final int width;
    public final int height;

    public DPTuple( int width, int height )
    {
        this( new double[ width*height ], width, height );
    }

    public DPTuple(double[] pixels, int width, int height) {
        this.pixels = pixels;
        this.width = width;
        this.height = height;
    }

    public DPTuple clone()
    {
        return new DPTuple( pixels.clone(), width, height );
    }


    public ArrayImg< DoubleType, DoubleArray> createDoubleArrayImg()
    {
        return ArrayImgs.doubles( pixels, width, height );
    }
}
