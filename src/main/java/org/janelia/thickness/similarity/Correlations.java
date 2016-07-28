package org.janelia.thickness.similarity;

import ij.process.FloatProcessor;
import net.imglib2.util.RealSum;

/**
 * @author Philipp Hanslovsky &lt;hanslovskyp@janelia.hhmi.org&gt;
 */
public class Correlations {

    public static double calculate( final FloatProcessor img1, final FloatProcessor img2 )
    {
        final RealSum sumA = new RealSum();
        final RealSum sumAA = new RealSum();
        final RealSum sumB = new RealSum();
        final RealSum sumBB = new RealSum();
        final RealSum sumAB = new RealSum();
        int n = 0;
        float[] d1 = (float[])img1.getPixels();
        float[] d2 = (float[])img2.getPixels();
        for ( int i = 0; i < d1.length; ++i )
        {
            final float va = d1[i];
            final float vb = d2[i];

            if ( Float.isNaN( va ) || Float.isNaN( vb ) )
                continue;
            ++n;
            sumA.add( va );
            sumAA.add( va * va );
            sumB.add( vb );
            sumBB.add( vb * vb );
            sumAB.add( va * vb );
        }
        final double suma = sumA.getSum();
        final double sumaa = sumAA.getSum();
        final double sumb = sumB.getSum();
        final double sumbb = sumBB.getSum();
        final double sumab = sumAB.getSum();

        return ( n * sumab - suma * sumb ) / Math.sqrt( n * sumaa - suma * suma ) / Math.sqrt( n * sumbb - sumb * sumb );
    }
}
