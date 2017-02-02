package org.janelia.thickness.similarity;

import net.imglib2.util.RealSum;

/**
 * @author Philipp Hanslovsky &lt;hanslovskyp@janelia.hhmi.org&gt;
 */
public class Correlations
{

	public static double calculate( final ImageAndMask img1, final ImageAndMask img2 )
	{
		final RealSum sumA = new RealSum();
		final RealSum sumAA = new RealSum();
		final RealSum sumB = new RealSum();
		final RealSum sumBB = new RealSum();
		final RealSum sumAB = new RealSum();
		double n = 0.0;
		final float[] d1 = ( float[] ) img1.image.getPixels();
		final float[] d2 = ( float[] ) img2.image.getPixels();
		final float[] m1 = ( float[] ) img1.mask.getPixels();
		final float[] m2 = ( float[] ) img2.mask.getPixels();
		for ( int i = 0; i < d1.length; ++i )
		{
			final float va = d1[ i ];
			final float vb = d2[ i ];

			if ( Float.isNaN( va ) || Float.isNaN( vb ) )
				continue;

			final double w = m1[ i ] * m2[ i ];
			n += w;
			sumA.add( w * va );
			sumAA.add( w * va * va );
			sumB.add( w * vb );
			sumBB.add( w * vb * vb );
			sumAB.add( w * va * vb );
		}
		final double suma = sumA.getSum();
		final double sumaa = sumAA.getSum();
		final double sumb = sumB.getSum();
		final double sumbb = sumBB.getSum();
		final double sumab = sumAB.getSum();

		return ( n * sumab - suma * sumb ) / Math.sqrt( n * sumaa - suma * suma ) / Math.sqrt( n * sumbb - sumb * sumb );
	}
}
