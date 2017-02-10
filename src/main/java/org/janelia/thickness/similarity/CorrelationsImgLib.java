package org.janelia.thickness.similarity;

import net.imglib2.Interval;
import net.imglib2.RandomAccessible;
import net.imglib2.type.numeric.RealType;
import net.imglib2.util.Pair;
import net.imglib2.util.RealSum;
import net.imglib2.view.Views;

/**
 * @author Philipp Hanslovsky &lt;hanslovskyp@janelia.hhmi.org&gt;
 */
public class CorrelationsImgLib
{

	//	public static < V1 extends RealType< V1 >, V2 extends RealType< V2 >, W1 extends RealType< W1 >, W2 extends RealType< W2 > > CorrelationAndWeight calculate( final RandomAccessible< Pair< V1, W1 > > img1, final RandomAccessible< Pair< V2, W2 > > img2, final Interval interval )
	//	{
	//		final RealSum sumA = new RealSum();
	//		final RealSum sumAA = new RealSum();
	//		final RealSum sumB = new RealSum();
	//		final RealSum sumBB = new RealSum();
	//		final RealSum sumAB = new RealSum();
	//		double n = 0.0;
	//		for ( final Pair< Pair< V1, W1 >, Pair< V2, W2 > > p : Views.flatIterable( Views.interval( Views.pair( img1, img2 ), interval ) ) )
	//		{
	//			final Pair< V1, W1 > pa = p.getA();
	//			final Pair< V2, W2 > pb = p.getB();
	//			final double va = pa.getA().getRealDouble();
	//			final double vb = pb.getA().getRealDouble();
	//
	//			if ( Double.isNaN( va ) || Double.isNaN( vb ) )
	//				continue;
	//
	//			final double w = pa.getB().getRealDouble() * pb.getB().getRealDouble();
	//			n += w;
	//			sumA.add( w * va );
	//			sumAA.add( w * va * va );
	//			sumB.add( w * vb );
	//			sumBB.add( w * vb * vb );
	//			sumAB.add( w * va * vb );
	//		}
	//		final double suma = sumA.getSum();
	//		final double sumaa = sumAA.getSum();
	//		final double sumb = sumB.getSum();
	//		final double sumbb = sumBB.getSum();
	//		final double sumab = sumAB.getSum();
	//
	//		return new CorrelationAndWeight( ( n * sumab - suma * sumb ) / Math.sqrt( n * sumaa - suma * suma ) / Math.sqrt( n * sumbb - sumb * sumb ), n );
	//	}

	public static CorrelationAndWeight calculate( final RandomAccessible< ? extends Pair< ? extends RealType< ? >, ? extends RealType< ? > > > img1, final RandomAccessible< ? extends Pair< ? extends RealType< ? >, ? extends RealType< ? > > > img2, final Interval interval )
	{
		final RealSum sumA = new RealSum();
		final RealSum sumAA = new RealSum();
		final RealSum sumB = new RealSum();
		final RealSum sumBB = new RealSum();
		final RealSum sumAB = new RealSum();
		double n = 0.0;
		for ( final Pair< ? extends Pair< ? extends RealType< ? >, ? extends RealType< ? > >, ? extends Pair< ? extends RealType< ? >, ? extends RealType< ? > > > p : Views.flatIterable( Views.interval( Views.pair( img1, img2 ), interval ) ) )
		{
			final Pair< ? extends RealType< ? >, ? extends RealType< ? > > pa = p.getA();
			final Pair< ? extends RealType< ? >, ? extends RealType< ? > > pb = p.getB();
			final double va = pa.getA().getRealDouble();
			final double vb = pb.getA().getRealDouble();

			if ( Double.isNaN( va ) || Double.isNaN( vb ) )
				continue;

			final double w = pa.getB().getRealDouble() * pb.getB().getRealDouble();
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

		return new CorrelationAndWeight( ( n * sumab - suma * sumb ) / Math.sqrt( n * sumaa - suma * suma ) / Math.sqrt( n * sumbb - sumb * sumb ), n );
	}

	public static CorrelationAndWeight calculateNoRealSum( final RandomAccessible< ? extends Pair< ? extends RealType< ? >, ? extends RealType< ? > > > img1, final RandomAccessible< ? extends Pair< ? extends RealType< ? >, ? extends RealType< ? > > > img2, final Interval interval )
	{
		double sumA = 0.0;
		double sumAA = 0.0;
		double sumB = 0.0;
		double sumBB = 0.0;
		double sumAB = 0.0;
		double n = 0.0;
		for ( final Pair< ? extends Pair< ? extends RealType< ? >, ? extends RealType< ? > >, ? extends Pair< ? extends RealType< ? >, ? extends RealType< ? > > > p : Views.flatIterable( Views.interval( Views.pair( img1, img2 ), interval ) ) )
		{
			final Pair< ? extends RealType< ? >, ? extends RealType< ? > > pa = p.getA();
			final Pair< ? extends RealType< ? >, ? extends RealType< ? > > pb = p.getB();
			final double va = pa.getA().getRealDouble();
			final double vb = pb.getA().getRealDouble();

			if ( Double.isNaN( va ) || Double.isNaN( vb ) )
				continue;

			final double w = pa.getB().getRealDouble() * pb.getB().getRealDouble();
			n += w;
			sumA += w * va;
			sumAA += w * va * va;
			sumB += w * vb;
			sumBB += w * vb * vb;
			sumAB += w * va * vb;
		}
		final double suma = sumA;
		final double sumaa = sumAA;
		final double sumb = sumB;
		final double sumbb = sumBB;
		final double sumab = sumAB;

		return new CorrelationAndWeight( ( n * sumab - suma * sumb ) / Math.sqrt( n * sumaa - suma * suma ) / Math.sqrt( n * sumbb - sumb * sumb ), n );
	}

	public static void calculateNoRealSum( final RandomAccessible< ? extends Pair< ? extends RealType< ? >, ? extends RealType< ? > > > img1, final RandomAccessible< ? extends Pair< ? extends RealType< ? >, ? extends RealType< ? > > > img2, final Interval interval, final RealType< ? > corr, final RealType< ? > weight )
	{
		double sumA = 0.0;
		double sumAA = 0.0;
		double sumB = 0.0;
		double sumBB = 0.0;
		double sumAB = 0.0;
		double n = 0.0;
		for ( final Pair< ? extends Pair< ? extends RealType< ? >, ? extends RealType< ? > >, ? extends Pair< ? extends RealType< ? >, ? extends RealType< ? > > > p : Views.flatIterable( Views.interval( Views.pair( img1, img2 ), interval ) ) )
		{
			final Pair< ? extends RealType< ? >, ? extends RealType< ? > > pa = p.getA();
			final Pair< ? extends RealType< ? >, ? extends RealType< ? > > pb = p.getB();
			final double va = pa.getA().getRealDouble();
			final double vb = pb.getA().getRealDouble();

			if ( Double.isNaN( va ) || Double.isNaN( vb ) )
				continue;

			final double w = pa.getB().getRealDouble() * pb.getB().getRealDouble();
			n += w;
			sumA += w * va;
			sumAA += w * va * va;
			sumB += w * vb;
			sumBB += w * vb * vb;
			sumAB += w * va * vb;
		}
		final double suma = sumA;
		final double sumaa = sumAA;
		final double sumb = sumB;
		final double sumbb = sumBB;
		final double sumab = sumAB;

		corr.setReal( ( n * sumab - suma * sumb ) / Math.sqrt( n * sumaa - suma * suma ) / Math.sqrt( n * sumbb - sumb * sumb ) );
		weight.setReal( n );
	}

}
