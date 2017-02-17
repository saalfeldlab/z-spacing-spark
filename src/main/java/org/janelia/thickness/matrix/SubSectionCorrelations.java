package org.janelia.thickness.matrix;

import java.lang.invoke.MethodHandles;
import java.util.ArrayList;
import java.util.Arrays;

import org.apache.log4j.Level;
import org.apache.log4j.LogManager;
import org.apache.log4j.Logger;
import org.apache.spark.api.java.function.PairFunction;
import org.apache.spark.broadcast.Broadcast;
import org.janelia.thickness.BlockCoordinates;
import org.janelia.thickness.BlockCoordinates.Coordinate;
import org.janelia.thickness.similarity.CorrelationsImgLib;
import org.janelia.thickness.similarity.ImageAndMask;

import ij.ImagePlus;
import net.imglib2.FinalInterval;
import net.imglib2.RandomAccessible;
import net.imglib2.img.Img;
import net.imglib2.img.array.ArrayImg;
import net.imglib2.img.array.ArrayImgs;
import net.imglib2.img.array.ArrayRandomAccess;
import net.imglib2.img.basictypeaccess.array.FloatArray;
import net.imglib2.img.display.imagej.ImageJFunctions;
import net.imglib2.type.numeric.RealType;
import net.imglib2.type.numeric.real.FloatType;
import net.imglib2.util.Intervals;
import net.imglib2.util.Pair;
import net.imglib2.view.Views;
import scala.Tuple2;

public class SubSectionCorrelations implements PairFunction< Tuple2< Tuple2< Integer, Integer >, Tuple2< ImageAndMask, ImageAndMask > >, Tuple2< Integer, Integer >, Tuple2< ArrayImg< FloatType, ? >, ArrayImg< FloatType, ? > >[] >
{
	public static final Logger LOG = LogManager.getLogger( MethodHandles.lookup().lookupClass() );
	static
	{
		LOG.setLevel( Level.INFO );
	}

	/**
	 *
	 */
	private static final long serialVersionUID = 4914446108059613538L;

	private final Broadcast< ArrayList< BlockCoordinates.Coordinate >[] > coordinates;

	private final Broadcast< int[] > dim;

	private final Broadcast< int[] > rs;


	public SubSectionCorrelations( final Broadcast< ArrayList< BlockCoordinates.Coordinate >[] > coordinates, final Broadcast< int[] > dim, final Broadcast< int[] > rs )
	{
		this.coordinates = coordinates;
		this.dim = dim;
		this.rs = rs;
	}

	@Override
	public Tuple2< Tuple2< Integer, Integer >, Tuple2< ArrayImg< FloatType, ? >, ArrayImg< FloatType, ? > >[] > call( final Tuple2< Tuple2< Integer, Integer >, Tuple2< ImageAndMask, ImageAndMask > > t ) throws Exception
	{
		final int z1 = t._1()._1();
		final int z2 = t._1()._2();
		final int dz = Math.abs( z2 - z1 );
		final ImageAndMask fp1 = t._2()._1();
		final ImageAndMask fp2 = t._2()._2();

		final int w = fp1.image.getWidth();
		final int h = fp1.image.getHeight();

		final int[] min = new int[] { 0, 0 };
		final int[] currentStart = new int[ 2 ];
		final int[] currentStop = new int[ 2 ];

		final Img< ? extends RealType< ? > > i1 = ImageJFunctions.wrapReal( new ImagePlus( "", fp1.image ) );
		final Img< ? extends RealType< ? > > i2 = ImageJFunctions.wrapReal( new ImagePlus( "", fp2.image ) );
		final Img< ? extends RealType< ? > > w1 = ImageJFunctions.wrapReal( new ImagePlus( "", fp1.mask ) );
		final Img< ? extends RealType< ? > > w2 = ImageJFunctions.wrapReal( new ImagePlus( "", fp2.mask ) );

		assert i1.dimension( 0 ) == w && i1.dimension( 1 ) == h;
		assert i2.dimension( 0 ) == w && i2.dimension( 1 ) == h;
		assert w1.dimension( 0 ) == w && w1.dimension( 1 ) == h;
		assert w2.dimension( 0 ) == w && w2.dimension( 1 ) == h;

		LOG.debug( "Calculating correlations for images and masks: " + Arrays.toString( Intervals.dimensionsAsLongArray( i1 ) ) + "\n" + Arrays.toString( Intervals.dimensionsAsLongArray( i2 ) ) + "\n" + Arrays.toString( Intervals.dimensionsAsLongArray( w1 ) ) + "\n" + Arrays.toString( Intervals.dimensionsAsLongArray( w2 ) ) + "\n" );

		final RandomAccessible< ? extends Pair< ? extends RealType< ? >, ? extends RealType< ? > > > p1 = Views.pair( i1, w1 );
		final RandomAccessible< ? extends Pair< ? extends RealType< ? >, ? extends RealType< ? > > > p2 = Views.pair( i2, w2 );

		final int[] dim = this.dim.getValue();


		final Tuple2< ArrayImg< FloatType, ? >, ArrayImg< FloatType, ? > >[] result = new Tuple2[ rs.getValue().length ];


		for ( int d = 0; d < result.length; ++d )
		{
			LOG.debug( dz + " " + rs.getValue()[ d ] );
			if ( dz > rs.getValue()[ d ] )
				result[ d ] = null;
			else {

				final ArrayList< Coordinate > coordinates = this.coordinates.getValue()[ d ];

				final int xMax = coordinates.stream().mapToInt( c -> c.getLocalCoordinates()._1() ).max().getAsInt();
				final int yMax = coordinates.stream().mapToInt( c -> c.getLocalCoordinates()._2() ).max().getAsInt();
				final int xDim = xMax + 1;
				final int yDim = yMax + 1;

				final float[] wData = new float[ xDim * yDim ];
				final float[] cData = new float[ xDim * yDim ];
				final ArrayImg< FloatType, FloatArray > weights = ArrayImgs.floats( wData, xDim, yDim );
				final ArrayImg< FloatType, FloatArray > corrs = ArrayImgs.floats( cData, xDim, yDim );

				final ArrayRandomAccess< FloatType > wAccess = weights.randomAccess();
				final ArrayRandomAccess< FloatType > cAccess = corrs.randomAccess();

				for ( final BlockCoordinates.Coordinate coord : coordinates )
				{
					final Tuple2< Integer, Integer > local = coord.getLocalCoordinates();
					final Tuple2< Integer, Integer > global = coord.getWorldCoordinates();
					final Tuple2< Integer, Integer > radius = coord.getRadius();
					currentStart[ 0 ] = Math.max( min[ 0 ], global._1() - radius._1() );
					currentStart[ 1 ] = Math.max( min[ 1 ], global._2() - radius._2() );
					currentStop[ 0 ] = Math.min( dim[ 0 ], global._1() + radius._1() );
					currentStop[ 1 ] = Math.min( dim[ 1 ], global._2() + radius._2() );
					final long[] currentMax = Arrays.stream( currentStop ).mapToLong( v -> v - 1 ).toArray();
					wAccess.setPosition( local._1(), 0 );
					wAccess.setPosition( local._2(), 1 );
					cAccess.setPosition( local._1(), 0 );
					cAccess.setPosition( local._2(), 1 );

					final FinalInterval interval = new FinalInterval( Arrays.stream( currentStart ).mapToLong( i -> i ).toArray(), currentMax );
					final long size = Intervals.numElements( interval );

					// LOG.debug( "Calculating coordinate for " +
					// Arrays.toString( Intervals.minAsLongArray( interval ) ) +
					// " " + Arrays.toString( Intervals.maxAsLongArray( interval
					// ) ) );
					// LOG.debug( "dims: " + Arrays.toString( dim ) + " " + xMax
					// + " " + yMax + " " + new Point( wAccess ) );
					// LOG.debug( Arrays.toString(
					// Intervals.dimensionsAsLongArray( i1 ) ) + " " +
					// Arrays.toString( Intervals.dimensionsAsLongArray( w1 ) )
					// );
					CorrelationsImgLib.calculateNoRealSum( p1, p2, interval, cAccess.get(), wAccess.get() );
					wAccess.get().mul( 1.0 / size );


				}
				result[ d ] = new Tuple2<>( corrs, weights );
				LOG.debug( "Returning " + Intervals.numElements( weights ) + " similarities with weight for level " + d );
			}
		}
		LOG.debug( Arrays.toString( result ) );
		return new Tuple2<>( t._1(), result );
	}
}