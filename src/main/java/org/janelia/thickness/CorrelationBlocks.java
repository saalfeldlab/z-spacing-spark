package org.janelia.thickness;

import java.io.Serializable;
import java.util.ArrayList;

import org.janelia.thickness.utility.Utility;

import scala.Tuple2;

/**
 * Created by hanslovskyp on 9/23/15.
 */
public class CorrelationBlocks implements Serializable
{
	private final Tuple2< Integer, Integer > radius;

	private final Tuple2< Integer, Integer > stride;

	public CorrelationBlocks( final int[] radius, final int[] stride )
	{
		this( Utility.tuple2( radius[ 0 ], radius[ 1 ] ), Utility.tuple2( stride[ 0 ], stride[ 1 ] ) );
	}

	public CorrelationBlocks( final Tuple2< Integer, Integer > radius, final int stride )
	{
		this( radius, Utility.tuple2( stride, stride ) );
	}

	public CorrelationBlocks( final Tuple2< Integer, Integer > radius, final Tuple2< Integer, Integer > stride )
	{
		this.radius = radius;
		this.stride = stride;
	}

	public Tuple2< Double, Double > translateCoordinateIntoThisBlockCoordinates( final CorrelationBlocks other, final Tuple2< Integer, Integer > localCoordinate )
	{
		return this.equals( other ) ? Utility.tuple2( localCoordinate._1().doubleValue(), localCoordinate._2().doubleValue() ) : Utility.tuple2( 0.0, 0.0 // TODO
		);
	}

	public Tuple2< Double, Double > translateCoordinateIntoThisBlockCoordinates( final Coordinate other )
	{
		return this.equals( other ) ? Utility.tuple2( other.getLocalCoordinates()._1().doubleValue(), other.getLocalCoordinates()._2().doubleValue() ) : Utility.tuple2( ( other.getWorldCoordinates()._1().doubleValue() - radius._1() ) / stride._1(), ( other.getWorldCoordinates()._2().doubleValue() - radius._2() ) / stride._2() );
	}

	public static interface Visitor
	{
		public void call( int x, int y, int xIndex, int yIndex, Tuple2< Integer, Integer > radius, Tuple2< Integer, Integer > stride );
	}

	public void iterateWithCallable( final int[] min, final int[] max, final Visitor func )
	{

		for ( int x = min[ 0 ] + radius._1(), xIndex = 0; x < max[ 0 ]; x += stride._1(), ++xIndex )
			for ( int y = min[ 1 ] + radius._2(), yIndex = 0; y < max[ 1 ]; y += stride._2(), ++yIndex )
				func.call( x, y, xIndex, yIndex, radius, stride );
	}

	public ArrayList< Coordinate > generateFromBoundingBox( final int[] max )
	{
		return generateFromBoundingBox( new int[] { 0, 0 }, max );
	}

	// always assume min == [ 0, 0 ]? YES!
	private ArrayList< Coordinate > generateFromBoundingBox( final int[] min, final int[] max )
	{
		final ArrayList< Coordinate > coordinates = new ArrayList< >();
		final Visitor visitor = ( x, y, xIndex, yIndex, radius, stride ) -> coordinates.add( new Coordinate( xIndex, yIndex, x, y ) );
//                ( x, y, xIndex, yIndex, radius, stride ) ->
//                { coordinates.add( new Coordinate( xIndex, yIndex, x, y ) ); return; };
		iterateWithCallable( min, max, visitor );

		return coordinates;
	}

	public Tuple2< Integer, Integer > worldToLocal( final Tuple2< Integer, Integer > worldCoordinates )
	{
		return Utility.tuple2( ( worldCoordinates._1() - this.radius._1() ) / this.stride._1(), ( worldCoordinates._2() - this.radius._2() ) / this.stride._2() );
	}

	@Override
	public boolean equals( final Object other )
	{
		return other instanceof CorrelationBlocks && ( ( CorrelationBlocks ) other ).radius.equals( radius ) && ( ( CorrelationBlocks ) other ).stride.equals( stride );
	}

	public class Coordinate implements Serializable
	{

		private final Tuple2< Integer, Integer > localCoordinates;

		private final Tuple2< Integer, Integer > worldCoordinates;

		public Coordinate( final int localCoordinate1, final int localCoordinate2, final int worldCoordinate1, final int worldCoordinate2 )
		{
			this( Utility.tuple2( localCoordinate1, localCoordinate2 ), Utility.tuple2( worldCoordinate1, worldCoordinate2 ) );
		}

		private Coordinate( final Tuple2< Integer, Integer > localCoordinates, final Tuple2< Integer, Integer > worldCoordinates )
		{
			this.localCoordinates = localCoordinates;
			this.worldCoordinates = worldCoordinates;
		}

		public Tuple2< Integer, Integer > getRadius()
		{
			return radius;
		}

		public Tuple2< Integer, Integer > getLocalCoordinates()
		{
			return localCoordinates;
		}

		public Tuple2< Integer, Integer > getWorldCoordinates()
		{
			return worldCoordinates;
		}

		@Override
		public boolean equals( final Object other )
		{
			return other instanceof Coordinate && ( ( Coordinate ) other ).worldCoordinates.equals( worldCoordinates ) && ( ( Coordinate ) other ).getRadius().equals( getRadius() );
		}

		@Override
		public int hashCode()
		{
			return worldCoordinates.hashCode() + getRadius().hashCode();
		}

		@Override
		public String toString()
		{
			return localCoordinates.toString() + worldCoordinates.toString();
		}

	}
}
