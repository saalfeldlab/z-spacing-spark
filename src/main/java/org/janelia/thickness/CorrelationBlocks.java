package org.janelia.thickness;

import org.janelia.thickness.utility.Utility;
import scala.Tuple2;

import java.io.Serializable;
import java.util.ArrayList;

/**
 * Created by hanslovskyp on 9/23/15.
 */
public class CorrelationBlocks implements Serializable
{
    /**
	 * 
	 */
	private static final long serialVersionUID = -8831385541854366385L;
	private final Tuple2< Integer, Integer > radius;
    private final Tuple2< Integer, Integer > stride;

    public CorrelationBlocks( int[] radius, int[] stride )
    {
        this(
                Utility.tuple2( radius[0], radius[1] ),
                Utility.tuple2( stride[0], stride[1] )
        );
    }

    public CorrelationBlocks( Tuple2<Integer, Integer> radius, final int stride )
    {
        this(radius, Utility.tuple2( stride, stride ) );
    }

    public CorrelationBlocks( Tuple2<Integer, Integer> radius, Tuple2< Integer, Integer > stride )
    {
        this.radius = radius;
        this.stride = stride;
    }

    public Tuple2< Double, Double > translateCoordinateIntoThisBlockCoordinates(
            Coordinate other
    )
    {
        return this.equals( other ) ?
                Utility.tuple2( other.getLocalCoordinates()._1().doubleValue(), other.getLocalCoordinates()._2().doubleValue() ) :
                Utility.tuple2(
                        ( other.getWorldCoordinates()._1().doubleValue() - radius._1() ) / stride._1(),
                        ( other.getWorldCoordinates()._2().doubleValue() - radius._2() ) / stride._2()
                )
                ;
    }

    public static interface Visitor
    {
        public void call( int x, int y, int xIndex, int yIndex, Tuple2< Integer, Integer > radius, Tuple2< Integer, Integer > stride );
    }

    public void iterateWithCallable( int[] min, int[] max, Visitor func )
    {

//        System.out.println( Arrays.toString(min) + " " + Arrays.toString( max ) );
        for( int x = min[0] + radius._1(), xIndex = 0; x < max[0]; x += stride._1(), ++xIndex )
        {
            for (int y = min[1] + radius._2(), yIndex = 0; y < max[1]; y += stride._2(), ++yIndex)
            {
//                System.out.println( x + " " + y + Arrays.toString( min ) + Arrays.toString( max ) );
                func.call(x, y, xIndex, yIndex, radius, stride);
//                if ( y + radius._2() >= max[1] )
//                    break;
            }
//            if ( x + radius._1() >= max[0] )
//                break;
        }
    }

    public ArrayList< Coordinate > generateFromBoundingBox( int[] max )
    {
        return generateFromBoundingBox( new int[] { 0, 0 }, max );
    }

    // always assume min == [ 0, 0 ]? YES!
    private ArrayList< Coordinate > generateFromBoundingBox( int[] min, int[] max )
    {
        final ArrayList<Coordinate> coordinates = new ArrayList<Coordinate>();
        Visitor visitor = new Visitor() {
            @Override
            public void call(int x, int y, int xIndex, int yIndex, Tuple2<Integer, Integer> radius, Tuple2<Integer, Integer> stride) {
                coordinates.add( new Coordinate( xIndex, yIndex, x, y ) );
            }
        };
//                ( x, y, xIndex, yIndex, radius, stride ) ->
//                { coordinates.add( new Coordinate( xIndex, yIndex, x, y ) ); return; };
        iterateWithCallable( min, max, visitor );

        return coordinates;
    }

    public Tuple2< Integer, Integer > worldToLocal( Tuple2< Integer, Integer > worldCoordinates )
    {
        return Utility.tuple2(
                ( worldCoordinates._1() - this.radius._1() ) / this.stride._1() ,
                ( worldCoordinates._2() - this.radius._2() ) / this.stride._2()
        );
    }

    @Override
    public boolean equals( Object other )
    {
        return other instanceof CorrelationBlocks &&
                ((CorrelationBlocks) other).radius.equals(radius) &&
                ((CorrelationBlocks) other).stride.equals( stride );
    }

    public class Coordinate implements Serializable
    {

        /**
		 * 
		 */
		private static final long serialVersionUID = -6001200020461134825L;
		private final Tuple2<Integer, Integer> localCoordinates;
        private final Tuple2<Integer, Integer> worldCoordinates;

        public Coordinate( int localCoordinate1, int localCoordinate2, int worldCoordinate1, int worldCoordinate2 )
        {
            this(
                    Utility.tuple2( localCoordinate1, localCoordinate2 ),
                    Utility.tuple2( worldCoordinate1, worldCoordinate2 )
            );
        }

        private Coordinate(Tuple2<Integer, Integer> localCoordinates, Tuple2<Integer, Integer> worldCoordinates) {
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
        public boolean equals( Object other )
        {
            return other instanceof Coordinate &&
                    ((Coordinate) other).worldCoordinates.equals(worldCoordinates) &&
                    ((Coordinate) other).getRadius().equals(getRadius());
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

    public static void main(String[] args) {

        int[] s1 = new int[]{234, 238};
        int[] r1 = new int[]{234, 238};

        int[] s2 = new int[]{117, 119};
        int[] r2 = new int[]{50, 60};

        int[] bb = new int[]{468, 477};

        CorrelationBlocks cbs1 = new CorrelationBlocks(r1, s1);
        CorrelationBlocks cbs2 = new CorrelationBlocks(r2, s2);

        ArrayList<Coordinate> cs1 = cbs1.generateFromBoundingBox(bb);
        ArrayList<Coordinate> cs2 = cbs2.generateFromBoundingBox(bb);

        for ( Coordinate c1 : cs1 )
            System.out.println( c1 );

        for ( Coordinate c2 : cs2 ) {
            System.out.println(c2);
            System.out.println( cbs1.translateCoordinateIntoThisBlockCoordinates( c2 ) );
        }

//        System.out.println( cbs2.translateCoordinateIntoThisBlockCoordinates( cs1.get( 0 ) ) );

    }
}
