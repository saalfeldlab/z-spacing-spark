package org.janelia.thickness;

import org.janelia.thickness.utility.Utility;
import scala.Tuple2;

import java.io.Serializable;
import java.util.ArrayList;

/**
 * @author Philipp Hanslovsky &lt;hanslovskyp@janelia.hhmi.org&gt;
 * 
 * Manage coordinates of blocks defined by radius and stride that are centered on integer positions of the original grid. 
 * 
 */
public class BlockCoordinates implements Serializable
{
    /**
	 * 
	 */
	private static final long serialVersionUID = -8831385541854366385L;
	private final Tuple2< Integer, Integer > radius;
    private final Tuple2< Integer, Integer > stride;

    public BlockCoordinates( int[] radius, int[] stride )
    {
        this(
                Utility.tuple2( radius[0], radius[1] ),
                Utility.tuple2( stride[0], stride[1] )
        );
    }

    public BlockCoordinates( Tuple2<Integer, Integer> radius, final int stride )
    {
        this(radius, Utility.tuple2( stride, stride ) );
    }

    public BlockCoordinates( Tuple2<Integer, Integer> radius, Tuple2< Integer, Integer > stride )
    {
        this.radius = radius;
        this.stride = stride;
    }

    /**
     * 
     * Translate other coordinate into this local coordinate space.
     * 
     * @param other coordinate, potentially from different block
     * @return Other coordinate in this local space (potentially off-grid).
     */
    public Tuple2< Double, Double > translateOtherLocalCoordiantesIntoLocalSpace (
            Coordinate other
    )
    {
        Tuple2<Integer, Integer> wc = other.getWorldCoordinates();
        return Utility.tuple2(
        		( wc._1().doubleValue() - radius._1() ) / stride._1(),
                ( wc._2().doubleValue() - radius._2() ) / stride._2() 
                );
    }

    
    /**
     * 
     * Generate coordinates from bounding box. Use {@link Generator} to specify how coordinates should be stored.
     *
     */
    public static interface Generator
    {
        public void call( int x, int y, int xIndex, int yIndex, Tuple2< Integer, Integer > radius, Tuple2< Integer, Integer > stride );
    }
    
    public void generate( int[] stop, Generator func ) {
    	generate( new int[] { 0, 0 }, stop, func );    	
    }

    private void generate( int[] start, int[] stop, Generator func )
    {
        for( int x = start[0] + radius._1(), xIndex = 0; x < stop[0]; x += stride._1(), ++xIndex )
        {
            for (int y = start[1] + radius._2(), yIndex = 0; y < stop[1]; y += stride._2(), ++yIndex)
            {
                func.call(x, y, xIndex, yIndex, radius, stride);
            }
        }
    }

    public ArrayList< Coordinate > generateFromBoundingBox( int[] stop )
    {
        return generateFromBoundingBox( new int[] { 0, 0 }, stop );
    }

    // always assume min == [ 0, 0 ]? YES!
    private ArrayList< Coordinate > generateFromBoundingBox( int[] start, int[] stop )
    {
        final ArrayList<Coordinate> coordinates = new ArrayList<Coordinate>();
        Generator generator = new Generator() {
            @Override
            public void call(int x, int y, int xIndex, int yIndex, Tuple2<Integer, Integer> radius, Tuple2<Integer, Integer> stride) {
                coordinates.add( new Coordinate( Utility.tuple2( xIndex, yIndex), Utility.tuple2( x, y ) ) );
            }
        };
        generate( start, stop, generator );
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
        return other instanceof BlockCoordinates &&
                ((BlockCoordinates) other).radius.equals(radius) &&
                ((BlockCoordinates) other).stride.equals( stride );
    }

    
    /**
     * 
     * Store local and world position.
     * 
     * @author Philipp Hanslovsky &lt;hanslovskyp@janelia.hhmi.org&gt;
     *
     */
    public class Coordinate implements Serializable
    {

        /**
		 * 
		 */
		private static final long serialVersionUID = -6001200020461134825L;
		private final Tuple2<Integer, Integer> localCoordinates;
        private final Tuple2<Integer, Integer> worldCoordinates;


        private Coordinate(Tuple2<Integer, Integer> localCoordinates, Tuple2<Integer, Integer> worldCoordinates) {
            this.localCoordinates = localCoordinates;
            this.worldCoordinates = worldCoordinates;
        }

        public Tuple2< Integer, Integer > getRadius()
        {
            return BlockCoordinates.this.radius;
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
                    ((Coordinate) other).localCoordinates.equals(localCoordinates);
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
