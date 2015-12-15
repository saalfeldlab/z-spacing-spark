package org.janelia.thickness;

import scala.Tuple2;

import java.io.Serializable;
import java.util.ArrayList;

/**
 * Created by hanslovskyp on 9/23/15.
 */
public class DataBlocks implements Serializable {

    private final Tuple2< Integer, Integer > stride;

    public DataBlocks(Tuple2<Integer, Integer> stride ) {
        this.stride = stride;
    }

    

    public static interface Visitor
    {
        public void call( int x, int y, int xIndex, int yIndex, Tuple2< Integer, Integer > stride );
    }

    public void iterateWithCallable( int[] min, int[] max, Visitor func )
    {
        for( int x = min[0], xIndex = min[0] / stride._1(); x < max[0]; x += stride._1(), ++xIndex )
            for (int y = min[1], yIndex = min[1] / stride._2(); y < max[1]; y += stride._2(), ++yIndex)
                func.call( x, y, xIndex, yIndex, stride );
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
            public void call(int x, int y, int xIndex, int yIndex, Tuple2<Integer, Integer> stride) {
                coordinates.add( new Coordinate( xIndex, yIndex, x, y ) );
            }
        };
//                ( x, y, xIndex, yIndex, stride ) ->
//                { coordinates.add( new Coordinate( xIndex, yIndex, x, y ) ); return; };
        iterateWithCallable( min, max, visitor );

        return coordinates;
    }

    public Tuple2< Integer, Integer > getNBlocks( int[] max )
    {
        return getNBlocks( new int[] { 0, 0 }, max );
    }

    public Tuple2< Integer, Integer > getNBlocks( int[] min, int[] max )
    {
        int[] snapMinToBlock = new int[]{
                (min[0] / stride._1()) * stride._1(),
                (min[1] / stride._2()) * stride._2()
        };
        Tuple2<Integer, Integer> result = Utility.tuple2(
                (int) Math.ceil((max[0] - min[0] ) * 1.0 / stride._1()),
                (int) Math.ceil((max[1] - min[1] ) * 1.0 / stride._2())
        );
        return result;
    }

    public ArrayList<Tuple2<Integer,Integer>> getCoverageTuplesLocalCoordinates(Tuple2<Integer, Integer> worldMin, Tuple2<Integer, Integer> worldMax )
    {
        return getCoverageTuplesLocalCoordinates( new int[] { worldMin._1(), worldMin._2() }, new int[] { worldMax._1(), worldMax._2() } );
    }

    public ArrayList<Tuple2<Integer,Integer>> getCoverageTuplesLocalCoordinates( int[] worldMin, int[] worldMax )
    {
        final ArrayList<Tuple2<Integer,Integer>> result = new ArrayList<Tuple2<Integer,Integer>>();

        int[] blockedWorldMin = new int[]{
                ( worldMin[0] / stride._1() ) * stride._1(),
                ( worldMin[1] / stride._2() ) * stride._2()
        };

        Visitor visitor = new Visitor() {
            @Override
            public void call(int x, int y, int xIndex, int yIndex, Tuple2<Integer, Integer> stride) {
                result.add( Utility.tuple2( xIndex, yIndex ) );
            }
        };
//                ( x, y, xIndex, yIndex, stride ) ->
//                { result.add( Utility.tuple2( xIndex, yIndex ) ); return; };

        iterateWithCallable( blockedWorldMin, worldMax, visitor );

        return result;
    }

    public static void main( String[] args )
    {
        int x = 170;
        int y = 190;
        int radius = 50;
        Tuple2< Integer, Integer > stride = Utility.tuple2( 32, 32 );
        Tuple2<Integer, Integer> pos = Utility.tuple2(x, y);
        Tuple2<Integer, Integer> min = Utility.tuple2(x - radius, y - radius);
        Tuple2<Integer, Integer> max = Utility.tuple2(x + radius, y + radius);
        new DataBlocks( stride ).getCoverageTuplesLocalCoordinates( min, max );
    }


    public class Coordinate implements Serializable
    {
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

        public Tuple2< Integer, Integer > getStride()
        {
            return stride;
        }

        public Tuple2< Integer, Integer> getLocalCoordinates()
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
                    ((Coordinate) other).getStride().equals(getStride());
        }

        @Override
        public int hashCode()
        {
            return worldCoordinates.hashCode() + getStride().hashCode();
        }

        @Override
        public String toString()
        {
            return localCoordinates.toString() + worldCoordinates.toString();
        }
    }
}
