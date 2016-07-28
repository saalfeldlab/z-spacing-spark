package org.janelia.thickness;

import java.util.ArrayList;

import org.janelia.thickness.BlockCoordinates.Coordinate;
import org.janelia.thickness.utility.Utility;
import org.junit.Assert;
import org.junit.Test;

import net.imglib2.RealPoint;
import net.imglib2.realtransform.ScaleAndTranslation;

/**
 * 
 * @author Philipp Hanslovsky &lt;hanslovskyp@janelia.hhmi.org&gt;
 *
 */
public class BlockCoordinatesTest {

	@Test
	public void test() {
        int[] s1 = new int[]{234, 238};
        int[] r1 = new int[]{234, 238};
        ScaleAndTranslation tf1 = new ScaleAndTranslation( new double[] { 1.0 * s1[0], 1.0 * s1[1] }, new double[] { r1[0], r1[1] } );

        int[] s2 = new int[]{117, 119};
        int[] r2 = new int[]{50, 60};
        ScaleAndTranslation tf2 = new ScaleAndTranslation( new double[] { 1.0 * s2[0], 1.0 * s2[1] }, new double[] { r2[0], r2[1] } );

        int[] bb = new int[]{468, 477};

        BlockCoordinates cbs1 = new BlockCoordinates(r1, s1);
        BlockCoordinates cbs2 = new BlockCoordinates(r2, s2);

        ArrayList<Coordinate> cs1 = cbs1.generateFromBoundingBox(bb);
        ArrayList<Coordinate> cs2 = cbs2.generateFromBoundingBox(bb);
        
        RealPoint p = new RealPoint(2);

        for ( Coordinate c1 : cs1 ) {
        	int[] w = Utility.arrayInt(c1.getWorldCoordinates());
            p.setPosition( Utility.arrayInt( c1.getLocalCoordinates() ) );
            tf1.apply(p, p);
            for ( int d = 0; d < w.length; ++d )
            	Assert.assertEquals( p.getDoublePosition( d ), w[d], 1e-10 );
            
            tf2.applyInverse( p, p );
            double[] w2 = Utility.arrayDouble( cbs2.translateOtherLocalCoordiantesIntoLocalSpace( c1 ) );
            for ( int d = 0; d < w.length; ++d )
            	Assert.assertEquals( p.getDoublePosition( d ), w2[d], 1e-10 );
            
        }

        for ( Coordinate c2 : cs2 ) {
        	int[] w = Utility.arrayInt(c2.getWorldCoordinates());
            p.setPosition( Utility.arrayInt( c2.getLocalCoordinates() ) );
            tf2.apply(p, p);
            for ( int d = 0; d < w.length; ++d )
            	Assert.assertEquals( p.getDoublePosition( d ), w[d], 1e-10 );
            
            tf1.applyInverse( p, p );
            double[] w2 = Utility.arrayDouble( cbs1.translateOtherLocalCoordiantesIntoLocalSpace( c2 ) );
            for ( int d = 0; d < w.length; ++d )
            	Assert.assertEquals( p.getDoublePosition( d ), w2[d], 1e-10 );
        }

	}

}
