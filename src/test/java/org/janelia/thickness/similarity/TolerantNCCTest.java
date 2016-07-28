package org.janelia.thickness.similarity;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertNotEquals;
import static org.junit.Assert.assertNotNull;
import static org.junit.Assert.assertNull;
import static org.junit.Assert.assertTrue;

import java.util.HashMap;

import org.janelia.thickness.utility.Utility;
import org.junit.Test;

import ij.ImagePlus;
import ij.process.FloatProcessor;
import mpicbg.ij.integral.BlockPMCC;
import scala.Tuple2;

/**
 * 
 * @author Philipp Hanslovsky &lt;hanslovskyp@janelia.hhmi.org&gt;
 *
 */
public class TolerantNCCTest {

	@Test
	public void testNoOffset() {
		String base = "/nobackup/saalfeld/hanslovskyp/CutOn4-15-2013_ImagedOn1-27-2014/aligned/substacks/1300-3449/4000x2500+5172+1416/downscale-by-2/1000x600x800+500+312+0";
		String pathFixed = base + "/data/0000.tif";
		String pathMoving = base + "/0001-warped.tif";

		FloatProcessor fixed = new ImagePlus(pathFixed).getProcessor().convertToFloatProcessor();
		FloatProcessor moving = new ImagePlus(pathMoving).getProcessor().convertToFloatProcessor();
		
		for ( int i = 2; i < 10; ++i )
		{

			int[] blockRadius = new int[] { fixed.getWidth()/i, fixed.getHeight()/i };
			noOffset( fixed, moving, blockRadius );
			
		}
	}
	
	
	public void noOffset(
			final FloatProcessor fixed,
			final FloatProcessor moving,
			final int[] blockRadius
			) {

		int[] maxDistance = new int[] { 0, 0 };
		int[] averageBlockSize = { 0, 0 };
		int[] averageBlockStep = { 1, 1 };

		Tuple2<FloatProcessor, FloatProcessor> maxCorrs = TolerantNCC.tolerantNCC(fixed, moving, maxDistance, blockRadius, 0, 0);
		HashMap<Tuple2<Integer, Integer>, Double> hm = TolerantNCC.average(maxCorrs._1(), maxCorrs._2(), averageBlockSize, averageBlockStep);

		BlockPMCC pmcc = new BlockPMCC(fixed, moving);
		pmcc.setOffset( 0, 0 );
		pmcc.r( blockRadius[0], blockRadius[1] );
	        
		FloatProcessor maxCorrsFp = maxCorrs._1();
		final int w = fixed.getWidth();
		final int h = fixed.getHeight();
		final int wb = w - blockRadius[0];
		final int hb = h - blockRadius[1];
		FloatProcessor tp = pmcc.getTargetProcessor();
		for ( int y = 0; y < h; ++y )
		{
			for ( int x = 0; x < w; ++x )
			{
				final float val = maxCorrsFp.getf( x, y );
				if ( x < blockRadius[0] || y < blockRadius[1] || x > wb || y > hb ) {
					assertEquals( 0.0f, val, 0.0f );
					assertNull( hm.get( Utility.tuple2( x, y ) ) );
				}
				else {
					assertEquals( tp.getf( x, y ), val, 0.0f );
				}
			}
		}
	}
	
	
	@Test
	public void testOffsetSameImages() {
		String base = "/nobackup/saalfeld/hanslovskyp/CutOn4-15-2013_ImagedOn1-27-2014/aligned/substacks/1300-3449/4000x2500+5172+1416/downscale-by-2/1000x600x800+500+312+0";
		String pathFixed = base + "/data/0000.tif";
		String pathMoving = base + "/data/0000.tif";
		testOffset( pathFixed, pathMoving );
	}
	
	@Test
	public void testOffsetDifferentImages() {
		String base = "/nobackup/saalfeld/hanslovskyp/CutOn4-15-2013_ImagedOn1-27-2014/aligned/substacks/1300-3449/4000x2500+5172+1416/downscale-by-2/1000x600x800+500+312+0";
		String pathFixed = base + "/data/0000.tif";
		String pathMoving = base + "/0001-warped.tif";
		testOffset( pathFixed, pathMoving );
	}
	
	public void testOffset(String pathFixed, String pathMoving ) {

		FloatProcessor fixed = new ImagePlus(pathFixed).getProcessor().convertToFloatProcessor();
		FloatProcessor moving = new ImagePlus(pathMoving).getProcessor().convertToFloatProcessor();
		
		for ( int xOff = 2; xOff <= 4; xOff += 2 )
		{
			for ( int yOff = 1; yOff <= 3; yOff += 2 )
			{
				int[] blockRadius = new int[] { fixed.getWidth()/2, fixed.getHeight()/2 };
				int[] offset = new int[] { xOff, yOff };
				offset( fixed, moving, blockRadius, offset, pathFixed.equals( pathMoving ) );
			}
		}
	}
	
	
	public void offset(
			final FloatProcessor fixed,
			final FloatProcessor moving,
			final int[] blockRadius,
			final int[] offset,
			final boolean fixedAndMovingAreTheSame
			) {

		int[] averageBlockSize = { 0, 0 };
		int[] averageBlockStep = { 1, 1 };
		
		FloatProcessor fixedOff = new FloatProcessor( fixed.getWidth() - offset[0], fixed.getHeight() - offset[1] );
		FloatProcessor movingOff = new FloatProcessor( moving.getWidth() - offset[0], moving.getHeight() - offset[1] );
		final int w = fixedOff.getWidth();
		final int h = fixedOff.getHeight();
		for ( int y = 0; y < h; ++y )
		{
			for ( int x = 0; x < w; ++x )
			{
				fixedOff.setf( x, y, fixed.getf( x + offset[0], y + offset[1] ) );
				movingOff.setf( x, y, moving.getf( x, y ) );
			}
		}

		Tuple2<FloatProcessor, FloatProcessor> maxCorrs = TolerantNCC.tolerantNCC(fixedOff, movingOff, offset, blockRadius, 0, 0);
		HashMap<Tuple2<Integer, Integer>, Double> hm = TolerantNCC.average(maxCorrs._1(), maxCorrs._2(), averageBlockSize, averageBlockStep);

		BlockPMCC pmcc = new BlockPMCC(fixed, moving);
		pmcc.setOffset( 0, 0 );
		pmcc.r( blockRadius[0], blockRadius[1] );
			        
		FloatProcessor maxCorrsFp = maxCorrs._1();
		final int wb = w - blockRadius[0];
		final int hb = h - blockRadius[1];
		FloatProcessor tp = pmcc.getTargetProcessor();
		for ( int y = 0; y < h; ++y )
		{
			for ( int x = 0; x < w; ++x )
			{
				final float val = maxCorrsFp.getf( x, y );
				if ( x < blockRadius[0] || y < blockRadius[1] || x > wb || y > hb ) {
					assertEquals( 0.0f, val, 0.0f );
					assertNull( hm.get( Utility.tuple2( x, y ) ) );
				}
				else {
					Double matrixEntry = hm.get( Utility.tuple2( x, y ) );
					assertNotNull( matrixEntry );
					assertNotEquals( 0.0f, val, 0.0f );
					final float ref = tp.getf( x + offset[0], y + offset[1] );
					if ( fixedAndMovingAreTheSame )
						assertEquals( ref, matrixEntry.floatValue(), 0.0f );
					else
						assertTrue( String.format("Failed: %f >= %f", matrixEntry.floatValue(), ref), matrixEntry.floatValue() >= ref );
				}
			}
		}
	}
	

}
