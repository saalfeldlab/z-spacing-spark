package org.janelia.thickness.utility;

import ij.process.FloatProcessor;

import java.io.Serializable;

/**
 * Created by hanslovskyp on 9/20/15.
 */
public class FPTuple implements Serializable
{

	public final float[] pixels;

	public final int width;

	public final int height;

	public FPTuple( float[] pixels, int width, int height )
	{
		this.pixels = pixels;
		this.width = width;
		this.height = height;
	}

	public FPTuple( FloatProcessor fp )
	{
		this( ( float[] ) fp.getPixels(), fp.getWidth(), fp.getHeight() );
	}

	public FloatProcessor rebuild()
	{
		return new FloatProcessor( width, height, pixels );
	}

	public static FPTuple create( FloatProcessor fp )
	{
		return new FPTuple( fp );
	}

}
