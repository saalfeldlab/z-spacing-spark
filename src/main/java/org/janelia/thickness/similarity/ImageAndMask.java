package org.janelia.thickness.similarity;

import java.io.Serializable;

import ij.process.FloatProcessor;

public class ImageAndMask implements Serializable
{
	public final FloatProcessor image;

	public final FloatProcessor mask;

	public ImageAndMask( final FloatProcessor image, final FloatProcessor mask )
	{
		super();
		this.image = image;
		this.mask = mask;
	}
}