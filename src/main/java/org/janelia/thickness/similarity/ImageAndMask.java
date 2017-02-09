package org.janelia.thickness.similarity;

import java.io.Serializable;

import ij.process.ImageProcessor;

public class ImageAndMask implements Serializable
{
	public final ImageProcessor image;

	public final ImageProcessor mask;

	public ImageAndMask( final ImageProcessor image, final ImageProcessor mask )
	{
		super();
		this.image = image;
		this.mask = mask;
	}
}