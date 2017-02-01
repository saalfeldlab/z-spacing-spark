package org.janelia.thickness.weight;

public class Weights
{
	public final double[] estimateWeights;

	public final double[] shiftWeights;

	public Weights( final double[] estimateWeights, final double[] shiftWeights )
	{
		super();
		this.estimateWeights = estimateWeights;
		this.shiftWeights = shiftWeights;
	}

}
