package org.janelia.thickness.similarity;

import scala.Tuple2;

public class CorrelationAndWeight
{
	public final double corr;

	public final double weight;

	public CorrelationAndWeight( final double corr, final double weight )
	{
		super();
		this.corr = corr;
		this.weight = weight;
	}

	@Override
	public String toString()
	{
		return new Tuple2<>( corr, weight ).toString();
	}

}