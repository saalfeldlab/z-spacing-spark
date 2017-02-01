package org.janelia.thickness.weight;

import org.apache.spark.api.java.JavaPairRDD;

import scala.Tuple2;

public interface WeightsCalculator
{

	public JavaPairRDD< Tuple2< Integer, Integer >, Weights > calculate(
			int[] offset,
			int[] step );

}
