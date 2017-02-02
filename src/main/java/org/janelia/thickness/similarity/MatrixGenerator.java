package org.janelia.thickness.similarity;

import org.apache.spark.api.java.JavaPairRDD;
import org.apache.spark.api.java.JavaSparkContext;

import ij.process.FloatProcessor;
import scala.Tuple2;

public interface MatrixGenerator
{

	public static interface Factory
	{
		public MatrixGenerator create();
	}

	public JavaPairRDD< Tuple2< Integer, Integer >, FloatProcessor > generateMatrices(
			final JavaSparkContext sc,
			final JavaPairRDD< Tuple2< Integer, Integer >, Tuple2< ImageAndMask, ImageAndMask > > sectionPairs,
			final int[] blockRadius,
			final int[] stepSize,
			final int range,
			final int startIndex,
			final int size );

}
