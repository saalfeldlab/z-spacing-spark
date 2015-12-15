package org.janelia.thickness

import ij.process.FloatProcessor
import org.apache.spark.SparkContext
import org.apache.spark.rdd.RDD

import scala.collection.immutable.HashMap

/**
 * Created by hanslovskyp on 9/18/15.
 */
class MatrixCalculator {

	def cutOut( index: Int, fpTuple: ScalaFPTuple, min: Tuple2[Int,Int], max: Tuple2[Int,Int] ): Tuple2[Int,ScalaFPTuple] =
	{
		val dimensions = ( max._1 - min._1, max._2 - min._2 )
		val fp = new FloatProcessor( dimensions._1, dimensions._2 )
		val source = fpTuple.rebuild()
		for( x <- 0 to dimensions._1; y <- 0 to dimensions._2 )
			fp.setf( x, y, source.get( x, y ) )
		return ( index, ScalaFPTuple.create( fp ) )
	}

	def calculateMatrixFromBigChunks(
		                                images: RDD[Tuple2[Int,ScalaFPTuple]],
		                                range: Int,
		                                sc: SparkContext,
		                                blockSize: Tuple2[Int, Int],
		                                dimension: Tuple2[Int, Int]
		                                ): RDD[Tuple2[Tuple2[Int, Int], ScalaFPTuple]] =
	{
		for(
			x <- new Range( 0, dimension._1.toInt, blockSize._1.toInt );
			y <- new Range( 0, dimension._2.toInt, blockSize._2.toInt )
		)
		{
			val min = ( x, y )
			val max = (
				Math.min( x + blockSize._1, dimension._1 ),
				Math.min( y + blockSize._2, dimension._2 )
				)
			val currentMatrices = images.map( ( input ) => cutOut( input._1, input._2, min, max ) )
		}
		null
	}

}
