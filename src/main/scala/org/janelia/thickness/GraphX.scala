package org.janelia.thickness

import java.util.Random

import ij.io.FileSaver
import ij.{ImageJ, ImagePlus}
import ij.process.FloatProcessor
import net.imglib2.util.RealSum
import org.apache.spark.graphx.{Edge, Graph}
import org.apache.spark.storage.StorageLevel
import org.apache.spark.{RangePartitioner, SparkContext, SparkConf}

import scala.collection.mutable.ArrayBuffer

/**
 * Created by hanslovskyp on 9/17/15.
 */
object GraphX {

	def calculate(d1: Array[Float], d2: Array[Float]): Double = {
		var sumA = new RealSum()
		var sumAA = new RealSum()
		var sumB = new RealSum()
		var sumBB = new RealSum()
		var sumAB = new RealSum()
		var n = 0
		for (i <- 0 until d1.length) {
			val va = d1.apply(i)
			val vb = d2.apply(i)

			if (java.lang.Double.isNaN(va) || java.lang.Double.isNaN(vb)) {
				// do nothing
			} else {
				n += 1
				sumA.add(va)
				sumAA.add(va * va)
				sumB.add(vb)
				sumBB.add(vb * vb)
				sumAB.add(va * vb)
			}
		}
		val suma = sumA.getSum()
		val sumaa = sumAA.getSum()
		val sumb = sumB.getSum()
		val sumbb = sumBB.getSum()
		val sumab = sumAB.getSum()

		return (n * sumab - suma * sumb) / Math.sqrt(n * sumaa - suma * suma) / Math.sqrt(n * sumbb - sumb * sumb);
	}

	def generate(size: Int, rng: Random = new Random()): Array[Float] = {
		val result = new Array[Float](size)
		for (i <- 0 until size)
			result.update(i, rng.nextFloat())
		return result
	}

	def read(pattern: String, n: Int): Array[Float] = {
		val fn = pattern.format(n)
		return read(fn)
	}

	def read(fn: String): Array[Float] = {
		val imp = new ImagePlus(fn)
		val fp = imp.getProcessor().convertToFloatProcessor()
		return fp.getPixels().asInstanceOf[Array[Float]]
	}

	def main(args: Array[String]): Unit = {

		//    val arr = read( "/data/hanslovskyp/jain-nobackup/export/scale10/NVision40-3802_13-06-25_224024_InLens.tif.tif" )
		//    println( arr.length )
		//    System.exit( 9001 )

		val pattern = args.apply( 0 ) // "/nobackup/saalfeld/hanslovskyp/high-res/%05d.tif" // args.apply(0)

		val start = Integer.parseInt( args.apply( 1 ) )
		val stop = Integer.parseInt( args.apply( 2 ) )
		val range = Integer.parseInt( args.apply( 3 ) )

		val target = args.apply( 4 )

		//    val width = 200
		//    val height = 100

		//    val size = width * height

		//    val rng = new Random( 100 )


		val conf = new SparkConf().setAppName("Scala Test")

		var sc = new SparkContext(conf)

		var edgeSequence = new ArrayBuffer[Edge[Double]]()
		for (i <- start until stop)
			for (j <- (i + 1) until Math.min(i + 1 + range, stop))
				edgeSequence.append(new Edge[Double](i, j, Double.NaN))
		//        edgeSequence.append( new Tuple2( i, j ) );

		val edges = sc.parallelize(edgeSequence)



		val graph = Graph.fromEdges(edges, 0, StorageLevel.MEMORY_ONLY, StorageLevel.MEMORY_ONLY)
		//    val imageGraph = graph.mapVertices[Array[Float]]( (id, value) => generate( size ) ).persist( StorageLevel.MEMORY_ONLY )
		//    val correlationGraph = imageGraph.mapTriplets( (triplet) => rng.nextDouble() + triplet.srcAttr.apply( 0 )*triplet.dstAttr.apply( 0 ) )
		val imageGraph = graph.mapVertices[Array[Float]]((id, value) => read(pattern, id.toInt)).persist(StorageLevel.MEMORY_ONLY)
		val tStart = System.currentTimeMillis()
		val correlationGraph = imageGraph.mapTriplets((triplet) => calculate(triplet.srcAttr, triplet.dstAttr))
		val edgeValues = correlationGraph.edges.collect()

		val tEnd = System.currentTimeMillis()

		val time = tEnd - tStart

		val timeString = "start=%d stop=%d range=%d runtime=%dms".format( start, stop, range, time )
		println( timeString )

		val nElements = (stop - start);
		val resultMatrix = new FloatProcessor(nElements, nElements)
		resultMatrix.add(Double.NaN)
		for (i <- 0 until nElements)
			resultMatrix.setf(i, i, 1.0f)

		for (v <- edgeValues) {
			val x = v.srcId.toInt
			val y = v.dstId.toInt
			val similarity = v.attr.toFloat
			resultMatrix.setf( x, y, similarity )
			resultMatrix.setf( y, x, similarity )
		}

		new FileSaver( new ImagePlus( "", resultMatrix ) ).saveAsTiff( target )

//		new ImageJ()
//		new ImagePlus("bla", resultMatrix).show()

//		println(edgeValues.length)
//		for (v <- edgeValues)
//			println(v.attr)


	}

}
