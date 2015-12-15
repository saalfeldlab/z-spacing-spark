package org.janelia.thickness

import ij.process.FloatProcessor

/**
 * Created by hanslovskyp on 9/18/15.
 */
class ScalaFPTuple( val fp: FPTuple ) extends Serializable {

	def rebuild(): FloatProcessor = {
		return fp.rebuild()
	}

	def pixels(): Array[Float] =
	{
		return fp.pixels
	}

	def width(): Int = return fp.width

	def height(): Int = return fp.height


}

object ScalaFPTuple {
	def create( fp: FloatProcessor ): ScalaFPTuple = {
		return new ScalaFPTuple( new FPTuple( fp ) )
	}

	def create( pixels: Array[Float], width: Int, height: Int ): ScalaFPTuple =
	{
		return new ScalaFPTuple( new FPTuple( pixels, width, height ) )
	}
}
