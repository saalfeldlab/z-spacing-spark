/**
 * 
 */
package org.janelia.thickness;

import org.apache.spark.serializer.KryoRegistrator;

import com.esotericsoftware.kryo.Kryo;
import com.esotericsoftware.kryo.Serializer;
import com.esotericsoftware.kryo.io.Input;
import com.esotericsoftware.kryo.io.Output;

import ij.process.FloatProcessor;

/**
 * @author Philipp Hanslovsky &lt;hanslovskyp@janelia.hhmi.org&gt;
 *
 */
public class KryoSerialization {
	
	public static class Registrator implements KryoRegistrator
	{
		/* (non-Javadoc)
		 * @see org.apache.spark.serializer.KryoRegistrator#registerClasses(com.esotericsoftware.kryo.Kryo)
		 */
		@Override
		public void registerClasses(Kryo kryo) {
			kryo.register( FloatProcessor.class, new FloatProcessorSerializer() );
		}
	}
	
	public static class FloatProcessorSerializer extends Serializer< FloatProcessor > {
		
		// write width, height, data, min, max
		// read in same order
		public static final boolean optimizePositive = true;

		@Override
		public FloatProcessor read(Kryo kryo, Input input, Class<FloatProcessor> type) {
			final int width = input.readInt( optimizePositive );
			final int height = input.readInt( optimizePositive );
			final float[] pixels = new float[ width * height ];
			for ( int i = 0; i < pixels.length; ++i )
				pixels[i]  = input.readFloat();
			final double min = input.readDouble();
			final double max = input.readDouble();
			final FloatProcessor fp = new FloatProcessor( width, height, pixels );
			fp.setMinAndMax( min, max );
			return fp;
		}

		@Override
		public void write(Kryo kryo, Output output, FloatProcessor object) {
			output.writeInt( object.getWidth(), optimizePositive );
			output.writeInt( object.getHeight(), optimizePositive );
			float[] data = (float[]) object.getPixels();
			for ( float d : data )
				output.writeFloat( d );
			output.writeDouble( object.getMin() );
			output.writeDouble( object.getMax() );
		}
		
	}

}
