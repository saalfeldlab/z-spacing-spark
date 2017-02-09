package org.janelia.thickness.kryo;

import java.lang.invoke.MethodHandles;

import org.apache.log4j.Level;
import org.apache.log4j.LogManager;
import org.apache.log4j.Logger;

import com.esotericsoftware.kryo.Kryo;
import com.esotericsoftware.kryo.Serializer;
import com.esotericsoftware.kryo.io.Input;
import com.esotericsoftware.kryo.io.Output;

import ij.process.FloatProcessor;

public class FloatProcessorSerializer extends Serializer< FloatProcessor >
{

	public static final Logger LOG = LogManager.getLogger( MethodHandles.lookup().lookupClass() );
	static
	{
		LOG.setLevel( Level.INFO );
	}

	// write width, height, data, min, max
	// read in same order
	public static final boolean optimizePositive = true;

	@Override
	public FloatProcessor read( final Kryo kryo, final Input input, final Class< FloatProcessor > type )
	{
		final int width = input.readInt( optimizePositive );
		final int height = input.readInt( optimizePositive );
		LOG.debug( "Reading FloatProcessor: width=" + width + ", height+" + height );
		final float[] pixels = input.readFloats( width * height );
		final double min = input.readDouble();
		final double max = input.readDouble();
		final FloatProcessor fp = new FloatProcessor( width, height, pixels );
		fp.setMinAndMax( min, max );
		return fp;
	}

	@Override
	public void write( final Kryo kryo, final Output output, final FloatProcessor object )
	{
		LOG.debug( "Writing FloatProcessor: width=" + object.getWidth() + ", height+" + object.getHeight() );
		output.writeInt( object.getWidth(), optimizePositive );
		output.writeInt( object.getHeight(), optimizePositive );
		output.writeFloats( ( float[] ) object.getPixels() );
		output.writeDouble( object.getMin() );
		output.writeDouble( object.getMax() );
	}

}