package org.janelia.thickness.kryo.ij;

import java.lang.invoke.MethodHandles;

import org.apache.log4j.Level;
import org.apache.log4j.LogManager;
import org.apache.log4j.Logger;

import com.esotericsoftware.kryo.Kryo;
import com.esotericsoftware.kryo.Serializer;
import com.esotericsoftware.kryo.io.Input;
import com.esotericsoftware.kryo.io.Output;

import ij.process.ByteProcessor;

public class ByteProcessorSerializer extends Serializer< ByteProcessor >
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
	public ByteProcessor read( final Kryo kryo, final Input input, final Class< ByteProcessor > type )
	{
		final int width = input.readInt( optimizePositive );
		final int height = input.readInt( optimizePositive );
		LOG.debug( "Reading ByteProcessor: width=" + width + ", height+" + height );
		final byte[] pixels = input.readBytes( width * height );
		final double min = input.readDouble();
		final double max = input.readDouble();
		final ByteProcessor sp = new ByteProcessor( width, height, pixels );
		sp.setMinAndMax( min, max );
		return sp;
	}

	@Override
	public void write( final Kryo kryo, final Output output, final ByteProcessor object )
	{
		LOG.debug( "Writing ByteProcessor: width=" + object.getWidth() + ", height+" + object.getHeight() );
		output.writeInt( object.getWidth(), optimizePositive );
		output.writeInt( object.getHeight(), optimizePositive );
		output.writeBytes( ( byte[] ) object.getPixels() );
		output.writeDouble( object.getMin() );
		output.writeDouble( object.getMax() );
	}

}