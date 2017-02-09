package org.janelia.thickness.kryo;

import java.lang.invoke.MethodHandles;

import org.apache.log4j.Level;
import org.apache.log4j.LogManager;
import org.apache.log4j.Logger;

import com.esotericsoftware.kryo.Kryo;
import com.esotericsoftware.kryo.Serializer;
import com.esotericsoftware.kryo.io.Input;
import com.esotericsoftware.kryo.io.Output;

import ij.process.ColorProcessor;

public class ColorProcessorSerializer extends Serializer< ColorProcessor >
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
	public ColorProcessor read( final Kryo kryo, final Input input, final Class< ColorProcessor > type )
	{
		final int width = input.readInt( optimizePositive );
		final int height = input.readInt( optimizePositive );
		LOG.debug( "Reading ColorProcessor: width=" + width + ", height+" + height );
		final int[] pixels = input.readInts( width * height );
		final double min = input.readDouble();
		final double max = input.readDouble();
		final ColorProcessor sp = new ColorProcessor( width, height, pixels );
		sp.setMinAndMax( min, max );
		return sp;
	}

	@Override
	public void write( final Kryo kryo, final Output output, final ColorProcessor object )
	{
		LOG.debug( "Writing ColorProcessor: width=" + object.getWidth() + ", height+" + object.getHeight() );
		output.writeInt( object.getWidth(), optimizePositive );
		output.writeInt( object.getHeight(), optimizePositive );
		output.writeInts( ( int[] ) object.getPixels() );
		output.writeDouble( object.getMin() );
		output.writeDouble( object.getMax() );
	}

}