package org.janelia.thickness.kryo.ij;

import java.lang.invoke.MethodHandles;

import org.apache.log4j.Level;
import org.apache.log4j.LogManager;
import org.apache.log4j.Logger;

import com.esotericsoftware.kryo.Kryo;
import com.esotericsoftware.kryo.Serializer;
import com.esotericsoftware.kryo.io.Input;
import com.esotericsoftware.kryo.io.Output;

import ij.process.ShortProcessor;

public class ShortProcessorSerializer extends Serializer< ShortProcessor >
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
	public ShortProcessor read( final Kryo kryo, final Input input, final Class< ShortProcessor > type )
	{
		final int width = input.readInt( optimizePositive );
		final int height = input.readInt( optimizePositive );
		LOG.debug( "Reading ShortProcessor: width=" + width + ", height+" + height );
		final short[] pixels = input.readShorts( width * height );
		final double min = input.readDouble();
		final double max = input.readDouble();
		final ShortProcessor sp = new ShortProcessor( width, height, pixels, null );
		sp.setMinAndMax( min, max );
		return sp;
	}

	@Override
	public void write( final Kryo kryo, final Output output, final ShortProcessor object )
	{
		LOG.debug( "Writing ShortProcessor: width=" + object.getWidth() + ", height+" + object.getHeight() );
		output.writeInt( object.getWidth(), optimizePositive );
		output.writeInt( object.getHeight(), optimizePositive );
		output.writeShorts( ( short[] ) object.getPixels() );
		output.writeDouble( object.getMin() );
		output.writeDouble( object.getMax() );
	}

}