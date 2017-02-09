/**
 *
 */
package org.janelia.thickness.kryo;

import java.lang.invoke.MethodHandles;
import java.util.ArrayList;
import java.util.HashMap;

import org.apache.log4j.Level;
import org.apache.log4j.LogManager;
import org.apache.log4j.Logger;
import org.apache.spark.serializer.KryoRegistrator;
import org.janelia.thickness.BlockCoordinates;
import org.janelia.thickness.utility.DPTuple;

import com.esotericsoftware.kryo.Kryo;

import ij.process.ByteProcessor;
import ij.process.ColorProcessor;
import ij.process.FloatProcessor;
import ij.process.ShortProcessor;

/**
 * @author Philipp Hanslovsky &lt;hanslovskyp@janelia.hhmi.org&gt;
 *
 */
public class KryoSerialization
{

	// Register all used classes so integer is used for identification rather
	// than fully quallified class name
	// https://github.com/EsotericSoftware/kryo#registration

	public static class Registrator implements KryoRegistrator
	{

		public static final Logger LOG = LogManager.getLogger( MethodHandles.lookup().lookupClass() );
		static
		{
			LOG.setLevel( Level.INFO );
		}

		/*
		 * (non-Javadoc)
		 *
		 * @see org.apache.spark.serializer.KryoRegistrator#registerClasses(com.
		 * esotericsoftware.kryo.Kryo)
		 */
		@Override
		public void registerClasses( final Kryo kryo )
		{
			kryo.register( ByteProcessor.class, new ByteProcessorSerializer() );
			kryo.register( ColorProcessor.class, new ColorProcessorSerializer() );
			kryo.register( FloatProcessor.class, new FloatProcessorSerializer() );
			kryo.register( ShortProcessor.class, new ShortProcessorSerializer() );
			kryo.register( DPTuple.class );
			kryo.register( String.class );
			kryo.register( HashMap.class );
			kryo.register( ArrayList.class );
			kryo.register( BlockCoordinates.class );
			kryo.register( BlockCoordinates.Coordinate.class );
			kryo.register( double[].class );
		}
	}

}
