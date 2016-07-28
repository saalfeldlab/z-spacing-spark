/**
 * 
 */
package org.janelia.thickness;

import java.io.ByteArrayInputStream;
import java.io.ByteArrayOutputStream;
import java.util.Random;

import org.janelia.thickness.KryoSerialization.Registrator;
import org.junit.Assert;
import org.junit.Before;
import org.junit.Test;

import com.esotericsoftware.kryo.Kryo;
import com.esotericsoftware.kryo.io.Input;
import com.esotericsoftware.kryo.io.Output;

import ij.process.FloatProcessor;

/**
 * @author Philipp Hanslovsky &lt;hanslovskyp@janelia.hhmi.org&gt;
 *
 */
public class KryoSerializationTest {
	
	Kryo kryo = new Kryo();
	
	@Before
	public void setUp() {
		Registrator registrator = new Registrator();
		registrator.registerClasses(kryo);
	}
	
	public <T> T serializeAndDeserialize( T t )
	{
		ByteArrayOutputStream bos = new ByteArrayOutputStream();
		Output ko = new Output(bos);
		this.kryo.writeObject( ko, t );
		ko.flush();
		ByteArrayInputStream bis = new ByteArrayInputStream(bos.toByteArray());
		Input ki = new Input(bis);
		@SuppressWarnings("unchecked")
		T deserialized = (T)this.kryo.readObject( ki, t.getClass() );
		return deserialized;
	}

	@Test
	public void testFloatProcessor() {
		FloatProcessor fp = new FloatProcessor( 100, 200 );
		float[] pixels = (float[]) fp.getPixels();
		Random rng = new Random(100);
		for ( int i = 0; i < pixels.length; ++i )
			pixels[i] = rng.nextFloat();
		fp.setMinAndMax( -1e-10, 0.5 );
		FloatProcessor deserialized = serializeAndDeserialize( fp );
		Assert.assertEquals( fp.getWidth(), deserialized.getWidth() );
		Assert.assertEquals( fp.getHeight(), deserialized.getHeight() );
		Assert.assertEquals( fp.getMin(), deserialized.getMin(), 0.0 );
		Assert.assertEquals( fp.getMax(), deserialized.getMax(), 0.0 );
		Assert.assertArrayEquals( pixels, (float[])deserialized.getPixels(), 0.0f );
	}

}
