package org.janelia.thickness.kryo.imglib.type;

import com.esotericsoftware.kryo.Kryo;
import com.esotericsoftware.kryo.Serializer;
import com.esotericsoftware.kryo.io.Input;
import com.esotericsoftware.kryo.io.Output;

import net.imglib2.type.numeric.real.FloatType;

public class FloatTypeSerializer extends Serializer< FloatType >
{

	@Override
	public void write( final Kryo kryo, final Output output, final FloatType f )
	{
		output.writeFloat( f.getRealFloat() );
	}

	@Override
	public FloatType read( final Kryo kryo, final Input input, final Class< FloatType > type )
	{
		return new FloatType( input.readFloat() );
	}

//	@Override
//	public void write( final Kryo kryo, final Output output, final C c )
//	{
//		if ( c instanceof IntegerType< ? > )
//		{
//			output.writeByte( TYPE_OF.INTEGER_TYPE.ordinal() );
//			output.writeLong( ( ( IntegerType< ? > ) c ).getIntegerLong() );
//		}
//		else if ( c instanceof DoubleType )
//		{
//			output.writeByte( TYPE_OF.DOUBLE_TYPE.ordinal() );
//			output.writeDouble( ( ( RealType< ? > ) c ).getRealDouble() );
//		}
//		else if ( c instanceof FloatType )
//		{
//			output.writeByte( TYPE_OF.FLOAT_TYPE.ordinal() );
//			output.writeFloat( ( ( FloatType ) c ).getRealFloat() );
//		}
//		else if ( c instanceof ComplexType< ? > )
//		{
//			output.writeByte( TYPE_OF.COMPLEX_TYPE.ordinal() );
//			output.writeDouble( ( ( ComplexType< ? > ) c ).getRealDouble() );
//			output.writeDouble( ( ( ComplexType< ? > ) c ).getImaginaryDouble() );
//		}
//		else
//			throw new IllegalArgumentException( "Wrong type for argument c: " + c.getClass().getName() );
//	}
//
//	@Override
//	public C read( final Kryo kryo, final Input input, final Class< C > type )
//	{
//		final TYPE_OF t = TYPE_OF.values()[ input.readByte() ];
//		switch ( t )
//		{
//		case COMPLEX_TYPE:
//			return ( C ) new ComplexDoubleType( input.readDouble(), input.readDouble() );
//		case DOUBLE_TYPE:
//			return ( C ) new DoubleType( input.readDouble() );
//		case FLOAT_TYPE:
//			return ( C ) new FloatType( input.readFloat() );
//		case INTEGER_TYPE:
//			return ( C ) new LongType( input.readLong() );
//		default:
//			throw new UnsupportedOperationException( "Write not implemented for " + t );
//		}
//	}

}
