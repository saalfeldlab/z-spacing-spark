package org.janelia.thickness.kryo.imglib;

import java.util.Arrays;

import com.esotericsoftware.kryo.Kryo;
import com.esotericsoftware.kryo.Serializer;
import com.esotericsoftware.kryo.io.Input;
import com.esotericsoftware.kryo.io.Output;

import net.imglib2.Cursor;
import net.imglib2.img.array.ArrayImg;
import net.imglib2.img.array.ArrayImgs;
import net.imglib2.type.NativeType;
import net.imglib2.type.numeric.IntegerType;
import net.imglib2.type.numeric.RealType;
import net.imglib2.type.numeric.integer.IntType;
import net.imglib2.type.numeric.integer.LongType;
import net.imglib2.type.numeric.real.DoubleType;
import net.imglib2.type.numeric.real.FloatType;
import net.imglib2.util.Intervals;

public class ArrayImgSerializer extends Serializer< ArrayImg< ? extends NativeType< ? >, ? > >
{

	protected enum TYPE_OF
	{
		BIT_TYPE, COMPLEX_DOUBLE_TYPE, COMPLEX_FLOAT_TYPE, DOUBLE_TYPE, FLOAT_TYPE, BYTE_TYPE, UNSIGNED_BYTE_TYPE, INT_TYPE, UNSIGNED_INT_TYPE, LONG_TYPE, UNSIGNED_LONG_TYPE, SHORT_TYPE, UNSIGNED_SHORT_TYPE, NATIVE_ARGB_DOIUBLE_TYPE, UNSIGNED_128_BIT_TYPE
	}

	protected static final boolean OPTIMIZE_POSITIVE_NUM_DIM = true;

	protected static final boolean OPTIMIZE_POSITIVE_DIMS = true;

	protected static final boolean OPTIMIZE_POSITIVE_TYPE_OF = true;

	@SuppressWarnings( "unchecked" )
	@Override
	public void write( final Kryo kryo, final Output output, final ArrayImg< ? extends NativeType< ? >, ? > img )
	{
		// no single dim can be larger than integer, no need to store long array
		output.writeInt( img.numDimensions(), OPTIMIZE_POSITIVE_NUM_DIM );
		final int[] dims = Intervals.dimensionsAsIntArray( img );
		output.writeInts( dims, OPTIMIZE_POSITIVE_DIMS );
		final NativeType< ? > t = img.firstElement();
		if ( t instanceof FloatType )
			writeFloat( output, ( Cursor< FloatType > ) img.cursor() );
		else if ( t instanceof DoubleType )
			writeDouble( output, ( Cursor< DoubleType > ) img.cursor() );
		else if ( t instanceof IntType )
			writeInt( output, ( Cursor< IntType > ) img.cursor() );
		else if ( t instanceof LongType )
			writeLong( output, ( Cursor< LongType > ) img.cursor() );
		else
			throw new UnsupportedOperationException( "Write not implemented for " + t.getClass().getName() );
	}

	@Override
	public ArrayImg< ?, ? > read( final Kryo kryo, final Input input, final Class< ArrayImg< ? extends NativeType< ? >, ? > > type )
	{
		final int nDim = input.readInt( OPTIMIZE_POSITIVE_NUM_DIM );
		final int[] dims = input.readInts( nDim, OPTIMIZE_POSITIVE_DIMS );
		return readTypeSpecific( kryo, input, Arrays.stream( dims ).mapToLong( i -> i ).toArray(), ( int ) Intervals.numElements( dims ) );
	}

	protected ArrayImg< ?, ? > readTypeSpecific( final Kryo kryo, final Input input, final long[] dims, final int nElements )
	{
		final TYPE_OF type = TYPE_OF.values()[ input.readByte() ];
		switch ( type )
		{
		case FLOAT_TYPE:
			return ArrayImgs.floats( input.readFloats( nElements ), dims );
		case DOUBLE_TYPE:
			return ArrayImgs.doubles( input.readDoubles( nElements ), dims );
		case INT_TYPE:
			return ArrayImgs.ints( input.readInts( nElements ), dims );
		case LONG_TYPE:
			return ArrayImgs.longs( input.readLongs( nElements ), dims );
		default:
			throw new UnsupportedOperationException( "Write not implemented for " + type );
		}
	}

	// Helper methods

	public static < T extends RealType< T > > void writeFloat( final Output output, final Cursor< T > c )
	{
		output.writeByte( ( byte ) TYPE_OF.FLOAT_TYPE.ordinal() );
		while ( c.hasNext() )
			output.writeFloat( c.next().getRealFloat() );
	}

	public static < T extends RealType< T > > void writeDouble( final Output output, final Cursor< T > c )
	{
		output.writeByte( ( byte ) TYPE_OF.DOUBLE_TYPE.ordinal() );
		while ( c.hasNext() )
			output.writeDouble( c.next().getRealDouble() );
	}

	public static < T extends IntegerType< T > > void writeInt( final Output output, final Cursor< T > c )
	{
		output.writeByte( ( byte ) TYPE_OF.INT_TYPE.ordinal() );
		while ( c.hasNext() )
			output.writeInt( c.next().getInteger() );
	}

	public static < T extends IntegerType< T > > void writeLong( final Output output, final Cursor< T > c )
	{
		output.writeByte( ( byte ) TYPE_OF.LONG_TYPE.ordinal() );
		while ( c.hasNext() )
			output.writeLong( c.next().getIntegerLong() );
	}


}
