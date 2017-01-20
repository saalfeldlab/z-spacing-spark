package org.janelia.thickness;

import org.janelia.thickness.utility.DPTuple;
import org.janelia.thickness.utility.Utility;

import java.util.Arrays;

/**
 * Created by hanslovskyp on 9/29/15.
 */
public class FillHoles
{

	public static DPTuple fill( DPTuple input )
	{
		// final FloatProcessor fpCopy = ( FloatProcessor )fp.duplicate();

		int width = input.width;
		int height = input.height;

		int w = width - 1;
		int h = height - 1;

		int wh = input.pixels.length;

		int numSaturatedPixels = 0, numSaturatedPixelsBefore;
		do
		{
			double[] pixels = input.pixels.clone();
			numSaturatedPixelsBefore = numSaturatedPixels;
			numSaturatedPixels = 0;
			for ( int i = 0; i < wh; ++i )
			{
				final double v = pixels[ i ];

				if ( Double.isNaN( v ) )
				{
					++numSaturatedPixels;

					final int y = i / width;
					final int x = i % width;

					double s = 0;
					double n = 0;
					if ( y > 0 )
					{
						if ( x > 0 )
						{
							// top left
							final double tl = pixels[ Utility.xyToLinear( width, x - 1, y - 1 ) ];
							if ( !Double.isNaN( tl ) )
							{
								s += 0.5 * tl;
								n += 0.5;
							}
						}

						final double t = pixels[ Utility.xyToLinear( width, x, y - 1 ) ];
						if ( !Double.isNaN( t ) )
						{
							// top
							s += t;
							n += 1;
						}
						if ( x < w ) // TODO should be x < w ?
						{
							// top right
							final double tr = pixels[ Utility.xyToLinear( width, x + 1, y - 1 ) ];
							if ( !Double.isNaN( tr ) )
							{
								s += 0.5 * tr;
								n += 0.5;
							}
						}
					}

					if ( x > 0 )
					{
						// left
						final double l = pixels[ Utility.xyToLinear( width, x - 1, y ) ];
						if ( !Double.isNaN( l ) )
						{
							s += l;
							n += 1;
						}
					}
					if ( x < w ) // TODO should be x < w ?
					{
						// right
						final double r = pixels[ Utility.xyToLinear( width, x + 1, y ) ];
						if ( !Double.isNaN( r ) )
						{
							s += r;
							n += 1;
						}
					}

					if ( y < h ) // TODO should be y < h ?
					{
						if ( x > 0 )
						{
							// bottom left
							final double bl = pixels[ Utility.xyToLinear( width, x - 1, y + 1 ) ];
							if ( !Double.isNaN( bl ) )
							{
								s += 0.5 * bl;
								n += 0.5;
							}
						}
						// bottom
						final double b = pixels[ Utility.xyToLinear( width, x, y + 1 ) ];
						if ( !Double.isNaN( b ) )
						{
							s += b;
							n += 1;
						}
						if ( x < w ) // TODO should be x < w ?
						{
							// bottom right
							final double br = pixels[ Utility.xyToLinear( width, x + 1, y + 1 ) ];
							if ( !Double.isNaN( br ) )
							{
								s += 0.5 * br;
								n += 0.5;
							}
						}
					}

					if ( n > 0 )
						input.pixels[ i ] = s / n;
				}
			}
			pixels = input.pixels.clone();
		}
		while ( numSaturatedPixels != numSaturatedPixelsBefore );

		return input;
	}

	public static void main( String[] args )
	{
		int width = 4;
		int height = 3;
		int n = width * height;
		double[] withHoles = new double[ n ];
		double[] reference = new double[ n ];
		for ( int i = 0; i < n; ++i )
		{
			withHoles[ i ] = i;
			reference[ i ] = i;
		}

		withHoles[ 5 ] = Double.NaN;
		withHoles[ 6 ] = Double.NaN;

		reference[ 5 ] = 4.8;
		reference[ 6 ] = 6.2;

		DPTuple img = new DPTuple( withHoles, width, height );
		fill( img );

		System.out.println( Arrays.toString( withHoles ) );
		System.out.println( Arrays.toString( reference ) );

	}

}
