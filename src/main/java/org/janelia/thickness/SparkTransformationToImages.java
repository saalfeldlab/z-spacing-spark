package org.janelia.thickness;

import java.util.ArrayList;
import java.util.Iterator;

import org.apache.spark.api.java.JavaPairRDD;
import org.apache.spark.api.java.function.Function2;
import org.apache.spark.api.java.function.PairFlatMapFunction;
import org.apache.spark.api.java.function.PairFunction;
import org.janelia.thickness.utility.FPTuple;
import org.janelia.thickness.utility.Utility;

import ij.process.FloatProcessor;
import scala.Tuple2;
import scala.Tuple3;

public class SparkTransformationToImages
{

	public static JavaPairRDD< Integer, FPTuple > toImages( final JavaPairRDD< Tuple2< Integer, Integer >, double[] > input, final int[] dim, final int[] radius, final int[] step )
	{
		final JavaPairRDD< Integer, FPTuple > output = input // (x, y) ->
																// double[]
				.flatMapToPair( new FromArrayToIndexValuePairs() ) // (x, y) ->
																	// (z,
																	// value)
				.mapToPair( new FromXYKeyToZKey() ) // z -> (x, y, value)
				.aggregateByKey( // z -> FPTuple
						new FPTuple( new FloatProcessor( dim[ 0 ], dim[ 1 ] ) ), new WriteToFloatProcessor( radius, step ), new MergeDisjointFloatProcessors() );

		return output;
	}

	public static class FromArrayToIndexValuePairs implements PairFlatMapFunction< Tuple2< Tuple2< Integer, Integer >, double[] >, Tuple2< Integer, Integer >, Tuple2< Integer, Double > >
	{
		private static final long serialVersionUID = 6343315847242166966L;

		@Override
		public Iterator< Tuple2< Tuple2< Integer, Integer >, Tuple2< Integer, Double > > > call( final Tuple2< Tuple2< Integer, Integer >, double[] > t ) throws Exception
		{
			final ArrayList< Tuple2< Tuple2< Integer, Integer >, Tuple2< Integer, Double > > > res = new ArrayList<>();
			final Tuple2< Integer, Integer > xy = t._1();
			final double[] coord = t._2();
			for ( int j = 0; j < coord.length; j++ )
				res.add( Utility.tuple2( xy, Utility.tuple2( new Integer( j ), coord[ j ] ) ) );
			return res.iterator();
		}
	}

	public static class FromXYKeyToZKey implements PairFunction< Tuple2< Tuple2< Integer, Integer >, Tuple2< Integer, Double > >, Integer, Tuple3< Integer, Integer, Double > >
	{

		private static final long serialVersionUID = 4039365688638871552L;

		@Override
		public Tuple2< Integer, Tuple3< Integer, Integer, Double > > call( final Tuple2< Tuple2< Integer, Integer >, Tuple2< Integer, Double > > t ) throws Exception
		{
			final Tuple2< Integer, Integer > xy = t._1();
			final Tuple2< Integer, Double > t2 = t._2();
			return Utility.tuple2( t2._1(), Utility.tuple3( xy._1(), xy._2(), t2._2() ) );
		}
	}

	public static class WriteToFloatProcessor implements Function2< FPTuple, Tuple3< Integer, Integer, Double >, FPTuple >
	{
		private static final long serialVersionUID = 426010164977854733L;

		private final int[] radius;

		private final int[] step;

		public WriteToFloatProcessor( final int[] radius, final int[] step )
		{
			super();
			this.radius = radius;
			this.step = step;
		}

		@Override
		public FPTuple call( final FPTuple fp, final Tuple3< Integer, Integer, Double > t ) throws Exception
		{
			final int x = t._1();
			final int y = t._2();
			final FloatProcessor proc = fp.rebuild();
			if ( x >= 0 && x < proc.getWidth() && y >= 0 && y < proc.getHeight() ) // TODO
																					// pass
																					// correct
																					// dims,
																					// so
																					// no
																					// data
																					// is
																					// lost
				proc.setf( t._1(), t._2(), t._3().floatValue() );
			return fp;
		}
	}

	public static class MergeDisjointFloatProcessors implements Function2< FPTuple, FPTuple, FPTuple >
	{
		private static final long serialVersionUID = 1608128472889969913L;

		@Override
		public FPTuple call( final FPTuple fp1, final FPTuple fp2 ) throws Exception
		{
			final float[] p1 = ( float[] ) fp1.rebuild().getPixels();
			final float[] p2 = ( float[] ) fp2.rebuild().getPixels();
			for ( int j = 0; j < p1.length; j++ )
				p1[ j ] += p2[ j ];
			return fp1;
		}
	}

}
