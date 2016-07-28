package org.janelia.thickness;

import ij.process.FloatProcessor;
import mpicbg.models.AffineModel1D;
import org.apache.spark.api.java.JavaPairRDD;
import org.apache.spark.api.java.function.Function2;
import org.apache.spark.api.java.function.PairFlatMapFunction;
import org.apache.spark.api.java.function.PairFunction;
import org.janelia.thickness.utility.Utility;
import scala.Tuple2;

import java.io.Serializable;
import java.util.ArrayList;
import java.util.HashMap;

/**
 * @author Philipp Hanslovsky &lt;hanslovskyp@janelia.hhmi.org&gt;
 */
public class Chunk implements Serializable
{

	/**
	 * 
	 */
	private static final long serialVersionUID = -2772585991397579231L;

	private final int stepSize;

	private final int overlap;

	private final int length;

	private final int range;

	public Chunk( int stepSize, int overlap, int length, int range )
	{
		this.stepSize = stepSize;
		this.overlap = overlap;
		this.length = length;
		this.range = range;
	}

	public < K > JavaPairRDD< K, Tuple2< Integer, Tuple2< FloatProcessor, double[] > > > getChunks(
			JavaPairRDD< K, Tuple2< FloatProcessor, double[] > > rdd )
	{
		return rdd.flatMapToPair( new CreateChunks< K >() );
	}

	public < K > JavaPairRDD< K, double[] > mergeTransforms(
			JavaPairRDD< K, Tuple2< Integer, double[] > > transforms )
	{
		JavaPairRDD< K, HashMap< Integer, double[] > > aggregatedAsMap =
				transforms.aggregateByKey( new HashMap< Integer, double[] >(), new AggregateAfterInferenceSeqFunc(), new AggregateAfterInferenceCombFunc() );

		return aggregatedAsMap.mapToPair( new MergeTransforms< K >() );
	}

	public class CreateChunks< K > implements
			PairFlatMapFunction< Tuple2< K, Tuple2< FloatProcessor, double[] > >, K, Tuple2< Integer, Tuple2< FloatProcessor, double[] > > >
	{

		/**
		 * 
		 */
		private static final long serialVersionUID = 8680726954953293122L;

		@Override
		public Iterable< Tuple2< K, Tuple2< Integer, Tuple2< FloatProcessor, double[] > > > >
				call( Tuple2< K, Tuple2< FloatProcessor, double[] > > t ) throws Exception
		{
			final FloatProcessor fp = t._2()._1();
			double[] startingCoordinates = t._2()._2();
			final K xy = t._1();
			int w = fp.getWidth();
			int h = fp.getHeight();
			ArrayList< Tuple2< K, Tuple2< Integer, Tuple2< FloatProcessor, double[] > > > > al = new ArrayList<>();
			if ( stepSize < 1 || stepSize >= h / 2 )
			{
				al.add( Utility.tuple2( xy, Utility.tuple2( 0, t._2() ) ) );
			}
			else
			{
				for ( int z = 0, idx = 0; z < h; z += stepSize, ++idx )
				{
					int lower = Math.max( z - overlap, 0 );
					int upper = Math.min( z + overlap + stepSize, h );
					double[] sc = new double[ upper - lower ];
					FloatProcessor crop = new FloatProcessor( w, sc.length );

					for ( int y = 0, yWorld = lower; yWorld < upper; ++y, ++yWorld )
					{
						sc[ y ] = startingCoordinates[ yWorld ] - startingCoordinates[ lower ];
						for ( int x = 0; x < w; ++x )
						{

							crop.setf( x, y, fp.getf( x, yWorld ) );
						}
					}
					al.add( Utility.tuple2( xy, Utility.tuple2( idx, Utility.tuple2( crop, sc ) ) ) );
				}
			}
			return al;
		}
	}

	public class AggregateAfterInferenceSeqFunc implements
			Function2< HashMap< Integer, double[] >, Tuple2< Integer, double[] >, HashMap< Integer, double[] > >
	{
		/**
		 * 
		 */
		private static final long serialVersionUID = 2464715442114363017L;

		@Override
		public HashMap< Integer, double[] > call( HashMap< Integer, double[] > m, Tuple2< Integer, double[] > t ) throws Exception
		{
			m.put( t._1(), t._2() );
			return m;
		}
	}

	public class AggregateAfterInferenceCombFunc implements
			Function2< HashMap< Integer, double[] >, HashMap< Integer, double[] >, HashMap< Integer, double[] > >
	{

		/**
		 * 
		 */
		private static final long serialVersionUID = 5515881176929911485L;

		@Override
		public HashMap< Integer, double[] > call( HashMap< Integer, double[] > m1, HashMap< Integer, double[] > m2 ) throws Exception
		{
			m1.putAll( m2 );
			return m1;
		}
	}

	public class MergeTransforms< K > implements
			PairFunction< Tuple2< K, HashMap< Integer, double[] > >, K, double[] >
	{

		/**
		 * 
		 */
		private static final long serialVersionUID = -2354364405849240616L;

		@Override
		public Tuple2< K, double[] > call( Tuple2< K, HashMap< Integer, double[] > > t ) throws Exception
		{
			final double[] transform = new double[ length ];
			final HashMap< Integer, double[] > map = t._2();
			final int size = map.size();
			System.arraycopy( map.get( 0 ), 0, transform, 0, map.get( 0 ).length );

			for ( int i = 1, currentLowerZ = stepSize; i < size; ++i, currentLowerZ += stepSize )
			{
				final double[] currentLut = map.get( i );
				final int lowerComp = range;
				final int upperComp = overlap - range;
				final int lowerCompWorld = currentLowerZ - overlap + lowerComp;
				final int upperCompWorld = currentLowerZ - overlap + upperComp;
				final int overlapLength = upperComp - lowerComp;

				AffineModel1D m = new AffineModel1D();

				double[][] from = new double[ 1 ][ upperComp - lowerComp ];
				double[][] to = new double[ 1 ][ upperComp - lowerComp ];
				double[] weights = new double[ upperComp - lowerComp ];

				for ( int z = lowerComp, zWorld = lowerCompWorld, idx = 0; z < upperComp; ++z, ++zWorld, ++idx )
				{
					from[ 0 ][ idx ] = currentLut[ z ];
					to[ 0 ][ idx ] = transform[ zWorld ];
					weights[ idx ] = 1.0;
				}

				m.fit( from, to, weights );

				double[] dummy = new double[ 1 ];

				for ( int z = lowerComp, zWorld = lowerCompWorld; z < upperComp; ++z, ++zWorld )
				{
					final double weight = ( z - lowerComp ) * 1.0 / overlapLength;
					dummy[ 0 ] = currentLut[ z ];
					m.applyInPlace( dummy );
					final double previousVal = transform[ zWorld ];
					transform[ zWorld ] = weight * dummy[ 0 ] + ( 1 - weight ) * previousVal;
				}

				for ( int z = upperComp, zWorld = upperCompWorld; z < currentLut.length; ++z, ++zWorld )
				{
					dummy[ 0 ] = currentLut[ z ];
					m.applyInPlace( dummy );
					transform[ zWorld ] = dummy[ 0 ];
				}

			}

			return Utility.tuple2( t._1(), transform );
		}
	}

}
