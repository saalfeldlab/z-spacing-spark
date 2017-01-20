package org.janelia.thickness;

import java.util.ArrayList;
import java.util.HashMap;
import java.util.Iterator;
import java.util.Map;
import java.util.Random;

import org.apache.spark.SparkConf;
import org.apache.spark.api.java.JavaPairRDD;
import org.apache.spark.api.java.JavaSparkContext;
import org.apache.spark.api.java.function.Function2;
import org.apache.spark.api.java.function.PairFlatMapFunction;
import org.apache.spark.api.java.function.PairFunction;
import org.janelia.thickness.utility.DPTuple;
import org.janelia.thickness.utility.Utility;

import scala.Tuple2;

/**
 * Created by hanslovskyp on 9/28/15.
 */
public class ColumnsAndSections
{

	private final int[] dim;

	private final int size;

	public ColumnsAndSections( final int[] dim, final int size )
	{
		this.dim = dim;
		this.size = size;
	}

	public int xyToLinear( final int x, final int y )
	{
		return Utility.xyToLinear( dim[ 0 ], x, y );
	}

	public int[] lienarToXY( final int i )
	{
		return linearToXY( i, new int[ 2 ] );
	}

	public int[] linearToXY( final int i, final int[] xy )
	{
		return Utility.linearToXY( dim[ 0 ], i, xy );
	}

	public JavaPairRDD< Integer, DPTuple > columnsToSections( final JavaSparkContext sc, final JavaPairRDD< Tuple2< Integer, Integer >, double[] > input )
	{
		final JavaPairRDD< Integer, HashMap< Tuple2< Integer, Integer >, Double > > xyToZMap = input.flatMapToPair( new XYToZ( size ) );

		final JavaPairRDD< Integer, HashMap< Tuple2< Integer, Integer >, Double > > joined = xyToZMap.reduceByKey( new Joiner< Tuple2< Integer, Integer >, Double, HashMap< Tuple2< Integer, Integer >, Double > >()
		// new Function2<HashMap<Tuple2<Integer, Integer>, Double>,
		// HashMap<Tuple2<Integer, Integer>, Double>, HashMap<Tuple2<Integer,
		// Integer>, Double>>() {
		// @Override
		// public HashMap<Tuple2<Integer, Integer>, Double> call(
		// HashMap<Tuple2<Integer, Integer>, Double> hm1,
		// HashMap<Tuple2<Integer, Integer>, Double> hm2) throws Exception {
		// HashMap<Tuple2<Integer, Integer>, Double> hm = new
		// HashMap<Tuple2<Integer, Integer>, Double>();
		// hm.putAll(hm1);
		// hm.putAll(hm2);
		// return hm;
		// }
		// }
		);

		final JavaPairRDD< Integer, DPTuple > imgs = joined.mapToPair( new ConvertToImages( dim ) );

		// JavaPairRDD<Integer, DPTuple> joined = xyToZMap.aggregateByKey(
		// new DPTuple(dim[0], dim[1]),
		// new Function2<DPTuple, HashMap<Tuple2<Integer, Integer>, Double>,
		// DPTuple>() {
		//
		// @Override
		// public DPTuple call(DPTuple img, HashMap<Tuple2<Integer, Integer>,
		// Double> hm) throws Exception {
		// for (Map.Entry<Tuple2<Integer, Integer>, Double> entry :
		// hm.entrySet()) {
		// Tuple2<Integer, Integer> xy = entry.getKey();
		// img.pixels[ xyToLinear( img.width, xy._1(), xy._2() ) ] =
		// entry.getValue();
		// }
		// return img;
		// }
		// },
		// new Function2<DPTuple, DPTuple, DPTuple>() {
		//
		// @Override
		// public DPTuple call(DPTuple dpTuple, DPTuple dpTuple2) throws
		// Exception {
		// return null;
		// }
		// }
		// );
		return imgs;
	}

	public static class XYToZ implements PairFlatMapFunction< Tuple2< Tuple2< Integer, Integer >, double[] >, Integer, HashMap< Tuple2< Integer, Integer >, Double > >
	{

		private final int size;

		public XYToZ( final int size )
		{
			this.size = size;
		}

		@Override
		public Iterator< Tuple2< Integer, HashMap< Tuple2< Integer, Integer >, Double > > > call( final Tuple2< Tuple2< Integer, Integer >, double[] > t ) throws Exception
		{
			final Tuple2< Integer, Integer > xy = t._1();
			final double[] arr = t._2();
			final Iterable< Tuple2< Integer, HashMap< Tuple2< Integer, Integer >, Double > > > it = new Iterable< Tuple2< Integer, HashMap< Tuple2< Integer, Integer >, Double > > >()
			{
				@Override
				public Iterator< Tuple2< Integer, HashMap< Tuple2< Integer, Integer >, Double > > > iterator()
				{
					return new Iterator< Tuple2< Integer, HashMap< Tuple2< Integer, Integer >, Double > > >()
					{
						int count = 0;

						@Override
						public boolean hasNext()
						{
							return count < size;
						}

						@Override
						public Tuple2< Integer, HashMap< Tuple2< Integer, Integer >, Double > > next()
						{
							final int currCount = count++;
							final double val = arr == null ? Double.NaN : arr[ currCount ];
							final HashMap< Tuple2< Integer, Integer >, Double > hm = new HashMap<>();
							hm.put( xy, val );
							return Utility.tuple2( currCount, hm );
						}

						@Override
						public void remove()
						{
							throw new UnsupportedOperationException();
						}
					};
				}
			};
			return it.iterator();
		}
	}

	public static class Joiner< K, V, M extends Map< K, V > > implements Function2< M, M, M >
	{

		@Override
		public M call( final M m1, final M m2 ) throws Exception
		{
			final M result = ( M ) m1.getClass().newInstance();
			result.putAll( m1 );
			result.putAll( m2 );
			// String logString = StringUtils.join(m1) + StringUtils.join(m2) +
			// StringUtils.join(result);
			// System.out.println( logString );
			return result;
		}
	}

	public static class ConvertToImages implements PairFunction< Tuple2< Integer, HashMap< Tuple2< Integer, Integer >, Double > >, Integer, DPTuple >
	{
		private final int[] dim;

		public ConvertToImages( final int[] dim )
		{
			this.dim = dim;
		}

		@Override
		public Tuple2< Integer, DPTuple > call( final Tuple2< Integer, HashMap< Tuple2< Integer, Integer >, Double > > t ) throws Exception
		{
			final DPTuple img = new DPTuple( dim[ 0 ], dim[ 1 ] );
			for ( int i = 0; i < img.pixels.length; ++i )
				img.pixels[ i ] = Double.NaN;
			for ( final Map.Entry< Tuple2< Integer, Integer >, Double > entry : t._2().entrySet() )
			{
				final Tuple2< Integer, Integer > xy = entry.getKey();
				img.pixels[ Utility.xyToLinear( dim[ 0 ], xy._1(), xy._2() ) ] = entry.getValue();
			}
			return Utility.tuple2( t._1(), img );
		}
	}

	public JavaPairRDD< Tuple2< Integer, Integer >, double[] > sectionsToColumns( final JavaSparkContext sc, final JavaPairRDD< Integer, DPTuple > input )
	{

		final JavaPairRDD< Tuple2< Integer, Integer >, HashMap< Integer, Double > > xyToZMap = input.flatMapToPair( new ZToXY( dim ) );

		final JavaPairRDD< Tuple2< Integer, Integer >, HashMap< Integer, Double > > joined = xyToZMap.reduceByKey( new Joiner< Integer, Double, HashMap< Integer, Double > >() );

		final JavaPairRDD< Tuple2< Integer, Integer >, double[] > columns = joined.mapToPair( new ConvertToArray() );

		return columns;
	}

	public static class ZToXY implements PairFlatMapFunction< Tuple2< Integer, DPTuple >, Tuple2< Integer, Integer >, HashMap< Integer, Double > >
	{
		private final int[] dim;

		public ZToXY( final int[] dim )
		{
			this.dim = dim;
		}

		@Override
		public Iterator< Tuple2< Tuple2< Integer, Integer >, HashMap< Integer, Double > > > call( final Tuple2< Integer, DPTuple > t ) throws Exception
		{
			final Integer z = t._1();
			final DPTuple img = t._2();
			final Iterable< Tuple2< Tuple2< Integer, Integer >, HashMap< Integer, Double > > > it = new Iterable< Tuple2< Tuple2< Integer, Integer >, HashMap< Integer, Double > > >()
			{
				@Override
				public Iterator< Tuple2< Tuple2< Integer, Integer >, HashMap< Integer, Double > > > iterator()
				{
					return new Iterator< Tuple2< Tuple2< Integer, Integer >, HashMap< Integer, Double > > >()
					{
						int i = 0;

						int[] xy = new int[ 2 ];

						@Override
						public boolean hasNext()
						{
							return i < img.pixels.length;
						}

						@Override
						public Tuple2< Tuple2< Integer, Integer >, HashMap< Integer, Double > > next()
						{
							final int currentI = i++;
							Utility.linearToXY( dim[ 0 ], currentI, xy );
							final HashMap< Integer, Double > hm = new HashMap<>();
							hm.put( z, img.pixels[ currentI ] );
							return Utility.tuple2( Utility.tuple2( xy[ 0 ], xy[ 1 ] ), hm );
						}

						@Override
						public void remove()
						{
							throw new UnsupportedOperationException();
						}
					};
				}
			};
			return it.iterator();
		}
	}

	public static class ConvertToArray implements PairFunction< Tuple2< Tuple2< Integer, Integer >, HashMap< Integer, Double > >, Tuple2< Integer, Integer >, double[] >
	{
		@Override
		public Tuple2< Tuple2< Integer, Integer >, double[] > call( final Tuple2< Tuple2< Integer, Integer >, HashMap< Integer, Double > > t ) throws Exception
		{
			final HashMap< Integer, Double > hm = t._2();
			final double[] data = new double[ hm.size() ];
			for ( final Map.Entry< Integer, Double > entry : hm.entrySet() )
				data[ entry.getKey() ] = entry.getValue();
			return Utility.tuple2( t._1(), data );
		}
	}

	public static void main( final String[] args )
	{
		final int width = 3, height = 4;
		final int size = 2;
		final int[] dim = new int[] { width, height };
		final ColumnsAndSections cas = new ColumnsAndSections( dim, size );
		final Random rng = new Random();
		final ArrayList< Tuple2< Tuple2< Integer, Integer >, double[] > > al = new ArrayList<>();
		for ( int x = 0; x < width; ++x )
			for ( int y = 0; y < height; ++y )
			{
				final double[] data = new double[ size ];
				for ( int i = 0; i < data.length; ++i )
					data[ i ] = rng.nextDouble();
				al.add( Utility.tuple2( Utility.tuple2( x, y ), data ) );
			}

		final SparkConf conf = new SparkConf().setAppName( ColumnsAndSections.class.getName() ).setMaster( "local[*]" );
		final JavaSparkContext sc = new JavaSparkContext( conf );
		final JavaPairRDD< Tuple2< Integer, Integer >, double[] > in = sc.parallelizePairs( al );
		final JavaPairRDD< Integer, DPTuple > outRdd = cas.columnsToSections( sc, in );
		outRdd.count();
		final Map< Integer, DPTuple > out = outRdd.collectAsMap();

		final JavaPairRDD< Tuple2< Integer, Integer >, double[] > compRdd = cas.sectionsToColumns( sc, outRdd );

		final Map< Tuple2< Integer, Integer >, double[] > compOut = compRdd.collectAsMap();

		sc.close();

		try
		{
			Thread.sleep( 5000 );
		}
		catch ( final InterruptedException e )
		{
			e.printStackTrace();
		}

		for ( final Tuple2< Tuple2< Integer, Integer >, double[] > t : al )
		{
			final int x = t._1()._1();
			final int y = t._1()._2();
			final double[] data = t._2();
			final double[] dataComp = compOut.get( t._1() );
			for ( int z = 0; z < data.length; ++z )
			{
				final DPTuple img = out.get( z );
				final double ref = data[ z ];
				final double comp = img.pixels[ cas.xyToLinear( x, y ) ];
				if ( comp != ref )
				{
					System.err.println( Utility.tuple3( x, y, z ) + ": " + comp + " " + ref + " (columnsToSections)" );
					System.exit( 9001 );
				}
				if ( dataComp[ z ] != ref )
				{
					System.err.println( Utility.tuple3( x, y, z ) + ": " + dataComp[ z ] + " " + ref + " (sectionsToColumns)" );
					System.exit( 9001 );
				}
			}
		}

		System.out.println( "Done." );

	}

}
