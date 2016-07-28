package org.janelia.thickness;

import org.apache.spark.SparkConf;
import org.apache.spark.api.java.JavaPairRDD;
import org.apache.spark.api.java.JavaSparkContext;
import org.apache.spark.api.java.function.Function2;
import org.apache.spark.api.java.function.PairFlatMapFunction;
import org.apache.spark.api.java.function.PairFunction;
import org.janelia.thickness.utility.DPTuple;
import org.janelia.thickness.utility.Utility;
import scala.Tuple2;

import java.util.*;

/**
 * @author Philipp Hanslovsky &lt;hanslovskyp@janelia.hhmi.org&gt;
 */
public class ColumnsAndSections
{

	private final int[] dim;

	private final int size;

	public ColumnsAndSections( int[] dim, int size )
	{
		this.dim = dim;
		this.size = size;
	}

	public int xyToLinear( int x, int y )
	{
		return Utility.xyToLinear( dim[ 0 ], x, y );
	}

	public int[] lienarToXY( int i )
	{
		return linearToXY( i, new int[ 2 ] );
	}

	public int[] linearToXY( int i, int[] xy )
	{
		return Utility.linearToXY( dim[ 0 ], i, xy );
	}

	public JavaPairRDD< Integer, DPTuple > columnsToSections(
			JavaSparkContext sc,
			JavaPairRDD< Tuple2< Integer, Integer >, double[] > input )
	{
		JavaPairRDD< Integer, HashMap< Tuple2< Integer, Integer >, Double > > xyToZMap = input
				.flatMapToPair( new XYToZ( size ) );

		JavaPairRDD< Integer, HashMap< Tuple2< Integer, Integer >, Double > > joined = xyToZMap.reduceByKey(
				new Joiner< Tuple2< Integer, Integer >, Double, HashMap< Tuple2< Integer, Integer >, Double > >() );

		JavaPairRDD< Integer, DPTuple > imgs = joined
				.mapToPair( new ConvertToImages( dim ) );

		return imgs;
	}

	public static class XYToZ implements PairFlatMapFunction< Tuple2< Tuple2< Integer, Integer >, double[] >, Integer, HashMap< Tuple2< Integer, Integer >, Double > >
	{

		/**
		 * 
		 */
		private static final long serialVersionUID = -8639870212459317088L;

		private final int size;

		public XYToZ( int size )
		{
			this.size = size;
		}

		@Override
		public Iterable< Tuple2< Integer, HashMap< Tuple2< Integer, Integer >, Double > > >
				call( Tuple2< Tuple2< Integer, Integer >, double[] > t ) throws Exception
		{
			final Tuple2< Integer, Integer > xy = t._1();
			final double[] arr = t._2();
			return new Iterable< Tuple2< Integer, HashMap< Tuple2< Integer, Integer >, Double > > >()
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
							int currCount = count++;
							double val = arr == null ? Double.NaN : arr[ currCount ];
							HashMap< Tuple2< Integer, Integer >, Double > hm = new HashMap< Tuple2< Integer, Integer >, Double >();
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
		}
	}

	public static class Joiner< K, V, M extends Map< K, V > > implements Function2< M, M, M >
	{

		/**
		 * 
		 */
		private static final long serialVersionUID = 5419988792646298247L;

		@Override
		public M call( M m1, M m2 ) throws Exception
		{
			@SuppressWarnings( "unchecked" )
			M result = ( M ) m1.getClass().newInstance();
			result.putAll( m1 );
			result.putAll( m2 );
			return result;
		}
	}

	public static class ConvertToImages implements PairFunction< Tuple2< Integer, HashMap< Tuple2< Integer, Integer >, Double > >, Integer, DPTuple >
	{
		/**
		 * 
		 */
		private static final long serialVersionUID = -1435035403092923883L;

		private final int[] dim;

		public ConvertToImages( int[] dim )
		{
			this.dim = dim;
		}

		@Override
		public Tuple2< Integer, DPTuple > call( Tuple2< Integer, HashMap< Tuple2< Integer, Integer >, Double > > t ) throws Exception
		{
			DPTuple img = new DPTuple( dim[ 0 ], dim[ 1 ] );
			for ( int i = 0; i < img.pixels.length; ++i )
				img.pixels[ i ] = Double.NaN;
			for ( Map.Entry< Tuple2< Integer, Integer >, Double > entry : t._2().entrySet() )
			{
				Tuple2< Integer, Integer > xy = entry.getKey();
				img.pixels[ Utility.xyToLinear( dim[ 0 ], xy._1(), xy._2() ) ] = entry.getValue();
			}
			return Utility.tuple2( t._1(), img );
		}
	}

	public JavaPairRDD< Tuple2< Integer, Integer >, double[] > sectionsToColumns( JavaSparkContext sc, JavaPairRDD< Integer, DPTuple > input )
	{

		JavaPairRDD< Tuple2< Integer, Integer >, HashMap< Integer, Double > > xyToZMap = input
				.flatMapToPair( new ZToXY( dim ) )
				.cache();

		JavaPairRDD< Tuple2< Integer, Integer >, HashMap< Integer, Double > > joined = xyToZMap
				.reduceByKey(
						new Joiner< Integer, Double, HashMap< Integer, Double > >() )
				.cache();

		JavaPairRDD< Tuple2< Integer, Integer >, double[] > columns = joined.mapToPair( new ConvertToArray() );

		return columns;
	}

	public static class ZToXY implements PairFlatMapFunction< Tuple2< Integer, DPTuple >, Tuple2< Integer, Integer >, HashMap< Integer, Double > >
	{
		/**
		 * 
		 */
		private static final long serialVersionUID = -3093241807946238137L;

		private final int[] dim;

		public ZToXY( int[] dim )
		{
			this.dim = dim;
		}

		@Override
		public Iterable< Tuple2< Tuple2< Integer, Integer >, HashMap< Integer, Double > > > call( Tuple2< Integer, DPTuple > t ) throws Exception
		{
			final Integer z = t._1();
			final DPTuple img = t._2();
			return new Iterable< Tuple2< Tuple2< Integer, Integer >, HashMap< Integer, Double > > >()
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
							int currentI = i++;
							Utility.linearToXY( dim[ 0 ], currentI, xy );
							HashMap< Integer, Double > hm = new HashMap< Integer, Double >();
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
		}
	}

	public static class ConvertToArray implements PairFunction< Tuple2< Tuple2< Integer, Integer >, HashMap< Integer, Double > >, Tuple2< Integer, Integer >, double[] >
	{
		/**
		 * 
		 */
		private static final long serialVersionUID = -7360491626120581864L;

		@Override
		public Tuple2< Tuple2< Integer, Integer >, double[] > call( Tuple2< Tuple2< Integer, Integer >, HashMap< Integer, Double > > t ) throws Exception
		{
			HashMap< Integer, Double > hm = t._2();
			double[] data = new double[ hm.size() ];
			for ( Map.Entry< Integer, Double > entry : hm.entrySet() )
				data[ entry.getKey() ] = entry.getValue();
			return Utility.tuple2( t._1(), data );
		}
	}

	public static void main( String[] args )
	{
		int width = 3, height = 4;
		int size = 2;
		int[] dim = new int[] { width, height };
		ColumnsAndSections cas = new ColumnsAndSections( dim, size );
		Random rng = new Random();
		ArrayList< Tuple2< Tuple2< Integer, Integer >, double[] > > al = new ArrayList< Tuple2< Tuple2< Integer, Integer >, double[] > >();
		for ( int x = 0; x < width; ++x )
		{
			for ( int y = 0; y < height; ++y )
			{
				double[] data = new double[ size ];
				for ( int i = 0; i < data.length; ++i )
				{
					data[ i ] = rng.nextDouble();
				}
				al.add( Utility.tuple2( Utility.tuple2( x, y ), data ) );
			}
		}

		SparkConf conf = new SparkConf().setAppName( ColumnsAndSections.class.getName() ).setMaster( "local[*]" );
		JavaSparkContext sc = new JavaSparkContext( conf );
		JavaPairRDD< Tuple2< Integer, Integer >, double[] > in = sc.parallelizePairs( al );
		JavaPairRDD< Integer, DPTuple > outRdd = cas.columnsToSections( sc, in ).cache();
		outRdd.count();
		Map< Integer, DPTuple > out = outRdd.collectAsMap();

		JavaPairRDD< Tuple2< Integer, Integer >, double[] > compRdd = cas.sectionsToColumns( sc, outRdd ).cache();

		Map< Tuple2< Integer, Integer >, double[] > compOut = compRdd.collectAsMap();

		sc.close();

		try
		{
			Thread.sleep( 5000 );
		}
		catch ( InterruptedException e )
		{
			e.printStackTrace();
		}

		for ( Tuple2< Tuple2< Integer, Integer >, double[] > t : al )
		{
			int x = t._1()._1();
			int y = t._1()._2();
			double[] data = t._2();
			double[] dataComp = compOut.get( t._1() );
			for ( int z = 0; z < data.length; ++z )
			{
				DPTuple img = out.get( z );
				double ref = data[ z ];
				double comp = img.pixels[ cas.xyToLinear( x, y ) ];
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
