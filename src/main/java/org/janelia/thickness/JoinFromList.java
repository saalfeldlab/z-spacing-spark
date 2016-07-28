package org.janelia.thickness;

import org.apache.spark.api.java.JavaPairRDD;
import org.apache.spark.api.java.function.PairFlatMapFunction;
import org.apache.spark.api.java.function.PairFunction;
import org.apache.spark.broadcast.Broadcast;
import org.janelia.thickness.utility.Utility;
import scala.Tuple2;

import java.util.Iterator;
import java.util.List;
import java.util.Map;

/**
 * @author Philipp Hanslovsky &lt;hanslovskyp@janelia.hhmi.org&gt;
 */
public class JoinFromList
{

	public static < K, L extends List< K >, M extends Map< K, L >, V > JavaPairRDD< Tuple2< K, K >, Tuple2< V, V > > projectOntoSelf(
			JavaPairRDD< K, V > rdd,
			Broadcast< M > keyPairList )
	{

		JavaPairRDD< Tuple2< K, V >, L > keysImagesListOfKeys = rdd
				.mapToPair( new AssociateWith< K, V, L, M >( keyPairList ) )
				.mapToPair( new MoveToKey< K, V, L >() );
		JavaPairRDD< K, Tuple2< K, V > > flat = keysImagesListOfKeys
				.flatMapToPair( new FlatOut< Tuple2< K, V >, K, L >() )
				.mapToPair( new Utility.SwapKeyValue< Tuple2< K, V >, K >() );

		JavaPairRDD< Tuple2< K, K >, Tuple2< V, V > > joint = flat
				.join( rdd )
				.mapToPair( new RearrangeSameKeysAndValues< K, V >() );

		return joint;
	}

	/**
	 * 
	 * Add association A to value V at appropriate key K
	 *
	 * rdd in: ( K -> V ) map M: { K -> A } rdd out: ( K -> V,A )
	 *
	 * @param <K>
	 * @param <V>
	 * @param <A>
	 * @param <M>
	 */
	public static class AssociateWith< K, V, A, M extends Map< K, A > > implements PairFunction< Tuple2< K, V >, K, Tuple2< V, A > >
	{

		private final Broadcast< M > keysAndValues;

		public AssociateWith( Broadcast< M > keysAndValues )
		{
			super();
			this.keysAndValues = keysAndValues;
		}

		private static final long serialVersionUID = 6199058917722338402L;

		public Tuple2< K, Tuple2< V, A > > call( Tuple2< K, V > t ) throws Exception
		{
			K key = t._1();
			return Utility.tuple2( key, Utility.tuple2( t._2(), keysAndValues.getValue().get( key ) ) );
		}
	}

	/**
	 * 
	 * Make first value part of key.
	 *
	 * rdd in: ( K1 -> V1,V2 ) rdd out: ( K1,V1 -> V2 )
	 *
	 * @param <K>
	 * @param <V1>
	 * @param <V2>
	 */
	public static class MoveToKey< K, V1, V2 > implements PairFunction< Tuple2< K, Tuple2< V1, V2 > >, Tuple2< K, V1 >, V2 >
	{

		/**
		 * 
		 */
		private static final long serialVersionUID = 5621896946938206708L;

		@Override
		public Tuple2< Tuple2< K, V1 >, V2 > call( Tuple2< K, Tuple2< V1, V2 > > t ) throws Exception
		{
			Tuple2< V1, V2 > valuePair = t._2();
			return Utility.tuple2( Utility.tuple2( t._1(), valuePair._1() ), valuePair._2() );
		}
	}

	/**
	 *
	 * Flatten PairRDD that stores pairs of key and Iterable<V> into pairs of
	 * key and V
	 *
	 * rdd in: ( T -> [U] ) rdd out: ( T -> U )
	 *
	 * @param <K>
	 * @param <V>
	 * @param <I>
	 */
	public static class FlatOut< K, V, I extends Iterable< V > > implements PairFlatMapFunction< Tuple2< K, I >, K, V >
	{

		class FlatteningIterator implements Iterator< Tuple2< K, V > >
		{

			private final Iterator< V > it;

			public FlatteningIterator( Iterator< V > it, K constant )
			{
				super();
				this.it = it;
				this.constant = constant;
			}

			private final K constant;

			public boolean hasNext()
			{
				return it.hasNext();
			}

			public Tuple2< K, V > next()
			{
				return Utility.tuple2( constant, it.next() );
			}

			@Override
			public void remove()
			{
				throw new UnsupportedOperationException();
			}

		}

		private static final long serialVersionUID = -6076962646695402045L;

		public Iterable< Tuple2< K, V > > call( final Tuple2< K, I > t ) throws Exception
		{
			Iterable< Tuple2< K, V > > iterable = new Iterable< Tuple2< K, V > >()
			{
				public Iterator< Tuple2< K, V > > iterator()
				{
					return new FlatteningIterator( t._2().iterator(), t._1() );
				}

			};
			return iterable;
		}
	}

	public static class RearrangeSameKeysAndValues< K, V > extends Rearrange< K, K, V, V >
	{

		/**
		 * 
		 */
		private static final long serialVersionUID = 6750532985494792621L;
	}

	/**
	 * 
	 * Rearrange RDD such that both keys form a pair and both values form a
	 * separate pair.
	 *
	 * rdd in: ( K1 -> (K2,V2),V2 ) rdd out: ( K1,K2 -> V1,V2 )
	 *
	 * @param <K1>
	 * @param <V1>
	 */
	public static class Rearrange< K1, K2, V1, V2 > implements
			PairFunction< Tuple2< K1, Tuple2< Tuple2< K2, V2 >, V1 > >, Tuple2< K1, K2 >, Tuple2< V1, V2 > >
	{

		private static final long serialVersionUID = -5511873062115999278L;

		public Tuple2< Tuple2< K1, K2 >, Tuple2< V1, V2 > > call( Tuple2< K1, Tuple2< Tuple2< K2, V2 >, V1 > > t ) throws Exception
		{
			Tuple2< Tuple2< K2, V2 >, V1 > t2 = t._2();
			Tuple2< K2, V2 > t21 = t2._1();
			return Utility.tuple2( Utility.tuple2( t._1(), t21._1() ), Utility.tuple2( t2._2(), t21._2() ) );
		}
	}

}
