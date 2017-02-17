package org.janelia.thickness;

import org.apache.spark.Partitioner;

import net.imglib2.util.IntervalIndexer;
import net.imglib2.util.Intervals;
import scala.Tuple2;

public class IntervalIndexedPartitioner2D extends Partitioner
{

	private final long[] dim;

	private final long[] pos = new long[ 2 ];

	private final int numPartitions;

	private final double partitionSize;

	public IntervalIndexedPartitioner2D( final long[] dim, final int numPartitions )
	{
		super();
		this.dim = dim;
		this.numPartitions = Math.min( numPartitions, ( int ) Intervals.numElements( dim ) );
		this.partitionSize = Intervals.numElements( dim ) * 1.0 / this.numPartitions;
	}

	@Override
	public int getPartition( final Object o )
	{
		if ( o instanceof Tuple2 )
		{
			final Tuple2< ?, ? > t = ( Tuple2< ?, ? > ) o;
			final Object t1 = t._1();
			final Object t2 = t._2();
			if ( t1 instanceof Integer && t2 instanceof Integer )
			{
				pos[ 0 ] = ( ( Integer ) t1 ).intValue();
				pos[ 1 ] = ( ( Integer ) t2 ).intValue();
				return ( int ) ( IntervalIndexer.positionToIndex( pos, dim ) / this.partitionSize );
			}
		}
		return 0;
	}

	@Override
	public int numPartitions()
	{
		return this.numPartitions;
	}

}
