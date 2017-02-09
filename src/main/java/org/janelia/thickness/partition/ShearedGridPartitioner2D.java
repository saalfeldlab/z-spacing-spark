package org.janelia.thickness.partition;

import java.lang.invoke.MethodHandles;
import java.util.Arrays;

import org.apache.log4j.Level;
import org.apache.log4j.LogManager;
import org.apache.log4j.Logger;
import org.apache.spark.Partitioner;

import net.imglib2.util.IntervalIndexer;
import net.imglib2.util.Intervals;
import scala.Tuple2;

public class ShearedGridPartitioner2D extends Partitioner
{

	public static final Logger LOG = LogManager.getLogger( MethodHandles.lookup().lookupClass() );
	static
	{
		LOG.setLevel( Level.INFO );
	}

	private final int[] min;

	private final int[] dim;

	private final int[] pos;

	private final int partitionSize;

	private final int numPartitions;


	public ShearedGridPartitioner2D( final int[] min, final int[] dim, final int parallelism )
	{
		super();
		this.min = min;
		this.dim = dim;
		this.pos = new int[ 2 ];
		final int nElements = ( int ) Intervals.numElements( dim );
		this.numPartitions = Math.min( parallelism, nElements );
		this.partitionSize = Math.max( ( int ) Math.ceil( nElements * 1.0 / this.numPartitions ), 1 );
		LOG.info( "Created grid partitioner with dim=" + Arrays.toString( dim ) + " min=" + Arrays.toString( min ) + " numPartitions=" + numPartitions + " partitionSize=" + partitionSize );
	}

	@Override
	public int getPartition( final Object o )
	{
		if ( o instanceof Tuple2 )
		{
			final Tuple2< ?, ? > t = ( Tuple2< ?, ? > ) o;
			final Object v1 = t._1();
			final Object v2 = t._2();
			if ( v1 instanceof Integer && v2 instanceof Integer )
			{
				final int i1 = ( ( Integer ) v1 ).intValue();
				pos[ 0 ] = i1 - min[ 0 ];
				pos[ 1 ] = ( ( Integer ) v2 ).intValue() - min[ 1 ] - i1;
				final int index = IntervalIndexer.positionToIndex( pos, dim );
				final int partition = index / partitionSize;
				LOG.debug( "Partitioning " + o + " to index " + partition + " (" + numPartitions() + ") " );
				return partition;
			}
		}
		throw new IllegalArgumentException( "Only Tuple2< Integer, Integer > is valid argument. Passed: " + o.getClass().getSimpleName() );
	}

	@Override
	public int numPartitions()
	{
		return numPartitions;
	}

}
