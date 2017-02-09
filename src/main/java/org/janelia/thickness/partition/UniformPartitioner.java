package org.janelia.thickness.partition;

import java.lang.invoke.MethodHandles;
import java.util.Random;

import org.apache.log4j.Level;
import org.apache.log4j.LogManager;
import org.apache.log4j.Logger;
import org.apache.spark.Partitioner;

public class UniformPartitioner extends Partitioner
{

	public static final Logger LOG = LogManager.getLogger( MethodHandles.lookup().lookupClass() );
	static
	{
		LOG.setLevel( Level.INFO );
	}

	private final int numPartitions;

	public UniformPartitioner( final int numPartitions )
	{
		super();
		this.numPartitions = numPartitions;
		LOG.info( "Created " + MethodHandles.lookup().lookupClass().getSimpleName() + " with " + numPartitions + " partitions." );
	}

	@Override
	public int getPartition( final Object o )
	{
		final int part = new Random().nextInt( numPartitions );
		LOG.debug( "Assigning object " + o + " to partition " + part );
		return part;
	}

	@Override
	public int numPartitions()
	{
		return numPartitions;
	}

}
