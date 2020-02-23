package com.becomingmachinic.kafka.streams.executor;

public interface PartitionThreadFactory {
	
	public PartitionThread create(PartitionId partitionId, Runnable worker);
	
}
