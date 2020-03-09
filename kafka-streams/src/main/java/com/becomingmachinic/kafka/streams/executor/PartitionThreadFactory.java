package com.becomingmachinic.kafka.streams.executor;

public interface PartitionThreadFactory {
	
	public PartitionThread create(int partitionCode, Runnable worker);
	
}
