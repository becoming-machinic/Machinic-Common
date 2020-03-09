package com.becomingmachinic.kafka.streams.executor;

public interface PartitionId {
	
	public default int getPartitionCode(int maxPartitionCount){
		return this.hashCode() % maxPartitionCount;
	}

}
