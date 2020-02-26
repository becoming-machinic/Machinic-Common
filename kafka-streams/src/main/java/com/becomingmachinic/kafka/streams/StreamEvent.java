package com.becomingmachinic.kafka.streams;

import com.becomingmachinic.kafka.streams.executor.PartitionId;

public interface StreamEvent<K, V> {
	
	//TODO add headers and stuff
	
	public K getKey();
	
	public V getValue();
	
	public PartitionId getPartitionId();
}
