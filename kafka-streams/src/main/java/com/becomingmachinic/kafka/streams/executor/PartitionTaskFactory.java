package com.becomingmachinic.kafka.streams.executor;

import com.becomingmachinic.kafka.streams.Callback;
import com.becomingmachinic.kafka.streams.StreamEvent;
import com.becomingmachinic.kafka.streams.StreamFlow;

public interface PartitionTaskFactory<K,V> {
	public PartitionTask create(StreamFlow<K,V> streamFlow,StreamEvent streamEvent, Callback<K,V> callback);
}
