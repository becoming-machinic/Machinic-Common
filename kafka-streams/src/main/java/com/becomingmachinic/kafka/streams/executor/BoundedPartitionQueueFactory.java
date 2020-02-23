package com.becomingmachinic.kafka.streams.executor;

import java.util.concurrent.ArrayBlockingQueue;
import java.util.concurrent.BlockingQueue;
import java.util.concurrent.LinkedTransferQueue;

public class BoundedPartitionQueueFactory implements PartitionQueueFactory {
	
	private final int queueSize;
	
	public BoundedPartitionQueueFactory(int queueSize) {
		if (queueSize < 1) {
			throw new IllegalArgumentException("%s must have a size greater then or equal to one.");
		}
		
		this.queueSize = queueSize;
	}
	
	@Override
	public BlockingQueue<PartitionTask> create() {
		if (this.queueSize > 1) {
			return new ArrayBlockingQueue<>(queueSize);
		}
		return new LinkedTransferQueue<PartitionTask>();
	}
}
