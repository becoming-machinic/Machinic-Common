package com.becomingmachinic.kafka.streams.executor;

import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.ConcurrentMap;
import java.util.concurrent.TimeUnit;

public class PartitionExecutorFactory {
	private final int threadsPerPartition;
	private final long threadKeepAliveTime;
	private final TimeUnit unit;
	private final PartitionQueueFactory partitionQueueFactory;
	private final PartitionThreadFactory threadFactory;
	
	private final ConcurrentMap<PartitionId, PartitionExecutor> partitionExecutorMap = new ConcurrentHashMap<>();
	
	public PartitionExecutorFactory(int threadsPerPartition, long threadKeepAliveTime, TimeUnit unit, PartitionQueueFactory partitionQueueFactory, PartitionThreadFactory threadFactory) {
		this.threadsPerPartition = threadsPerPartition;
		this.threadKeepAliveTime = threadKeepAliveTime;
		this.unit = unit;
		this.partitionQueueFactory = partitionQueueFactory;
		this.threadFactory = threadFactory;
	}
	
	public PartitionExecutor create(PartitionId partitionId) {
		return new PartitionExecutor(partitionId, threadsPerPartition, threadKeepAliveTime, unit, partitionQueueFactory, threadFactory);
	}
}
