package com.becomingmachinic.kafka.streams.executor;

import java.util.concurrent.BlockingQueue;

public interface PartitionQueueFactory {
	BlockingQueue<PartitionTask> create();
}