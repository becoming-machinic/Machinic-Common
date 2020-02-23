package com.becomingmachinic.kafka.streams;

import java.util.ArrayList;
import java.util.Collection;
import java.util.List;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.ConcurrentMap;
import java.util.concurrent.TimeUnit;

import com.becomingmachinic.kafka.streams.executor.PartitionExecutor;
import com.becomingmachinic.kafka.streams.executor.PartitionExecutorFactory;
import com.becomingmachinic.kafka.streams.executor.PartitionId;
import com.becomingmachinic.kafka.streams.executor.PartitionTask;

public abstract class AbstractStreamSource {
	
	private final PartitionExecutorFactory partitionExecutorFactory;
	private final ConcurrentMap<PartitionId, PartitionExecutor> partitionExecutorMap = new ConcurrentHashMap<>();
	
	protected AbstractStreamSource(PartitionExecutorFactory partitionExecutorFactory) {
		this.partitionExecutorFactory = partitionExecutorFactory;
	}
	
	/**
	 * Offer task to the PartitionExecutor. If the partition is accepting tasks and the queue is not full it will be accepted. If the PartitionExecutor for the passed PartitionId does not exist it will be created.
	 *
	 * @param partitionId
	 *          the identity of the partition
	 * @param task
	 *          the task that will be offered to the PartitionExecutor
	 * @return {@code true} if the element was added to task queue, else {@code false}
	 * @throws InterruptedException
	 *           if interrupted while waiting
	 */
	protected boolean offer(PartitionId partitionId, PartitionTask task) {
		PartitionExecutor partitionExecutor = this.partitionExecutorMap.computeIfAbsent(partitionId, k -> createPartitionExecutor(k));
		return partitionExecutor.offer(task);
	}
	
	/**
	 * @param partitionId
	 * @param task
	 * @param timeout
	 * @param unit
	 * @return {@code true} if the element was added to task queue, else {@code false}
	 * @throws InterruptedException
	 *           if interrupted while waiting
	 */
	protected boolean offer(PartitionId partitionId, PartitionTask task, long timeout, TimeUnit unit) throws InterruptedException {
		PartitionExecutor partitionExecutor = this.partitionExecutorMap.computeIfAbsent(partitionId, k -> createPartitionExecutor(k));
		return partitionExecutor.offer(task, timeout, unit);
	}
	
	protected void assignPartitions(Collection<PartitionId> partitionIds) {
		for (PartitionId partitionId : partitionIds) {
			this.partitionExecutorMap.computeIfAbsent(partitionId, k -> createPartitionExecutor(k));
		}
	}
	
	protected List<PartitionExecutor> revokePartitions(Collection<PartitionId> partitionIds) {
		List<PartitionExecutor> revokedPartitions = new ArrayList<>();
		for (PartitionId partitionId : partitionIds) {
			PartitionExecutor partitionExecutor = this.partitionExecutorMap.remove(partitionId);
			partitionExecutor.shutdown();
			revokedPartitions.add(partitionExecutor);
		}
		return revokedPartitions;
	}
	
	/**
	 * Stop accepting new tasks. Already queued tasks will be executed normally.
	 */
	public void shutdown() {
		for (PartitionExecutor executor : this.partitionExecutorMap.values()) {
			executor.shutdown();
		}
	}
	/**
	 * Stop accepting new tasks and cancel tasks that have been queued but not started. Already running tasks will complete normally.
	 */
	public void shutdownNow() {
		for (PartitionExecutor executor : this.partitionExecutorMap.values()) {
			executor.shutdownNow();
		}
	}
	/**
	 * @return true if all partitions have a status of shutdown, terminating or terminated
	 */
	public boolean isShutdown() {
		for (PartitionExecutor executor : this.partitionExecutorMap.values()) {
			if (!executor.isShutdown()) {
				return false;
			}
		}
		return true;
	}
	/**
	 * @return true if all partitions have a status of terminated
	 */
	public boolean isTerminated() {
		for (PartitionExecutor executor : this.partitionExecutorMap.values()) {
			if (!executor.isTerminated()) {
				return false;
			}
		}
		return true;
	}
	
	private PartitionExecutor createPartitionExecutor(PartitionId partitionId) {
		return partitionExecutorFactory.create(partitionId);
	}
}
