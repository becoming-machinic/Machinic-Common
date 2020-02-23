package com.becomingmachinic.kafka.streams.executor;

import java.util.HashSet;
import java.util.concurrent.BlockingQueue;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.concurrent.locks.ReentrantLock;

public class PartitionExecutor {
	
	private final ReentrantLock mainLock = new ReentrantLock();
	private final Status status = new Status();
	private final HashSet<Worker> workers = new HashSet<>();
	private final AtomicInteger workerCount = new AtomicInteger(0);
	
	private final PartitionId partitionId;
	private final int threadCount;
	private final long keepAliveNanos;
	private final BlockingQueue<PartitionTask> partitionQueue;
	private final PartitionThreadFactory threadFactory;
	
	public PartitionExecutor(PartitionId partitionId, int threadCount, long keepAliveTime, TimeUnit unit, PartitionQueueFactory partitionQueueFactory, PartitionThreadFactory threadFactory) {
		this.partitionId = partitionId;
		this.threadCount = threadCount;
		this.keepAliveNanos = unit.toNanos(keepAliveTime);
		this.partitionQueue = partitionQueueFactory.create();
		this.threadFactory = threadFactory;
	}
	
	/**
	 * Offer task to the executor. If the partition is accepting tasks and the queue is not full it will be accepted.
	 *
	 * @param task
	 * @return {@code true} if the element was added to task queue, else
	 *         {@code false}
	 * @throws InterruptedException if interrupted while waiting
	 */
	public boolean offer(PartitionTask task) {
		if (this.status.isAcceptingTasks() && this.partitionQueue.offer(task)) {
			this.checkWorkerCount();
			return true;
		}
		return false;
	}
	
	/**
	 * Offer task to the executor. If the partition is accepting tasks and the queue is not full it will be accepted.
	 *
	 * @param task
	 * @param timeout
	 * @param unit
	 * @return {@code true} if the element was added to task queue, else
	 *         {@code false}
	 * @throws InterruptedException if interrupted while waiting
	 */
	public boolean offer(PartitionTask task, long timeout, TimeUnit unit) throws InterruptedException {
		if (this.status.isAcceptingTasks() && this.partitionQueue.offer(task, timeout, unit)) {
			this.checkWorkerCount();
			return true;
		}
		return false;
	}
	
	/**
	 * Triggers an orderly shutdown of the {@code PartitionExecutor}. New tasks will no longer be accepted.
	 * Queued tasks will continue to be executed. When all tasks are completed the {@code PartitionExecutor} will be terminated.
	 */
	public void shutdown() {
		if (!isShutdown()) {
			this.mainLock.lock();
			this.status.toShutdown();
			interruptIdleWorkers();
			this.addWorkers();
			this.mainLock.unlock();
		}
	}
	/**
	 * Triggers an expedited shutdown of the {@code PartitionExecutor}. New tasks will no longer be accepted. Queued tasks will be canceled. Already running tasks will be allowed to complete. When all tasks are completed the {@code PartitionExecutor} will be
	 * terminated.
	 */
	public void shutdownNow() {
		if (!this.status.isTerminated()) {
			this.shutdown();
			this.status.toTerminating();
			interruptIdleWorkers();
			this.addWorkers();
		}
	}
	
	public boolean isAcceptingTasks() {
		return this.status.isAcceptingTasks();
	}
	
	public boolean isShutdown() {
		return this.status.isShuttingDown();
	}
	
	public boolean isTerminated() {
		return this.status.isTerminated();
	}
	
	public void awaitTermination() throws InterruptedException {
		this.shutdown();
		this.status.awaitTermination();
	}
	
	public boolean awaitTermination(long timeout, TimeUnit unit) throws InterruptedException {
		this.shutdown();
		return this.status.awaitTermination(timeout, unit);
	}
	
	private void checkWorkerCount() {
		if (this.workerCount.get() < threadCount) {
			addWorkers();
		}
	}
	
	private void addWorkers() {
		this.mainLock.lock();
		try {
			if (this.status.isExecutingTasks()) {
				while (this.workerCount.get() < this.threadCount) {
					this.workers.add(new Worker());
				}
			}
		} finally {
			this.mainLock.unlock();
		}
	}
	
	private void removeWorker(Worker worker) {
		this.mainLock.lock();
		try {
			this.workers.remove(worker);
			if (!this.status.isExecutingTasks()) {
				if (this.workers.isEmpty() && this.partitionQueue.isEmpty()) {
					this.status.toTerminated();
				}
			}
		} finally {
			this.mainLock.unlock();
		}
	}
	
	private void interruptIdleWorkers() {
		this.mainLock.lock();
		try {
			for (Worker worker : this.workers) {
				worker.interruptIfIdle();
			}
		} finally {
			this.mainLock.unlock();
		}
	}
	
	private PartitionTask getTask() {
		boolean timedOut = false; // Did the last poll() time out?
		boolean threadsTimeout = this.keepAliveNanos > 0;
		
		while (!timedOut) {
			if (status.isAcceptingTasks()) {
				try {
					PartitionTask task = threadsTimeout ? this.partitionQueue.poll(this.keepAliveNanos, TimeUnit.NANOSECONDS) : this.partitionQueue.take();
					if (task != null) {
						return task;
					}
					timedOut = true;
				} catch (InterruptedException retry) {
					timedOut = false;
				}
			} else if (status.isShutdown()) {
				PartitionTask task = this.partitionQueue.poll();
				if (task == null) {
					status.toTerminating();
				}
				return task;
			} else {
				// terminating or terminated
				PartitionTask task = null;
				while ((task = this.partitionQueue.poll()) != null) {
					task.onCancel();
				}
				break;
			}
		}
		return null;
	}
	
	private class Worker implements Runnable {
		private final ReentrantLock threadLock = new ReentrantLock(true);
		private final PartitionThread thread;
		
		public Worker() {
			thread = threadFactory.create(partitionId, this);
			workerCount.incrementAndGet();
			thread.start();
		}
		
		@Override
		public void run() {
			thread.onThreadStartup();
			boolean endedExceptionally = true;
			PartitionTask task = null;
			try {
				while (true) {
					if ((task = getTask()) != null) {
						try {
							threadLock.lock();
							Thread.interrupted();
							task.onBeforeExecute();
							task.run();
							task.onAfterExecute(null);
						} catch (Exception e) {
							task.onAfterExecute(e);
							throw e;
						} finally {
							threadLock.unlock();
						}
					} else {
						endedExceptionally = false;
						break;
					}
				}
			} finally {
				threadLock.lock();
				try {
					workerCount.decrementAndGet();
					thread.onThreadShutdown();
				} finally {
					if (endedExceptionally) {
						addWorkers();
					}
					removeWorker(this);
					threadLock.unlock();
				}
			}
		}
		
		public boolean interruptIfIdle() {
			if (threadLock.tryLock()) {
				this.thread.interrupt();
				threadLock.unlock();
				return true;
			}
			return false;
		}
	}
	
	public class Status {
		public static final int RUNNING = 3;
		public static final int SHUTDOWN = 2;
		public static final int TERMINATING = 1;
		public static final int TERMINATED = 0;
		
		private final CountDownLatch statusLatch = new CountDownLatch(RUNNING);
		
		private int getStatus() {
			return (int) this.statusLatch.getCount();
		}
		
		public boolean isTerminated() {
			return this.getStatus() == TERMINATED;
		}
		
		public boolean isExecutingTasks() {
			int status = this.getStatus();
			return status > TERMINATING;
		}
		
		public boolean isAcceptingTasks() {
			return this.getStatus() == RUNNING;
		}
		
		public boolean isShutdown() {
			return this.getStatus() == SHUTDOWN;
		}
		
		public boolean isShuttingDown() {
			return this.getStatus() <= SHUTDOWN;
		}
		
		public synchronized void toShutdown() {
			while (this.getStatus() > SHUTDOWN) {
				this.statusLatch.countDown();
			}
		}
		
		public synchronized void toTerminating() {
			while (this.getStatus() > TERMINATING) {
				this.statusLatch.countDown();
			}
		}
		
		public void toTerminated() {
			while (this.getStatus() > TERMINATED) {
				this.statusLatch.countDown();
			}
		}
		
		public void awaitTermination() throws InterruptedException {
			this.statusLatch.await();
		}
		
		public boolean awaitTermination(long timeout, TimeUnit unit) throws InterruptedException {
			return this.statusLatch.await(timeout, unit);
		}
	}
}
