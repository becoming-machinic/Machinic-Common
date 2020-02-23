package com.becomingmachinic.kafka.streams.executor;

import java.util.concurrent.ThreadFactory;
import java.util.concurrent.atomic.AtomicLong;

public class AutoCloseableThreadLocalFactory implements ThreadFactory {
	
	private final AtomicLong counter = new AtomicLong(0);
	private final String threadPoolName;
	private final int partitionId;
	private final boolean daemon;
	private final int threadPriority;
	
	public AutoCloseableThreadLocalFactory(String threadPoolName, int partitionId, boolean isDaemon, int threadPriority) {
		this.threadPoolName = threadPoolName;
		this.partitionId = partitionId;
		this.daemon = isDaemon;
		this.threadPriority = threadPriority;
	}
	
	@Override
	public Thread newThread(Runnable runnable) {
		Thread thread = new Thread(new RunnableWrapper(runnable));
		thread.setDaemon(daemon);
		thread.setName(String.format("%s-partition-%s-thread-%s", this.threadPoolName, this.partitionId, counter.getAndIncrement()));
		thread.setPriority(this.threadPriority);
		return null;
	}
	
	public static class RunnableWrapper implements Runnable {
		private final Runnable runnable;
		public RunnableWrapper(Runnable runnable) {
			this.runnable = runnable;
		}
		
		@Override
		public void run() {
			try {
				this.onStartup();
				this.runnable.run();
			} finally {
				onShutdown();
			}
		}
		
		protected void onStartup() {
		}
		
		protected void onShutdown() {
			AutoCloseableThreadLocal.threadTerminated();
		}
	}
}