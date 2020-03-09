package com.becomingmachinic.kafka.streams.executor;

public class PartitionThread extends Thread {
	
	protected final int partitionCode;
	
	public PartitionThread(int partitionCode, Runnable runnable) {
		super(runnable);
		this.partitionCode = partitionCode;
	}
	public void onThreadStartup() {
	}
	public void onThreadShutdown() {
		AutoCloseableThreadLocal.threadTerminated();
	}
}
