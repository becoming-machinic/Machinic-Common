package com.becomingmachinic.kafka.streams.executor;

public class PartitionThread extends Thread {
	
	public PartitionThread(Runnable runnable) {
		super(runnable);
	}
	public void onThreadStartup() {
	}
	public void onThreadShutdown() {
		AutoCloseableThreadLocal.threadTerminated();
	}
}
