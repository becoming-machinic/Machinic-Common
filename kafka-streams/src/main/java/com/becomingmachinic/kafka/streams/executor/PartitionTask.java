package com.becomingmachinic.kafka.streams.executor;

public interface PartitionTask {
	
	public void onBefore();
	
	public void run();
	
	public void onCompletion(Exception e);
	
	public void onCancel();
}
