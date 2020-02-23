package com.becomingmachinic.kafka.streams.executor;

public interface PartitionTask {
	
	public void onBeforeExecute();
	public void run();
	public void onAfterExecute(Exception e);
	public void onCancel();
}
