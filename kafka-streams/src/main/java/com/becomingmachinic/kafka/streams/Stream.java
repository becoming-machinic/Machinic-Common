package com.becomingmachinic.kafka.streams;

import java.util.concurrent.TimeUnit;

public interface Stream<K,V> {
	
	public boolean submit(StreamEvent<K,V> event, Callback callback);
	public boolean submit(StreamEvent<K,V> event, Callback callback,long timeout,TimeUnit unit) throws InterruptedException;
	
	public void shutdown();
	public void shutdownNow();
	public boolean isShutdown();
	public boolean isTerminated();
	public boolean awaitTermination(long timeout, TimeUnit unit) throws InterruptedException;
}
