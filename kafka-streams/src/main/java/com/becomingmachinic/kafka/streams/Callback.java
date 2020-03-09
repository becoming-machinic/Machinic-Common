package com.becomingmachinic.kafka.streams;

public interface Callback<K,V> {
	
	public void onBefore(StreamEvent<K,V> streamEvent);
	
	public void onCompletion(StreamEvent<K,V> streamEvent, Exception exception);
	
	public void onCancel(StreamEvent<K,V> streamEvent);
}
