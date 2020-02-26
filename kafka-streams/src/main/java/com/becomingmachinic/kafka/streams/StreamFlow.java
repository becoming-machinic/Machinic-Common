package com.becomingmachinic.kafka.streams;

public interface StreamFlow<K,V> {
	
	public void run(StreamEvent<K,V> event);
}
