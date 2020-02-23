package com.becomingmachinic.kafka.streams;

import java.util.Map;

public interface StreamEvent<K, V> {
	public long getCreatedTimestamp();
	
	public long getEventTimestamp();
	
	public K getKey();
	
	public V getValue();
	
	public String getIdentifier();
	
	public Map<String, Object> getMetadata();
}
