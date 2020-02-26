package com.becomingmachinic.kafka.streams;

public interface Callback<E extends StreamEvent> {
	
	public void onBefore(E streamEvent);
	
	public void onCompletion(E streamEvent, Exception exception);
	
	public void onCancel(E streamEvent);
}
