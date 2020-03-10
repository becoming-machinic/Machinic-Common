package com.becomingmachinic.kafka.streams;

import java.util.concurrent.TimeUnit;

import com.becomingmachinic.kafka.streams.executor.PartitionTask;
import com.becomingmachinic.kafka.streams.executor.PartitionTaskFactory;

public abstract class AbstractStream<K, V> extends AbstractStreamSource implements Stream<K, V> {
	
	private final StreamPartitionTaskFactory<K, V> streamPartitionTaskFactory;
	private final StreamFlow<K, V> streamFlow;
	
	protected AbstractStream(final StreamConfig streamConfig, final StreamFlow<K, V> streamFlow) {
		super(streamConfig);
		this.streamPartitionTaskFactory = streamConfig.getPartitionTaskFactory();
		this.streamFlow = streamFlow;
	}
	
	@Override
	public boolean submit(StreamEvent<K, V> event, Callback<K, V> callback) {
		return this.offer(event.getPartitionId(), this.streamPartitionTaskFactory.create(streamFlow, event, callback));
	}
	@Override
	public boolean submit(StreamEvent<K, V> event, Callback<K, V> callback, long timeout, TimeUnit unit) throws InterruptedException {
		return this.offer(event.getPartitionId(), this.streamPartitionTaskFactory.create(streamFlow, event, callback), timeout, unit);
	}
	
	public static class StreamPartitionTask<K, V> implements PartitionTask {
		
		private final StreamFlow<K, V> streamFlow;
		private final StreamEvent<K, V> event;
		private final Callback<K,V> callback;
		
		public StreamPartitionTask(StreamFlow<K, V> streamFlow, StreamEvent<K, V> event, Callback<K,V> callback) {
			this.streamFlow = streamFlow;
			this.event = event;
			this.callback = callback;
		}
		
		@Override
		public void onBefore() {
			if (this.callback != null) {
				this.callback.onBefore(event);
			}
		}
		@Override
		public void run() {
			this.streamFlow.run(event);
		}
		@Override
		public void onCompletion(Exception e) {
			if (this.callback != null) {
				this.callback.onCompletion(event, e);
			}
		}
		@Override
		public void onCancel() {
			if (this.callback != null) {
				this.callback.onCancel(event);
			}
		}
	}
	
	public static class StreamPartitionTaskFactory<K, V> implements PartitionTaskFactory<K, V> {
		@Override
		public PartitionTask create(StreamFlow<K, V> streamFlow, StreamEvent<K, V> streamEvent, Callback<K, V> callback) {
			return new AbstractStream.StreamPartitionTask<>(streamFlow, streamEvent, callback);
		}
	}
}
