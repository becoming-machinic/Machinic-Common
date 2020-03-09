package com.becomingmachinic.kafka.streams;

import java.util.Objects;

import com.becomingmachinic.kafka.streams.executor.PartitionId;

public class KafkaPartitionId implements PartitionId {
	private final String topic;
	private final int partition;
	
	public KafkaPartitionId(String topic, int partition) {
		this.topic = topic;
		this.partition = partition;
	}
	public String getTopic() {
		return topic;
	}
	public int getPartition() {
		return partition;
	}
	
	@Override
	public boolean equals(Object o) {
		if (o instanceof KafkaPartitionId) {
			KafkaPartitionId other = (KafkaPartitionId) o;
			return this.topic.equalsIgnoreCase(other.topic) &&
					this.partition == other.partition;
		}
		return false;
	}
	@Override
	public int hashCode() {
		return Objects.hash(this.topic.toLowerCase(), partition);
	}
	
	@Override
	public String toString() {
		return String.format("%s:%s", topic, partition);
	}
}
