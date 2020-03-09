package com.becomingmachinic.kafka.streams;

import org.apache.kafka.clients.consumer.ConsumerRecord;

public class KafkaStreamEvent<K, V> implements StreamEvent<K,V> {
	private final ConsumerRecord<K, V> consumerRecord;
	private final KafkaPartitionId partitionId;
	
	public KafkaStreamEvent(ConsumerRecord<K, V> consumerRecord) {
		this.consumerRecord = consumerRecord;
		this.partitionId = new KafkaPartitionId(consumerRecord.topic(), consumerRecord.partition());
	}
	
	@Override
	public K getKey() {
		return consumerRecord.key();
	}
	@Override
	public V getValue() {
		return consumerRecord.value();
	}
	@Override
	public KafkaPartitionId getPartitionId() {
		return this.partitionId;
	}
	public long getOffset(){
		return this.consumerRecord.offset();
	}
	
}
