package com.becomingmachinic.kafka.streams;

import org.apache.kafka.clients.consumer.ConsumerRecord;

import com.becomingmachinic.kafka.streams.executor.PartitionId;

public class KafkaStreamRecord<K, V> implements StreamEvent<K,V> {
	private final ConsumerRecord<K, V> consumerRecord;
	private final PartitionId partitionId;
	
	public KafkaStreamRecord(ConsumerRecord<K, V> consumerRecord) {
		this.consumerRecord = consumerRecord;
		this.partitionId = new PartitionId(consumerRecord.topic(), consumerRecord.partition());
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
	public PartitionId getPartitionId() {
		return this.partitionId;
	}
	public long getOffset(){
		return this.consumerRecord.offset();
	}
	
}
