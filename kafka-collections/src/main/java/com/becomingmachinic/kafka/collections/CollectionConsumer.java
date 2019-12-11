/*
 * Copyright (C) 2019 Becoming Machinic Inc.
 *
 * Licensed under the Apache License, Version 2.0 (the "License"); you may not use this file except
 * in compliance with the License. You may obtain a copy of the License at
 *
 * http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software distributed under the License
 * is distributed on an "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express
 * or implied. See the License for the specific language governing permissions and limitations under
 * the License.
 */

package com.becomingmachinic.kafka.collections;

import java.time.Duration;
import java.util.ArrayList;
import java.util.List;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.TimeUnit;

import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.apache.kafka.clients.consumer.ConsumerRecords;
import org.apache.kafka.clients.consumer.KafkaConsumer;
import org.apache.kafka.common.KafkaException;
import org.apache.kafka.common.PartitionInfo;
import org.apache.kafka.common.TopicPartition;
import org.apache.kafka.common.errors.InterruptException;
import org.apache.kafka.common.errors.WakeupException;
import org.apache.kafka.common.serialization.Deserializer;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * The kafka consumer reads events from the assigned topic and calls the {@link AbstractKafkaCollection#onKafkaEvents} with each batch of kafka records.
 *
 * @param <K>
 *          The Kafka key type
 * @param <V>
 *          The Kafka value type
 */
class CollectionConsumer<K, V> implements Runnable, AutoCloseable {
	private static final Logger logger = LoggerFactory.getLogger(CollectionConsumer.class);
	
	protected final KafkaConsumer<K, V> consumer;
	protected AbstractKafkaCollection<K, V> collection;
	protected final String name;
	protected final String topic;
	protected final Duration maxPollInterval;
	protected final Duration warmupPollInterval;
	protected final String resetOffset;
	protected final CountDownLatch closeLatch = new CountDownLatch(1);
	
	private volatile boolean stop = false;
	
	public CollectionConsumer(CollectionConfig collectionConfig, Deserializer<K> keyDeserializer, Deserializer<V> valueDeserializer) throws KafkaCollectionConfigurationException, KafkaCollectionException {
		this.name = collectionConfig.getName();
		this.topic = collectionConfig.getTopic();
		this.maxPollInterval = collectionConfig.getMaxPollIntervalDuration();
		this.warmupPollInterval = collectionConfig.getWarmupPollIntervalDuration();
		this.resetOffset = collectionConfig.isResetOffset();
		
		try {
			this.consumer = new KafkaConsumer<K, V>(collectionConfig.getConsumerConfig(keyDeserializer, valueDeserializer));
		} catch (KafkaCollectionException e) {
			throw e;
		} catch (Exception e) {
			throw new KafkaCollectionException("Create consumer for collection %s failed", e, this.name);
		}
	}
	
	public void setKafkaCollection(AbstractKafkaCollection<K, V> collection) {
		synchronized (this) {
			this.collection = collection;
		}
	}
	
	private void assign() throws KafkaCollectionException {
		try {
			List<TopicPartition> topicPartitions = getTopicPartitions();
			this.consumer.assign(topicPartitions);
			if (CollectionConfig.COLLECTION_RESET_OFFSET_BEGINNING.equals(this.resetOffset)) {
				this.consumer.seekToBeginning(topicPartitions);
			} else if (CollectionConfig.COLLECTION_RESET_OFFSET_END.equals(this.resetOffset)) {
				this.consumer.seekToEnd(topicPartitions);
			}
			
		} catch (Exception e) {
			throw new KafkaCollectionException("Assign to topic %s for collection %s failed", e, this.topic, this.name);
		}
	}
	
	protected List<TopicPartition> getTopicPartitions() {
		List<TopicPartition> partitions = new ArrayList<>();
		for (PartitionInfo info : this.consumer.partitionsFor(this.topic)) {
			partitions.add(new TopicPartition(info.topic(), info.partition()));
			logger.debug("Collection {} will listen to topic {} partition {}", this.name, info.topic(), info.partition());
		}
		if (partitions.isEmpty()) {
			throw new KafkaCollectionException("Collection %s consumer could not find partions for topic %s", this.name, this.topic);
		}
		return partitions;
	}
	
	protected void consume() throws KafkaCollectionException {
		if (this.collection == null) {
			throw new KafkaCollectionException("Collection instance not set");
		}
		try {
			this.assign();
			
			boolean warmed = false;
			long startTimestamp = System.currentTimeMillis();
			while (!this.stop && !warmed) {
				ConsumerRecords<K, V> records = this.consumer.poll(this.warmupPollInterval);
				if (!records.isEmpty()) {
					logger.debug("Collection {} received {} events", this.name, records.count());
					collection.onKafkaEvents(records);
					for (ConsumerRecord<K, V> record : records) {
						if (record.timestamp() >= startTimestamp) {
							warmed = true;
							break;
						}
					}
				} else {
					warmed = true;
				}
			}
			if (warmed) {
				long warmedDuration = System.currentTimeMillis() - startTimestamp;
				logger.debug("Warmed collection {} in {} milliseconds", this.name, warmedDuration);
				collection.onWarmupComplete(warmedDuration);
			}
			
			while (!this.stop) {
				ConsumerRecords<K, V> records = this.consumer.poll(this.maxPollInterval);
				if (!records.isEmpty()) {
					logger.debug("Collection {} received {} events", this.name, records.count());
					collection.onKafkaEvents(records);
				}
			}
		} catch (WakeupException | InterruptException e) {
			// woke by close
		} catch (KafkaException e) {
			logger.info(String.format("Collection %s consumer failed", this.name), e);
			throw new KafkaCollectionException("Collection %s consumer failed", e, this.name);
		} finally {
			this.consumer.close(Duration.ofSeconds(30));
			logger.debug("Collection {} consumer stopped", this.name);
			this.closeLatch.countDown();
		}
		
	}
	
	@Override
	public void run() {
		String threadName = Thread.currentThread().getName();
		Thread.currentThread().setName(String.format("kafka-consumer-%s", this.name));
		try {
			this.consume();
		} catch (KafkaCollectionException e) {
			this.collection.onException(e);
		} catch (Exception e) {
			logger.info(String.format("Collection %s consumer failed", this.name), e);
			this.collection.onException(new KafkaCollectionException("Consume for collection %s failed with unexpected error", e, this.name));
		}
		Thread.currentThread().setName(threadName);
	}
	
	@Override
	public void close() {
		logger.debug("Collection {} consumer shutdown requested", this.name);
		this.stop = true;
		this.consumer.wakeup();
		try {
			this.closeLatch.await(30l, TimeUnit.SECONDS);
		} catch (InterruptedException e) {
		}
		logger.debug("Collection {} consumer shutdown", this.name);
	}
	
}
