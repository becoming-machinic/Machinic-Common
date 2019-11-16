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

import java.nio.ByteBuffer;
import java.util.concurrent.TimeUnit;

import org.apache.kafka.clients.producer.KafkaProducer;
import org.apache.kafka.clients.producer.ProducerRecord;
import org.apache.kafka.common.header.Headers;
import org.apache.kafka.common.header.internals.RecordHeader;
import org.apache.kafka.common.header.internals.RecordHeaders;
import org.apache.kafka.common.serialization.Serializer;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

class CollectionProducer<K, V> implements AutoCloseable {
	private static final Logger logger = LoggerFactory.getLogger(CollectionConsumer.class);
	
	protected final KafkaProducer<K, V> producer;
	protected final String name;
	protected final String topic;
	protected final long sendTimeout;
	private volatile boolean stop = false;
	
	public CollectionProducer(CollectionConfig config, Serializer<K> keySerializer, Serializer<V> valueSerialiser) throws KafkaCollectionException {
		this.name = config.getName();
		this.topic = config.getTopic();
		this.sendTimeout = config.getSendTimeoutMs();
		
		if (config.getCreateTopic()) {
			KafkaUtils.createTopic(config);
		}
		
		try {
			this.producer = new KafkaProducer<K, V>(config.getProducerConfig(), keySerializer, valueSerialiser);
		} catch (Exception e) {
			logger.info(String.format("Collection %s create producer failed", this.name), e);
			throw new KafkaCollectionException("Create producer for collection %s failed", e, this.name);
		}
	}
	
	public boolean send(SendTask<K, V> sendTask) throws KafkaCollectionException {
		if (!this.stop && (sendTask.getKey() != null || sendTask.getValue() != null)) {
			try {
				Headers headers = new RecordHeaders();
				headers.add(new RecordHeader(CollectionConfig.COLLECTION_RECORD_HEADER_NAME, sendTask.getRecordHeader().array()));
				this.producer.send(new ProducerRecord<K, V>(this.topic, null, sendTask.getKey(), sendTask.getValue(), headers), sendTask::onSendCompletion);
				return true;
			} catch (Exception e) {
				logger.info(String.format("Collection %s producer send message failed", this.name), e);
				throw new KafkaCollectionException("Collection %s producer send message failed", e, this.name);
			}
		}
		return false;
	}
	
	public void sendAsync(final long instanceId, final K key, final V value) {
		if (!this.stop && (key != null || value != null)) {
			Headers headers = new RecordHeaders();
			headers.add(new RecordHeader(CollectionConfig.COLLECTION_RECORD_HEADER_NAME, ByteBuffer.allocate(8).putLong(instanceId).array()));
			this.producer.send(new ProducerRecord<>(this.topic, null, key, value, headers));
		}
	}
	
	@Override
	public void close() throws Exception {
		logger.debug("Collection {} producer shutdown requested", this.name);
		this.stop = true;
		this.producer.flush();
		this.producer.close(10, TimeUnit.SECONDS);
		logger.debug("Collection {} producer shutdown", this.name);
	}
}
