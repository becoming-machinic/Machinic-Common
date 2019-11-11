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

import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.apache.kafka.clients.consumer.ConsumerRecords;
import org.apache.kafka.common.header.Header;

import java.nio.ByteBuffer;
import java.security.SecureRandom;
import java.util.concurrent.ConcurrentSkipListSet;
import java.util.concurrent.CopyOnWriteArrayList;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.TimeUnit;

public abstract class AbstractKafkaCollection<K, V> implements AutoCloseable {

		protected final CollectionConfig collectionConfig;
		private final CollectionProducer<K, V> producer;
		private final CollectionConsumer<K, V> consumer;
		private final Long instanceId;

		protected final String name;
		protected final String sendMode;
		protected final long sendTimeout;
		protected final String writeMode;
		protected final ConcurrentSkipListSet<SendTask<K, V>> sendTasks = new ConcurrentSkipListSet<>();
		protected final CopyOnWriteArrayList<KafkaCollectionEventListener<K, V>> listeners = new CopyOnWriteArrayList<>();

		private volatile KafkaCollectionException exception = null;
		private final CountDownLatch warmedLatch = new CountDownLatch(1);

		protected AbstractKafkaCollection(CollectionConfig collectionConfig, CollectionProducer<K, V> producer, CollectionConsumer<K, V> consumer) {
				this.collectionConfig = collectionConfig;
				this.instanceId = generateInstanceId();

				this.producer = producer;
				this.consumer = consumer;

				this.name = collectionConfig.getName();
				this.sendMode = collectionConfig.getSendMode();
				this.sendTimeout = collectionConfig.getSendTimeoutMs();
				this.writeMode = collectionConfig.getWriteMode();
				collectionConfig.logConfig();

				checkConnectivity();

				this.consumer.setKafkaCollection(this);
		}

		protected void sendKafkaEvent(K key, V value) {
				if (CollectionConfig.COLLECTION_SEND_MODE_SYNCHRONOUS.equals(this.sendMode)) {
						try {
								SendTask<K, V> sendTask = new SendTask<>(this.instanceId, key, value);
								this.sendTasks.add(sendTask);
								if (this.producer.send(sendTask)) {
										if (!sendTask.await(this.sendTimeout, TimeUnit.MILLISECONDS)) {
												throw new SendTimeoutException("Collection %s send message timeout %s expired on kafka key %s", this.name, Long.toString(this.sendTimeout), String.valueOf(key));
										}
								} else {
										this.sendTasks.remove(sendTask);
								}
						} catch (InterruptedException e) {
								throw new SendTimeoutException("Collection %s send message was interrupted", this.name);
						}
				} else if (CollectionConfig.COLLECTION_SEND_MODE_ASYNCHRONOUS.equals(this.sendMode)) {
						this.producer.sendAsync(key, value);
				} else {
						throw new KafkaCollectionConfigurationException("The %s value %s is not supported by this collection", CollectionConfig.COLLECTION_SEND_MODE, this.sendMode);
				}
		}

		public void addKafkaCollectionEventListener(KafkaCollectionEventListener<K, V> listener) {
				this.listeners.add(listener);
		}
		public void removeKafkaCollectionEventListener(KafkaCollectionEventListener<K, V> listener) {
				this.listeners.remove(listener);
		}
		public KafkaCollectionException getException() {
				return this.exception;
		}

		protected void checkConnectivity() {
				if (!collectionConfig.isSkipConnectivityCheck()) {
						KafkaUtils.checkConnectivity(this.collectionConfig);
				}
		}
		protected void start() {
				Thread worker = new Thread(this.consumer);
				worker.setDaemon(true);
				worker.start();
		}

		protected abstract void onKafkaEvent(K rawKey, V rawValue);

		protected void onKafkaEvents(ConsumerRecords<K, V> records) {
				for (ConsumerRecord<K, V> record : records) {

						Long recordInstanceId = this.getRecordInstanceId(record);
						K key = record.key();
						V value = record.value();
						if (this.writeMode.equals(CollectionConfig.COLLECTION_WRITE_MODE_AHEAD) || !this.instanceId.equals(recordInstanceId)) {
								this.onKafkaEvent(key, record.value());
						}

						for (SendTask<K, V> task : sendTasks) {
								if (task.onReceive(record)) {
										this.sendTasks.remove(task);
								}
						}

						for (KafkaCollectionEventListener<K, V> listener : this.listeners) {
								listener.onEvent(this, key, value);
						}
				}
		}
		protected void onWarmupComplete(long warmupDuration) {
				this.warmedLatch.countDown();
				for (KafkaCollectionEventListener<K, V> listener : this.listeners) {
						listener.onWarmupComplete(warmupDuration);
				}
		}
		protected void onException(KafkaCollectionException exception) {
				this.exception = exception;
				for (KafkaCollectionEventListener listener : this.listeners) {
						listener.onException(exception);
				}
		}

		/**
		 * Wait for the collection has been backfilled with the data that exists on the kafka topic
		 * at the time that the collection is created.
		 *
		 * @param timeout the maximum time to wait
		 * @param unit    the time unit of the {@code timeout} argument
		 * @return {@code true} if the backfill completes and {@code false}
		 * if the waiting time elapsed before the backfill completes
		 * @throws InterruptedException if the current thread is interrupted
		 *                              while waiting
		 */
		public boolean awaitWarmupComplete(long timeout, TimeUnit unit) throws InterruptedException {
				return this.warmedLatch.await(timeout, unit);
		}
		@Override
		public void close() throws Exception {
				if (this.consumer != null) {
						try {
								this.consumer.close();
						} catch (Exception e) {
						}
				}
				if (this.producer != null) {
						try {
								this.producer.close();
						} catch (Exception e) {
						}
				}
				this.warmedLatch.countDown();
		}

		private static long generateInstanceId() {
				ByteBuffer idBuffer = ByteBuffer.allocate(8);
				idBuffer.putInt((int) (System.currentTimeMillis() - 1546300800000l));
				idBuffer.putInt(new SecureRandom().nextInt());
				return idBuffer.getLong(0);
		}

		private Long getRecordInstanceId(ConsumerRecord<K, V> record) {
				Header header = record.headers().lastHeader(CollectionConfig.COLLECTION_RECORD_HEADER_NAME);
				if (header != null) {
						ByteBuffer buffer = ByteBuffer.wrap(header.value());
						if (buffer.remaining() >= 8) {
								return buffer.getLong();
						}
				}
				return null;
		}
}
