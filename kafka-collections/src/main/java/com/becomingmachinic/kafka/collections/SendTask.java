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
import java.util.Objects;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicLong;

import org.apache.kafka.clients.producer.RecordMetadata;

class SendTask<K, V> implements Comparable<SendTask<K, V>> {
	
	private static AtomicLong counter = new AtomicLong(0);
	
	private final long id = counter.getAndIncrement();
	private final CountDownLatch latch = new CountDownLatch(1);
	private final Long instanceId;
	private final K key;
	private final V value;
	
	public SendTask(long instanceId, K key, V value) {
		this.instanceId = instanceId;
		
		this.key = key;
		this.value = value;
	}
	
	public void onSendCompletion(RecordMetadata metadata, Exception e) {
	}
	
	public boolean onReceive(CollectionConsumerRecord<K, V> record) {
		if (this.instanceId.equals(record.getInstanceId()) && Objects.equals(this.id, record.getRecordId())) {
			this.latch.countDown();
			return true;
		}
		return false;
	}
	
	public boolean await(long timeout, TimeUnit unit) throws InterruptedException {
		return latch.await(timeout, unit);
	}
	
	public K getKey() {
		return key;
	}
	public V getValue() {
		return value;
	}
	@Override
	public int compareTo(SendTask<K, V> o) {
		if (o != null) {
			return Long.compare(this.id, o.id);
		}
		return -1;
	}
	
	public ByteBuffer getRecordHeader() {
		return ByteBuffer.allocate(16).putLong(this.instanceId).putLong(id);
	}
}
