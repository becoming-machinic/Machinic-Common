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
import org.apache.kafka.clients.producer.RecordMetadata;
import org.apache.kafka.common.header.Header;

import java.nio.ByteBuffer;
import java.util.Arrays;
import java.util.Objects;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicLong;

class SendTask<K,V> implements Comparable<SendTask<K,V>> {

		private static AtomicLong counter = new AtomicLong(0);

		private final long id = counter.getAndIncrement();
		private final CountDownLatch latch = new CountDownLatch(1);
		private final ByteBuffer recordId;
		private final K key;
		private final V value;

		public SendTask(long instanceId,K key,V value) {
				this.recordId = ByteBuffer.allocate(16);
				this.recordId.putLong(instanceId);
				this.recordId.putLong(id);

				this.key = key;
				this.value = value;
		}

		public void onSendCompletion(RecordMetadata metadata, Exception e) {
		}

		public boolean onReceive(ConsumerRecord<K, V> record) {
				Header header = record.headers().lastHeader(CollectionConfig.COLLECTION_RECORD_HEADER_NAME);
				if(header != null && header.value() != null && Arrays.equals(this.recordId.array(),header.value())){
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
		public int compareTo(SendTask<K,V> o) {
				if (o != null) {
						return Long.compare(this.id, o.id);
				}
				return -1;
		}

		public ByteBuffer getRecordId() {
				return recordId;
		}
		private static boolean isEqual(Object a,Object b){
			if(a != null && b != null && a instanceof byte[] && b instanceof byte[]){
					return Arrays.equals((byte[])a,(byte[])b);
			}
			return Objects.equals(a,b);
		}
}
