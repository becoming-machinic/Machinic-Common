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
import org.apache.kafka.common.header.Header;
import org.apache.kafka.common.header.Headers;
import org.apache.kafka.common.record.TimestampType;

import java.nio.ByteBuffer;
import java.util.Optional;

public class CollectionConsumerRecord<K,V> {
		private final ConsumerRecord<K,V> consumerRecord;
		private final Long instanceId;
		private final Long recordId;

		CollectionConsumerRecord(final ConsumerRecord<K,V> consumerRecord){
				this.consumerRecord = consumerRecord;

				Header header = consumerRecord.headers().lastHeader(CollectionConfig.COLLECTION_RECORD_HEADER_NAME);
				if (header != null) {
						ByteBuffer buffer = ByteBuffer.wrap(header.value());
						if (buffer.remaining() >= 8) {
								this.instanceId = buffer.getLong();
								if (buffer.remaining() >= 8) {
										this.recordId = buffer.getLong();
								} else {
										this.recordId = null;
								}
						} else {
								this.instanceId = null;
								this.recordId = null;
						}
				} else {
						this.instanceId = null;
						this.recordId = null;
				}
		}

		public Long getInstanceId() {
				return instanceId;
		}
		public Long getRecordId() {
				return recordId;
		}
		public String topic() {
				return consumerRecord.topic();
		}
		public int partition() {
				return consumerRecord.partition();
		}
		public Headers headers() {
				return consumerRecord.headers();
		}
		public K key() {
				return consumerRecord.key();
		}
		public V value() {
				return consumerRecord.value();
		}
		public long offset() {
				return consumerRecord.offset();
		}
		public long timestamp() {
				return consumerRecord.timestamp();
		}
		public TimestampType timestampType() {
				return consumerRecord.timestampType();
		}
		public int serializedKeySize() {
				return consumerRecord.serializedKeySize();
		}
		public int serializedValueSize() {
				return consumerRecord.serializedValueSize();
		}
		public Optional<Integer> leaderEpoch() {
				return consumerRecord.leaderEpoch();
		}
		@Override
		public String toString() {
				return consumerRecord.toString();
		}
}
