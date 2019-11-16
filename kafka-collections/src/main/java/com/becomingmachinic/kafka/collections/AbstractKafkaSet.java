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

public abstract class AbstractKafkaSet<KK, K> extends AbstractKafkaCollection<KK, String> {
	
	protected static final String VALUE = "1";
	
	protected final CollectionSerde<KK, K> keySerde;
	protected final CollectionSerde<String, String> valueSerde;
	
	public AbstractKafkaSet(CollectionConfig collectionConfig, CollectionSerde<KK, K> keySerde, CollectionSerde<String, String> valueSerde) {
		super(collectionConfig,
				new CollectionProducer<KK, String>(collectionConfig, keySerde.getRawSerializer(), valueSerde.getRawSerializer()),
				new CollectionConsumer<KK, String>(collectionConfig, keySerde.getRawDeserializer(), valueSerde.getRawDeserializer()));
		
		this.keySerde = keySerde;
		this.valueSerde = valueSerde;
	}
	
	@Override
	protected void onKafkaEvent(CollectionConsumerRecord<KK, String> collectionRecord) {
		KK rawKey = collectionRecord.key();
		String rawValue = collectionRecord.value();
		
		if (rawKey != null) {
			if (rawValue != null) {
				this.addLocal(this.keySerde.deserialize(rawKey));
				// TODO update metrics
			} else {
				this.removeLocal(this.keySerde.deserialize(rawKey));
				// TODO update metrics
			}
		}
	}
	
	protected abstract boolean addLocal(K key);
	protected abstract boolean removeLocal(K key);
	protected abstract boolean containsLocal(K key);
	
	protected boolean collectionAdd(K k) {
		if (k != null) {
			if (CollectionConfig.COLLECTION_WRITE_MODE_BEHIND.equals(this.writeMode)) {
				boolean added = this.addLocal(k);
				if (added) {
					super.sendKafkaEvent(this.keySerde.serialize(k), this.valueSerde.serialize(VALUE));
				}
				return added;
			} else if (CollectionConfig.COLLECTION_WRITE_MODE_AHEAD.equals(this.writeMode)) {
				boolean contains = this.containsLocal(k);
				super.sendKafkaEvent(this.keySerde.serialize(k), VALUE);
				return !contains;
			} else {
				throw new KafkaCollectionConfigurationException("The %s value %s is not supported by this collection", CollectionConfig.COLLECTION_WRITE_MODE, this.writeMode);
			}
		}
		return false;
	}
	
	protected boolean collectionRemove(K k) {
		if (k != null) {
			if (CollectionConfig.COLLECTION_WRITE_MODE_BEHIND.equals(this.writeMode)) {
				boolean remove = this.removeLocal(k);
				if (remove) {
					super.sendKafkaEvent(this.keySerde.serialize(k), null);
				}
				return remove;
			} else if (CollectionConfig.COLLECTION_WRITE_MODE_AHEAD.equals(this.writeMode)) {
				boolean contains = this.containsLocal(k);
				super.sendKafkaEvent(this.keySerde.serialize(k), null);
				return contains;
			} else {
				throw new KafkaCollectionConfigurationException("The %s value %s is not supported by this collection", CollectionConfig.COLLECTION_WRITE_MODE, this.writeMode);
			}
		}
		return false;
	}
}
