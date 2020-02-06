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

import java.util.concurrent.ConcurrentMap;
import java.util.function.Function;

public abstract class AbstractKafkaMap<K, V, KK, KV> extends AbstractKafkaCollection<KK, KV> {
	
	protected final ConcurrentMap<K, V> delegateMap;
	protected final CollectionSerde<KK, K> keySerde;
	protected final CollectionSerde<KV, V> valueSerde;
	
	
	public AbstractKafkaMap(ConcurrentMap<K, V> delegateMap, CollectionConfig collectionConfig, CollectionSerde<KK, K> keySerde, CollectionSerde<KV, V> valueSerde) {
		super(collectionConfig,
				(!collectionConfig.isReadOnly() ? new CollectionProducer<KK, KV>(collectionConfig, keySerde.getRawSerializer(), valueSerde.getRawSerializer()) : null),
				new CollectionConsumer<KK, KV>(collectionConfig, keySerde.getRawDeserializer(), valueSerde.getRawDeserializer()));
		
		this.delegateMap = delegateMap;
		this.keySerde = keySerde;
		this.valueSerde = valueSerde;
		super.start();
	}
	
	protected V updateCollectionLocal(K key, V value) {
		if (value != null) {
			if (CollectionConfig.COLLECTION_WRITE_MODE_BEHIND.equals(this.writeMode)) {
				V oldValue = this.delegateMap.put(key, value);
				super.sendKafkaEvent(this.keySerde.serialize(key), this.valueSerde.serialize(value));
				return oldValue;
			} else if (CollectionConfig.COLLECTION_WRITE_MODE_AHEAD.equals(this.writeMode)) {
				V oldValue = this.delegateMap.get(key);
				super.sendKafkaEvent(this.keySerde.serialize(key), this.valueSerde.serialize(value));
				return oldValue;
			} else {
				throw new KafkaCollectionConfigurationException("The %s value %s is not supported by this collection", CollectionConfig.COLLECTION_WRITE_MODE, this.writeMode);
			}
		} else {
			if (CollectionConfig.COLLECTION_WRITE_MODE_BEHIND.equals(this.writeMode)) {
				V oldValue = this.delegateMap.remove(key);
				super.sendKafkaEvent(this.keySerde.serialize(key), null);
				return oldValue;
			} else if (CollectionConfig.COLLECTION_WRITE_MODE_AHEAD.equals(this.writeMode)) {
				V oldValue = this.delegateMap.get(key);
				super.sendKafkaEvent(this.keySerde.serialize(key), null);
				return oldValue;
			} else {
				throw new KafkaCollectionConfigurationException("The %s value %s is not supported by this collection", CollectionConfig.COLLECTION_WRITE_MODE, this.writeMode);
			}
		}
	}
	
	protected V putIfAbsentLocal(K key, V value) {
		if (CollectionConfig.COLLECTION_WRITE_MODE_BEHIND.equals(this.writeMode)) {
			V oldValue = this.delegateMap.putIfAbsent(key, value);
			if (oldValue == null) {
				super.sendKafkaEvent(this.keySerde.serialize(key), this.valueSerde.serialize(value));
			}
			return oldValue;
		} else if (CollectionConfig.COLLECTION_WRITE_MODE_AHEAD.equals(this.writeMode)) {
			V oldValue = this.delegateMap.get(key);
			if (oldValue == null) {
				super.sendKafkaEvent(this.keySerde.serialize(key), this.valueSerde.serialize(value));
			}
			return oldValue;
		} else {
			throw new KafkaCollectionConfigurationException("The %s value %s is not supported by this collection", CollectionConfig.COLLECTION_WRITE_MODE, this.writeMode);
		}
	}
	
	protected V computeIfAbsentLocal(K key, Function<? super K, ? extends V> mappingFunction) {
		if (CollectionConfig.COLLECTION_WRITE_MODE_BEHIND.equals(this.writeMode)) {
			V oldOrNewValue = this.delegateMap.computeIfAbsent(key, mappingFunction);
			super.sendKafkaEvent(this.keySerde.serialize(key), this.valueSerde.serialize(oldOrNewValue));
			return oldOrNewValue;
		} else if (CollectionConfig.COLLECTION_WRITE_MODE_AHEAD.equals(this.writeMode)) {
			V oldValue = this.delegateMap.get(key);
			if (oldValue == null) {
				V newValue = mappingFunction.apply(key);
				super.sendKafkaEvent(this.keySerde.serialize(key), this.valueSerde.serialize(newValue));
				return newValue;
			}
			return oldValue;
		} else {
			throw new KafkaCollectionConfigurationException("The %s value %s is not supported by this collection", CollectionConfig.COLLECTION_WRITE_MODE, this.writeMode);
		}
	}
	
	protected boolean removeLocal(K key, V value) {
		if (CollectionConfig.COLLECTION_WRITE_MODE_BEHIND.equals(this.writeMode)) {
			boolean changed = this.delegateMap.remove(key, value);
			if (changed) {
				super.sendKafkaEvent(this.keySerde.serialize(key), null);
			}
			return changed;
		} else if (CollectionConfig.COLLECTION_WRITE_MODE_AHEAD.equals(this.writeMode)) {
			V oldValue = this.delegateMap.get(key);
			if (oldValue != null) {
				super.sendKafkaEvent(this.keySerde.serialize(key), null);
				return true;
			}
			return false;
		} else {
			throw new KafkaCollectionConfigurationException("The %s value %s is not supported by this collection", CollectionConfig.COLLECTION_WRITE_MODE, this.writeMode);
		}
	}
	
	protected boolean replaceLocal(K key, V oldValue,V newValue) {
		if (CollectionConfig.COLLECTION_WRITE_MODE_BEHIND.equals(this.writeMode)) {
			boolean changed = this.delegateMap.replace(key, oldValue,newValue);
			if (changed) {
				super.sendKafkaEvent(this.keySerde.serialize(key), this.valueSerde.serialize(newValue));
			}
			return changed;
		} else if (CollectionConfig.COLLECTION_WRITE_MODE_AHEAD.equals(this.writeMode)) {
			boolean changed = this.delegateMap.replace(key, oldValue,oldValue);
			if (changed) {
				super.sendKafkaEvent(this.keySerde.serialize(key), this.valueSerde.serialize(newValue));
				return true;
			}
			return false;
		} else {
			throw new KafkaCollectionConfigurationException("The %s value %s is not supported by this collection", CollectionConfig.COLLECTION_WRITE_MODE, this.writeMode);
		}
	}
	
	@Override
	protected void onKafkaEvent(CollectionConsumerRecord<KK, KV> collectionRecord) {
		KK rawKey = collectionRecord.key();
		KV rawValue = collectionRecord.value();
		
		if (rawKey != null) {
			if (rawValue != null) {
				this.delegateMap.put(this.keySerde.deserialize(rawKey), this.valueSerde.deserialize(rawValue));
				// TODO update metrics
			} else {
				this.delegateMap.remove(this.keySerde.deserialize(rawKey));
				// TODO update metrics
			}
		}
	}
	
}
