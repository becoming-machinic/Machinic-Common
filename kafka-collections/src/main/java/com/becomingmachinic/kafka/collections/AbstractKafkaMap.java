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

import java.util.ConcurrentModificationException;
import java.util.Objects;
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
	
	protected void sendToKafka(K key, V value) {
		{
			super.sendKafkaEvent(this.keySerde.serialize(key),(value != null ? this.valueSerde.serialize(value) : null));
			if (this.checkConcurrentModification && Objects.equals(this.delegateMap.get(key), value)) {
				throw new ConcurrentModificationException(String.format("The value was modified by another thread/instance during update"));
			}
		}
	}
	
	protected V updateCollectionLocal(K key, V value) {
		if (value != null) {
			V oldValue = this.delegateMap.put(key, value);
			sendToKafka(key, value);
			return oldValue;
		} else {
			V oldValue = this.delegateMap.remove(key);
			sendToKafka(key, null);
			return oldValue;
		}
	}
	
	protected V putIfAbsentLocal(K key, V value) {
		V oldValue = this.delegateMap.putIfAbsent(key, value);
		if (oldValue == null) {
			sendToKafka(key, value);
		}
		return oldValue;
	}
	
	protected V computeIfAbsentLocal(K key, Function<? super K, ? extends V> mappingFunction) {
		V oldOrNewValue = this.delegateMap.computeIfAbsent(key, mappingFunction);
		sendToKafka(key, oldOrNewValue);
		return oldOrNewValue;
	}
	
	protected boolean removeLocal(K key, V value) {
		boolean changed = this.delegateMap.remove(key, value);
		if (changed) {
			sendToKafka(key, null);
		}
		return changed;
	}
	
	protected boolean replaceLocal(K key, V oldValue, V newValue) {
		boolean changed = this.delegateMap.replace(key, oldValue, newValue);
		if (changed) {
			sendToKafka(key, newValue);
		}
		return changed;
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
