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

import java.util.Collection;
import java.util.Iterator;
import java.util.Map;
import java.util.Objects;
import java.util.Set;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.ConcurrentMap;

public class KafkaMap<K, V, KK, KV> extends AbstractKafkaCollection<KK, KV> implements KMap<K, V> {
	
	protected final ConcurrentMap<K, V> delegateMap;
	protected final CollectionSerde<KK, K> keySerde;
	protected final CollectionSerde<KV, V> valueSerde;
	
	public KafkaMap(CollectionConfig config, CollectionSerde<KK, K> keySerde, CollectionSerde<KV, V> valueSerde) {
		this(new ConcurrentHashMap<K, V>(), config, keySerde, valueSerde);
	}
	
	public KafkaMap(ConcurrentMap<K, V> delegateMap, CollectionConfig collectionConfig, CollectionSerde<KK, K> keySerde, CollectionSerde<KV, V> valueSerde) {
		super(collectionConfig,
				(!collectionConfig.isReadOnly() ? new CollectionProducer<KK, KV>(collectionConfig, keySerde.getRawSerializer(), valueSerde.getRawSerializer()) : null),
				new CollectionConsumer<KK, KV>(collectionConfig, keySerde.getRawDeserializer(), valueSerde.getRawDeserializer()));
		
		this.delegateMap = delegateMap;
		this.keySerde = keySerde;
		this.valueSerde = valueSerde;
		super.start();
	}
	
	protected V updateCollection(K key, V value) {
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
	
	@Override
	public int size() {
		return delegateMap.size();
	}
	
	@Override
	public boolean isEmpty() {
		return delegateMap.isEmpty();
	}
	
	@Override
	public boolean containsKey(Object key) {
		return delegateMap.containsKey(key);
	}
	
	@Override
	public boolean containsValue(Object value) {
		return delegateMap.containsValue(value);
	}
	
	@Override
	public V get(Object key) {
		KafkaCollectionException exp = this.getException();
		if (exp != null) {
			throw exp;
		}
		return delegateMap.get(key);
	}
	
	@Override
	public V getOrDefault(Object key, V defaultValue) {
		if (this.getException() != null) {
			throw getException();
		}
		return delegateMap.getOrDefault(key, defaultValue);
	}
	
	@Override
	public V put(K key, V value) {
		checkErrors();
		
		return this.updateCollection(key, value);
	}
	
	@Override
	public void putAll(Map<? extends K, ? extends V> m) {
		checkErrors();
		
		for (Entry<? extends K, ? extends V> entry : m.entrySet()) {
			this.updateCollection(entry.getKey(), entry.getValue());
		}
	}
	
	@SuppressWarnings("unchecked")
	@Override
	public V remove(Object key) {
		checkErrors();
		
		try {
			return this.updateCollection((K) key, null);
		} catch (ClassCastException e) {
		}
		return null;
	}
	
	@Override
	public boolean containsAll(Collection<K> c) {
		for (K entry : c) {
			if (!this.delegateMap.containsKey(entry)) {
				return false;
			}
		}
		return true;
	}
	
	@Override
	public void clear() {
		checkErrors();
		
		Iterator<K> it = this.delegateMap.keySet().iterator();
		while (it.hasNext()) {
			this.updateCollection(it.next(), null);
		}
	}
	
	@Override
	public Set<K> keySet() {
		return delegateMap.keySet();
	}
	
	@Override
	public Collection<V> values() {
		return delegateMap.values();
	}
	
	@Override
	public Set<Entry<K, V>> entrySet() {
		return delegateMap.entrySet();
	}
	
	@Override
	public boolean equals(Object o) {
		if (o != null) {
			if (o instanceof KafkaMap) {
				KafkaMap<?, ?, ?, ?> other = (KafkaMap<?, ?, ?, ?>) o;
				return Objects.equals(delegateMap, other.delegateMap);
			}
		}
		return false;
	}
	
	@Override
	public int hashCode() {
		return Objects.hash(this.delegateMap);
	}
}
