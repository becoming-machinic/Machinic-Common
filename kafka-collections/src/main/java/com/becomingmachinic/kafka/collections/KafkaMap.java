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
import java.util.function.Function;

public class KafkaMap<K, V, KK, KV> extends AbstractKafkaMap<K,V,KK, KV> implements KMap<K, V> {
	
	public KafkaMap(CollectionConfig config, CollectionSerde<KK, K> keySerde, CollectionSerde<KV, V> valueSerde) {
		this(new ConcurrentHashMap<K, V>(), config, keySerde, valueSerde);
	}
	
	public KafkaMap(ConcurrentMap<K, V> delegateMap, CollectionConfig collectionConfig, CollectionSerde<KK, K> keySerde, CollectionSerde<KV, V> valueSerde) {
		super(delegateMap,collectionConfig,keySerde,valueSerde);
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
		
		return this.updateCollectionLocal(key, value);
	}
	
	@Override
	public void putAll(Map<? extends K, ? extends V> m) {
		checkErrors();
		
		for (Entry<? extends K, ? extends V> entry : m.entrySet()) {
			this.updateCollectionLocal(entry.getKey(), entry.getValue());
		}
	}
	
	@Override
	public V putIfAbsent(K key, V value) {
		checkErrors();
		
		return this.putIfAbsentLocal(key, value);
	}
	
	@Override
	public V computeIfAbsent(K key, Function<? super K, ? extends V> mappingFunction) {
		checkErrors();
		
		return this.computeIfAbsentLocal(key, mappingFunction);
	}
	
	@SuppressWarnings("unchecked")
	@Override
	public V remove(Object key) {
		checkErrors();
		
		try {
			return this.updateCollectionLocal((K) key, null);
		} catch (ClassCastException e) {
		}
		return null;
	}
	
	@SuppressWarnings("unchecked")
	@Override
	public boolean remove(Object key, Object value) {
		checkErrors();
		
		try {
			return this.removeLocal((K) key, (V) value);
		} catch (ClassCastException e) {
		}
		return false;
	}
	
	@Override
	public boolean replace(K key, V oldValue, V newValue) {
		checkErrors();
		
		return this.replaceLocal(key,oldValue,newValue);
	}
	
	@Override
	public V replace(K key, V value) {
		checkErrors();
		
		return this.updateCollectionLocal(key, value);
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
			this.updateCollectionLocal(it.next(), null);
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
