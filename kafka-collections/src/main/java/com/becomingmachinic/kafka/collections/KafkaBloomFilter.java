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
import java.util.Objects;
import java.util.concurrent.atomic.AtomicLong;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class KafkaBloomFilter<K> extends AbstractKafkaHashSet<K> implements KBloomFilter<K> {
	private static final Logger logger = LoggerFactory.getLogger(KafkaBloomFilter.class);
	
	protected final BloomFilterBitStore store;
	protected final AtomicLong itemCount = new AtomicLong(0);
	protected final int numberOfHashFunctionsK;
	
	public KafkaBloomFilter(BloomFilterBitStore store, CollectionConfig collectionConfig, HashingSerializer<K> hashingSerializer, HashStreamProvider hashStreamProvider) {
		this(store, collectionConfig, hashingSerializer, hashStreamProvider, CollectionSerde.byteArrayToHash(), CollectionSerde.stringToString());
	}
	
	public KafkaBloomFilter(BloomFilterBitStore store, CollectionConfig collectionConfig, HashingSerializer<K> hashingSerializer, HashStreamProvider hashStreamProvider, CollectionSerde<byte[], Hash> keySerde, CollectionSerde<String, String> valueSerde) {
		super(collectionConfig, hashingSerializer, hashStreamProvider, keySerde, valueSerde);
		this.store = store;
		this.numberOfHashFunctionsK = Math.min(KBloomFilter.optimalNumberOfHashFunctionsK(this.store.getExpectedNumberOfItemsN(), this.store.size()), hashStreamProvider.getNumberOfHashFunctions());
		logger.debug("KafkaBloomFilter {} will use {} hash functions out of the avalible {} hash functions provided by the HashStreamProvider", collectionConfig.getName(), this.numberOfHashFunctionsK, hashStreamProvider.getNumberOfHashFunctions());
		super.start();
	}
	
	@Override
	protected boolean addLocal(Hash key) {
		if (this.store.setHash(key, this.numberOfHashFunctionsK)) {
			this.itemCount.incrementAndGet();
			return true;
		}
		return false;
	}
	@Override
	protected boolean removeLocal(Hash key) {
		return false;
	}
	@Override
	protected boolean containsLocal(Hash key) {
		return this.store.containsHash(key, this.numberOfHashFunctionsK);
	}
	
	@Override
	public boolean add(K value) throws KafkaCollectionException {
		KafkaCollectionException exp = this.getException();
		if (exp != null) {
			throw exp;
		}
		if (value != null) {
			return this.addKey(value);
		}
		return false;
	}
	@Override
	public boolean addAll(Collection<K> values) throws KafkaCollectionException {
		if (this.getException() != null) {
			throw this.getException();
		}
		
		boolean changed = false;
		for (K key : values) {
			if (this.addKey(key)) {
				changed = true;
			}
		}
		return changed;
	}
	@Override
	public boolean contains(K value) throws KafkaCollectionException {
		return this.containsLocal(this.getHash(value));
	}
	@SuppressWarnings("unchecked")
	@Override
	public boolean containsAll(Collection<K> values) throws KafkaCollectionException {
		for (Object entry : values) {
			try {
				if (!this.containsLocal(this.getHash((K) entry))) {
					return false;
				}
			} catch (ClassCastException e) {
				return false;
			}
		}
		return true;
	}
	@Override
	public long size() {
		return this.store.getExpectedNumberOfItemsN();
	}
	@Override
	public long count() {
		return this.itemCount.get();
	}
	@Override
	public double getFalsePositiveProbability() {
		return KBloomFilter.probabilityOfFalsePositives(this.itemCount.get(), this.store.size(), this.numberOfHashFunctionsK);
	}
	
	@Override
	public double getExpectedFalsePositiveProbability() {
		return this.store.getExpectedFalsePositiveProbability();
	}
	
	@Override
	public boolean equals(Object o) {
		if (o != null) {
			if (o instanceof KafkaBloomFilter) {
				KafkaBloomFilter<?> other = (KafkaBloomFilter<?>) o;
				if (this.hashStreamProvider.getClass().equals(other.hashStreamProvider.getClass())) {
					return Objects.equals(this.store, other.store);
				}
			}
		}
		return false;
	}
	@Override
	public int hashCode() {
		return Objects.hash(this.store);
	}
}
