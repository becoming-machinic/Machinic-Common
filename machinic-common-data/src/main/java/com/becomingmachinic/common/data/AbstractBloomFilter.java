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

package com.becomingmachinic.common.data;

import java.util.Collection;

/**
 * An abstract implementation for the bloom filter.
 * @author Caleb Shingledecker
 *
 * @param <T>
 */
public abstract class AbstractBloomFilter<T> implements BloomFilter<T> {
	
	static final double LOG_2 = Math.log(2);
	static final double LOG_2_SQUARE = LOG_2 * LOG_2;
	
	protected final double falsePositiveProbability;
	protected final Serializer<T> serializer;
	protected final HashStreamProvider hashStreamProvider;
	
	protected AbstractBloomFilter(double falsePositiveProbability, Serializer<T> serializer, HashStreamProvider hashStreamProvider) {
		this.falsePositiveProbability = falsePositiveProbability;
		this.serializer = serializer;
		this.hashStreamProvider = hashStreamProvider;
	}
	/**
	 * Calculates the optimal number of bits for a given number of items.
	 * @param expectedItemsN
	 * @return
	 */
	protected long optimalBitSizeM(long expectedItemsN) {
		return (long) Math.ceil((-1 * expectedItemsN * Math.log(this.falsePositiveProbability) / (LOG_2_SQUARE)));
	}
	
	protected int optimalNumberofHashFunctionsK(final long expectedItemsN, final long sizeM) {
		return Math.max(1, (int) Math.round(sizeM / expectedItemsN * Math.log(2)));
	}
	
	protected abstract boolean set(long[] hashes);
	
	protected abstract boolean get(long[] hashes);
	
	protected long[] getHash(T value) throws MachinicDataException {
		try (HashStream hashStream = this.hashStreamProvider.createHashStream()) {
			try (DataStream dataStream = new DataStream(hashStream)) {
				// TODO respect the response boolean
				this.serializer.serialize(dataStream, value);
			}
			return hashStream.getHashes();
		} catch (Exception e) {
			throw new MachinicDataException("Serializing value failed", e);
		}
	}
	
	@Override
	public boolean add(T value) throws MachinicDataException {
		try {
			return this.set(getHash(value));
		} catch (MachinicDataException e) {
			throw e;
		} catch (Exception e) {
			throw new MachinicDataException("Add value failed", e);
		}
	}
	
	@Override
	public boolean addAll(Collection<T> values) throws MachinicDataException {
		boolean changed = false;
		for (T value : values) {
			if (this.add(value)) {
				changed = true;
			}
		}
		return changed;
	}
	
	@Override
	public boolean contains(T value) throws MachinicDataException {
		try {
			return this.get(getHash(value));
		} catch (MachinicDataException e) {
			throw e;
		} catch (Exception e) {
			throw new MachinicDataException("Contains value failed", e);
		}
	}
	
	@Override
	public boolean containsAll(Collection<T> values) throws MachinicDataException {
		boolean contains = true;
		for (T value : values) {
			if (!this.contains(value)) {
				contains = false;
			}
		}
		return contains;
	}
	
	@Override
	public double getFalsePositiveProbability() {
		return 0;
	}
	
	@Override
	public void close() throws Exception {
		
	}
}
