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

import java.util.BitSet;
import java.util.concurrent.atomic.AtomicLong;
import java.util.concurrent.locks.ReentrantReadWriteLock;

public class InMemoryBloomFilter<T> extends AbstractBloomFilter<T> {
	
	private final int size;
	private final int optimalHashCount;
	private final BitSet bitset;
	private AtomicLong itemCount = new AtomicLong(0);
	private final ReentrantReadWriteLock lock = new ReentrantReadWriteLock();
		
	public InMemoryBloomFilter(int expectedItems, double falsePositiveProbability, Serializer<T> serializer, HashStreamProvider hashStreamProvider) {
		super(falsePositiveProbability, serializer,hashStreamProvider);
		this.size = (int) super.optimalBitSizeM(expectedItems);
		this.optimalHashCount = super.optimalNumberofHashFunctionsK(expectedItems, this.size);
		this.bitset = new BitSet(this.size); // TODO replace this with custom long based implementation with index locking
	}
	
	protected boolean set(long[] hashes) {
		// check with read lock
		if (get(hashes)) {
			return false;
		}
		
		// Upgrade to write lock, then check and set.
		lock.writeLock().lock();
		try {
			boolean updated = false;
			for (int i = 0; i < hashes.length && i < optimalHashCount; i++) {
				if (setBit(hashes[i])) {
					updated = true;
				}
			}
			return updated;
		} finally {
			lock.writeLock().unlock();
		}
	}
	
	protected boolean get(long[] hashes) {
		lock.readLock().lock();
		try {
			for (int i = 0; i < hashes.length && i < optimalHashCount; i++) {
				if (!getBit(hashes[i])) {
					return false;
				}
			}
		} finally {
			lock.readLock().unlock();
		}
		return true;
	}
	
	public long count() {
		return this.itemCount.get();
	}
	public long size() {
		return this.size;
	}
	
	private boolean getBit(long hash) {
		return this.bitset.get((int) (hash % this.size));
	}
	
	private boolean setBit(long hash) {
		if (!this.bitset.get((int) (hash % this.size))) {
			this.bitset.set((int) (hash % this.size), true);
			return true;
		}
		return false;
	}
}
