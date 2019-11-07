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

import java.util.BitSet;
import java.util.concurrent.locks.ReentrantReadWriteLock;

public class BloomFilterBitSetStore implements BloomFilterBitStore {

		private final long size;
		private final long expectedItems;
		private final double expectedFalsePositiveProbability;
		private final BitSet bitSet;
		private final ReentrantReadWriteLock lock = new ReentrantReadWriteLock();

		public BloomFilterBitSetStore(int expectedItems, double expectedFalsePositiveProbability) {
				this.size = KBloomFilter.optimalBitsM((long) expectedItems, expectedFalsePositiveProbability);
				this.expectedItems = expectedItems;
				this.expectedFalsePositiveProbability = expectedFalsePositiveProbability;
				this.bitSet = new BitSet((int) this.size);
		}

		@Override
		public boolean setHash(Hash hash, int hashCount) {
				int[] vector = hash.getVector32();
				boolean updated = false;
				for (int i = 0; i < hashCount; i++) {
						if (setBit(vector[i])) {
								updated = true;
						}
				}
				return updated;
		}
		@Override
		public boolean containsHash(Hash hash, int hashCount) {
				int[] vector = hash.getVector32();
				for (int i = 0; i < hashCount; i++) {
						if (!getBit(vector[i])) {
								return false;
						}
				}
				return true;
		}
		@Override
		public long size() {
				return this.size;
		}
		@Override
		public long getExpectedNumberOfItemsN() {
				return this.expectedItems;
		}
		@Override
		public double getExpectedFalsePositiveProbability() {
				return this.expectedFalsePositiveProbability;
		}

		private boolean getBit(int hash) {
				this.lock.readLock().lock();
				try {
						return this.bitSet.get((int) (hash % this.size));
				} finally {
						this.lock.readLock().unlock();
				}
		}

		private boolean setBit(int hash) {
				this.lock.writeLock().lock();
				try {
						if (!this.bitSet.get((int) (hash % this.size))) {
								this.bitSet.set((int) (hash % this.size), true);
								return true;
						}
						return false;
				} finally {
						this.lock.writeLock().unlock();
				}
		}
}
