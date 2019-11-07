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
import java.util.concurrent.TimeUnit;

/**
 * A Bloom Filter is a space-efficient probabilistic data structure, that is used to test whether an element is a member of a set. False positive matches are possible, but false negatives are not.
 *
 * A Bloom Filter can be used with a various hash algorithms. This implementation differers from other implementations by specializing in cryptographic hash functions 
 * and uses as much of the hash value as needed to obtain the minimal false positive probability for a given filter size.
 *
 * @author Caleb Shingledecker
 *
 * @param <T>
 * 	Type of value that the bloom filter will contain.
 */
public interface KBloomFilter<T> extends AutoCloseable{
		static final double LOG_2 = Math.log(2);
		static final double LOG_2_SQUARE = LOG_2 * LOG_2;

		public static long optimalBitsM(long expectedItemsN,double falsePositiveProbabilityP) {
				return (long) Math.ceil((-1 * expectedItemsN * Math.log(falsePositiveProbabilityP) / (LOG_2_SQUARE)));
		}
		public static int optimalNumberOfHashFunctionsK(final long expectedItemsN, final long sizeM) {
				return Math.max(1, (int) Math.round(sizeM / expectedItemsN * Math.log(2)));
		}
		public static double probabilityOfFalsePositives(final long expectedItemsN, final long sizeM,final int numberOfHashFunctionsK){
				return Math.pow(1 - Math.exp(-numberOfHashFunctionsK / (sizeM / expectedItemsN)), numberOfHashFunctionsK);
		}


		public boolean awaitWarmupComplete(long timeout, TimeUnit unit) throws InterruptedException;


		/**
		 * Add value to the bloom filter. If the process of adding this item modifies the bloom filter then the return value will be true.
		 * @param value
		 * 	the value
		 * @return <code>true</code> if the bloom filter was modified,
		 *         <code>false</code> otherwise
		 * @throws KafkaCollectionException
		 */
		public boolean add(T value) throws KafkaCollectionException;

		/**
		 * Add all value to the bloom filter. The value will be converted to bytes using the provided serializer
		 * @param bytes
		 * @return <code>true</code> if the bloom filter was modified,
		 *         <code>false</code> otherwise
		 * @throws KafkaCollectionException
		 */
		public boolean addAll(Collection<T> values) throws KafkaCollectionException;

		/**
		 * Check to see if the bloom filter contains value.
		 * @param value
		 * 	the value
		 * @return <code>true</code> if the bloom filter contains the value,
		 *         <code>false</code> otherwise
		 * @throws KafkaCollectionException
		 */
		public boolean contains(T value) throws KafkaCollectionException;

		/**
		 * Check to see if the bloom filter contains all values.
		 * @param values
		 *   collection of values
		 * @return <code>true</code> if the bloom filter contains all values,
		 *         <code>false</code> otherwise
		 * @throws KafkaCollectionException
		 */
		public boolean containsAll(Collection<T> values) throws KafkaCollectionException;


		/**
		 * Returns the size of the bloom filter.
		 *
		 * @return the size of the bloom filter
		 */
		public long size();

		/**
		 * Returns the number of items in the bloom filter.
		 * @return the number of items in the bloom filter
		 */
		public long count();

		/**
		 * Returns the estimate of the current false positive rate with the current number of items.
		 * @param numInsertedElements
		 * @return
		 */
		public double getFalsePositiveProbability();

}