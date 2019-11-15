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

/**
 * A Murmur3 has is less computationally expensive than a standard SHA256 hash.
 * It is also not a cryptographic hash function, so it should not be used where an attacker can gain benefit by predicting hash collisions
 *
 * @author caleb
 */
public class HashStreamProviderMurmur3 implements HashStreamProvider {

		private final int seed;

		public HashStreamProviderMurmur3() {
				this(0);
		}

		public HashStreamProviderMurmur3(int seed) {
				this.seed = seed;
		}

		@Override
		public HashStream createHashStream() throws HashStreamException {
				return new HashStreamMurmur3(this.seed);
		}

		@Override
		public int getNumberOfHashFunctions() {
				return 4;
		}
}