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
 * The SHA256 hash provider hashes the input value using a standard SHA256 hash.
 * @author caleb
 *
 */
public class HashStreamProviderMD5 implements HashStreamProvider{

	@Override
	public HashStream createHashStream() throws HashStreamException {
		return new HashStreamMD5();
	}

		@Override
		public int getNumberOfHashFunctions() {
				return 4;
		}
}