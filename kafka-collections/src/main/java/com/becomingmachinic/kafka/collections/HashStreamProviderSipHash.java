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
 * A SipHash has is less computationally expensive than an HmacSHA256 hash but also has the advantage of being keyed.
 *
 * @author caleb
 */
public class HashStreamProviderSipHash implements HashStreamProvider {
	
	private final long k0;
	private final long k1;
	
	public HashStreamProviderSipHash() {
		this(0, 0);
	}
	
	public HashStreamProviderSipHash(long k0, long k1) {
		this.k0 = k0;
		this.k1 = k1;
	}
	
	@Override
	public HashStream createHashStream() throws HashStreamException {
		return new HashStreamSipHash(this.k0, this.k1);
	}
	
	@Override
	public int getNumberOfHashFunctions() {
		return 2;
	}
}