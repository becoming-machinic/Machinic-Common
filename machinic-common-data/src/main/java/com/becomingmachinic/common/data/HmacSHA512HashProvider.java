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

/**
 * The HMAC SHA512 provider hashes the data using the SHA512 HMAC (Keyed hash) to make it impossible for an attacker to predict a 
 * resulting and therefore exploit a collision in the bloom filter. For this to be true the hashKey must remain unknown to the attacker. 
 * A HMAC SHA512 has is more computationally expensive than a standard SHA512 hash so if this added security is not needed then the standard 
 * SHA512 provider should be used.
 * 
 * @author caleb
 *
 */
public class HmacSHA512HashProvider implements HashStreamProvider{

	private final byte[] hashKey;
	
	/**
	 * While any key size will work the recommended key size is 64 bytes.
	 * @param hashKey
	 */
	public HmacSHA512HashProvider(final byte[] hashKey) {
		this.hashKey = hashKey;
	}
	
	@Override
	public HashStream createHashStream() throws HashStreamException {
		return new HmacSHA512HashStream(hashKey);
	}
	
}
