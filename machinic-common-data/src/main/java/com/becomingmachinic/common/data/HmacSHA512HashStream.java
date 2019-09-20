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

import java.io.IOException;
import java.security.InvalidKeyException;
import java.security.NoSuchAlgorithmException;

import javax.crypto.Mac;
import javax.crypto.SecretKey;
import javax.crypto.spec.SecretKeySpec;

/**
 * The HMAC SHA512 hash stream hashes the data using the SHA256 HMAC (Keyed hash) to make it impossible for an attacker to predict a 
 * resulting and therefore exploit a collision in the bloom filter. For this to be true the hashKey must remain unknown to the attacker. 
 * A HMAC SHA512 has is more computationally expensive than a standard SHA512 hash so if this added security is not needed then the standard 
 * SHA512 provider should be used.
 * 
 * @author caleb
 *
 */
public class HmacSHA512HashStream extends HashStream {
	
	private final Mac mac;
	private long[] result = null;
	
	public HmacSHA512HashStream(byte[] hashKey) throws HashStreamException {
		try {
			mac = Mac.getInstance("HmacSHA512");
			SecretKey key = new SecretKeySpec(hashKey, "HmacSHA512");
			mac.init(key);
		} catch (InvalidKeyException e) {
			throw new HashStreamException("Hmac Key Not Valid.", e);
		} catch (NoSuchAlgorithmException e) {
			throw new HashStreamException("HmacSHA512 Algorithm Not Found.", e);
		}
	}
	
	@Override
	public void write(int b) throws IOException {
		this.mac.update((byte) b);
	}
	
	@Override
	public void write(byte[] b) throws IOException {
		this.write(b, 0, b.length);
	}
	
	@Override
	public void write(byte[] b, int off, int len) throws IOException {
		this.mac.update(b, off, len);
	}
	
	public long[] getHashes() {
		if (this.result == null) {
			this.result = bytesArrayToHashes(this.mac.doFinal());
		}
		return this.result;
	}
}
