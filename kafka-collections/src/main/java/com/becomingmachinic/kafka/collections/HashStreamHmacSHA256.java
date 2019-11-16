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

import java.security.InvalidKeyException;
import java.security.NoSuchAlgorithmException;

import javax.crypto.Mac;
import javax.crypto.SecretKey;
import javax.crypto.spec.SecretKeySpec;

public class HashStreamHmacSHA256 extends AbstractHmacHashStream {
	
	public HashStreamHmacSHA256(byte[] hashKey) {
		super(hashKey);
	}
	
	@Override
	protected Mac getMacInstance(byte[] hashKey) throws HashStreamException {
		try {
			Mac mac = Mac.getInstance("HmacSHA256");
			SecretKey key = new SecretKeySpec(hashKey, "HmacSHA256");
			mac.init(key);
			return mac;
		} catch (InvalidKeyException e) {
			throw new HashStreamException("Hmac Key Not Valid.", e);
		} catch (NoSuchAlgorithmException e) {
			throw new HashStreamException("HmacSHA256 Algorithm Not Found.", e);
		}
	}
}