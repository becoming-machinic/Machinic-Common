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
import java.security.MessageDigest;
import java.security.NoSuchAlgorithmException;

public class SHA256HashStream extends HashStream{
	
	private final MessageDigest digest;
	private long[] result = null;
	
	public SHA256HashStream() throws HashStreamException {
		try {
			digest = MessageDigest.getInstance("SHA-256");
		} catch (NoSuchAlgorithmException e) {
			throw new HashStreamException("SHA256 Algorithm Not Found.", e);
		}
	}
	
	@Override
	public void write(int b) throws IOException {
		this.digest.update((byte) b);
	}
	
	@Override
	public void write(byte[] b) throws IOException {
		this.write(b, 0, b.length);
	}
	
	@Override
	public void write(byte[] b, int off, int len) throws IOException {
		this.digest.update(b, off, len);
	}
	
	public long[] getHashes() {
		if (this.result == null) {
			this.result = bytesArrayToHashes(this.digest.digest());
		}
		return this.result;
	}
}
