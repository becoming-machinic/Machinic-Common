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

import java.io.IOException;

import com.google.common.hash.Hasher;
import com.google.common.hash.Hashing;

public class HashStreamSipHash extends HashStream implements GuavaHashing {
	
	private final Hasher hasher;
	private Hash result = null;
	
	public HashStreamSipHash(long k0, long k1) {
		this.hasher = Hashing.sipHash24(k0, k1).newHasher();
	}
	
	@Override
	public void write(int b) throws IOException {
		this.write(new byte[] { (byte) b });
	}
	
	@Override
	public void write(byte[] b) throws IOException {
		this.write(b, 0, b.length);
	}
	
	@Override
	public void write(byte[] b, int off, int len) throws IOException {
		this.hasher.putBytes(b, off, len);
	}
	
	@Override
	public Hash getHashes() {
		if (this.result == null) {
			this.result = Hash.wrap(this.hasher.hash().asBytes());
		}
		return this.result;
	}
}