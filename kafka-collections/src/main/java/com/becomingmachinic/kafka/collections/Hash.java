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

import java.nio.ByteBuffer;
import java.util.Arrays;

public class Hash implements Comparable<Hash> {
	
	/**
	 * When converting a byte[] to an array of hashes prime numbers are used to reduce the likelihood of a hash collision between hashes that produce the same long values but in a different order.
	 */
	private int[] primes = new int[] {
			1, 31, 37, 41, 43, 47, 53, 59, 61, 67, 71, 73, 79, 83, 89, 97, 101, 103, 107, 109, 113, 127, 131, 137, 139, 149, 151, 157, 163, 167, 173, 179
	};
	
	byte[] buffer;
	
	public Hash(byte[] buffer) {
		this.buffer = buffer;
	}
	
	public byte[] getBuffer() {
		return buffer;
	}
	
	@Override
	public boolean equals(Object o) {
		if (this == o)
			return true;
		if (o == null || getClass() != o.getClass())
			return false;
		Hash hash = (Hash) o;
		return Arrays.equals(buffer, hash.buffer);
	}
	@Override
	public int hashCode() {
		return Arrays.hashCode(buffer);
	}
	
	public long[] getVector64() {
		ByteBuffer bbuffer = ByteBuffer.wrap(buffer);
		long[] longs = new long[buffer.length / 8];
		for (int i = 0; i < longs.length; i++) {
			longs[i] = bbuffer.getLong();
		}
		
		for (int i = 0; i < longs.length; i++) {
			longs[i] = Math.abs(longs[i] * primes[i]);
		}
		
		return longs;
	}
	
	public int[] getVector32() {
		ByteBuffer bbuffer = ByteBuffer.wrap(buffer);
		int[] ints = new int[buffer.length / 4];
		for (int i = 0; i < ints.length; i++) {
			ints[i] = bbuffer.getInt();
		}
		
		for (int i = 0; i < ints.length; i++) {
			ints[i] = Math.abs(ints[i] * primes[i]);
		}
		
		return ints;
	}
	
	@Override
	public int compareTo(Hash o) {
		if (o != null) {
			if (this == o)
				return 0;
			final int len = Math.min(buffer.length, o.buffer.length);
			for (int i = 0; i < len; i++) {
				int b1 = this.buffer[i] & 0xFF;
				int b2 = o.buffer[i] & 0xFF;
				if (b1 != b2)
					return b1 - b2;
			}
			return this.buffer.length - o.buffer.length;
		}
		return -1;
	}
	
	public static Hash wrap(byte[] buffer) {
		return new Hash(buffer);
	}
	
	@Override
	public String toString() {
		return Arrays.toString(this.buffer);
	}
}
