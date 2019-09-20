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
import java.io.OutputStream;
import java.nio.ByteBuffer;

/**
 * Base HashStream that all HashStream implementations should extend.
 * @author Caleb Shingledecker
 *
 */
public abstract class HashStream extends OutputStream implements AutoCloseable {
	
	/**
	 * When converting a byte[] to an array of hashes prime numbers are used to reduce the likelihood 
	 * of a hash collision between hashes that produce the same long values but in a different order.
	 */
	private int[] primes = new int[] {
			1,31,37,41,43,47,53,59,61,67,71,73,79,83,89,97
	};
	
	public abstract void write(int b) throws IOException;
	
	public abstract void write(byte[] b) throws IOException;
	
	public abstract void write(byte[] b, int off, int len) throws IOException;
	
	public abstract long[] getHashes() throws IOException;
		
	protected long[] bytesArrayToHashes(byte[] bytes) {
		ByteBuffer buffer = ByteBuffer.wrap(bytes);
		long[] longs = new long[bytes.length / 8];
		for(int i = 0; i < longs.length;i++) {
			longs[i] = buffer.getLong();
		}
		
		for (int i = 0; i < longs.length; i++) {
			longs[i] = Math.abs(longs[i] * primes[i]);
		}
		
		return longs;
	}
}
