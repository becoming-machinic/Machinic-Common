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

package com.becomingmachinic.kafka.collections.extensions;

import java.io.IOException;
import java.util.Arrays;
import java.util.Comparator;
import java.util.Objects;

import org.mapdb.CC;
import org.mapdb.DataInput2;
import org.mapdb.DataOutput2;
import org.mapdb.Serializer;
import org.mapdb.serializer.GroupSerializer;

import com.becomingmachinic.kafka.collections.Hash;

import net.jpountz.xxhash.XXHash32;

public class MapDBSerializerHash implements GroupSerializer<Hash> {
	
	private static final XXHash32 HASHER = CC.HASH_FACTORY.hash32();
	
	@Override
	public void serialize(DataOutput2 out, Hash value) throws IOException {
		out.packInt(value.getBuffer().length);
		out.write(value.getBuffer());
	}
	
	@Override
	public Hash deserialize(DataInput2 in, int available) throws IOException {
		int size = in.unpackInt();
		byte[] ret = new byte[size];
		in.readFully(ret);
		return Hash.wrap(ret);
	}
	
	@Override
	public boolean isTrusted() {
		return true;
	}
	
	@Override
	public boolean equals(Hash a1, Hash a2) {
		return Objects.equals(a1, a2);
	}
	
	@Override
	public int hashCode(Hash hash, int seed) {
		return HASHER.hash(hash.getBuffer(), 0, hash.getBuffer().length, seed);
	}
	
	@Override
	public int compare(Hash o1, Hash o2) {
		if (o1 == o2)
			return 0;
		return o1.compareTo(o2);
	}
	
	@Override
	public int valueArraySearch(Object keys, Hash key) {
		
		return Arrays.binarySearch((byte[][]) keys, key.getBuffer(), Serializer.BYTE_ARRAY);
	}
	
	@Override
	public void valueArraySerialize(DataOutput2 out, Object vals) throws IOException {
		byte[][] vals2 = (byte[][]) vals;
		out.packInt(vals2.length);
		for (byte[] b : vals2) {
			Serializer.BYTE_ARRAY.serialize(out, b);
		}
	}
	
	@Override
	public byte[][] valueArrayDeserialize(DataInput2 in, int size) throws IOException {
		int s = in.unpackInt();
		byte[][] ret = new byte[s][];
		for (int i = 0; i < s; i++) {
			ret[i] = Serializer.BYTE_ARRAY.deserialize(in, -1);
		}
		return ret;
	}
	
	@SuppressWarnings({ "unchecked", "rawtypes" })
	@Override
	public int valueArraySearch(Object keys, Hash key, Comparator comparator) {
		// TODO PERF optimize search
		Object[] v = valueArrayToArray(keys);
		return Arrays.binarySearch(v, key, comparator);
	}
	
	@Override
	public Hash valueArrayGet(Object vals, int pos) {
		return Hash.wrap(((byte[][]) vals)[pos]);
	}
	
	@Override
	public int valueArraySize(Object vals) {
		return ((byte[][]) vals).length;
	}
	
	@Override
	public byte[][] valueArrayEmpty() {
		return new byte[0][];
	}
	
	@Override
	public byte[][] valueArrayPut(Object vals, int pos, Hash newValue) {
		byte[][] array = (byte[][]) vals;
		final byte[][] ret = Arrays.copyOf(array, array.length + 1);
		if (pos < array.length) {
			System.arraycopy(array, pos, ret, pos + 1, array.length - pos);
		}
		ret[pos] = newValue.getBuffer();
		return ret;
	}
	
	@Override
	public byte[][] valueArrayUpdateVal(Object vals, int pos, Hash newValue) {
		byte[][] vals2 = (byte[][]) vals;
		vals2 = vals2.clone();
		vals2[pos] = newValue.getBuffer();
		return vals2;
	}
	
	@Override
	public byte[][] valueArrayFromArray(Object[] objects) {
		byte[][] ret = new byte[objects.length][];
		for (int i = 0; i < ret.length; i++) {
			ret[i] = ((Hash) objects[i]).getBuffer();
		}
		return ret;
	}
	
	@Override
	public byte[][] valueArrayCopyOfRange(Object vals, int from, int to) {
		return Arrays.copyOfRange((byte[][]) vals, from, to);
	}
	
	@Override
	public byte[][] valueArrayDeleteValue(Object vals, int pos) {
		byte[][] vals2 = new byte[((byte[][]) vals).length - 1][];
		System.arraycopy(vals, 0, vals2, 0, pos - 1);
		System.arraycopy(vals, pos, vals2, pos - 1, vals2.length - (pos - 1));
		return vals2;
	}
}
