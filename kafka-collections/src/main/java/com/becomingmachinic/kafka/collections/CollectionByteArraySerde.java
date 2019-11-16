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

import org.apache.kafka.common.serialization.ByteArrayDeserializer;
import org.apache.kafka.common.serialization.ByteArraySerializer;
import org.apache.kafka.common.serialization.Deserializer;
import org.apache.kafka.common.serialization.Serializer;

public interface CollectionByteArraySerde<V> extends CollectionSerde<byte[], V> {
	
	@Override
	public byte[] serialize(V value) throws SerializationException;
	
	@Override
	public V deserialize(byte[] raw) throws SerializationException;
	
	@Override
	public default Serializer<byte[]> getRawSerializer() {
		return new ByteArraySerializer();
	}
	
	@Override
	public default Deserializer<byte[]> getRawDeserializer() {
		return new ByteArrayDeserializer();
	}
}
