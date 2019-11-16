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

import com.becomingmachinic.kafka.collections.CollectionStringSerde;
import com.becomingmachinic.kafka.collections.SerializationException;
import com.fasterxml.jackson.core.type.TypeReference;
import com.fasterxml.jackson.databind.ObjectMapper;

public class JacksonToStringCollectionSerde<T> implements CollectionStringSerde<T> {
	
	protected final ObjectMapper objectMapper;
	protected final Class<T> classOfT;
	protected final TypeReference<T> typeOfT;
	
	public JacksonToStringCollectionSerde(ObjectMapper objectMapper, Class<T> classOfT) {
		this.objectMapper = objectMapper;
		this.classOfT = classOfT;
		this.typeOfT = null;
	}
	public JacksonToStringCollectionSerde(ObjectMapper objectMapper, TypeReference<T> typeOfT) {
		this.objectMapper = objectMapper;
		this.classOfT = null;
		this.typeOfT = typeOfT;
	}
	
	@Override
	public String serialize(T value) throws SerializationException {
		if (value != null) {
			try {
				return this.objectMapper.writeValueAsString(value);
			} catch (Exception e) {
				throw new SerializationException("Serialize value failed", e);
			}
		}
		return null;
	}
	@Override
	public T deserialize(String raw) throws SerializationException {
		if (raw != null) {
			try {
				if (this.classOfT != null) {
					return this.objectMapper.readValue(raw, this.classOfT);
				} else {
					return this.objectMapper.readValue(raw, typeOfT);
				}
			} catch (Exception e) {
				throw new SerializationException("Deserialize value failed", e);
			}
		}
		return null;
	}
}
