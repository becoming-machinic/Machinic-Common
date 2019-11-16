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

import com.becomingmachinic.kafka.collections.DataStream;
import com.becomingmachinic.kafka.collections.HashingSerializer;
import com.becomingmachinic.kafka.collections.SerializationException;
import com.fasterxml.jackson.databind.MapperFeature;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.fasterxml.jackson.databind.SerializationFeature;

public class HashingJacksonSerializer<T> implements HashingSerializer<T> {
	protected final ObjectMapper objectMapper;
	
	public HashingJacksonSerializer() {
		this(new ObjectMapper().configure(MapperFeature.SORT_PROPERTIES_ALPHABETICALLY, true).configure(SerializationFeature.ORDER_MAP_ENTRIES_BY_KEYS, true));
	}
	
	public HashingJacksonSerializer(ObjectMapper objectMapper) {
		this.objectMapper = objectMapper;
	}
	
	@Override
	public boolean serialize(DataStream out, T value) throws SerializationException {
		if (value != null) {
			try {
				out.putString(objectMapper.writeValueAsString(value));
				return true;
			} catch (Exception e) {
				throw new SerializationException("Serializing value failed", e);
			}
		}
		return false;
	}
}