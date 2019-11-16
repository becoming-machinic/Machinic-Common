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

import java.lang.reflect.Type;

import com.becomingmachinic.kafka.collections.CollectionStringSerde;
import com.becomingmachinic.kafka.collections.SerializationException;
import com.google.gson.Gson;

public class GsonToStringCollectionSerde<T> implements CollectionStringSerde<T> {
	
	protected final Gson gson;
	protected final Class<T> classOfT;
	protected final Type typeOfT;
	
	public GsonToStringCollectionSerde(Gson gson, Class<T> classOfT) {
		this.gson = gson;
		this.classOfT = classOfT;
		this.typeOfT = null;
	}
	public GsonToStringCollectionSerde(Gson gson, Type typeOfT) {
		this.gson = gson;
		this.classOfT = null;
		this.typeOfT = typeOfT;
	}
	
	@Override
	public String serialize(T value) throws SerializationException {
		if (value != null) {
			try {
				return this.gson.toJson(value);
			} catch (Exception e) {
				throw new SerializationException("Serialize value failed.", e);
			}
		}
		return null;
	}
	@Override
	public T deserialize(String raw) throws SerializationException {
		if (raw != null) {
			try {
				if (this.classOfT != null) {
					return this.gson.fromJson(raw, classOfT);
				} else {
					return this.gson.fromJson(raw, typeOfT);
				}
			} catch (Exception e) {
				throw new SerializationException("Deserialize value failed", e);
			}
		}
		return null;
	}
}
