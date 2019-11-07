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
import com.google.gson.Gson;
import com.google.gson.GsonBuilder;

import java.io.IOException;
import java.lang.reflect.Type;

public class HashingGsonSerializer<T> implements HashingSerializer<T> {
		protected final Gson gson;

		public HashingGsonSerializer(){
				this(new GsonBuilder().create());
		}

		public HashingGsonSerializer(Gson gson) {
				this.gson = gson;
		}

		@Override
		/**
		 * Gson orders the json fields in the same order as the code, so code changes could change the message signature.
		 */
		public boolean serialize(DataStream out, T value) throws SerializationException {
				if (value != null) {
						try {
								out.putString(gson.toJson(value));
								return true;
						} catch (Exception e) {
								throw new SerializationException("Serializing value failed", e);
						}
				}
				return false;
		}
}