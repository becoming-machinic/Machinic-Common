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

public class HashingIntegerSerializer implements HashingSerializer<Integer> {

		@Override
		public boolean serialize(DataStream out, Integer value) throws SerializationException {
				if (value != null) {
						try {
								out.putInt(value);
								return true;
						} catch (IOException e) {
								throw new SerializationException("Serializing value failed", e);
						}
				}
				return false;
		}
}