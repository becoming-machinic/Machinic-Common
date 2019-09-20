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
import java.io.Serializable;
import java.time.Instant;
import java.time.ZonedDateTime;
import java.util.UUID;

/**
 * This serializer implementation converts common Java objects to binary and writes them to the HashStream for hashing.
 * @author Caleb Shingledecker
 *
 */
public class ObjectSerializer implements Serializer<Object>{

	@Override
	public boolean serialize(DataStream out, Object value) throws SerializerException {
		if(value != null) {
			try {
				if(value instanceof String) {
					out.putString((String) value);
					return true;
				}
				if(value instanceof byte[]) {
					out.putBytes((byte[]) value);
					return true;
				}
				if(value instanceof Long) {
					out.putLong((Long) value);
					return true;
				}
				if(value instanceof Integer) {
					out.putInt((Integer) value);
					return true;
				}
				if(value instanceof Short) {
					out.putShort((Short) value);
					return true;
				}
				if(value instanceof Byte) {
					out.putByte((Byte) value);
					return true;
				}
				if(value instanceof Double) {
					out.putDouble((Double) value);
					return true;
				}
				if(value instanceof Float) {
					out.putFloat((Float) value);
					return true;
				}
				if(value instanceof Boolean) {
					out.putBoolean((Boolean) value);
					return true;
				}
				if(value instanceof UUID) {
					out.putUUID((UUID) value);
					return true;
				}
				if(value instanceof Instant) {
					out.putInstant((Instant) value);
					return true;
				}
				if(value instanceof ZonedDateTime) {
					out.putZonedDateTime((ZonedDateTime) value);
					return true;
				}
				if(value instanceof Serializable) {
					out.putObject(value);
					return true;
				}
				// Note that if the object does not override toString this will not work as expected.
				out.putString(value.toString());
				return true;
			} catch (IOException e) {
				throw new SerializerException("Serializing value failed",e);
			}
		}
		return false;
	}
}
