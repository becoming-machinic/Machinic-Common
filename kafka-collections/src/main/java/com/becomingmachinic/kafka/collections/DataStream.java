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

import java.io.ByteArrayOutputStream;
import java.io.IOException;
import java.io.ObjectOutputStream;
import java.io.OutputStream;
import java.time.Instant;
import java.time.ZonedDateTime;
import java.util.UUID;

/**
 * Utility class which writes common types to the HashStream.
 * 
 * @author Caleb Shingledecker
 *
 */
public class DataStream implements AutoCloseable {
	
	protected ObjectOutputStream dataStream;
	
	protected DataStream() throws IOException {
		this(new ByteArrayOutputStream());
	}
	
	protected DataStream(OutputStream outputStream) throws IOException {
		this.dataStream = new ObjectOutputStream(outputStream);
	}
	
	public DataStream putByte(byte b) throws IOException {
		this.dataStream.writeByte(b);
		return this;
	}
	
	public DataStream putBytes(byte[] bytes) throws IOException {
		this.dataStream.write(bytes);
		return this;
	}
	
	public DataStream putBytes(byte[] bytes, int offset, int length) throws IOException {
		this.dataStream.write(bytes, offset, length);
		return this;
	}
	
	public DataStream putChar(char c) throws IOException {
		this.dataStream.writeChar(c);
		return this;
	}
	
	public DataStream putShort(short s) throws IOException {
		this.dataStream.writeShort(s);
		return this;
	}
	
	public DataStream putInt(int i) throws IOException {
		this.dataStream.writeInt(i);
		return this;
	}
	
	public DataStream putLong(long l) throws IOException {
		this.dataStream.writeLong(l);
		return this;
	}
	
	public DataStream putFloat(float f) throws IOException {
		this.dataStream.writeFloat(f);
		return this;
	}
	
	public DataStream putDouble(double d) throws IOException {
		this.dataStream.writeDouble(d);
		return this;
	}
	
	public DataStream putBoolean(boolean b) throws IOException {
		this.dataStream.writeBoolean(b);
		return this;
	}
	
	public DataStream putString(String string) throws IOException {
		this.dataStream.writeUTF(string);
		return this;
	}
	
	public DataStream putChars(CharSequence charSequence) throws IOException {
		return this.putString(charSequence.toString());
	}
	
	public DataStream putUUID(UUID uuid) throws IOException {
		this.dataStream.writeLong(uuid.getMostSignificantBits());
		this.dataStream.writeLong(uuid.getLeastSignificantBits());
		return this;
	}
	
	public DataStream putInstant(Instant instant) throws IOException {
		return this.putLong(instant.toEpochMilli());
	}
	
	public DataStream putZonedDateTime(ZonedDateTime dateTime) throws IOException {
		return this.putLong(dateTime.toInstant().toEpochMilli());
	}
	
	/**
	 * Writes object using java serialization
	 * 
	 * @param object
	 * @return
	 * @throws IOException
	 */
	public DataStream putObject(Object object) throws IOException {
		this.dataStream.writeObject(object);
		return this;
	}
	
	@Override
	public void close() throws Exception {
		this.dataStream.flush();
	}
}