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

public abstract class AbstractKafkaHashSet<K> extends AbstractKafkaSet<byte[], Hash> {
	
	protected static final String VALUE = "1";
	
	protected final CollectionSerde<byte[], Hash> keySerde;
	protected final CollectionSerde<String, String> valueSerde;
	protected final HashingSerializer<K> hashingSerializer;
	protected final HashStreamProvider hashStreamProvider;
	
	public AbstractKafkaHashSet(CollectionConfig collectionConfig, HashingSerializer<K> hashingSerializer, HashStreamProvider hashStreamProvider, CollectionSerde<byte[], Hash> keySerde, CollectionSerde<String, String> valueSerde) {
		super(collectionConfig, keySerde, valueSerde);
		
		this.keySerde = keySerde;
		this.valueSerde = valueSerde;
		this.hashingSerializer = hashingSerializer;
		this.hashStreamProvider = hashStreamProvider;
	}
	
	protected Hash getHash(K value) throws HashStreamException {
		try (HashStream hashStream = this.hashStreamProvider.createHashStream()) {
			try (DataStream dataStream = new DataStream(hashStream)) {
				if (!this.hashingSerializer.serialize(dataStream, value)) {
					return null;
				}
			}
			return hashStream.getHashes();
		} catch (ClassCastException e) {
			return null;
		} catch (Exception e) {
			throw new HashStreamException("Serializing value via hashingSerializer failed", e);
		}
	}
	
	protected boolean addKey(K k) {
		return this.collectionAdd(this.getHash(k));
	}
	
	protected boolean removeKey(K k) {
		return this.collectionRemove(this.getHash(k));
	}
	
}
