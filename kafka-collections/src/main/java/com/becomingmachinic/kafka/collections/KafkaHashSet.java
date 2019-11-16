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

import java.util.*;
import java.util.concurrent.ConcurrentHashMap;

public class KafkaHashSet<K> extends AbstractKafkaHashSet<K> implements KSet<K> {

		protected final Set<Hash> delegateSet;

		public KafkaHashSet(CollectionConfig collectionConfig, HashingSerializer<K> hashingSerializer, HashStreamProvider hashStreamProvider) {
				this(Collections.newSetFromMap(new ConcurrentHashMap<>()), collectionConfig, hashingSerializer, hashStreamProvider);
		}

		public KafkaHashSet(Set<Hash> delegateSet, CollectionConfig collectionConfig, HashingSerializer<K> hashingSerializer, HashStreamProvider hashStreamProvider) {
				this(delegateSet, collectionConfig, hashingSerializer, hashStreamProvider, CollectionSerde.byteArrayToHash(), CollectionSerde.stringToString());
		}

		public KafkaHashSet(Set<Hash> delegateSet, CollectionConfig collectionConfig, HashingSerializer<K> hashingSerializer, HashStreamProvider hashStreamProvider, CollectionSerde<byte[], Hash> keySerde, CollectionSerde<String, String> valueSerde) {
				super(collectionConfig, hashingSerializer, hashStreamProvider, keySerde, valueSerde);
				this.delegateSet = delegateSet;

				super.start();
		}

		@Override
		protected boolean addLocal(Hash key) {
				return this.delegateSet.add(key);
		}
		@Override
		protected boolean removeLocal(Hash key) {
				return this.delegateSet.remove(key);
		}
		@Override
		protected boolean containsLocal(Hash key) {
				return this.delegateSet.contains(key);
		}

		public int size() {
				return delegateSet.size();
		}
		public boolean isEmpty() {
				return delegateSet.isEmpty();
		}
		public boolean contains(Object o) {
				try {
						return this.containsLocal(this.getHash((K) o));
				} catch (ClassCastException e) {
						return false;
				}
		}
		public boolean add(K k) {
				KafkaCollectionException exp = this.getException();
				if (exp != null) {
						throw exp;
				}
				if (k != null) {
						return this.addKey(k);
				}
				return false;
		}
		public boolean remove(Object o) {
				KafkaCollectionException exp = this.getException();
				if (exp != null) {
						throw exp;
				}
				try {
						return this.removeKey((K) o);
				} catch (ClassCastException e) {
						return false;
				}
		}
		public boolean containsAll(Collection<?> c) {
				for (Object entry : c) {
						try {
								if (!this.containsLocal(this.getHash((K) entry))) {
										return false;
								}
						} catch (ClassCastException e) {
								return false;
						}
				}
				return true;
		}
		public boolean addAll(Collection<? extends K> c) {
				if (this.getException() != null) {
						throw this.getException();
				}

				boolean changed = false;
				for (K key : c) {
						if (this.addKey(key)) {
								changed = true;
						}
				}
				return changed;
		}
		@Override
		public boolean retainAll(Collection<?> c) {
				//TODO this could likely be implemented
				throw new UnsupportedOperationException("KafkaHashSet does not retain the elements, only their hashes");
		}
		@Override
		public boolean removeAll(Collection<?> c) throws KafkaCollectionException {
				if (this.getException() != null) {
						throw this.getException();
				}

				boolean changed = false;
				for (Object key : c) {
						try {
								if (this.removeKey((K) key)) {
										changed = true;
								}
						} catch (ClassCastException e) {
						}
				}
				return changed;
		}
		public void clear() throws KafkaCollectionException {
				if (this.getException() != null) {
						throw this.getException();
				}

				Iterator<Hash> it = this.delegateSet.iterator();
				while (it.hasNext()) {
						this.collectionRemove(it.next());
				}
		}
		@Override
		public Iterator<K> iterator() throws UnsupportedOperationException {
				throw new UnsupportedOperationException("KafkaHashSet does not retain the elements, only their hashes");
		}
		@Override
		public Object[] toArray() throws UnsupportedOperationException {
				throw new UnsupportedOperationException("KafkaHashSet does not retain the elements, only their hashes");
		}
		@Override
		public <T> T[] toArray(T[] a) throws UnsupportedOperationException {
				throw new UnsupportedOperationException("KafkaHashSet does not retain the elements, only their hashes");
		}
		@Override
		public Spliterator<K> spliterator() {
				throw new UnsupportedOperationException("KafkaHashSet does not retain the elements, only their hashes");
		}

		@Override
		public boolean equals(Object o) {
				if (o != null) {
						if (o instanceof KafkaHashSet) {
								KafkaHashSet<?> other = (KafkaHashSet<?>) o;
								return Objects.equals(this.delegateSet, other.delegateSet);
						}
				}
				return false;
		}
		@Override
		public int hashCode() {
				return Objects.hash(this.delegateSet);
		}

}
