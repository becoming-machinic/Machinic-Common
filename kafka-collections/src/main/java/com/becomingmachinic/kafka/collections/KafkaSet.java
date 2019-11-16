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

/**
 *
 * @param <K> Type of objects in the collection
 * @param <KK> Raw serialized Kafka type. Typically String or byte[]
 */
public class KafkaSet<KK,K> extends AbstractKafkaSet<KK, K> implements KSet<K> {

		protected static final String VALUE = "1";

		protected final Set<K> delegateSet;

		public KafkaSet(CollectionConfig config, CollectionSerde<KK, K> keySarde) {
				this(Collections.newSetFromMap(new ConcurrentHashMap<>()), config, keySarde);
		}

		public KafkaSet(Set<K> delegateSet, CollectionConfig collectionConfig, CollectionSerde<KK, K> keySarde) {
				this(delegateSet, collectionConfig, keySarde, CollectionSerde.stringToString());
		}

		public KafkaSet(Set<K> delegateSet, CollectionConfig collectionConfig, CollectionSerde<KK, K> keySarde, CollectionSerde<String, String> valueSarde) {
				super(collectionConfig,keySarde,valueSarde);

				this.delegateSet = delegateSet;
				super.start();
		}

		@Override
		protected boolean addLocal(K key) {
				return this.delegateSet.add(key);
		}
		@Override
		protected boolean removeLocal(K key) {
				return this.delegateSet.remove(key);
		}
		@Override
		protected boolean containsLocal(K key) {
				return false;
		}

		@Override
		public int size() {
				return delegateSet.size();
		}
		@Override
		public boolean isEmpty() {
				return delegateSet.isEmpty();
		}
		@Override
		public boolean contains(Object o) {
				return delegateSet.contains(o);
		}
		@Override
		public Iterator<K> iterator() {
				return delegateSet.iterator();
		}
		@Override
		public Object[] toArray() {
				return delegateSet.toArray();
		}
		@Override
		public <T> T[] toArray(T[] a) {
				return delegateSet.toArray(a);
		}
		@Override
		public boolean add(K k) {
				KafkaCollectionException exp = this.getException();
				if (exp != null) {
						throw exp;
				}
				if (k != null) {
						return this.collectionAdd(k);
				}
				return false;
		}
		@Override
		public boolean remove(Object o) {
				KafkaCollectionException exp = this.getException();
				if (exp != null) {
						throw exp;
				}
				try {
						return this.collectionRemove((K) o);
				} catch (ClassCastException e) {
						return false;
				}
		}
		@Override
		public boolean containsAll(Collection<?> c) {
				return delegateSet.containsAll(c);
		}
		@Override
		public boolean addAll(Collection<? extends K> c) {
				if (this.getException() != null) {
						throw this.getException();
				}

				boolean changed = false;
				for (K key : c) {
						if (this.collectionAdd(key)) {
								changed = true;
						}
				}
				return changed;
		}
		@Override
		public boolean retainAll(Collection<?> c) {
				if (this.getException() != null) {
						throw this.getException();
				}

				boolean changed = false;
				Iterator<K> it = this.delegateSet.iterator();
				while (it.hasNext()) {
						K next = it.next();
						if (!c.contains(next)) {
								this.collectionRemove(next);
								changed = true;
						}
				}
				return changed;
		}
		@Override
		public boolean removeAll(Collection<?> c) {
				if (this.getException() != null) {
						throw this.getException();
				}

				boolean changed = false;
				Iterator<?> it = c.iterator();
				while (it.hasNext()) {
						try {
								if (this.collectionRemove((K) it.next())) {
										changed = true;
								}
						} catch (ClassCastException e) {
						}
				}
				return changed;
		}
		@Override
		public void clear() {
				if (this.getException() != null) {
						throw this.getException();
				}

				Iterator<K> it = this.delegateSet.iterator();
				while (it.hasNext()) {
						this.collectionRemove(it.next());
				}
		}
		@Override
		public Spliterator<K> spliterator() {
				return delegateSet.spliterator();
		}

		@Override
		public boolean equals(Object o) {
				if (o != null) {
						if (o instanceof KafkaSet) {
								KafkaSet<?, ?> other = (KafkaSet<?, ?>) o;
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
