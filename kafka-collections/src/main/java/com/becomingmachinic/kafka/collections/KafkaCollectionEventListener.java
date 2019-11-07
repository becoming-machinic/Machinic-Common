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

public interface KafkaCollectionEventListener<K,V> {

		public void onWarmupComplete(long warmupDuration);

		public void onEvent(AbstractKafkaCollection<K, V> kafkaCollection, K key, V value);

		public void onException(KafkaCollectionException exception);

		public void onShutdown();

}
