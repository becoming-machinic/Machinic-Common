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

import java.util.concurrent.CountDownLatch;
import java.util.concurrent.TimeUnit;

public class CountDownLatchKafkaCollectionEventListener<K,V> implements KafkaCollectionEventListener<K,V> {

		private final CountDownLatch latch;
		private final boolean releaseOnShutdown;

		public CountDownLatchKafkaCollectionEventListener(int eventCount) {
				this(eventCount,true);
		}
		public CountDownLatchKafkaCollectionEventListener(int eventCount,boolean releaseOnShutdown) {
				this.latch = new CountDownLatch(eventCount);
				this.releaseOnShutdown = releaseOnShutdown;
		}

		@Override
		public void onWarmupComplete(long warmupDuration) {

		}
		@Override
		public void onEvent(AbstractKafkaCollection<K, V> kafkaCollection, CollectionConsumerRecord<K, V> collectionRecord) {
				this.latch.countDown();
				if (this.latch.getCount() <= 0) {
						kafkaCollection.removeKafkaCollectionEventListener(this);
				}
		}
		@Override
		public void onException(KafkaCollectionException exception) {

		}
		@Override
		public void onShutdown() {
			if(this.releaseOnShutdown){
					while(latch.getCount() > 0){
							latch.countDown();
					}
			}
		}

		public void await() throws InterruptedException {
				latch.await();
		}

		public boolean await(long timeout, TimeUnit unit) throws InterruptedException {
				return latch.await(timeout, unit);
		}

		public long getCount() {
				return latch.getCount();
		}
}
