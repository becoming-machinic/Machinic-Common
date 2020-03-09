package com.becomingmachinic.kafka.streams;

import java.time.Duration;
import java.util.ArrayList;
import java.util.Collection;
import java.util.HashMap;
import java.util.Iterator;
import java.util.List;
import java.util.Map;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.ConcurrentMap;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicInteger;

import org.apache.kafka.clients.consumer.ConsumerRebalanceListener;
import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.apache.kafka.clients.consumer.ConsumerRecords;
import org.apache.kafka.clients.consumer.KafkaConsumer;
import org.apache.kafka.clients.consumer.OffsetAndMetadata;
import org.apache.kafka.common.TopicPartition;
import org.apache.kafka.common.serialization.Deserializer;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.becomingmachinic.kafka.streams.executor.PartitionId;

public class KafkaStream<K, V> extends AbstractStream<K, V> {
	private static final Logger logger = LoggerFactory.getLogger(KafkaStream.class);
	
	private final KafkaStreamCommitMessageCallback commitMessageCallback;
	private final KafkaStreamConsumerWorker kafkaStreamConsumerWorker;
	
	public KafkaStream(final StreamConfig streamConfig, final StreamFlow<K, V> streamFlow, Map<String, Object> consumerProperties, Deserializer<K> keyDeserializer,
			Deserializer<V> valueDeserializer) {
		super(streamConfig, streamFlow);
		
		this.commitMessageCallback = new KafkaStreamCommitMessageCallback();
		this.kafkaStreamConsumerWorker = new KafkaStreamConsumerWorker(keyDeserializer, valueDeserializer);
	}
	
	class KafkaStreamConsumerWorker implements Runnable, AutoCloseable {
		
		private final long maxPollInterval;
		private final long commitInterval;
		
		private final KafkaConsumer<K, V> consumer;
		private final CountDownLatch closeLatch = new CountDownLatch(1);
		
		private volatile boolean stop = false;
		private long lastCommit = 0;
		
		public KafkaStreamConsumerWorker(final Deserializer<K> keyDeserializer, final Deserializer<V> valueDeserializer) {
			this.maxPollInterval = streamConfig.getSourceConsumerMaxPollInterval();
			this.commitInterval = streamConfig.getCommitInterval();
			
			this.consumer = new KafkaConsumer<>(streamConfig.getConsumerConfig(), keyDeserializer, valueDeserializer);
		}
		
		@Override
		public void run() {
			if (streamConfig.getSourceTopics() != null && !streamConfig.getSourceTopics().isEmpty()) {
				this.consumer.subscribe(streamConfig.getSourceTopics(), commitMessageCallback);
			} else if (streamConfig.getSourceTopicPattern() != null) {
				this.consumer.subscribe(streamConfig.getSourceTopicPattern(), commitMessageCallback);
			} else {
				// TODO different exception
				throw new IllegalArgumentException("No source topic set");
			}
			
		}
		
		private void standardCommitStrategy() throws InterruptedException {
			// Start polling at the minimum interval
			long pollInterval = Math.min(this.maxPollInterval,this.commitInterval);
			while (!stop) {
				this.commit();
				ConsumerRecords<K, V> consumerRecords = this.consumer.poll(Duration.ofMillis(pollInterval));
				if (!consumerRecords.isEmpty()) {
					for (ConsumerRecord<K, V> consumerRecord : consumerRecords) {
						while (!submit(new KafkaStreamEvent<>(consumerRecord), commitMessageCallback, timeRemaining(), TimeUnit.MILLISECONDS)) {
							this.commit();
						}
					}
					pollInterval = Math.min(this.maxPollInterval,this.commitInterval);
				} else {
					// if no records are found and we don't have any commits waiting start backoff.
					if(commitMessageCallback.getWaitingRecords() == 0) {
						pollInterval = Math.min(pollInterval + pollInterval/2, this.maxPollInterval);
					}
				}
			}
		}
		
		private long timeRemaining() {
			return Math.max(0, (lastCommit + commitInterval) - System.currentTimeMillis());
		}
		
		private void commit() {
			this.lastCommit = System.nanoTime();
			commitMessageCallback.commitOffsets(this.consumer);
		}
		
		@Override
		public void close() {
			logger.debug("KafkaStreamConsumerWorker {} shutdown requested", getName());
			this.stop = true;
			this.consumer.wakeup();
			try {
				this.closeLatch.await(30l, TimeUnit.SECONDS);
			} catch (InterruptedException e) {
			}
			logger.debug("KafkaStreamConsumerWorker {} shutdown", getName());
		}
	}
	
	public class KafkaStreamCommitMessageCallback implements ConsumerRebalanceListener, Callback<K, V> {
		
		private final ConcurrentMap<KafkaPartitionId, Long> commitMap = new ConcurrentHashMap<>();
		private final AtomicInteger waitingRecords = new AtomicInteger(0);
		
		@Override
		public void onPartitionsRevoked(Collection<TopicPartition> partitions) {
			List<PartitionId> partitionIdList = new ArrayList<>();
			for (TopicPartition topicPartition : partitions) {
				partitionIdList.add(new KafkaPartitionId(topicPartition.topic(), topicPartition.partition()));
			}
			assignPartitions(partitionIdList);
		}
		
		@Override
		public void onPartitionsAssigned(Collection<TopicPartition> partitions) {
			List<PartitionId> partitionIdList = new ArrayList<>();
			for (TopicPartition topicPartition : partitions) {
				partitionIdList.add(new KafkaPartitionId(topicPartition.topic(), topicPartition.partition()));
			}
			revokePartitions(partitionIdList);
		}
		
		/**
		 * This method should only ever be called by the consumer worker thread.
		 *
		 * @param kafkaConsumer
		 * @return
		 */
		boolean commitOffsets(KafkaConsumer<K, V> kafkaConsumer) {
			final Map<TopicPartition, OffsetAndMetadata> offsets = new HashMap<>();
			
			this.waitingRecords.set(0);
			// Create a Iterator to EntrySet of HashMap
			Iterator<KafkaPartitionId> it = commitMap.keySet().iterator();
			while (it.hasNext()) {
				KafkaPartitionId partitionId = it.next();
				commitMap.computeIfPresent(partitionId, (k, v) -> {
					if (k != null) {
						offsets.put(new TopicPartition(partitionId.getTopic(), partitionId.getPartition()), new OffsetAndMetadata(v, ""));
					}
					return null;
				});
			}
			if (!offsets.isEmpty()) {
				kafkaConsumer.commitSync(offsets);
				return true;
			}
			return false;
		}
		
		@Override
		public void onBefore(StreamEvent<K, V> streamEvent) {
		
		}
		@Override
		public void onCompletion(StreamEvent<K, V> streamEvent, Exception exception) {
			if (exception != null) {
				KafkaStreamEvent<K, V> kafkaStreamEvent = (KafkaStreamEvent) streamEvent;
				this.commitMap.compute(kafkaStreamEvent.getPartitionId(), (k, v) -> {
					if (v != null && kafkaStreamEvent.getOffset() > v) {
						this.waitingRecords.incrementAndGet();
						return kafkaStreamEvent.getOffset() + 1;
					}
					return v;
				});
			}
		}
		@Override
		public void onCancel(StreamEvent streamEvent) {
		
		}
		
		/**
		 * Get an estimate of the number of records waiting to be committed.
		 * @return
		 */
		public int getWaitingRecords(){
			return this.waitingRecords.get();
		}
	}
}
