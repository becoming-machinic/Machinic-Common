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
	
	private final KafkaStreamRebalanceListener rebalanceListener;
	private final KafkaStreamConsumerWorker kafkaStreamConsumerWorker;
	
	public KafkaStream(final StreamConfig streamConfig, final StreamFlow<K, V> streamFlow, Map<String, Object> consumerProperties, Deserializer<K> keyDeserializer,
			Deserializer<V> valueDeserializer) {
		super(streamConfig, streamFlow);
		
		this.rebalanceListener = new KafkaStreamRebalanceListener();
		this.kafkaStreamConsumerWorker = new KafkaStreamConsumerWorker(keyDeserializer, valueDeserializer);
	}
	
	class KafkaStreamConsumerWorker implements Runnable, AutoCloseable {
		
		private final Duration maxPollInterval;
		private final long commitInterval;
		
		private final KafkaConsumer<K, V> consumer;
		private final CountDownLatch closeLatch = new CountDownLatch(1);
		
		private volatile boolean stop = false;
		private long lastCommit = 0;
		
		public KafkaStreamConsumerWorker(final Deserializer<K> keyDeserializer, final Deserializer<V> valueDeserializer) {
			this.maxPollInterval = Duration.ofMillis(streamConfig.getSourceConsumerMaxPollInterval());
			this.commitInterval = streamConfig.getCommitInterval();
			
			this.consumer = new KafkaConsumer<>(streamConfig.getConsumerConfig(), keyDeserializer, valueDeserializer);
		}
		
		@Override
		public void run() {
			if (streamConfig.getSourceTopics() != null && !streamConfig.getSourceTopics().isEmpty()) {
				this.consumer.subscribe(streamConfig.getSourceTopics(), rebalanceListener);
			} else if (streamConfig.getSourceTopicPattern() != null) {
				this.consumer.subscribe(streamConfig.getSourceTopicPattern(), rebalanceListener);
			} else {
				// TODO different exception
				throw new IllegalArgumentException("No source topic set");
			}
			
		}
		
		private void standardCommitStrategy() throws InterruptedException {
			while (!stop) {
				this.commit();
				ConsumerRecords<K, V> consumerRecords = this.consumer.poll(this.maxPollInterval);
				for (ConsumerRecord<K, V> consumerRecord : consumerRecords) {
					while (!submit(new KafkaStreamRecord<>(consumerRecord), rebalanceListener, timeRemaining(),TimeUnit.MILLISECONDS)) {
						if (System.currentTimeMillis() - lastCommit >= commitInterval) {
							this.commit();
						}
					}
				}
				
			}
		}
		
		private long timeRemaining() {
			return Math.max(0, (lastCommit + commitInterval) - System.currentTimeMillis());
		}
		
		private void commit() {
			this.lastCommit = System.currentTimeMillis();
			rebalanceListener.commitOffsets(this.consumer);
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
	
	public class KafkaStreamRebalanceListener implements ConsumerRebalanceListener, Callback<KafkaStreamRecord<K, V>> {
		
		private final ConcurrentMap<PartitionId, Long> commitMap = new ConcurrentHashMap<>();
		
		@Override
		public void onPartitionsRevoked(Collection<TopicPartition> partitions) {
			List<PartitionId> partitionIdList = new ArrayList<>();
			for (TopicPartition topicPartition : partitions) {
				partitionIdList.add(new PartitionId(topicPartition.topic(), topicPartition.partition()));
			}
			assignPartitions(partitionIdList);
		}
		
		@Override
		public void onPartitionsAssigned(Collection<TopicPartition> partitions) {
			List<PartitionId> partitionIdList = new ArrayList<>();
			for (TopicPartition topicPartition : partitions) {
				partitionIdList.add(new PartitionId(topicPartition.topic(), topicPartition.partition()));
			}
			revokePartitions(partitionIdList);
		}
		
		boolean commitOffsets(KafkaConsumer<?, ?> kafkaConsumer) {
			final Map<TopicPartition, OffsetAndMetadata> offsets = new HashMap<>();
			// Create a Iterator to EntrySet of HashMap
			Iterator<PartitionId> it = commitMap.keySet().iterator();
			while (it.hasNext()) {
				PartitionId partitionId = it.next();
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
		public void onBefore(KafkaStreamRecord<K, V> streamEvent) {
		
		}
		@Override
		public void onCompletion(KafkaStreamRecord<K, V> streamEvent, Exception exception) {
			this.commitMap.compute(streamEvent.getPartitionId(), (k, v) -> {
				if (v != null && streamEvent.getOffset() > v) {
					return streamEvent.getOffset() + 1;
				}
				return v;
			});
		}
		@Override
		public void onCancel(KafkaStreamRecord<K, V> streamEvent) {
		
		}
	}
}
