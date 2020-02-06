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

import java.time.Duration;
import java.util.Arrays;
import java.util.HashMap;
import java.util.Map;
import java.util.Objects;

import org.apache.kafka.clients.CommonClientConfigs;
import org.apache.kafka.clients.consumer.ConsumerConfig;
import org.apache.kafka.common.config.TopicConfig;
import org.apache.kafka.common.serialization.Deserializer;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.becomingmachinic.kafka.collections.config.BooleanConfigKey;
import com.becomingmachinic.kafka.collections.config.ConfigDef;
import com.becomingmachinic.kafka.collections.config.IntegerRangeConfigKey;
import com.becomingmachinic.kafka.collections.config.LongRangeConfigKey;
import com.becomingmachinic.kafka.collections.config.NameConfigKey;
import com.becomingmachinic.kafka.collections.config.StringEnumerationConfigKey;
import com.becomingmachinic.kafka.collections.config.StringListConfigKey;
import com.becomingmachinic.kafka.collections.config.TopicConfigKey;

public class CollectionConfig {
	private static final Logger logger = LoggerFactory.getLogger(CollectionConfig.class);
	
	/**
	 * Kafka collection name.
	 */
	public static String COLLECTION_NAME = "collection.name";
	/**
	 * Kafka topic name used to back the collection.
	 */
	public static String COLLECTION_TOPIC = "collection.topic";
	/**
	 * Maximum amount of time that the Kafka Consumer will wait for a response before re-requesting records from the Kafka Broker.
	 */
	public static String COLLECTION_MAX_POLL_INTERVAL_MS = "collection.max.poll.interval.ms";
	/**
	 * Maximum amount of time that the Kafka Consumer will wait for a response from the Kafka Broker before considering the warmup complete.
	 */
	public static String COLLECTION_WARMUP_POLL_INTERVAL_MS = "collection.warmup.poll.interval.ms";
	/**
	 * Maximum amount of time that the collection will wait for a message to be sent when in synchronous mode.
	 */
	public static String COLLECTION_SEND_TIMEOUT_MS = "collection.send.timeout.ms";
	/**
	 * Run Collection in read only mode. It will only receive updates from the kafka topic and not produce events.
	 */
	public static String COLLECTION_READONLY = "collection.readonly";
	/**
	 * Toggle collection between synchronous and asynchronous mode.
	 * {@value CollectionConfig#COLLECTION_SEND_MODE_ASYNCHRONOUS}: Sending data to the kafka topic will be queued and return immediately.
	 * {@value CollectionConfig#COLLECTION_SEND_MODE_SYNCHRONOUS}: Sending data to the kafka topic will not return until the message is read back from the broker or the timeout is reached.
	 */
	public static String COLLECTION_SEND_MODE = "collection.send.mode";
	/**
	 * When enabled the collection instance will read its own writes from the kafka topic.
	 */
	public static String COLLECTION_READ_OWN_WRITES = "collection.read.own.writes";
	/**
	 * Create the Kafka topic if it does not yet exist.
	 */
	public static String COLLECTION_CREATE_TOPIC = "collection.create.topic";
	/**
	 * Sets the cleanup.policy on the collection topic if it is created by the KafkaCollection
	 */
	public static String COLLECTION_CLEANUP_POLICY = "collection.cleanup.policy";
	/**
	 * Sets the retention.ms on the collection topic if it is created by the KafkaCollection
	 */
	public static String COLLECTION_RETENTION_MS = "collection.retention.ms";
	/**
	 * Sets the delete.retention.ms on the collection topic if it is created by the KafkaCollection
	 */
	public static String COLLECTION_DELETE_RETENTION_MS = "collection.delete.retention.ms";
	/**
	 * Sets the max.message.bytes on the collection topic if it is created by the KafkaCollection
	 */
	public static String COLLECTION_MAX_MESSAGE_BYTES = "collection.max.message.bytes";
	/**
	 * Whether to should check the connection to Kafka Broker before creating collection.
	 */
	public static String COLLECTION_SKIP_CONNECTIVITY_CHECK = "collection.skip.connectivity.check";
	/**
	 * Seek to beginning or end of the topic before starting the collection consumer.
	 */
	public static String COLLECTION_RESET_OFFSET = "collection.reset.offset";
	/**
	 * Sets the replication.factor on the collection topic if it is created by the KafkaCollection
	 */
	public static String COLLECTION_REPLICATION_FACTOR = "collection.replication.factor";
	/**
	 * Sets the partitions on the collection topic if it is created by the KafkaCollection
	 */
	public static String COLLECTION_PARTITIONS = "collection.partitions";
	
	public static Long COLLECTION_MAX_POLL_INTERVAL_MS_DEFAULT_VALUE = 60000l;
	public static Long COLLECTION_WARMUP_POLL_INTERVAL_MS_DEFAULT_VALUE = 5000l;
	public static Long COLLECTION_SEND_TIMEOUT_MS_DEFAULT_VALUE = 30000l;
	public static Long COLLECTION_RETENTION_MS_DEFAULT_VALUE = 7 * 24 * 3600 * 1000l;
	public static Long COLLECTION_DELETE_RETENTION_MS_DEFAULT_VALUE = 1 * 24 * 3600 * 1000l;
	public static Boolean COLLECTION_READONLY_DEFAULT_VALUE = false;
	public static String COLLECTION_SEND_MODE_SYNCHRONOUS = "synchronous";
	public static String COLLECTION_SEND_MODE_ASYNCHRONOUS = "asynchronous";
	public static String COLLECTION_RESET_OFFSET_BEGINNING = "beginning";
	public static String COLLECTION_RESET_OFFSET_END = "end";
	public static String COLLECTION_RECORD_HEADER_NAME = "id";
	
	protected static final ConfigDef CONFIG;
	
	static {
		CONFIG = new ConfigDef(
				new StringListConfigKey(CommonClientConfigs.BOOTSTRAP_SERVERS_CONFIG,
						null),
				new NameConfigKey(),
				new TopicConfigKey(),
				new LongRangeConfigKey(COLLECTION_MAX_POLL_INTERVAL_MS,
						COLLECTION_MAX_POLL_INTERVAL_MS_DEFAULT_VALUE,
						1l,
						null),
				new LongRangeConfigKey(COLLECTION_WARMUP_POLL_INTERVAL_MS,
						COLLECTION_WARMUP_POLL_INTERVAL_MS_DEFAULT_VALUE,
						1l,
						null),
				new LongRangeConfigKey(COLLECTION_SEND_TIMEOUT_MS,
						COLLECTION_SEND_TIMEOUT_MS_DEFAULT_VALUE,
						1l,
						null),
				new BooleanConfigKey(COLLECTION_READONLY,
						COLLECTION_READONLY_DEFAULT_VALUE),
				new StringEnumerationConfigKey(COLLECTION_SEND_MODE,
						COLLECTION_SEND_MODE_ASYNCHRONOUS,
						Arrays.asList(COLLECTION_SEND_MODE_ASYNCHRONOUS, COLLECTION_SEND_MODE_SYNCHRONOUS)),
				new BooleanConfigKey(COLLECTION_READ_OWN_WRITES,
						Boolean.FALSE),
				new BooleanConfigKey(COLLECTION_SKIP_CONNECTIVITY_CHECK,
						Boolean.FALSE),
				new StringEnumerationConfigKey(COLLECTION_RESET_OFFSET,
						"none",
						Arrays.asList("none", COLLECTION_RESET_OFFSET_BEGINNING, COLLECTION_RESET_OFFSET_END)),
				new BooleanConfigKey(COLLECTION_CREATE_TOPIC,
						true),
				new IntegerRangeConfigKey(COLLECTION_REPLICATION_FACTOR,
						1,
						1,
						null),
				new IntegerRangeConfigKey(COLLECTION_PARTITIONS,
						1,
						1,
						Short.MAX_VALUE - 1),
				new StringEnumerationConfigKey(COLLECTION_CLEANUP_POLICY,
						TopicConfig.CLEANUP_POLICY_COMPACT,
						Arrays.asList(TopicConfig.CLEANUP_POLICY_COMPACT, TopicConfig.CLEANUP_POLICY_DELETE)),
				new LongRangeConfigKey(COLLECTION_RETENTION_MS,
						COLLECTION_RETENTION_MS_DEFAULT_VALUE,
						-1l,
						null),
				new IntegerRangeConfigKey(COLLECTION_MAX_MESSAGE_BYTES,
						IntegerRangeConfigKey.NO_DEFAULT,
						1,
						null),
				new LongRangeConfigKey(COLLECTION_DELETE_RETENTION_MS,
						COLLECTION_DELETE_RETENTION_MS_DEFAULT_VALUE,
						1l,
						null));
	}
	
	protected Map<String, Object> collectionConfigMap;
	protected Map<String, Object> configurationMap;
	
	public CollectionConfig(Map<String, Object> configurationMap) throws KafkaCollectionConfigurationException {
		// Override the default values of ConsumerConfig
		configurationMap.putIfAbsent(ConsumerConfig.ENABLE_AUTO_COMMIT_CONFIG, false);
		configurationMap.putIfAbsent(ConsumerConfig.AUTO_OFFSET_RESET_CONFIG, "earliest");
		
		this.configurationMap = new HashMap<>(configurationMap);
		this.collectionConfigMap = CONFIG.getConfigMap(configurationMap);
	}
	
	/**
	 * Kafka collection name
	 *
	 * @return
	 */
	public String getName() {
		return (String) this.collectionConfigMap.get(COLLECTION_NAME);
	}
	/**
	 * Kafka topic name used to back the collection.
	 *
	 * @return
	 */
	public String getTopic() {
		return (String) this.collectionConfigMap.get(COLLECTION_TOPIC);
	}
	
	/**
	 * Maximum amount of time that the Kafka Consumer will wait for a response before re-requesting records from the Kafka Broker.
	 *
	 * @return
	 */
	public Duration getMaxPollIntervalDuration() {
		return Duration.ofMillis((Long) this.collectionConfigMap.get(COLLECTION_MAX_POLL_INTERVAL_MS));
	}
	
	/**
	 * Maximum amount of time that the Kafka Consumer will wait for a response from the Kafka Broker before considering the warmup complete.
	 *
	 * @return
	 */
	public Duration getWarmupPollIntervalDuration() {
		return Duration.ofMillis((Long) this.collectionConfigMap.get(COLLECTION_WARMUP_POLL_INTERVAL_MS));
	}
	
	/**
	 * Maximum amount of time that the collection will wait for a message to be sent when in synchronous mode.
	 *
	 * @return
	 */
	public long getSendTimeoutMs() {
		return (Long) this.collectionConfigMap.get(COLLECTION_SEND_TIMEOUT_MS);
	}
	
	/**
	 * Return true if a collection is read only. It will only receive updates from the kafka topic.
	 *
	 * @return
	 */
	public boolean isReadOnly() {
		return (boolean) this.collectionConfigMap.get(COLLECTION_READONLY);
	}
	/**
	 * Toggle collection between synchronous and asynchronous mode.
	 * {@value CollectionConfig#COLLECTION_SEND_MODE_ASYNCHRONOUS}: Sending data to the kafka topic will be queued and return immediately.
	 * {@value CollectionConfig#COLLECTION_SEND_MODE_SYNCHRONOUS}: Sending data to the kafka topic will not return until the message is read back from the broker or the timeout is reached.
	 *
	 * @return
	 */
	public String getSendMode() {
		return (String) this.collectionConfigMap.get(COLLECTION_SEND_MODE);
	}
	/**
	 * When enabled the collection should read its own writes from the Kafka topic.
	 *
	 * @return
	 */
	public boolean isReadOwnWrites() {
		return (boolean) this.collectionConfigMap.get(COLLECTION_READ_OWN_WRITES);
	}
	
	/**
	 * If true then the kafka connectivity check will be skipped before creating the collection.
	 *
	 * @return
	 */
	public boolean isSkipConnectivityCheck() {
		return (boolean) this.collectionConfigMap.get(COLLECTION_SKIP_CONNECTIVITY_CHECK);
	}
	/**
	 * Seek to beginning or end of the topic before starting the collection consumer.
	 *
	 * @return
	 */
	public String isResetOffset() {
		return (String) this.collectionConfigMap.get(COLLECTION_RESET_OFFSET);
	}
	/**
	 * Create the Kafka topic if it does not yet exist.
	 */
	public boolean getCreateTopic() {
		return (boolean) this.collectionConfigMap.get(COLLECTION_CREATE_TOPIC);
	}
	
	public int getReplicationFactor() {
		return (int) this.collectionConfigMap.get(COLLECTION_REPLICATION_FACTOR);
	}
	
	public int getPartitions() {
		return (int) this.collectionConfigMap.get(COLLECTION_PARTITIONS);
	}
	
	public String getCleanupPolicy() {
		return (String) this.collectionConfigMap.get(COLLECTION_CLEANUP_POLICY);
	}
	
	public Long getRetentionMs() {
		return (Long) this.collectionConfigMap.get(COLLECTION_RETENTION_MS);
	}
	
	public Long getDeleteRetentionMs() {
		return (Long) this.collectionConfigMap.get(COLLECTION_DELETE_RETENTION_MS);
	}
	
	public Integer getMaxMessageBytes() {
		return (Integer) this.collectionConfigMap.get(COLLECTION_MAX_MESSAGE_BYTES);
	}
	
	/**
	 * Remove local properties from the propertiesMap
	 *
	 * @return
	 */
	protected Map<String, Object> getKafkaProperties() {
		Map<String, Object> propertiesMap = new HashMap<>(this.configurationMap);
		
		// Remove collection specific keys
		for (String key : this.configurationMap.keySet()) {
			if (key.startsWith("collection.")) {
				propertiesMap.remove(key);
			}
		}
		return propertiesMap;
	}
	
	/**
	 * Get kafka consumer properties map
	 *
	 * @param keyDeserializer
	 * @param valueDeserializer
	 * @return
	 */
	public Map<String, Object> getConsumerConfig(Deserializer<?> keyDeserializer, Deserializer<?> valueDeserializer) {
		return ConsumerConfig.addDeserializerToConfig(this.getKafkaProperties(), keyDeserializer, valueDeserializer);
	}
	
	/**
	 * Get Kafka Producer properties map
	 *
	 * @return
	 */
	public Map<String, Object> getProducerConfig() {
		Map<String, Object> producerMap = getKafkaProperties();
		producerMap.remove(ConsumerConfig.ENABLE_AUTO_COMMIT_CONFIG);
		producerMap.remove(ConsumerConfig.MAX_POLL_RECORDS_CONFIG);
		producerMap.remove(ConsumerConfig.AUTO_OFFSET_RESET_CONFIG);
		producerMap.remove(ConsumerConfig.GROUP_ID_CONFIG);
		return producerMap;
	}
	
	public Map<String, Object> getAdminClientConfig() {
		return this.getProducerConfig();
	}
	
	public Map<String, Object> getConfig() {
		return new HashMap<>(this.configurationMap);
	}
	
	public Map<String, Object> getSourceConfig() {
		return new HashMap<>(this.collectionConfigMap);
	}
	
	@Override
	public boolean equals(Object o) {
		if (o != null && (o instanceof CollectionConfig)) {
			CollectionConfig other = (CollectionConfig) o;
			if (this.configurationMap.equals(other.configurationMap)) {
				return true;
			}
		}
		return false;
	}
	
	@Override
	public int hashCode() {
		return Objects.hash(this.configurationMap);
	}
	
	public void logConfig() {
		StringBuilder sb = new StringBuilder("CollectionConfig values:\n");
		for (Map.Entry<String, Object> entry : this.collectionConfigMap.entrySet()) {
			sb.append("\t").append(entry.getKey()).append(" = ").append(entry.getValue()).append("\n");
		}
		logger.info(sb.toString());
	}
}
