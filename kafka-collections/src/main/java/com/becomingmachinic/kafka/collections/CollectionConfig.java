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

import org.apache.kafka.clients.consumer.ConsumerConfig;
import org.apache.kafka.common.config.TopicConfig;
import org.apache.kafka.common.serialization.Deserializer;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.time.Duration;
import java.util.HashMap;
import java.util.Map;
import java.util.Objects;

public class CollectionConfig {
		private static final Logger logger = LoggerFactory.getLogger(CollectionConfig.class);

		public static String COLLECTION_NAME = "collection.name";
		public static String COLLECTION_TOPIC = "collection.topic";
		public static String COLLECTION_MAX_POLL_INTERVAL_MS = "collection.max.poll.interval.ms";
		public static String COLLECTION_WARMUP_POLL_INTERVAL_MS = "collection.warmup.poll.interval.ms";
		public static String COLLECTION_SEND_TIMEOUT_MS = "collection.send.timeout.ms";
		public static String COLLECTION_READONLY = "collection.readonly";
		public static String COLLECTION_SEND_MODE = "collection.send.mode";
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
		 * When strong local consistency is required it is recommended to use {@value CollectionConfig#COLLECTION_WRITE_MODE} {@value CollectionConfig#COLLECTION_WRITE_MODE_BEHIND}.
		 * When strong topic consistency is more important then local consistency it is recommended to use {@value CollectionConfig#COLLECTION_WRITE_MODE} {@value CollectionConfig#COLLECTION_WRITE_MODE_AHEAD}.
		 */
		public static String COLLECTION_WRITE_MODE = "collection.write.mode";
		public static String COLLECTION_SKIP_CONNECTIVITY_CHECK = "collection.skip.connectivity.check";
		public static String COLLECTION_RESET_OFFSET = "collection.reset.offset";
		public static String COLLECTION_CREATE_TOPIC = "collection.create.topic";
		public static String COLLECTION_REPLICATION_FACTOR = "collection.replication.factor";
		public static String COLLECTION_PARTITIONS = "collection.partitions";

		public static Long COLLECTION_MAX_POLL_INTERVAL_MS_DEFAULT_VALUE = 60000l;
		public static Long COLLECTION_WARMUP_POLL_INTERVAL_MS_DEFAULT_VALUE = 5000l;
		public static Long COLLECTION_SEND_TIMEOUT_MS_DEFAULT_VALUE = 30000l;
		public static Boolean COLLECTION_READONLY_DEFAULT_VALUE = false;
		/**
		 * Sending data to the kafka topic will not return until the message is read back from the broker or the timeout is reached.
		 */
		public static String COLLECTION_SEND_MODE_SYNCHRONOUS = "synchronous";
		/**
		 * Sending data to the kafka topic will be queued and return immediately.
		 */
		public static String COLLECTION_SEND_MODE_ASYNCHRONOUS = "asynchronous";
		/**
		 * Sends data to Kafka topic. The collection consumer will receive the update from the topic and update the local collection.
		 * Values will be sent to the Kafka topic even if they are already present in the local instance.
		 */
		public static String COLLECTION_WRITE_MODE_AHEAD = "ahead";
		/**
		 * Updates local collection first, then sends update to kafka.
		 */
		public static String COLLECTION_WRITE_MODE_BEHIND = "behind";
		/**
		 * Move the consumer offsets to the beginning before consuming.
		 */
		public static String COLLECTION_RESET_OFFSET_BEGINNING = "beginning";
		/**
		 * Move the consumer offset to the end before consuming.
		 */
		public static String COLLECTION_RESET_OFFSET_END = "end";

		public static String COLLECTION_RECORD_HEADER_NAME = "id";

		protected Map<String, Object> configurationMap;

		public CollectionConfig(Map<String, Object> configurationMap) throws KafkaCollectionConfigurationException {
				this.configurationMap = new HashMap<>(configurationMap);

				if (configurationMap.get(COLLECTION_NAME) == null || !(configurationMap.get(COLLECTION_NAME) instanceof String)
						|| ((String) configurationMap.get(COLLECTION_NAME)).isEmpty()) {
						throw new KafkaCollectionConfigurationException("Parameter %s is required",
								COLLECTION_NAME);
				}
				if (!((String) configurationMap.get(COLLECTION_NAME)).replaceAll("[^0-9A-Za-z_-]+", "").equals((String) configurationMap.get(COLLECTION_NAME))) {
						throw new KafkaCollectionConfigurationException("Parameter %s contains invalid characters. It should contain alphanumeric, hyphen and underscore only", COLLECTION_NAME);
				}

				if (configurationMap.get(COLLECTION_TOPIC) != null && !(configurationMap.get(COLLECTION_TOPIC) instanceof String)) {
						throw new KafkaCollectionConfigurationException("Parameter %s is not valid", COLLECTION_TOPIC);
				}
				if (configurationMap.get(COLLECTION_TOPIC) != null && !((String) configurationMap.get(COLLECTION_TOPIC)).replaceAll("[^0-9A-Za-z_-]", "").equals((String) configurationMap.get(COLLECTION_TOPIC))) {
						throw new KafkaCollectionConfigurationException("Parameter %s contains invalid characters. It should contain alphanumeric, hyphen and underscore only", COLLECTION_TOPIC);
				}

				if (this.getString(COLLECTION_TOPIC) == null ) {
						this.configurationMap.put(COLLECTION_TOPIC,this.getName().toLowerCase() + "_collection");
				}

				if(this.getLong(COLLECTION_MAX_POLL_INTERVAL_MS) == null || !(this.getLong(COLLECTION_MAX_POLL_INTERVAL_MS) > 1)){
						this.configurationMap.put(COLLECTION_MAX_POLL_INTERVAL_MS,COLLECTION_MAX_POLL_INTERVAL_MS_DEFAULT_VALUE);
				}

				if(this.getLong(COLLECTION_WARMUP_POLL_INTERVAL_MS) == null || !(this.getLong(COLLECTION_WARMUP_POLL_INTERVAL_MS) > 1)){
						this.configurationMap.put(COLLECTION_WARMUP_POLL_INTERVAL_MS,COLLECTION_WARMUP_POLL_INTERVAL_MS_DEFAULT_VALUE);
				}

				if(this.getLong(COLLECTION_SEND_TIMEOUT_MS) == null || !(this.getLong(COLLECTION_SEND_TIMEOUT_MS) > 1)){
						this.configurationMap.put(COLLECTION_SEND_TIMEOUT_MS,COLLECTION_SEND_TIMEOUT_MS_DEFAULT_VALUE);
				}

				if(this.getBoolean(COLLECTION_READONLY) == null){
						this.configurationMap.put(COLLECTION_READONLY,COLLECTION_READONLY_DEFAULT_VALUE);
				}

				if(this.getString(COLLECTION_SEND_MODE) == null || !(COLLECTION_SEND_MODE_SYNCHRONOUS.equals(this.getString(COLLECTION_SEND_MODE)) || COLLECTION_SEND_MODE_ASYNCHRONOUS.equals(this.getString(COLLECTION_SEND_MODE)))){
						this.configurationMap.put(COLLECTION_SEND_MODE,COLLECTION_SEND_MODE_ASYNCHRONOUS);
				}

				if(this.getString(COLLECTION_WRITE_MODE) == null || !(COLLECTION_WRITE_MODE_AHEAD.equals(this.getString(COLLECTION_WRITE_MODE)) || COLLECTION_WRITE_MODE_BEHIND.equals(this.getString(COLLECTION_WRITE_MODE)))){
						this.configurationMap.put(COLLECTION_WRITE_MODE,COLLECTION_WRITE_MODE_BEHIND);
				}

				if(this.getBoolean(COLLECTION_SKIP_CONNECTIVITY_CHECK) == null){
						this.configurationMap.put(COLLECTION_SKIP_CONNECTIVITY_CHECK,false);
				}

				if(this.getString(COLLECTION_RESET_OFFSET) == null || !(COLLECTION_RESET_OFFSET_BEGINNING.equals(this.getString(COLLECTION_RESET_OFFSET)) || COLLECTION_RESET_OFFSET_END.equals(this.getString(COLLECTION_RESET_OFFSET)))){
						this.configurationMap.put(COLLECTION_RESET_OFFSET,null);
				}

				if(this.getBoolean(COLLECTION_CREATE_TOPIC) == null){
						this.configurationMap.put(COLLECTION_CREATE_TOPIC,true);
				}

				if(this.getLong(COLLECTION_REPLICATION_FACTOR) == null){
						this.configurationMap.put(COLLECTION_REPLICATION_FACTOR,1);
				}

				if(this.getLong(COLLECTION_PARTITIONS) == null){
						this.configurationMap.put(COLLECTION_PARTITIONS,1);
				}

				if(!TopicConfig.CLEANUP_POLICY_COMPACT.equals(this.getString(COLLECTION_CLEANUP_POLICY)) && !TopicConfig.CLEANUP_POLICY_DELETE.equals(this.getString(COLLECTION_CLEANUP_POLICY))){
						this.configurationMap.put(COLLECTION_CLEANUP_POLICY,TopicConfig.CLEANUP_POLICY_COMPACT);
				}

				if (this.getBoolean(ConsumerConfig.ENABLE_AUTO_COMMIT_CONFIG) == null) {
						this.configurationMap.put(ConsumerConfig.ENABLE_AUTO_COMMIT_CONFIG, Boolean.FALSE);
				}
				if (configurationMap.get(ConsumerConfig.BOOTSTRAP_SERVERS_CONFIG) == null) {
						throw new KafkaCollectionConfigurationException("Parameter %s is missing", ConsumerConfig.BOOTSTRAP_SERVERS_CONFIG);
				}
				if (configurationMap.get(ConsumerConfig.AUTO_OFFSET_RESET_CONFIG) == null) {
						this.configurationMap.put(ConsumerConfig.AUTO_OFFSET_RESET_CONFIG, "earliest");
				}
		}

		public String getName() {
				return ((String) configurationMap.get(COLLECTION_NAME));
		}

		public String getTopic() {
				return this.getString(COLLECTION_TOPIC);
		}

		/**
		 * The maximum amount of time that the consumer will wait for poll to return records before giving up and calling poll again.
		 *
		 * @return
		 */
		public Duration getMaxPollIntervalDuration() {
				return Duration.ofMillis(this.getLong(COLLECTION_MAX_POLL_INTERVAL_MS));
		}

		/**
		 * The maximum amount of time that the consumer will wait for poll to return records when warming the collection
		 *
		 * @return
		 */
		public Duration getWarmupPollIntervalDuration() {
				return Duration.ofMillis(this.getLong(COLLECTION_WARMUP_POLL_INTERVAL_MS));
		}

		/**
		 * The maximum amount of time that the collection will wait for a message to be sent when in synchronous mode
		 *
		 * @return
		 */
		public long getSendTimeoutMs() {
				return this.getLong(COLLECTION_SEND_TIMEOUT_MS);
		}

		/**
		 * Return true if a collection is read only. It will only receive updates from the kafka topic.
		 *
		 * @return
		 */
		public boolean isReadOnly() {
				return this.getBoolean(COLLECTION_READONLY);
		}

		public String getSendMode(){
				return this.getString(COLLECTION_SEND_MODE);
		}

		public String getWriteMode(){
				return this.getString(COLLECTION_WRITE_MODE);
		}

		/**
		 * If true then the kafka connectivity check will be skipped before creating the collection.
		 * @return
		 */
		public boolean isSkipConnectivityCheck() {
				return this.getBoolean(COLLECTION_SKIP_CONNECTIVITY_CHECK);
		}

		/**
		 * The consumer will behave differently if running with a group id.
		 * @return
		 */
		public String getGroupId(){
				return this.getString(ConsumerConfig.GROUP_ID_CONFIG);
		}

		public CollectionConfig overrideAutoCommit(Boolean value){
				this.configurationMap.put(ConsumerConfig.ENABLE_AUTO_COMMIT_CONFIG,value);
				return this;
		}

		public String isResetOffset(){
				return this.getString(COLLECTION_RESET_OFFSET);
		}

		public boolean getCreateTopic(){
				Boolean value = this.getBoolean(COLLECTION_CREATE_TOPIC);
				return Boolean.TRUE == value;
		}

		public int getReplicationFactor(){
				return this.getLong(COLLECTION_REPLICATION_FACTOR).intValue();
		}

		public int getPartitions(){
				return this.getLong(COLLECTION_PARTITIONS).intValue();
		}

		public String getCleanupPolicy(){
				return this.getString(COLLECTION_CLEANUP_POLICY);
		}

		public Long getRetentionMs(){
				return this.getLong(COLLECTION_RETENTION_MS);
		}

		public Long getDeleteRetentionMs(){
				return this.getLong(COLLECTION_DELETE_RETENTION_MS);
		}

		public Long getMaxMessageBytes(){
				return this.getLong(COLLECTION_MAX_MESSAGE_BYTES);
		}

		/**
		 * Remove local properties from the propertiesMap
		 *
		 * @return
		 */
		protected Map<String, Object> getKafkaProperties() {
				Map<String, Object> propertiesMap = new HashMap<>(this.configurationMap);
				propertiesMap.remove(COLLECTION_NAME);
				propertiesMap.remove(COLLECTION_TOPIC);
				propertiesMap.remove(COLLECTION_MAX_POLL_INTERVAL_MS);
				propertiesMap.remove(COLLECTION_WARMUP_POLL_INTERVAL_MS);
				propertiesMap.remove(COLLECTION_SEND_TIMEOUT_MS);
				propertiesMap.remove(COLLECTION_READONLY);
				propertiesMap.remove(COLLECTION_WRITE_MODE);
				propertiesMap.remove(COLLECTION_SKIP_CONNECTIVITY_CHECK);
				propertiesMap.remove(COLLECTION_RESET_OFFSET);
				propertiesMap.remove(COLLECTION_SEND_MODE);
				propertiesMap.remove(COLLECTION_CREATE_TOPIC);
				propertiesMap.remove(COLLECTION_REPLICATION_FACTOR);
				propertiesMap.remove(COLLECTION_PARTITIONS);
				propertiesMap.remove(COLLECTION_CLEANUP_POLICY);
				propertiesMap.remove(COLLECTION_RETENTION_MS);
				propertiesMap.remove(COLLECTION_DELETE_RETENTION_MS);
				propertiesMap.remove(COLLECTION_MAX_MESSAGE_BYTES);
				return propertiesMap;
		}

		/**
		 * Get kafka consumer properties map
		 *
		 * @param keyDeserializer
		 * @param valueDeserializer
		 * @return
		 */
		public Map<String, Object> getConsumerConfig(Deserializer keyDeserializer, Deserializer valueDeserializer) {
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

		public Map<String, Object> getConfig(){
				return new HashMap<>(this.configurationMap);
		}

		protected Boolean getBoolean(String key) {
				if (this.configurationMap.get(key) != null) {
						if (this.configurationMap.get(key) instanceof Boolean) {
								return (Boolean) this.configurationMap.get(key);
						} else if (this.configurationMap.get(key) instanceof String) {
								String value = (String) this.configurationMap.get(key);
								if ("true".equalsIgnoreCase(value)) {
										return Boolean.TRUE;
								} else if ("false".equalsIgnoreCase(value)) {
										return Boolean.FALSE;
								}
						}
				}
				return null;
		}

		protected Long getLong(String key) {
				if (this.configurationMap.get(key) != null) {
						if (this.configurationMap.get(key) instanceof Long) {
								return (Long) this.configurationMap.get(key);
						} else if (this.configurationMap.get(key) instanceof Integer) {
								return ((Integer) this.configurationMap.get(key)).longValue();
						} else if (this.configurationMap.get(key) instanceof String) {
								try {
										return Long.parseLong((String) this.configurationMap.get(key));
								} catch (Exception e) {
								}
						}
				}
				return null;
		}

		protected String getString(String key) {
				if (this.configurationMap.get(key) != null) {
						if (this.configurationMap.get(key) instanceof String) {
								return (String) this.configurationMap.get(key);
						}
				}
				return null;
		}

		@Override
		public boolean equals(Object o) {
				if (o != null && (o instanceof CollectionConfig)) {
						CollectionConfig other = (CollectionConfig) o;
						if(this.configurationMap.equals(other.configurationMap)){
								return true;
						}
				}
				return false;
		}

		@Override
		public int hashCode() {
				return Objects.hash(this.configurationMap);
		}

		public void logConfig(){
				StringBuilder sb = new StringBuilder("CollectionConfig values:\n");
				for(Map.Entry<String,Object> entry : this.configurationMap.entrySet()){
						sb.append("\t").append(entry.getKey()).append(" = ").append(entry.getValue()).append("\n");
				}
				logger.info(sb.toString());
		}
}
