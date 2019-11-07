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

import org.apache.kafka.clients.admin.AdminClient;
import org.apache.kafka.clients.admin.CreateTopicsResult;
import org.apache.kafka.clients.admin.NewTopic;
import org.apache.kafka.clients.consumer.KafkaConsumer;
import org.apache.kafka.common.config.TopicConfig;
import org.apache.kafka.common.errors.TopicExistsException;
import org.apache.kafka.common.serialization.StringDeserializer;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.time.Duration;
import java.util.Arrays;
import java.util.HashMap;
import java.util.Map;
import java.util.concurrent.TimeUnit;

public class KafkaUtils {
		private static final Logger logger = LoggerFactory.getLogger(KafkaUtils.class);

		public static void checkConnectivity(CollectionConfig collectionConfig) throws KafkaCollectionException {
				try {
						KafkaConsumer<String, String> consumer = new KafkaConsumer<String, String>(collectionConfig.getConsumerConfig(new StringDeserializer(), new StringDeserializer()));
						consumer.listTopics(Duration.ofSeconds(5));
						logger.debug("Kafka broker connectivity verified");
				} catch (KafkaCollectionException e) {
						throw e;
				} catch (Exception e) {
						throw new KafkaCollectionException("CheckConnectivity failed", e);
				}
		}

		public static void createTopic(CollectionConfig collectionConfig) throws KafkaCollectionException {
				try (AdminClient admin = AdminClient.create(collectionConfig.getAdminClientConfig())) {

						CreateTopicsResult result = admin.createTopics(Arrays.asList(topicBuilder(collectionConfig)));
						result.values().get(collectionConfig.getTopic()).get(30, TimeUnit.SECONDS);
				} catch (KafkaCollectionException e) {
						throw e;
				} catch (Exception e) {
						if (e.getCause() != null && e.getCause() instanceof TopicExistsException) {
								logger.info(String.format("Topic %s already exists", collectionConfig.getTopic()));
						} else {
								logger.info(String.format("Create topic %s failed", collectionConfig.getTopic()), e);
								throw new KafkaCollectionException("Create topic %s failed", e, collectionConfig.getTopic());
						}
				}
		}

		private static NewTopic topicBuilder(CollectionConfig collectionConfig) {
				NewTopic topic = new NewTopic(collectionConfig.getTopic(), collectionConfig.getPartitions(), (short) collectionConfig.getReplicationFactor());

				Map<String,String> configMap = new HashMap<>();
				configMap.put(TopicConfig.CLEANUP_POLICY_CONFIG, collectionConfig.getCleanupPolicy());
				if (TopicConfig.CLEANUP_POLICY_DELETE.equals(collectionConfig.getCleanupPolicy())) {
						if (collectionConfig.getRetentionMs() != null) {
								configMap.put(TopicConfig.RETENTION_MS_CONFIG, Long.toString(collectionConfig.getRetentionMs()));
						}
						if (collectionConfig.getDeleteRetentionMs() != null) {
								configMap.put(TopicConfig.DELETE_RETENTION_MS_CONFIG, Long.toString(collectionConfig.getDeleteRetentionMs()));
						}
				}
				if(collectionConfig.getMaxMessageBytes() != null){
						configMap.put(TopicConfig.MAX_MESSAGE_BYTES_CONFIG,Long.toString(collectionConfig.getMaxMessageBytes()));
				}
				topic.configs(configMap);

				return topic;
		}
}
