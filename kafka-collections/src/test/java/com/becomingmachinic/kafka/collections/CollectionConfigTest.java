package com.becomingmachinic.kafka.collections;

import static org.junit.jupiter.api.parallel.ExecutionMode.CONCURRENT;

import java.time.Duration;
import java.util.HashMap;
import java.util.Map;

import org.apache.kafka.clients.CommonClientConfigs;
import org.apache.kafka.common.config.TopicConfig;
import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.parallel.Execution;

@Execution(CONCURRENT)
public class CollectionConfigTest {
	
	private Map<String, Object> configurationMap = new HashMap<>();
	
	@BeforeEach
	private void before() {
		configurationMap = new HashMap<>();
		configurationMap.put(CommonClientConfigs.BOOTSTRAP_SERVERS_CONFIG, "localhost:9092");
	}
	
	@Test
	void mapNameTest() throws Exception {
		configurationMap.put(CollectionConfig.COLLECTION_NAME, null);
		Assertions.assertThrows(KafkaCollectionConfigurationException.class, () -> {
			new CollectionConfig(configurationMap);
		});
		configurationMap.put(CollectionConfig.COLLECTION_NAME, "name with spaces");
		Assertions.assertThrows(KafkaCollectionConfigurationException.class, () -> {
			new CollectionConfig(configurationMap);
		});
		configurationMap.put(CollectionConfig.COLLECTION_NAME, "name/with/slashes");
		Assertions.assertThrows(KafkaCollectionConfigurationException.class, () -> {
			new CollectionConfig(configurationMap);
		});
		configurationMap.put(CollectionConfig.COLLECTION_NAME, "name.with.periods");
		Assertions.assertThrows(KafkaCollectionConfigurationException.class, () -> {
			new CollectionConfig(configurationMap);
		});
	}
	
	@Test
	void mapTopicTest() throws Exception {
		configurationMap.put(CollectionConfig.COLLECTION_NAME, "mapTopicTest");
		
		Assertions.assertEquals("maptopictest_collection", new CollectionConfig(configurationMap).getTopic());
		
		configurationMap.put(CollectionConfig.COLLECTION_TOPIC, "maptopictest_collection");
		Assertions.assertEquals("maptopictest_collection", new CollectionConfig(configurationMap).getTopic());
		
		configurationMap.put(CollectionConfig.COLLECTION_TOPIC, "mapTopicTest_collection");
		Assertions.assertEquals("mapTopicTest_collection", new CollectionConfig(configurationMap).getTopic());
		
		Assertions.assertEquals(CollectionConfig.COLLECTION_MAX_POLL_INTERVAL_MS_DEFAULT_VALUE, new CollectionConfig(configurationMap).getMaxPollIntervalDuration().toMillis());
		Assertions.assertEquals(CollectionConfig.COLLECTION_WARMUP_POLL_INTERVAL_MS_DEFAULT_VALUE, new CollectionConfig(configurationMap).getWarmupPollIntervalDuration().toMillis());
		
		configurationMap.put(CollectionConfig.COLLECTION_TOPIC, "topic with spaces");
		Assertions.assertThrows(KafkaCollectionConfigurationException.class, () -> {
			new CollectionConfig(configurationMap);
		});
		configurationMap.put(CollectionConfig.COLLECTION_TOPIC, "topic/with/slashes");
		Assertions.assertThrows(KafkaCollectionConfigurationException.class, () -> {
			new CollectionConfig(configurationMap);
		});
		configurationMap.put(CollectionConfig.COLLECTION_TOPIC, "topic.with.periods");
		Assertions.assertThrows(KafkaCollectionConfigurationException.class, () -> {
			new CollectionConfig(configurationMap);
		});
	}
	
	@Test
	void createTopicTest() {
		configurationMap.put(CollectionConfig.COLLECTION_NAME, "createTopicTest");
		Assertions.assertEquals(true, new CollectionConfig(this.configurationMap).getCreateTopic());
		
		Assertions.assertEquals(1, new CollectionConfig(this.configurationMap).getReplicationFactor());
		this.configurationMap.put(CollectionConfig.COLLECTION_REPLICATION_FACTOR, "2");
		Assertions.assertEquals(2, new CollectionConfig(this.configurationMap).getReplicationFactor());
		
		Assertions.assertEquals(1, new CollectionConfig(this.configurationMap).getPartitions());
		this.configurationMap.put(CollectionConfig.COLLECTION_PARTITIONS, "5");
		Assertions.assertEquals(5, new CollectionConfig(this.configurationMap).getPartitions());
		
		Assertions.assertEquals(TopicConfig.CLEANUP_POLICY_COMPACT, new CollectionConfig(this.configurationMap).getCleanupPolicy());
		this.configurationMap.put(CollectionConfig.COLLECTION_CLEANUP_POLICY, "compact");
		Assertions.assertEquals(TopicConfig.CLEANUP_POLICY_COMPACT, new CollectionConfig(this.configurationMap).getCleanupPolicy());
		this.configurationMap.put(CollectionConfig.COLLECTION_CLEANUP_POLICY, "delete");
		Assertions.assertEquals(TopicConfig.CLEANUP_POLICY_DELETE, new CollectionConfig(this.configurationMap).getCleanupPolicy());
		
		Assertions.assertEquals(CollectionConfig.COLLECTION_RETENTION_MS_DEFAULT_VALUE, new CollectionConfig(this.configurationMap).getRetentionMs());
		this.configurationMap.put(CollectionConfig.COLLECTION_RETENTION_MS, "999");
		Assertions.assertEquals(999l, new CollectionConfig(this.configurationMap).getRetentionMs());
		
		this.configurationMap.put(CollectionConfig.COLLECTION_DELETE_RETENTION_MS, "888");
		Assertions.assertEquals(888l, new CollectionConfig(this.configurationMap).getDeleteRetentionMs());
		
		Assertions.assertEquals(null, new CollectionConfig(this.configurationMap).getMaxMessageBytes());
		
		this.configurationMap.put(CollectionConfig.COLLECTION_MAX_MESSAGE_BYTES, 1048576);
		Assertions.assertEquals(1048576, new CollectionConfig(this.configurationMap).getMaxMessageBytes());
		
	}
	
	@Test
	void createConfigTest() {
		configurationMap.put(CollectionConfig.COLLECTION_NAME, "createTopicTest");
		Assertions.assertEquals(true, new CollectionConfig(this.configurationMap).getCreateTopic());
		
		this.configurationMap.put(CollectionConfig.COLLECTION_MAX_POLL_INTERVAL_MS, "1009");
		Assertions.assertEquals(Duration.ofMillis(1009), new CollectionConfig(this.configurationMap).getMaxPollIntervalDuration());
		
		this.configurationMap.put(CollectionConfig.COLLECTION_WARMUP_POLL_INTERVAL_MS, "1011");
		Assertions.assertEquals(Duration.ofMillis(1011), new CollectionConfig(this.configurationMap).getWarmupPollIntervalDuration());
		
		this.configurationMap.put(CollectionConfig.COLLECTION_SEND_TIMEOUT_MS, "1012");
		Assertions.assertEquals(1012l, new CollectionConfig(this.configurationMap).getSendTimeoutMs());
		
		this.configurationMap.put(CollectionConfig.COLLECTION_READONLY, "true");
		Assertions.assertEquals(true, new CollectionConfig(this.configurationMap).isReadOnly());
		
		this.configurationMap.put(CollectionConfig.COLLECTION_READONLY, "false");
		Assertions.assertEquals(false, new CollectionConfig(this.configurationMap).isReadOnly());
		
		this.configurationMap.put(CollectionConfig.COLLECTION_SEND_MODE, CollectionConfig.COLLECTION_SEND_MODE_ASYNCHRONOUS);
		Assertions.assertEquals(CollectionConfig.COLLECTION_SEND_MODE_ASYNCHRONOUS, new CollectionConfig(this.configurationMap).getSendMode());
		
		this.configurationMap.put(CollectionConfig.COLLECTION_SEND_MODE, CollectionConfig.COLLECTION_SEND_MODE_SYNCHRONOUS);
		Assertions.assertEquals(CollectionConfig.COLLECTION_SEND_MODE_SYNCHRONOUS, new CollectionConfig(this.configurationMap).getSendMode());
		
		this.configurationMap.put(CollectionConfig.COLLECTION_READ_OWN_WRITES, null);
		Assertions.assertEquals(false, new CollectionConfig(this.configurationMap).isReadOwnWrites());
		
		this.configurationMap.put(CollectionConfig.COLLECTION_READ_OWN_WRITES, true);
		Assertions.assertEquals(true, new CollectionConfig(this.configurationMap).isReadOwnWrites());
		
		Assertions.assertEquals(false, new CollectionConfig(this.configurationMap).isSkipConnectivityCheck());
		
		this.configurationMap.put(CollectionConfig.COLLECTION_SKIP_CONNECTIVITY_CHECK, true);
		Assertions.assertEquals(true, new CollectionConfig(this.configurationMap).isSkipConnectivityCheck());
		
		this.configurationMap.put(CollectionConfig.COLLECTION_SKIP_CONNECTIVITY_CHECK, false);
		Assertions.assertEquals(false, new CollectionConfig(this.configurationMap).isSkipConnectivityCheck());
		
		Assertions.assertEquals("none", new CollectionConfig(this.configurationMap).isResetOffset());
		
		this.configurationMap.put(CollectionConfig.COLLECTION_RESET_OFFSET, CollectionConfig.COLLECTION_RESET_OFFSET_BEGINNING);
		Assertions.assertEquals(CollectionConfig.COLLECTION_RESET_OFFSET_BEGINNING, new CollectionConfig(this.configurationMap).isResetOffset());
		
		this.configurationMap.put(CollectionConfig.COLLECTION_RESET_OFFSET, CollectionConfig.COLLECTION_RESET_OFFSET_END);
		Assertions.assertEquals(CollectionConfig.COLLECTION_RESET_OFFSET_END, new CollectionConfig(this.configurationMap).isResetOffset());
	}
}
