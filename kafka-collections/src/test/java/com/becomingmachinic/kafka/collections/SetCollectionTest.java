package com.becomingmachinic.kafka.collections;

import static org.junit.jupiter.api.parallel.ExecutionMode.CONCURRENT;

import java.time.Duration;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.concurrent.TimeUnit;

import org.apache.kafka.clients.CommonClientConfigs;
import org.apache.kafka.clients.consumer.ConsumerConfig;
import org.junit.ClassRule;
import org.junit.jupiter.api.AfterAll;
import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.BeforeAll;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.parallel.Execution;
import org.mapdb.DB;
import org.mapdb.DBMaker;
import org.mapdb.Serializer;
import org.testcontainers.containers.KafkaContainer;

@Execution(CONCURRENT)
public class SetCollectionTest {
	
	@ClassRule
	public static KafkaContainer kafka = new KafkaContainer("5.2.3-1")
			.withNetworkAliases("kafka_" + SetCollectionTest.class.getSimpleName())
			.withEmbeddedZookeeper()
			.withStartupTimeout(Duration.ofSeconds(60));
	
	@BeforeAll
	private static void beforeAll() {
		kafka.start();
	}
	
	@AfterAll
	private static void afterAll() {
		kafka.stop();
	}
	
	private Map<String, Object> configurationMap = new HashMap<>();
	
	@BeforeEach
	private void before() {
		configurationMap = new HashMap<>();
		configurationMap.put(CommonClientConfigs.BOOTSTRAP_SERVERS_CONFIG, kafka.getBootstrapServers());
		configurationMap.put(ConsumerConfig.MAX_POLL_RECORDS_CONFIG, 100);
		configurationMap.put(CollectionConfig.COLLECTION_WARMUP_POLL_INTERVAL_MS, 500l);
	}
	
	@Test
	void testConnectivity() {
		configurationMap.put(CollectionConfig.COLLECTION_NAME, "anyName");
		KafkaUtils.checkConnectivity(new CollectionConfig(configurationMap));
	}
	
	@SuppressWarnings("unlikely-arg-type")
	@Test
	void setCollectionSynchronousTest() throws Exception {
		configurationMap.put(CollectionConfig.COLLECTION_NAME, "setCollectionSynchronousTest");
		configurationMap.put(CollectionConfig.COLLECTION_SEND_MODE, CollectionConfig.COLLECTION_SEND_MODE_SYNCHRONOUS);
		
		try (KSet<String> set = new KafkaSet<String, String>(new CollectionConfig(configurationMap), CollectionSerde.stringToString())) {
			set.awaitWarmupComplete(30, TimeUnit.SECONDS);
			Assertions.assertEquals(0, set.size());
			
			for (int i = 0; i < 512; i++) {
				Assertions.assertTrue(set.add(Integer.toString(i)));
			}
			
			Assertions.assertEquals(512, set.size());
			
			for (int i = 0; i < 512; i++) {
				Assertions.assertTrue(set.contains(Integer.toString(i)));
			}
			
			Assertions.assertEquals(set, set);
			Assertions.assertEquals(set.hashCode(), set.hashCode());
			set.clear();
			Assertions.assertEquals(0, set.size());
			Assertions.assertEquals(0, set.toArray().length);
			Assertions.assertFalse(set.remove("non matching value"));
			Assertions.assertFalse(set.remove(1));
		}
	}
	
	@SuppressWarnings("unlikely-arg-type")
	@Test
	void setCollectionAsynchronousTest() throws Exception {
		configurationMap.put(CollectionConfig.COLLECTION_NAME, "setCollectionAsynchronousTest");
		configurationMap.put(CollectionConfig.COLLECTION_SEND_MODE, CollectionConfig.COLLECTION_SEND_MODE_ASYNCHRONOUS);
		
		try (KSet<String> set = new KafkaSet<String, String>(new CollectionConfig(configurationMap), CollectionSerde.stringToString())) {
			set.awaitWarmupComplete(30, TimeUnit.SECONDS);
			Assertions.assertEquals(0, set.size());
			
			for (int i = 0; i < 512; i++) {
				Assertions.assertTrue(set.add(Integer.toString(i)));
			}
			
			Assertions.assertEquals(512, set.size());
			
			for (int i = 0; i < 512; i++) {
				Assertions.assertTrue(set.contains(Integer.toString(i)));
			}
			
			Assertions.assertTrue(set.removeAll(Arrays.asList(set.toArray())));
			Thread.sleep(1000);
			Assertions.assertTrue(set.isEmpty());
			
			Assertions.assertEquals(set, set);
			Assertions.assertEquals(set.hashCode(), set.hashCode());
			set.clear();
			Assertions.assertEquals(0, set.size());
			Assertions.assertEquals(0, set.toArray().length);
			Assertions.assertFalse(set.remove("non matching value"));
			Assertions.assertFalse(set.remove(1));
		}
	}
	
	@SuppressWarnings("unlikely-arg-type")
	@Test
	void hTreeSetTest() throws Exception {
		configurationMap.put(CollectionConfig.COLLECTION_NAME, "hTreeSetTest");
		configurationMap.put(CollectionConfig.COLLECTION_SEND_MODE, CollectionConfig.COLLECTION_SEND_MODE_SYNCHRONOUS);
		
		DB db = DBMaker.memoryDB().make();
		Set<String> hTreeSet = db.hashSet("hTreeSet", Serializer.STRING).create();
		
		try (KSet<String> set = new KafkaSet<String, String>(hTreeSet, new CollectionConfig(configurationMap), CollectionSerde.stringToString())) {
			set.awaitWarmupComplete(30, TimeUnit.SECONDS);
			Assertions.assertEquals(0, set.size());
			
			for (int i = 0; i < 512; i++) {
				Assertions.assertTrue(set.add(Integer.toString(i)));
			}
			
			Assertions.assertEquals(512, set.size());
			
			for (int i = 0; i < 512; i++) {
				Assertions.assertTrue(set.contains(Integer.toString(i)));
			}
			
			Assertions.assertEquals(set, set);
			Assertions.assertEquals(set.hashCode(), set.hashCode());
			set.clear();
			Assertions.assertEquals(0, set.size());
			Assertions.assertEquals(0, set.toArray().length);
			Assertions.assertFalse(set.remove("non matching value"));
			Assertions.assertFalse(set.remove(1));
		}
	}
	
	@SuppressWarnings("unlikely-arg-type")
	@Test
	void setCollectionContainsAllTest() throws Exception {
		configurationMap.put(CollectionConfig.COLLECTION_NAME, "setCollectionContainsAllTest");
		configurationMap.put(CollectionConfig.COLLECTION_SEND_MODE, CollectionConfig.COLLECTION_SEND_MODE_ASYNCHRONOUS);
		
		try (KSet<String> set = new KafkaSet<String, String>(new CollectionConfig(configurationMap), CollectionSerde.stringToString())) {
			set.awaitWarmupComplete(30, TimeUnit.SECONDS);
			Assertions.assertEquals(0, set.size());
			
			List<String> tempList = new ArrayList<>();
			for (int i = 0; i < 512; i++) {
				tempList.add(Integer.toString(i));
			}
			Assertions.assertTrue(set.addAll(tempList));
			Assertions.assertEquals(512, set.size());
			
			for (int i = 0; i < 512; i++) {
				Assertions.assertTrue(set.contains(Integer.toString(i)));
			}
			
			Assertions.assertTrue(set.containsAll(tempList));
			Assertions.assertTrue(set.removeAll(tempList));
			Thread.sleep(2000);
			Assertions.assertTrue(set.isEmpty());
			
			Assertions.assertEquals(set, set);
			Assertions.assertEquals(set.hashCode(), set.hashCode());
			set.clear();
			Assertions.assertEquals(0, set.size());
			Assertions.assertEquals(0, set.toArray().length);
			Assertions.assertFalse(set.remove("non matching value"));
			Assertions.assertFalse(set.remove(1));
		}
	}
	
	@Test
	void setReadonlyTest() throws Exception {
		configurationMap.put(CollectionConfig.COLLECTION_NAME, "setReadonlyTest");
		configurationMap.put(CollectionConfig.COLLECTION_SEND_MODE, CollectionConfig.COLLECTION_SEND_MODE_ASYNCHRONOUS);
		
		try (KSet<String> set1 = new KafkaSet<String, String>(new CollectionConfig(configurationMap), CollectionSerde.stringToString())) {
			set1.awaitWarmupComplete(30, TimeUnit.SECONDS);
			Assertions.assertEquals(0, set1.size());
			
			for (int i = 0; i < 512; i++) {
				set1.add(Integer.toString(i));
			}
			Assertions.assertEquals(512, set1.size());
			Assertions.assertFalse(set1.isReadOnly());
			
			configurationMap.put(CollectionConfig.COLLECTION_READONLY,true);
			try (KSet<String> set2 = new KafkaSet<String, String>(new CollectionConfig(configurationMap), CollectionSerde.stringToString())) {
				set2.awaitWarmupComplete(30, TimeUnit.SECONDS);
				Assertions.assertEquals(512, set2.size());
				
				Assertions.assertThrows(UnsupportedOperationException.class, () -> {
					set2.add("key");
				});
				
				Assertions.assertThrows(UnsupportedOperationException.class, () -> {
					set2.addAll(Arrays.asList("key"));
				});
				
				Assertions.assertThrows(UnsupportedOperationException.class, () -> {
					set2.remove("key");
				});
				
				Assertions.assertThrows(UnsupportedOperationException.class, () -> {
					set2.removeAll(Arrays.asList("key"));
				});
				
				Assertions.assertThrows(UnsupportedOperationException.class, () -> {
					set2.retainAll(Arrays.asList("key"));
				});
				
				Assertions.assertThrows(UnsupportedOperationException.class, () -> {
					set2.clear();
				});
				
				Assertions.assertEquals(512, set2.size());
				Assertions.assertTrue(set2.isReadOnly());
			}
		
		}
	}
}
