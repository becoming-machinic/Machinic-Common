package com.becomingmachinic.kafka.collections;

import org.apache.kafka.clients.CommonClientConfigs;
import org.apache.kafka.clients.consumer.ConsumerConfig;
import org.junit.ClassRule;
import org.junit.jupiter.api.*;
import org.junit.jupiter.api.parallel.Execution;
import org.mapdb.DB;
import org.mapdb.DBMaker;
import org.mapdb.Serializer;
import org.testcontainers.containers.KafkaContainer;

import java.time.Duration;
import java.util.*;
import java.util.concurrent.TimeUnit;

import static org.junit.jupiter.api.parallel.ExecutionMode.CONCURRENT;

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

		@Test
		void setCollectionSynchronousWriteAheadTest() throws Exception {
				configurationMap.put(CollectionConfig.COLLECTION_NAME, "setCollectionSynchronousWriteAheadTest");
				configurationMap.put(CollectionConfig.COLLECTION_WRITE_MODE, CollectionConfig.COLLECTION_WRITE_MODE_AHEAD);
				configurationMap.put(CollectionConfig.COLLECTION_SEND_MODE, CollectionConfig.COLLECTION_SEND_MODE_SYNCHRONOUS);

				try (KSet<String> set = new KafkaSet<String, String>(new CollectionConfig(configurationMap), CollectionSarde.stringToString())) {
						set.awaitWarmupComplete(30, TimeUnit.SECONDS);
						Assertions.assertEquals(0, set.size());

						for (int i = 0; i < 512; i++) {
								Assertions.assertTrue(set.add(Integer.toString(i)));
						}

						Assertions.assertEquals(512, set.size());

						for (int i = 0; i < 512; i++) {
								Assertions.assertTrue(set.contains(Integer.toString(i)));
						}

						Assertions.assertEquals(set,set);
						Assertions.assertEquals(set.hashCode(),set.hashCode());
						set.clear();
						Assertions.assertEquals(0,set.size());
						Assertions.assertEquals(0,set.toArray().length);
						Assertions.assertEquals(0,set.toArray(new String[0]).length);
						Assertions.assertTrue(set.isEmpty());
						Assertions.assertFalse(set.remove("non matching value"));
						Assertions.assertFalse(set.remove(1));
				}
		}

		@Test
		void setCollectionAsynchronousWriteAheadTest() throws Exception {
				configurationMap.put(CollectionConfig.COLLECTION_NAME, "setCollectionAsynchronousWriteAheadTest");
				configurationMap.put(CollectionConfig.COLLECTION_WRITE_MODE, CollectionConfig.COLLECTION_WRITE_MODE_AHEAD);
				configurationMap.put(CollectionConfig.COLLECTION_SEND_MODE, CollectionConfig.COLLECTION_SEND_MODE_ASYNCHRONOUS);

				try (KSet<String> set = new KafkaSet<String, String>(new CollectionConfig(configurationMap), CollectionSarde.stringToString())) {
						set.awaitWarmupComplete(30, TimeUnit.SECONDS);
						Assertions.assertEquals(0, set.size());

						for (int i = 0; i < 512; i++) {
								Assertions.assertTrue(set.add(Integer.toString(i)));
						}

						Assertions.assertEquals(512, set.size());

						for (int i = 0; i < 512; i++) {
								Assertions.assertTrue(set.contains(Integer.toString(i)));
						}

						Assertions.assertEquals(set,set);
						Assertions.assertEquals(set.hashCode(),set.hashCode());
						set.clear();
						Thread.sleep(1000);
						Assertions.assertEquals(0,set.size());
						Assertions.assertEquals(0,set.toArray().length);
						Assertions.assertFalse(set.remove("non matching value"));
						Assertions.assertFalse(set.remove(1));
				}
		}

		@Test
		void setCollectionSynchronousWriteBehindTest() throws Exception {
				configurationMap.put(CollectionConfig.COLLECTION_NAME, "setCollectionSynchronousWriteBehindTest");
				configurationMap.put(CollectionConfig.COLLECTION_WRITE_MODE, CollectionConfig.COLLECTION_WRITE_MODE_BEHIND);
				configurationMap.put(CollectionConfig.COLLECTION_SEND_MODE, CollectionConfig.COLLECTION_SEND_MODE_SYNCHRONOUS);

				try (KSet<String> set = new KafkaSet<String, String>(new CollectionConfig(configurationMap), CollectionSarde.stringToString())) {
						set.awaitWarmupComplete(30, TimeUnit.SECONDS);
						Assertions.assertEquals(0, set.size());

						for (int i = 0; i < 512; i++) {
								Assertions.assertTrue(set.add(Integer.toString(i)));
						}

						Assertions.assertEquals(512, set.size());

						for (int i = 0; i < 512; i++) {
								Assertions.assertTrue(set.contains(Integer.toString(i)));
						}

						Assertions.assertEquals(set,set);
						Assertions.assertEquals(set.hashCode(),set.hashCode());
						set.clear();
						Assertions.assertEquals(0,set.size());
						Assertions.assertEquals(0,set.toArray().length);
						Assertions.assertFalse(set.remove("non matching value"));
						Assertions.assertFalse(set.remove(1));
				}
		}

		@Test
		void setCollectionAsynchronousWriteBehindTest() throws Exception {
				configurationMap.put(CollectionConfig.COLLECTION_NAME, "setCollectionAsynchronousWriteBehindTest");
				configurationMap.put(CollectionConfig.COLLECTION_WRITE_MODE, CollectionConfig.COLLECTION_WRITE_MODE_BEHIND);
				configurationMap.put(CollectionConfig.COLLECTION_SEND_MODE, CollectionConfig.COLLECTION_SEND_MODE_ASYNCHRONOUS);

				try (KSet<String> set = new KafkaSet<String, String>(new CollectionConfig(configurationMap), CollectionSarde.stringToString())) {
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

						Assertions.assertEquals(set,set);
						Assertions.assertEquals(set.hashCode(),set.hashCode());
						set.clear();
						Assertions.assertEquals(0,set.size());
						Assertions.assertEquals(0,set.toArray().length);
						Assertions.assertFalse(set.remove("non matching value"));
						Assertions.assertFalse(set.remove(1));
				}
		}

		@Test
		void hTreeSetTest() throws Exception {
				configurationMap.put(CollectionConfig.COLLECTION_NAME, "hTreeSetTest");
				configurationMap.put(CollectionConfig.COLLECTION_WRITE_MODE, CollectionConfig.COLLECTION_WRITE_MODE_AHEAD);
				configurationMap.put(CollectionConfig.COLLECTION_SEND_MODE, CollectionConfig.COLLECTION_SEND_MODE_SYNCHRONOUS);

				DB db = DBMaker.memoryDB().make();
				Set<String> hTreeSet = db.hashSet("hTreeSet",Serializer.STRING).create();

				try (KSet<String>set = new KafkaSet<String, String>(hTreeSet, new CollectionConfig(configurationMap), CollectionSarde.stringToString())) {
						set.awaitWarmupComplete(30, TimeUnit.SECONDS);
						Assertions.assertEquals(0, set.size());

						for (int i = 0; i < 512; i++) {
								Assertions.assertTrue(set.add(Integer.toString(i)));
						}

						Assertions.assertEquals(512, set.size());

						for (int i = 0; i < 512; i++) {
								Assertions.assertTrue(set.contains(Integer.toString(i)));
						}

						Assertions.assertEquals(set,set);
						Assertions.assertEquals(set.hashCode(),set.hashCode());
						set.clear();
						Assertions.assertEquals(0,set.size());
						Assertions.assertEquals(0,set.toArray().length);
						Assertions.assertFalse(set.remove("non matching value"));
						Assertions.assertFalse(set.remove(1));
				}
		}

		@Test
		void setCollectionContainsAllTest() throws Exception {
				configurationMap.put(CollectionConfig.COLLECTION_NAME, "setCollectionContainsAllTest");
				configurationMap.put(CollectionConfig.COLLECTION_WRITE_MODE, CollectionConfig.COLLECTION_WRITE_MODE_BEHIND);
				configurationMap.put(CollectionConfig.COLLECTION_SEND_MODE, CollectionConfig.COLLECTION_SEND_MODE_ASYNCHRONOUS);

				try (KSet<String> set = new KafkaSet<String, String>(new CollectionConfig(configurationMap), CollectionSarde.stringToString())) {
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

						Assertions.assertTrue(set.removeAll(tempList));
						Thread.sleep(2000);
						Assertions.assertTrue(set.isEmpty());

						Assertions.assertEquals(set,set);
						Assertions.assertEquals(set.hashCode(),set.hashCode());
						set.clear();
						Assertions.assertEquals(0,set.size());
						Assertions.assertEquals(0,set.toArray().length);
						Assertions.assertFalse(set.remove("non matching value"));
						Assertions.assertFalse(set.remove(1));
				}
		}
}
