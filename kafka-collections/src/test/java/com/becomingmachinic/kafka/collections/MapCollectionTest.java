package com.becomingmachinic.kafka.collections;

import ch.qos.logback.classic.spi.ILoggingEvent;
import com.becomingmachinic.kafka.collections.utils.LogbackTestAppender;
import com.becomingmachinic.kafka.collections.utils.RunnableTest;
import org.apache.kafka.clients.CommonClientConfigs;
import org.apache.kafka.clients.consumer.ConsumerConfig;
import org.junit.ClassRule;
import org.junit.jupiter.api.*;
import org.junit.jupiter.api.parallel.Execution;
import org.mapdb.*;
import org.testcontainers.containers.KafkaContainer;

import java.time.Duration;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.concurrent.TimeUnit;

import static org.junit.jupiter.api.parallel.ExecutionMode.CONCURRENT;

@Execution(CONCURRENT)
public class MapCollectionTest {

		@ClassRule
		public static KafkaContainer kafka = new KafkaContainer("5.2.3-1")
				.withNetworkAliases("kafka_" + MapCollectionTest.class.getSimpleName())
				.withEmbeddedZookeeper()
				.withStartupTimeout(Duration.ofSeconds(60));

		@BeforeAll
		private static void beforeAll() {
				kafka.start();
		}

		@AfterAll
		private static void afterAll() {
				kafka.stop();
				List<ILoggingEvent> configErrors = LogbackTestAppender.getEvents("The configuration '{}' was supplied but isn't a known config.");
				for(ILoggingEvent event : configErrors){
						Assertions.assertEquals("The configuration 'unused.property' was supplied but isn't a known config.",event.getFormattedMessage());
				}
		}

		private Map<String, Object> configurationMap = new HashMap<>();

		@BeforeEach
		private void before() {
				configurationMap = new HashMap<>();
				configurationMap.put(CommonClientConfigs.BOOTSTRAP_SERVERS_CONFIG, kafka.getBootstrapServers());
				configurationMap.put(ConsumerConfig.MAX_POLL_RECORDS_CONFIG, 100);
				configurationMap.put(CollectionConfig.COLLECTION_WARMUP_POLL_INTERVAL_MS, 500l);
				configurationMap.put(CollectionConfig.COLLECTION_CREATE_TOPIC, true);
		}

		@Test
		void testConnectivity() {
				configurationMap.put(CollectionConfig.COLLECTION_NAME, "anyName");
				KafkaUtils.checkConnectivity(new CollectionConfig(configurationMap));
		}

		@Test
		void mapTopicCreationTest() throws Exception {
				configurationMap.put(CollectionConfig.COLLECTION_NAME, "mapTopicCreationTest");
				configurationMap.put(CollectionConfig.COLLECTION_WRITE_MODE, CollectionConfig.COLLECTION_WRITE_MODE_AHEAD);
				configurationMap.put(CollectionConfig.COLLECTION_SEND_MODE, CollectionConfig.COLLECTION_SEND_MODE_SYNCHRONOUS);

				try (KMap<String, String>  map = new KafkaMap<String, String, String, String>(new CollectionConfig(configurationMap), CollectionSarde.stringToString(), CollectionSarde.stringToString())) {
						map.awaitWarmupComplete(30, TimeUnit.SECONDS);
						Assertions.assertEquals(0, map.size());

						Assertions.assertNull(map.put("test message", "test message"));
						Assertions.assertEquals(1, map.size());
				}

				try (KMap<String, String>  map = new KafkaMap<String, String, String, String>(new CollectionConfig(configurationMap), CollectionSarde.stringToString(), CollectionSarde.stringToString())) {
						map.awaitWarmupComplete(30, TimeUnit.SECONDS);
						Assertions.assertEquals(1, map.size());
						Assertions.assertEquals("test message", map.get("test message"));
				}
		}

		@Test
		void mapCollectionSynchronousWriteAheadTest() throws Exception {
				configurationMap.put(CollectionConfig.COLLECTION_NAME, "mapCollectionWriteAheadTest");
				configurationMap.put(CollectionConfig.COLLECTION_WRITE_MODE, CollectionConfig.COLLECTION_WRITE_MODE_AHEAD);
				configurationMap.put(CollectionConfig.COLLECTION_SEND_MODE, CollectionConfig.COLLECTION_SEND_MODE_SYNCHRONOUS);

				try (KMap<String, String>  map = new KafkaMap<String, String, String, String>(new CollectionConfig(configurationMap), CollectionSarde.stringToString(), CollectionSarde.stringToString())) {
						map.awaitWarmupComplete(30, TimeUnit.SECONDS);
						Assertions.assertEquals(0, map.size());

						for (int i = 0; i < 512; i++) {
								Assertions.assertNull(map.put(Integer.toString(i), String.format("Test_%s", i)));
						}

						Assertions.assertEquals(512, map.size());
						Assertions.assertEquals(512, map.values().size());
						Assertions.assertEquals(512, map.entrySet().size());
						Assertions.assertEquals(512, map.keySet().size());

						for (int i = 0; i < 512; i++) {
								Assertions.assertEquals(String.format("Test_%s", i), map.get(Integer.toString(i)));
						}

						Assertions.assertEquals(map, map);
						Assertions.assertEquals(map.hashCode(), map.hashCode());
						map.clear();
						Assertions.assertEquals(0, map.size());
						Assertions.assertEquals(0, map.values().size());
						Assertions.assertEquals(0, map.entrySet().size());
						Assertions.assertEquals(0, map.keySet().size());
						Assertions.assertTrue( map.isEmpty());
						Assertions.assertFalse( map.containsKey("non matching value"));
						Assertions.assertFalse( map.containsValue("non matching value"));
						Assertions.assertNull(map.remove("non matching value"));
						Assertions.assertNull(map.remove(1));
						Assertions.assertEquals("default", map.getOrDefault("non matching value", "default"));
				}
		}

		@Test
		void mapCollectionSynchronousWriteBehindTest() throws Exception {
				configurationMap.put(CollectionConfig.COLLECTION_NAME, "mapCollectionWriteBehindTest");
				configurationMap.put(CollectionConfig.COLLECTION_WRITE_MODE, CollectionConfig.COLLECTION_WRITE_MODE_BEHIND);
				configurationMap.put(CollectionConfig.COLLECTION_SEND_MODE, CollectionConfig.COLLECTION_SEND_MODE_SYNCHRONOUS);

				try (KMap<String, String>  map = new KafkaMap<String, String, String, String>(new CollectionConfig(configurationMap), CollectionSarde.stringToString(), CollectionSarde.stringToString())) {
						map.awaitWarmupComplete(30, TimeUnit.SECONDS);
						Assertions.assertEquals(0, map.size());

						for (int i = 0; i < 512; i++) {
								Assertions.assertNull(map.put(Integer.toString(i), String.format("Test_%s", i)));
						}

						Assertions.assertEquals(512, map.size());
						Assertions.assertEquals(512, map.values().size());
						Assertions.assertEquals(512, map.entrySet().size());
						Assertions.assertEquals(512, map.keySet().size());

						for (int i = 0; i < 512; i++) {
								Assertions.assertEquals(String.format("Test_%s", i), map.get(Integer.toString(i)));
						}

						Assertions.assertEquals(map, map);
						Assertions.assertEquals(map.hashCode(), map.hashCode());
						map.clear();
						Assertions.assertEquals(0, map.size());
						Assertions.assertEquals(0, map.values().size());
						Assertions.assertEquals(0, map.entrySet().size());
						Assertions.assertEquals(0, map.keySet().size());
						Assertions.assertNull(map.remove("non matching value"));
						Assertions.assertNull(map.remove(1));
						Assertions.assertEquals("default", map.getOrDefault("non matching value", "default"));
				}
		}

		@Test
		void mapCollectionConcurrentWriteAheadTest() throws Exception {
				configurationMap.put(CollectionConfig.COLLECTION_NAME, "mapCollectionConcurrentWriteAheadTest");
				configurationMap.put(CollectionConfig.COLLECTION_WRITE_MODE, CollectionConfig.COLLECTION_WRITE_MODE_AHEAD);
				configurationMap.put(CollectionConfig.COLLECTION_SEND_MODE, CollectionConfig.COLLECTION_SEND_MODE_SYNCHRONOUS);

				try (KMap<String, String>  map = new KafkaMap<String, String, String, String>(new CollectionConfig(configurationMap), CollectionSarde.stringToString(), CollectionSarde.stringToString())) {
						map.awaitWarmupComplete(30, TimeUnit.SECONDS);
						Assertions.assertEquals(0, map.size());

						RunnableTest task1 = new RunnableTest() {
								@Override
								public void runTest() {
										for (int i = 0; i < 512; i++) {
												Assertions.assertNull(map.put(Integer.toString(i), String.format("Test_%s", i)));
										}
								}
						};
						RunnableTest task2 = new RunnableTest() {
								@Override
								public void runTest() {
										for (int i = 0; i < 512; i++) {
												Assertions.assertNull(map.put(Integer.toString(i) + "a", String.format("Test_%s", i)));
										}
								}
						};
						task1.start();
						task2.start();
						task1.get(30000);
						task2.get(30000);

						Thread.sleep(1000);
						Assertions.assertEquals(1024, map.size());

						for (int i = 0; i < 512; i++) {
								Assertions.assertEquals(String.format("Test_%s", i), map.get(Integer.toString(i)));
						}
						for (int i = 0; i < 512; i++) {
								Assertions.assertEquals(String.format("Test_%s", Integer.toString(i)), map.get(Integer.toString(i) + "a"));
						}
				}
		}

		@Test
		void mapCollectionConcurrentWriteBehindTest() throws Exception {
				configurationMap.put(CollectionConfig.COLLECTION_NAME, "mapCollectionConcurrentWriteBehindTest");
				configurationMap.put(CollectionConfig.COLLECTION_WRITE_MODE, CollectionConfig.COLLECTION_WRITE_MODE_BEHIND);
				configurationMap.put(CollectionConfig.COLLECTION_SEND_MODE, CollectionConfig.COLLECTION_SEND_MODE_SYNCHRONOUS);

				try (KMap<String, String>  map = new KafkaMap<String, String, String, String>(new CollectionConfig(configurationMap), CollectionSarde.stringToString(), CollectionSarde.stringToString())) {
						map.awaitWarmupComplete(30, TimeUnit.SECONDS);
						Assertions.assertEquals(0, map.size());

						RunnableTest task1 = new RunnableTest() {
								@Override
								public void runTest() {
										for (int i = 0; i < 512; i++) {
												Assertions.assertNull(map.put(Integer.toString(i), String.format("Test_%s", i)));
										}
								}
						};
						RunnableTest task2 = new RunnableTest() {
								@Override
								public void runTest() {
										for (int i = 0; i < 512; i++) {
												Assertions.assertNull(map.put(Integer.toString(i) + "a", String.format("Test_%s", i)));
										}
								}
						};
						task1.start();
						task2.start();
						task1.get(30000);
						task2.get(30000);

						Assertions.assertEquals(1024, map.size());

						for (int i = 0; i < 512; i++) {
								Assertions.assertEquals(String.format("Test_%s", i), map.get(Integer.toString(i)));
						}
						for (int i = 0; i < 512; i++) {
								Assertions.assertEquals(String.format("Test_%s", Integer.toString(i)), map.get(Integer.toString(i) + "a"));
						}
				}
		}

		@Test
		void mapCollectionAsynchronousWriteBehindTest() throws Exception {
				configurationMap.put(CollectionConfig.COLLECTION_NAME, "mapCollectionAsynchronousWriteBehindTest");
				configurationMap.put(CollectionConfig.COLLECTION_SEND_MODE, CollectionConfig.COLLECTION_SEND_MODE_ASYNCHRONOUS);
				configurationMap.put(CollectionConfig.COLLECTION_WRITE_MODE, CollectionConfig.COLLECTION_WRITE_MODE_BEHIND);

				try (KMap<String, String>  map = new KafkaMap<String, String, String, String>(new CollectionConfig(configurationMap), CollectionSarde.stringToString(), CollectionSarde.stringToString())) {
						map.awaitWarmupComplete(30, TimeUnit.SECONDS);
						Assertions.assertEquals(0, map.size());

						for (int i = 0; i < 512; i++) {
								Assertions.assertNull(map.put(Integer.toString(i), String.format("Test_%s", i)));
						}
						Assertions.assertEquals(512, map.size());

						for (int i = 0; i < 512; i++) {
								Assertions.assertEquals(String.format("Test_%s", i), map.get(Integer.toString(i)));
						}

						for (int i = 0; i < 512; i++) {
								Assertions.assertEquals(String.format("Test_%s", i), map.put(Integer.toString(i), null));
						}

						Assertions.assertEquals(0, map.size());
				}
		}

		@Test
		void mapCollectionAsynchronousWriteAheadTest() throws Exception {
				configurationMap.put(CollectionConfig.COLLECTION_NAME, "mapCollectionAsynchronousWriteAheadTest");
				configurationMap.put(CollectionConfig.COLLECTION_SEND_MODE, CollectionConfig.COLLECTION_SEND_MODE_ASYNCHRONOUS);
				configurationMap.put(CollectionConfig.COLLECTION_WRITE_MODE, CollectionConfig.COLLECTION_WRITE_MODE_AHEAD);

				try (KMap<String, String>  map = new KafkaMap<String, String, String, String>(new CollectionConfig(configurationMap), CollectionSarde.stringToString(), CollectionSarde.stringToString())) {
						map.awaitWarmupComplete(30, TimeUnit.SECONDS);
						Assertions.assertEquals(0, map.size());

						for (int i = 0; i < 512; i++) {
								Assertions.assertNull(map.put(Integer.toString(i), String.format("Test_%s", i)));
						}
						Thread.sleep(1000);

						Assertions.assertEquals(512, map.size());

						for (int i = 0; i < 512; i++) {
								Assertions.assertEquals(String.format("Test_%s", i), map.put(Integer.toString(i), null));
						}
						Thread.sleep(1000);

						Assertions.assertEquals(0, map.size());
				}
		}

		@Test
		void mapCollectionContainsAllTest() throws Exception {
				configurationMap.put(CollectionConfig.COLLECTION_NAME, "mapCollectionAddContainsAllTest");
				configurationMap.put(CollectionConfig.COLLECTION_SEND_MODE, CollectionConfig.COLLECTION_SEND_MODE_ASYNCHRONOUS);
				configurationMap.put(CollectionConfig.COLLECTION_WRITE_MODE, CollectionConfig.COLLECTION_WRITE_MODE_BEHIND);

				try (KMap<String, String>  map = new KafkaMap<String, String, String, String>(new CollectionConfig(configurationMap), CollectionSarde.stringToString(), CollectionSarde.stringToString())) {
						map.awaitWarmupComplete(30, TimeUnit.SECONDS);
						Assertions.assertEquals(0, map.size());

						Map<String,String> tempMap = new HashMap<>();
						for (int i = 0; i < 512; i++) {
								tempMap.put(Integer.toString(i), String.format("Test_%s", i));
						}
						map.putAll(tempMap);

						Assertions.assertEquals(512, map.size());

						map.containsAll(tempMap.keySet());

						for (int i = 0; i < 512; i++) {
								Assertions.assertEquals(String.format("Test_%s", i), map.get(Integer.toString(i)));
						}

						for (int i = 0; i < 512; i++) {
								Assertions.assertEquals(String.format("Test_%s", i), map.put(Integer.toString(i), null));
						}

						Assertions.assertEquals(0, map.size());
				}
		}

		@Test
		void mapCollectionRapidUpdateTest() throws Exception {
				configurationMap.put(CollectionConfig.COLLECTION_NAME, "mapCollectionRapidUpdateTest");
				configurationMap.put(CollectionConfig.COLLECTION_WRITE_MODE, CollectionConfig.COLLECTION_WRITE_MODE_AHEAD);
				configurationMap.put(CollectionConfig.COLLECTION_SEND_MODE, CollectionConfig.COLLECTION_SEND_MODE_SYNCHRONOUS);

				try (KafkaMap<String,String,String, String>  map = new KafkaMap<String, String, String, String>(new CollectionConfig(configurationMap), CollectionSarde.stringToString(), CollectionSarde.stringToString())) {
						map.awaitWarmupComplete(30, TimeUnit.SECONDS);
						Assertions.assertEquals(0, map.size());

						CountDownLatchKafkaCollectionEventListener latch = new CountDownLatchKafkaCollectionEventListener(512);
						map.addKafkaCollectionEventListener(latch);

						Assertions.assertNull(map.put("testKey", Integer.toString(-1)));

						RunnableTest task1 = new RunnableTest() {
								@Override
								public void runTest() {
										for (int i = 0; i < 512; i++) {
												Assertions.assertEquals(Integer.toString(i - 1), map.put("testKey", Integer.toString(i)));
										}
								}
						};

						task1.start();

						int lastValue = -2;
						int checks = 0;
						while (lastValue < 511) {
								int currentValue = Integer.parseInt(map.get("testKey"));
								if (currentValue < lastValue) {
										Assertions.fail("value shifted back to old value");
								}
								lastValue = currentValue;
								checks++;
								try {
										Thread.sleep(1l);
								} catch (InterruptedException e) {
										e.printStackTrace();
								}
						}
						System.out.println(String.format("Consistency checks %s", checks));

						task1.get(30000);
				}
		}

		@Test
		void hTreeMapTest() throws Exception {
				configurationMap.put(CollectionConfig.COLLECTION_NAME, "hTreeMapTest");
				configurationMap.put(CollectionConfig.COLLECTION_WRITE_MODE, CollectionConfig.COLLECTION_WRITE_MODE_AHEAD);
				configurationMap.put(CollectionConfig.COLLECTION_SEND_MODE, CollectionConfig.COLLECTION_SEND_MODE_SYNCHRONOUS);

				DB db = DBMaker.memoryDB().make();
				HTreeMap<String, String> hTreeMap = db.hashMap("hTreeMap")
						.keySerializer(Serializer.STRING)
						.valueSerializer(Serializer.STRING)
						.create();

				try (KMap<String, String>  map = new KafkaMap<String, String, String, String>(hTreeMap, new CollectionConfig(configurationMap), CollectionSarde.stringToString(),
						CollectionSarde.stringToString())) {
						map.awaitWarmupComplete(30, TimeUnit.SECONDS);
						Assertions.assertEquals(0, map.size());

						for (int i = 0; i < 512; i++) {
								Assertions.assertNull(map.put(Integer.toString(i), String.format("Test_%s", i)));
						}

						Assertions.assertEquals(512, map.size());

						for (int i = 0; i < 512; i++) {
								Assertions.assertEquals(String.format("Test_%s", i), map.put(Integer.toString(i), String.format("Test_%s", i * -1)));
						}

						Assertions.assertEquals(512, map.size());
				}
		}

		@Test
		void mapGroupIdTest() throws Exception {
				configurationMap.put(CollectionConfig.COLLECTION_NAME, "mapGroupIdTest");
				configurationMap.put(CollectionConfig.COLLECTION_WRITE_MODE, CollectionConfig.COLLECTION_WRITE_MODE_AHEAD);
				configurationMap.put(CollectionConfig.COLLECTION_SEND_MODE, CollectionConfig.COLLECTION_SEND_MODE_ASYNCHRONOUS);
				configurationMap.put(CollectionConfig.COLLECTION_PARTITIONS, 16);
				configurationMap.put(ConsumerConfig.GROUP_ID_CONFIG, "mapGroupIdTest");

				try (KMap<String, String>  map = new KafkaMap<String, String, String, String>(new CollectionConfig(configurationMap), CollectionSarde.stringToString(), CollectionSarde.stringToString())) {
						map.awaitWarmupComplete(30, TimeUnit.SECONDS);
						Assertions.assertEquals(0, map.size());

						for (int i = 0; i < 1024; i++) {
								Assertions.assertNull(map.put(Integer.toString(i) + "a", String.format("Test_%s", i)));
						}
				}

				RunnableTest task1 = new RunnableTest() {
						@Override
						public void runTest() {
								try (KMap<String, String>  map = new KafkaMap<String, String, String, String>(new CollectionConfig(configurationMap), CollectionSarde.stringToString(), CollectionSarde.stringToString())) {
										map.awaitWarmupComplete(30, TimeUnit.SECONDS);
										Assertions.assertEquals(1024, map.size());

										for (int i = 0; i < 1024; i++) {
												Assertions.assertEquals(String.format("Test_%s", i),map.get(Integer.toString(i) + "a"));
										}

										Thread.sleep(1000);
								} catch (Exception e) {
										Assertions.fail("Map threw exception", e);
								}
						}
				};
				RunnableTest task2 = new RunnableTest() {
						@Override
						public void runTest() {
								try (KMap<String, String> map = new KafkaMap<String, String, String, String>(new CollectionConfig(configurationMap), CollectionSarde.stringToString(), CollectionSarde.stringToString())) {
										map.awaitWarmupComplete(30, TimeUnit.SECONDS);
										Assertions.assertEquals(1024, map.size());

										for (int i = 0; i < 1024; i++) {
												Assertions.assertEquals(String.format("Test_%s", i),map.get(Integer.toString(i) + "a"));
										}

										Thread.sleep(1000);
								} catch (Exception e) {
										Assertions.fail("Map threw exception", e);
								}
						}
				};

				task1.start();
				task2.start();

				task1.get(30000);
				task2.get(30000);
		}

		@Test
		void configLogTest() throws Exception {
				configurationMap.put(CollectionConfig.COLLECTION_NAME, "configLogTest");
				configurationMap.put("unused.property", "unusedPropertyValue");

				try (KafkaMap<String, String, String, String> map = new KafkaMap<String, String, String, String>(new CollectionConfig(configurationMap), CollectionSarde.stringToString(), CollectionSarde.stringToString())) {
						map.awaitWarmupComplete(30, TimeUnit.SECONDS);
						Assertions.assertEquals(0, map.size());
				}
				List<ILoggingEvent> configErrors = LogbackTestAppender.getEvents("The configuration '{}' was supplied but isn't a known config.");
				for(ILoggingEvent event : configErrors){
						Assertions.assertEquals("The configuration 'unused.property' was supplied but isn't a known config.",event.getFormattedMessage());
				}
				Assertions.assertEquals(4,configErrors.size());
		}
}
