package com.becomingmachinic.kafka.collections;

import org.apache.kafka.clients.CommonClientConfigs;
import org.apache.kafka.clients.consumer.ConsumerConfig;
import org.junit.ClassRule;
import org.junit.jupiter.api.*;
import org.junit.jupiter.api.parallel.Execution;
import org.testcontainers.containers.KafkaContainer;

import java.time.Duration;
import java.util.*;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicLong;

import static org.junit.jupiter.api.parallel.ExecutionMode.CONCURRENT;

@Execution(CONCURRENT)
public class BloomFilterCollectionTest {
	
	@ClassRule
	public static KafkaContainer kafka = new KafkaContainer("5.2.3-1")
			.withNetworkAliases("kafka_" + BloomFilterCollectionTest.class.getSimpleName())
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
		configurationMap.put(ConsumerConfig.MAX_POLL_RECORDS_CONFIG, 250);
		configurationMap.put(CollectionConfig.COLLECTION_WARMUP_POLL_INTERVAL_MS, 500l);
	}
	
	@Test
	void testConnectivity() {
		configurationMap.put(CollectionConfig.COLLECTION_NAME, "anyName");
		KafkaUtils.checkConnectivity(new CollectionConfig(configurationMap));
	}
	
	@Test
	void bloomFilterCollectionSynchronousWriteAheadTest() throws Exception {
		configurationMap.put(CollectionConfig.COLLECTION_NAME, "bloomFilterCollectionSynchronousWriteAheadTest");
		configurationMap.put(CollectionConfig.COLLECTION_WRITE_MODE, CollectionConfig.COLLECTION_WRITE_MODE_AHEAD);
		configurationMap.put(CollectionConfig.COLLECTION_SEND_MODE, CollectionConfig.COLLECTION_SEND_MODE_SYNCHRONOUS);
		
		try (KBloomFilter<String> bloomFilter = new KafkaBloomFilter<String>(new BloomFilterBitSetStore(1000, 0.001d), new CollectionConfig(configurationMap), HashingSerializer.stringSerializer(), new HashStreamProviderSHA256())) {
			bloomFilter.awaitWarmupComplete(30, TimeUnit.SECONDS);
			Assertions.assertEquals(0, bloomFilter.count());
			
			for (int i = 0; i < 512; i++) {
				Assertions.assertTrue(bloomFilter.add(Integer.toString(i)));
			}
			
			Assertions.assertEquals(512, bloomFilter.count());
			
			for (int i = 0; i < 512; i++) {
				Assertions.assertFalse(bloomFilter.add(Integer.toString(i)));
				Assertions.assertTrue(bloomFilter.contains(Integer.toString(i)));
			}
			
			for (int i = 513; i < 1024; i++) {
				Assertions.assertFalse(bloomFilter.contains(Integer.toString(i)), String.format("False positive on value %s", Integer.toString(i)));
			}
			
			Assertions.assertEquals(bloomFilter, bloomFilter);
			Assertions.assertEquals(bloomFilter.hashCode(), bloomFilter.hashCode());
			Assertions.assertEquals(1000, bloomFilter.size());
			Assertions.assertEquals(512, bloomFilter.count());
			Assertions.assertTrue(bloomFilter.getFalsePositiveProbability() <= bloomFilter.getExpectedFalsePositiveProbability());
		}
	}
	
	@Test
	void bloomFilterCollectionAsynchronousWriteAheadTest() throws Exception {
		configurationMap.put(CollectionConfig.COLLECTION_NAME, "bloomFilterCollectionAsynchronousWriteAheadTest");
		configurationMap.put(CollectionConfig.COLLECTION_WRITE_MODE, CollectionConfig.COLLECTION_WRITE_MODE_AHEAD);
		configurationMap.put(CollectionConfig.COLLECTION_SEND_MODE, CollectionConfig.COLLECTION_SEND_MODE_ASYNCHRONOUS);
		
		try (KBloomFilter<String> bloomFilter = new KafkaBloomFilter<String>(new BloomFilterBitSetStore(1000, 0.001d), new CollectionConfig(configurationMap), HashingSerializer.stringSerializer(), new HashStreamProviderSHA256())) {
			bloomFilter.awaitWarmupComplete(30, TimeUnit.SECONDS);
			Assertions.assertEquals(0, bloomFilter.count());
			
			for (int i = 0; i < 512; i++) {
				Assertions.assertTrue(bloomFilter.add(Integer.toString(i)));
			}
			Thread.sleep(1000);
			
			Assertions.assertEquals(512, bloomFilter.count());
			
			for (int i = 0; i < 512; i++) {
				Assertions.assertFalse(bloomFilter.add(Integer.toString(i)));
				Assertions.assertTrue(bloomFilter.contains(Integer.toString(i)));
			}
			
			for (int i = 513; i < 1024; i++) {
				Assertions.assertFalse(bloomFilter.contains(Integer.toString(i)));
			}
			
			Assertions.assertEquals(bloomFilter, bloomFilter);
			Assertions.assertEquals(bloomFilter.hashCode(), bloomFilter.hashCode());
			Assertions.assertEquals(1000, bloomFilter.size());
			Assertions.assertEquals(512, bloomFilter.count());
			Assertions.assertTrue(bloomFilter.getFalsePositiveProbability() <= bloomFilter.getExpectedFalsePositiveProbability());
		}
	}
	
	@Test
	void bloomFilterCollectionSynchronousWriteBehindTest() throws Exception {
		configurationMap.put(CollectionConfig.COLLECTION_NAME, "bloomFilterCollectionSynchronousWriteBehindTest");
		configurationMap.put(CollectionConfig.COLLECTION_WRITE_MODE, CollectionConfig.COLLECTION_WRITE_MODE_BEHIND);
		configurationMap.put(CollectionConfig.COLLECTION_SEND_MODE, CollectionConfig.COLLECTION_SEND_MODE_SYNCHRONOUS);
		
		try (KBloomFilter<String> bloomFilter = new KafkaBloomFilter<String>(new BloomFilterBitSetStore(1000, 0.001d), new CollectionConfig(configurationMap), HashingSerializer.stringSerializer(), new HashStreamProviderSHA256())) {
			bloomFilter.awaitWarmupComplete(30, TimeUnit.SECONDS);
			Assertions.assertEquals(0, bloomFilter.count());
			
			for (int i = 0; i < 512; i++) {
				Assertions.assertTrue(bloomFilter.add(Integer.toString(i)));
			}
			
			Assertions.assertEquals(512, bloomFilter.count());
			
			for (int i = 0; i < 512; i++) {
				Assertions.assertFalse(bloomFilter.add(Integer.toString(i)));
				Assertions.assertTrue(bloomFilter.contains(Integer.toString(i)));
			}
			
			for (int i = 513; i < 1024; i++) {
				Assertions.assertFalse(bloomFilter.contains(Integer.toString(i)));
			}
			
			Assertions.assertEquals(bloomFilter, bloomFilter);
			Assertions.assertEquals(bloomFilter.hashCode(), bloomFilter.hashCode());
			Assertions.assertEquals(1000, bloomFilter.size());
			Assertions.assertEquals(512, bloomFilter.count());
			Assertions.assertTrue(bloomFilter.getFalsePositiveProbability() <= bloomFilter.getExpectedFalsePositiveProbability());
		}
	}
	
	@Test
	void bloomFilterCollectionAsynchronousWriteBehindTest() throws Exception {
		configurationMap.put(CollectionConfig.COLLECTION_NAME, "bloomFilterCollectionAsynchronousWriteBehindTest");
		configurationMap.put(CollectionConfig.COLLECTION_WRITE_MODE, CollectionConfig.COLLECTION_WRITE_MODE_BEHIND);
		configurationMap.put(CollectionConfig.COLLECTION_SEND_MODE, CollectionConfig.COLLECTION_SEND_MODE_ASYNCHRONOUS);
		
		try (KBloomFilter<String> bloomFilter = new KafkaBloomFilter<String>(new BloomFilterBitSetStore(1000, 0.001d), new CollectionConfig(configurationMap), HashingSerializer.stringSerializer(), new HashStreamProviderSHA256())) {
			bloomFilter.awaitWarmupComplete(30, TimeUnit.SECONDS);
			Assertions.assertEquals(0, bloomFilter.count());
			
			for (int i = 0; i < 512; i++) {
				Assertions.assertTrue(bloomFilter.add(Integer.toString(i)));
			}
			
			Assertions.assertEquals(512, bloomFilter.count());
			
			for (int i = 0; i < 512; i++) {
				Assertions.assertFalse(bloomFilter.add(Integer.toString(i)));
				Assertions.assertTrue(bloomFilter.contains(Integer.toString(i)));
			}
			
			for (int i = 513; i < 1024; i++) {
				Assertions.assertFalse(bloomFilter.contains(Integer.toString(i)));
			}
			
			Assertions.assertEquals(bloomFilter, bloomFilter);
			Assertions.assertEquals(bloomFilter.hashCode(), bloomFilter.hashCode());
			Assertions.assertEquals(1000, bloomFilter.size());
			Assertions.assertEquals(512, bloomFilter.count());
			Assertions.assertTrue(bloomFilter.getFalsePositiveProbability() <= bloomFilter.getExpectedFalsePositiveProbability());
		}
	}
	
	@Test
	void bloomFilterCollectionAsynchronousWriteBehindDeduplicationTest() throws Exception {
		configurationMap.put(CollectionConfig.COLLECTION_NAME, "bloomFilterCollectionAsynchronousWriteBehindDeduplicationTest");
		configurationMap.put(CollectionConfig.COLLECTION_WRITE_MODE, CollectionConfig.COLLECTION_WRITE_MODE_BEHIND);
		configurationMap.put(CollectionConfig.COLLECTION_SEND_MODE, CollectionConfig.COLLECTION_SEND_MODE_ASYNCHRONOUS);
		configurationMap.put(ConsumerConfig.MAX_POLL_RECORDS_CONFIG, 1000);
		int size = 50000;
		double expectedFalsePositiveProbability = 0.001d;
		int threadCount = 8;
		
		try (KBloomFilter<String> bloomFilter = new KafkaBloomFilter<String>(new BloomFilterBitSetStore(size * 2, expectedFalsePositiveProbability), new CollectionConfig(configurationMap), HashingSerializer.stringSerializer(),
				new HashStreamProviderSHA256())) {
			bloomFilter.awaitWarmupComplete(30, TimeUnit.SECONDS);
			Assertions.assertEquals(0, bloomFilter.count());
			
			AtomicLong addedItems = new AtomicLong(0);
			long startTimestamp = System.currentTimeMillis();
			
			List<Thread> threads = new ArrayList<>();
			for (int i = 0; i < threadCount; i++) {
				threads.add(new Thread(new Runnable() {
					@Override
					public void run() {
						for (int i = 0; i < size; i++) {
							if (bloomFilter.add(Integer.toString(i))) {
								addedItems.incrementAndGet();
							}
						}
					}
				}));
			}
			for (Thread thread : threads) {
				thread.start();
			}
			for (Thread thread : threads) {
				thread.join(300000);
			}
			System.out.println(String.format("Deduplicated %s items in %s milliseconds", threadCount * size, System.currentTimeMillis() - startTimestamp));
			Assertions.assertTrue(addedItems.get() <= size);
			Assertions.assertTrue(addedItems.get() >= size - (size * expectedFalsePositiveProbability));
			Assertions.assertTrue(bloomFilter.count() >= size - (size * expectedFalsePositiveProbability));
			Assertions.assertTrue(bloomFilter.getFalsePositiveProbability() <= expectedFalsePositiveProbability);
		}
	}
	
	@Test
	void bloomFilterContainsAllTest() throws Exception {
		configurationMap.put(CollectionConfig.COLLECTION_NAME, "bloomFilterContainsAllTest");
		configurationMap.put(CollectionConfig.COLLECTION_WRITE_MODE, CollectionConfig.COLLECTION_WRITE_MODE_BEHIND);
		configurationMap.put(CollectionConfig.COLLECTION_SEND_MODE, CollectionConfig.COLLECTION_SEND_MODE_ASYNCHRONOUS);
		
		try (KBloomFilter<String> bloomFilter = new KafkaBloomFilter<String>(new BloomFilterBitSetStore(1000, 0.001d), new CollectionConfig(configurationMap), HashingSerializer.stringSerializer(), new HashStreamProviderSHA256())) {
			bloomFilter.awaitWarmupComplete(30, TimeUnit.SECONDS);
			Assertions.assertEquals(0, bloomFilter.count());
			
			List<String> tempList = new ArrayList<>();
			for (int i = 0; i < 512; i++) {
				tempList.add(Integer.toString(i));
			}
			Assertions.assertTrue(bloomFilter.addAll(tempList));
			Assertions.assertEquals(512, bloomFilter.count());
			Assertions.assertTrue(bloomFilter.containsAll(tempList));
			
			for (int i = 0; i < 512; i++) {
				Assertions.assertFalse(bloomFilter.add(Integer.toString(i)));
				Assertions.assertTrue(bloomFilter.contains(Integer.toString(i)));
			}
			
			for (int i = 513; i < 1024; i++) {
				Assertions.assertFalse(bloomFilter.contains(Integer.toString(i)));
			}
			
			Assertions.assertEquals(bloomFilter, bloomFilter);
			Assertions.assertEquals(bloomFilter.hashCode(), bloomFilter.hashCode());
			Assertions.assertEquals(1000, bloomFilter.size());
			Assertions.assertEquals(512, bloomFilter.count());
			Assertions.assertTrue(bloomFilter.getFalsePositiveProbability() <= bloomFilter.getExpectedFalsePositiveProbability());
		}
	}
	
	@Test
	void bloomFilterReadonlyTest() throws Exception {
		configurationMap.put(CollectionConfig.COLLECTION_NAME, "bloomFilterReadonlyTest");
		configurationMap.put(CollectionConfig.COLLECTION_WRITE_MODE, CollectionConfig.COLLECTION_WRITE_MODE_BEHIND);
		configurationMap.put(CollectionConfig.COLLECTION_SEND_MODE, CollectionConfig.COLLECTION_SEND_MODE_ASYNCHRONOUS);
		
		try (KBloomFilter<String> bloomFilter1 = new KafkaBloomFilter<String>(new BloomFilterBitSetStore(1000, 0.001d), new CollectionConfig(configurationMap), HashingSerializer.stringSerializer(), new HashStreamProviderSHA256())) {
			bloomFilter1.awaitWarmupComplete(30, TimeUnit.SECONDS);
			Assertions.assertEquals(0, bloomFilter1.count());
			
			for (int i = 0; i < 512; i++) {
				Assertions.assertTrue(bloomFilter1.add(Integer.toString(i)));
				Assertions.assertTrue(bloomFilter1.contains(Integer.toString(i)));
			}
			Assertions.assertEquals(512, bloomFilter1.count());
			
			configurationMap.put(CollectionConfig.COLLECTION_READONLY,true);
			try (KBloomFilter<String> bloomFilter2 = new KafkaBloomFilter<String>(new BloomFilterBitSetStore(1000, 0.001d), new CollectionConfig(configurationMap), HashingSerializer.stringSerializer(), new HashStreamProviderSHA256())) {
				bloomFilter2.awaitWarmupComplete(30, TimeUnit.SECONDS);
				Assertions.assertEquals(512, bloomFilter2.count());
				
				Assertions.assertThrows(UnsupportedOperationException.class, () -> {
					bloomFilter2.add("key");
				});
				
				Assertions.assertThrows(UnsupportedOperationException.class, () -> {
					bloomFilter2.addAll(Arrays.asList("key"));
				});
			}
		}
	}
	
	@Test
	@Disabled("This test is unnecessary for regression")
	void bloomFilterCollectionLargeDataTest() throws Exception {
		configurationMap.put(CollectionConfig.COLLECTION_NAME, "bloomFilterCollectionLargeDataTest");
		configurationMap.put(CollectionConfig.COLLECTION_WRITE_MODE, CollectionConfig.COLLECTION_WRITE_MODE_BEHIND);
		configurationMap.put(CollectionConfig.COLLECTION_SEND_MODE, CollectionConfig.COLLECTION_SEND_MODE_ASYNCHRONOUS);
		configurationMap.put(ConsumerConfig.MAX_POLL_RECORDS_CONFIG, 5000);
		
		
		try (KafkaBloomFilter<Integer> bloomFilter = new KafkaBloomFilter<>(new BloomFilterBitSetStore(10000000, 0.001d), new CollectionConfig(configurationMap), HashingSerializer.integerSerializer(), new HashStreamProviderMurmur3())) {
			bloomFilter.awaitWarmupComplete(30, TimeUnit.SECONDS);
			Assertions.assertEquals(0, bloomFilter.count());
			
			int falsePositiveCount = 0;
			for (int i = 0; i < 10000000; i++) {
				if (!bloomFilter.add(i)) {
					falsePositiveCount++;
				}
			}
			double falePositivePercent = falsePositiveCount / 10000000d;
			Assertions.assertTrue(falePositivePercent <= 0.001d);
			
			Assertions.assertTrue(bloomFilter.getFalsePositiveProbability() <= bloomFilter.getExpectedFalsePositiveProbability());
		}
	}
}
