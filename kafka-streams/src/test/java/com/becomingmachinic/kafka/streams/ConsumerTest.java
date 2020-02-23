package com.becomingmachinic.kafka.streams;

import static org.junit.jupiter.api.parallel.ExecutionMode.CONCURRENT;

import java.time.Duration;
import java.util.Arrays;
import java.util.HashMap;
import java.util.Map;
import java.util.concurrent.atomic.AtomicLong;

import org.apache.kafka.clients.CommonClientConfigs;
import org.apache.kafka.clients.consumer.ConsumerConfig;
import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.apache.kafka.clients.consumer.ConsumerRecords;
import org.apache.kafka.clients.consumer.KafkaConsumer;
import org.apache.kafka.clients.consumer.OffsetAndMetadata;
import org.apache.kafka.clients.producer.KafkaProducer;
import org.apache.kafka.clients.producer.ProducerRecord;
import org.apache.kafka.common.TopicPartition;
import org.apache.kafka.common.serialization.StringDeserializer;
import org.apache.kafka.common.serialization.StringSerializer;
import org.junit.ClassRule;
import org.junit.jupiter.api.AfterAll;
import org.junit.jupiter.api.BeforeAll;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Disabled;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.parallel.Execution;
import org.testcontainers.containers.KafkaContainer;

@Execution(CONCURRENT)
public class ConsumerTest {
	
	@ClassRule
	public static KafkaContainer kafka = new KafkaContainer("5.2.3-1")
			.withNetworkAliases("kafka_" + ConsumerTest.class.getSimpleName())
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
		
	}
	
	@Test
	@Disabled
	void testConnectivity() {
		String topic = "testTopic";
		
		Map<String, Object> consumerConfig = new HashMap<>(configurationMap);
		consumerConfig.put(ConsumerConfig.GROUP_ID_CONFIG, "testCommit");
		consumerConfig.put(ConsumerConfig.ENABLE_AUTO_COMMIT_CONFIG, true);
		consumerConfig.put("session.timeout.ms",300000);
		consumerConfig.put(ConsumerConfig.MAX_POLL_RECORDS_CONFIG,10);
		consumerConfig.put(ConsumerConfig.AUTO_OFFSET_RESET_CONFIG,"earliest");
		try (KafkaConsumer<String, String> kafkaConsumer = new KafkaConsumer<String, String>(consumerConfig, new StringDeserializer(), new StringDeserializer())) {
			kafkaConsumer.subscribe(Arrays.asList(topic));
			AtomicLong offset = new AtomicLong(0);
			
			RunnableTest task1 = new RunnableTest() {
				@Override
				public void runTest() {
					Thread.currentThread().setName("Consumer");
					try {
						final Map<TopicPartition, OffsetAndMetadata> offsets = new HashMap<>();
						while (true) {
							ConsumerRecords<String, String> records = kafkaConsumer.poll(Duration.ofSeconds(5));
							
							for (ConsumerRecord<String, String> record : records) {
								offsets.put(new TopicPartition(topic, 0), new OffsetAndMetadata(record.offset() + 1, null));
							}
							
							kafkaConsumer.commitSync(offsets);
							System.out.println(offsets.values());
							offsets.clear();
						}
					} catch (Exception e) {
						e.printStackTrace();
					}
				}
			};
			
			//			RunnableTest task2 = new RunnableTest() {
			//				@Override
			//				public void runTest() {
			//					Thread.currentThread().setName("Committer");
			//					try {
			//						final Map<TopicPartition, OffsetAndMetadata> offsets = new HashMap<>();
			//
			//						long last = 0;
			//						while (true) {
			//							long current = offset.get();
			//							if (current > last) {
			//								offsets.clear();
			//								offsets.put(new TopicPartition(topic, 0), new OffsetAndMetadata(current, null));
			//								kafkaConsumer.commitAsync(offsets, (o, e) -> {
			//								});
			//								last = current;
			//							}
			//							try {
			//								Thread.sleep(1000);
			//							} catch (InterruptedException e) {
			//								e.printStackTrace();
			//							}
			//						}
			//					} catch (Exception e) {
			//						e.printStackTrace();
			//					}
			//				}
			//			};
			
			RunnableTest task3 = new RunnableTest() {
				@Override
				public void runTest() {
					Map<String, Object> producerConfig = new HashMap<>(configurationMap);
					try (KafkaProducer<String, String> kafkaProducer = new KafkaProducer<String, String>(producerConfig, new StringSerializer(), new StringSerializer())) {
						int i = 0;
						while (i < 1000000) {
							kafkaProducer.send(new ProducerRecord<>(topic, Integer.toString(i), Integer.toString(i++)));
							
//							try {
//								Thread.sleep(250);
//							} catch (InterruptedException e) {
//								e.printStackTrace();
//							}
						}
					}
				}
			};
			
			task1.start();
			task3.start();
			
			//			task2.start();
			
			task1.join();
			//			task2.join();
			task3.join();
			
		} catch (InterruptedException e) {
			e.printStackTrace();
		}
	}
}
