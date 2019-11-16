package com.becomingmachinic.kafka.collections;

import com.becomingmachinic.kafka.collections.extensions.GsonToStringCollectionSerde;
import com.becomingmachinic.kafka.collections.extensions.HashingGsonSerializer;
import com.becomingmachinic.kafka.collections.extensions.HashingJacksonSerializer;
import com.becomingmachinic.kafka.collections.extensions.JacksonToStringCollectionSerde;
import com.becomingmachinic.kafka.collections.utils.TestObject;
import com.fasterxml.jackson.annotation.JsonCreator;
import com.fasterxml.jackson.annotation.JsonProperty;
import com.fasterxml.jackson.core.type.TypeReference;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.google.gson.GsonBuilder;
import com.google.gson.reflect.TypeToken;
import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.parallel.Execution;

import java.lang.reflect.Type;
import java.nio.charset.StandardCharsets;
import java.util.*;

import static org.junit.jupiter.api.parallel.ExecutionMode.CONCURRENT;

@Execution(CONCURRENT)
public class SardeTest {

		@BeforeEach
		private void before() {
		}

		@Test
		void stringSardeTest() throws Exception {
				Assertions.assertEquals("abc", CollectionSerde.stringToString().serialize("abc"));
				Assertions.assertEquals("abc", CollectionSerde.stringToString().deserialize("abc"));
		}

		@Test
		void binarySardeTest() throws Exception {
				Assertions.assertArrayEquals("abc".getBytes(StandardCharsets.UTF_8), CollectionSerde.byteArrayToByteArray().serialize("abc".getBytes(StandardCharsets.UTF_8)));
				Assertions.assertArrayEquals("abc".getBytes(StandardCharsets.UTF_8), CollectionSerde.byteArrayToByteArray().deserialize("abc".getBytes(StandardCharsets.UTF_8)));
		}

		@Test
		void binaryHashSardeTest() throws Exception {
				Assertions.assertArrayEquals("abc".getBytes(StandardCharsets.UTF_8), CollectionSerde.byteArrayToHash().serialize(Hash.wrap("abc".getBytes(StandardCharsets.UTF_8))));
				Assertions.assertEquals(Hash.wrap("abc".getBytes(StandardCharsets.UTF_8)), CollectionSerde.byteArrayToHash().deserialize("abc".getBytes(StandardCharsets.UTF_8)));
		}

		@Test
		void gsonSardeTest() throws Exception {
				Assertions.assertEquals("{\"field1\":\"value1\",\"field2\":\"value2\"}", new GsonToStringCollectionSerde<TestObject>(new GsonBuilder().create(), TestObject.class).serialize(new TestObject("value1", "value2")));
				Assertions.assertEquals(new TestObject("value1", "value2"), new GsonToStringCollectionSerde<TestObject>(new GsonBuilder().create(), TestObject.class).deserialize("{\"field1\":\"value1\",\"field2\":\"value2\"}"));

				Assertions.assertEquals("[{\"field1\":\"value1\",\"field2\":\"value2\"}]", new GsonToStringCollectionSerde<List<TestObject>>(new GsonBuilder().create(), new TypeToken<List<TestObject>>() {
				}.getType()).serialize(Arrays.asList(new TestObject("value1", "value2"))));
				Assertions.assertEquals(Arrays.asList(new TestObject("value1", "value2")), new GsonToStringCollectionSerde<List<TestObject>>(new GsonBuilder().create(), new TypeToken<List<TestObject>>() {
				}.getType()).deserialize("[{\"field1\":\"value1\",\"field2\":\"value2\"}]"));
		}

		@Test
		void jacksonSardeTest() throws Exception {
				Assertions.assertEquals("{\"field1\":\"value1\",\"field2\":\"value2\"}", new JacksonToStringCollectionSerde<TestObject>(new ObjectMapper(), TestObject.class).serialize(new TestObject("value1", "value2")));
				Assertions.assertEquals(new TestObject("value1", "value2"), new JacksonToStringCollectionSerde<TestObject>(new ObjectMapper(), TestObject.class).deserialize("{\"field1\":\"value1\",\"field2\":\"value2\"}"));

				Assertions.assertEquals("[{\"field1\":\"value1\",\"field2\":\"value2\"}]", new JacksonToStringCollectionSerde<List<TestObject>>(new ObjectMapper(), new TypeReference<List<TestObject>>() {
				}).serialize(Arrays.asList(new TestObject("value1", "value2"))));
				Assertions.assertEquals(Arrays.asList(new TestObject("value1", "value2")), new JacksonToStringCollectionSerde<List<TestObject>>(new ObjectMapper(), new TypeReference<List<TestObject>>() {
				}).deserialize("[{\"field1\":\"value1\",\"field2\":\"value2\"}]"));
		}


}
