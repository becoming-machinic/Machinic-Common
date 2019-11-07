package com.becomingmachinic.kafka.collections;

import com.becomingmachinic.kafka.collections.extensions.HashingGsonSerializer;
import com.becomingmachinic.kafka.collections.extensions.HashingJacksonSerializer;
import com.becomingmachinic.kafka.collections.utils.TestObject;
import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.parallel.Execution;

import java.nio.charset.StandardCharsets;
import java.util.HashMap;
import java.util.LinkedHashMap;
import java.util.Map;

import static org.junit.jupiter.api.parallel.ExecutionMode.CONCURRENT;

@Execution(CONCURRENT)
public class HashSerializerTest {

		@BeforeEach
		private void before() {
		}

		@Test
		void stringMd5ProviderTest() throws Exception {
				HashTester<String> tester = new HashTester<>(new HashStreamProviderMD5(), HashingSerializer.stringSerializer());
				Assertions.assertEquals(Hash.wrap(new byte[] { -95, 33, -102, -127, -109, 106, 103, -65, 0, -113, -100, -85, -100, -40, 54, 25 }),
						tester.getHash("1"));
				Assertions.assertArrayEquals(new int[] { 1591633279, 639463391, 348234935, 513845761 }, tester.getHash("1").getVector32());
				Assertions.assertEquals(4, tester.provider.getNumberOfHashFunctions());
				Assertions.assertEquals(Hash.wrap(new byte[] { 70, -117, 69, -61, -94, -52, -9, -106, 53, -84, -67, 111, -69, -118, 62, -27 }),
						tester.getHash("Hello World"));
				Assertions.assertArrayEquals(new int[] { 1183532483, 1227752662, 1040752373, 153359021 }, tester.getHash("Hello World").getVector32());
		}

		@Test
		void stringSha256ProviderTest() throws Exception {
				HashTester<String> tester = new HashTester<>(new HashStreamProviderSHA256(), HashingSerializer.stringSerializer());
				Assertions.assertEquals(Hash.wrap(new byte[] { -86, -89, -102, -71, -8, -6, -24, -124, -106, 56, 47, 108, 42, -65, -4, -122, -116, 100, 3, -54, 3, -40, 4, -5, 30, -31, -74, 58, 94, -22, 126, 4 }),
						tester.getHash("1"));
				Assertions.assertArrayEquals(new int[] { 1431856455, 643966972, 1239491940, 658542218, 1798528274, 1263998443, 1689958914, 536147220 }, tester.getHash("1").getVector32());
				Assertions.assertEquals(8, tester.provider.getNumberOfHashFunctions());
				Assertions.assertEquals(Hash.wrap(new byte[] { 64, -23, -59, -63, 106, 86, 24, 54, 87, 120, -117, -11, -71, 61, -109, 110, -62, -29, 97, -126, 32, -75, -124, 112, 90, -84, 27, 106, 71, 117, -107, -85 }),
						tester.getHash("Hello World"));
				Assertions.assertArrayEquals(new int[] { 1089062337, 529731958, 1536411031, 1428382562, 1137614634, 22237328, 979260174, 2014936681 }, tester.getHash("Hello World").getVector32());
		}

		@Test
		void stringSha512ProviderTest() throws Exception {
				HashTester<String> tester = new HashTester<>(new HashStreamProviderSHA512(), HashingSerializer.stringSerializer());
				Assertions.assertEquals(Hash.wrap(
						new byte[] { -19, -1, -80, -59, 60, -32, 85, -35, 86, -9, -10, 126, 74, -40, -121, 72, 36, -54, -42, -60, -28, -20, -108, 52, 14, -82, -27, -65, 90, -82, -44, -40, 30, -44, 40, 79, 125, -108, 113, -10, 116, 70, -71, -20, 22, -98, -48, 5, 19,
								-33, -38, -20, -5, 11, 111, 64, -8, 100, 90, -43, -50, 112, 12, 48 }),
						tester.getHash("1"));
				Assertions.assertArrayEquals(new int[] { 302010171, 1596614083, 1848205258, 55727480, 772936428, 124728716, 171282571, 431288888, 1485675219, 572927134, 1067290740, 1934184813, 571772628, 1689588160, 1524864013, 947166768 },
						tester.getHash("1").getVector32());
				Assertions.assertEquals(16, tester.provider.getNumberOfHashFunctions());
				Assertions.assertEquals(Hash.wrap(
						new byte[] { -86, -82, 113, -53, 59, 102, 25, 25, -60, 59, 116, -91, 42, 85, -5, 17, 61, 94, 74, 80, -112, 53, 13, -19, -65, -64, 32, -64, 104, 87, 37, -72, 6, 57, 93, -93, -122, -65, 34, -26, -86, 63, -64, -110, -36, 107, -38, 45, -10, 109,
								-100, 23, -100, 110, -99, 100, -116, 82, 75, -15, 99, -43, 20, 49 }),
						tester.getHash("Hello World"));
				Assertions.assertArrayEquals(new int[] { 1431408181, 828180999, 1553456089, 943376967, 1322679152, 2042465923, 1295595584, 202748264, 2074824663, 1141252658, 934176894, 624871723, 198388505, 1210251412, 929274167, 742611311 },
						tester.getHash("Hello World").getVector32());
		}

		@Test
		void stringHmacSha256ProviderTest() throws Exception {
				HashTester<String> tester = new HashTester<>(new HashStreamProviderHmacSHA256("password1".getBytes(StandardCharsets.UTF_8)), HashingSerializer.stringSerializer());
				Assertions.assertEquals(Hash.wrap(new byte[] { 56, 83, 10, 8, -50, 7, 69, 106, 19, 61, -29, -40, -15, -48, -126, -121, 43, 4, 30, -30, -18, 45, -53, -48, 104, -18, 94, -85, 8, 40, 24, -97 }),
						tester.getHash("1"));
				Assertions.assertArrayEquals(new int[] { 944966152, 220108842, 940446152, 1167792225, 967913462, 1167561936, 1185310361, 516051803 }, tester.getHash("1").getVector32());
				Assertions.assertEquals(8, tester.provider.getNumberOfHashFunctions());
				Assertions.assertEquals(Hash.wrap(new byte[] { 123, 122, -19, -83, 114, -95, -8, 117, 94, -125, 0, -72, -58, 74, 3, 3, 16, -7, -74, -98, -54, 26, -10, -108, 14, -24, -68, -121, 73, 115, 30, 116 }),
						tester.getHash("Hello World"));
				Assertions.assertArrayEquals(new int[] { 2071653805, 509798869, 1460725096, 1042646149, 638473078, 452150572, 372180979, 309852996 }, tester.getHash("Hello World").getVector32());

				HashTester<String> tester2 = new HashTester<>(new HashStreamProviderHmacSHA256("password2".getBytes(StandardCharsets.UTF_8)), HashingSerializer.stringSerializer());
				Assertions.assertNotEquals(tester.getHash("1"), tester2.getHash("1"));
		}

		@Test
		void stringHmacSha512ProviderTest() throws Exception {
				HashTester<String> tester = new HashTester<>(new HashStreamProviderHmacSHA512("password1".getBytes(StandardCharsets.UTF_8)), HashingSerializer.stringSerializer());
				Assertions.assertEquals(Hash.wrap(
						new byte[] { -90, 122, -120, 40, 120, -84, -117, 75, -124, -68, -88, 75, -8, 112, -26, -77, -51, -93, -99, 88, 95, 79, -14, 65, -55, -9, 62, -12, -122, -125, -120, 59, -19, 57, -25, 95, -5, 19, 31, -75, -112, 10, -33, -81, -28, 44, 123, -15,
								17, -114, 64, 90, 118, 24, -32, -92, -110, 31, 76, -32, -114, -107, 116, 81 }),
						tester.getHash("1"));
				Assertions.assertArrayEquals(new int[] { 1501919192, 1662722539, 793006807, 904596821, 1971622456, 2142075375, 802158460, 5268889, 2033442397, 1241428897, 217839223, 279926713, 1793448902, 1242617132, 857622048, 111219377 },
						tester.getHash("1").getVector32());
				Assertions.assertEquals(16, tester.provider.getNumberOfHashFunctions());
				Assertions.assertEquals(Hash.wrap(
						new byte[] { -62, 84, -52, 76, -58, -87, 29, 81, 107, -24, -13, 56, 25, 116, 52, 74, -57, -66, 76, 97, -98, -70, 45, -4, -98, -13, -21, -40, 100, -121, 123, -104, -60, -84, 3, 73, 59, -6, -70, 38, -9, 12, 56, 7, 100, -107, -113, -104, -107,
								-84, -40, -107, 71, -78, 9, -6, 52, 66, -126, -7, 45, 42, -120, -63 }),
						tester.getHash("Hello World"));
				Assertions.assertArrayEquals(new int[] { 1034630068, 242912463, 1733613800, 329015258, 1929980853, 607023428, 394275912, 725187592, 587413403, 1298221070, 2073851407, 1364856232, 810997243, 1052326926, 723486865, 488493345 },
						tester.getHash("Hello World").getVector32());

				HashTester<String> tester2 = new HashTester<>(new HashStreamProviderHmacSHA512("password2".getBytes(StandardCharsets.UTF_8)), HashingSerializer.stringSerializer());
				Assertions.assertNotEquals(tester.getHash("1"), tester2.getHash("1"));
		}

		@Test
		void stringCrc32ProviderTest() throws Exception {
				HashTester<String> tester = new HashTester<>(new HashStreamProviderCRC32(), HashingSerializer.stringSerializer());
				Assertions.assertEquals(Hash.wrap(new byte[] { -67, 3, 102, 31 }), tester.getHash("1"));
				Assertions.assertArrayEquals(new int[] { 1123850721 }, tester.getHash("1").getVector32());
				Assertions.assertEquals(1, tester.provider.getNumberOfHashFunctions());
				Assertions.assertEquals(Hash.wrap(new byte[] { 60, -93, 74, 57 }), tester.getHash("Hello World"));
				Assertions.assertArrayEquals(new int[] { 1017334329 }, tester.getHash("Hello World").getVector32());
		}

		@Test
		void integerSerializerTest() {
				HashTester<Integer> tester = new HashTester<>(new HashStreamProviderCRC32(), new HashingIntegerSerializer());

				Assertions.assertEquals(Hash.wrap(new byte[] { -86, 75, -18, 112 }), tester.getHash(12345));
		}

		@Test
		void gsonSerializerTest() {
				HashTester<TestObject> tester = new HashTester<>(new HashStreamProviderCRC32(), new HashingGsonSerializer<>());
				Assertions.assertEquals(Hash.wrap(new byte[] { -120, -113, -92, 81 }), tester.getHash(new TestObject("value1", "value2")));
				// Gson is not able to sort the fields in a consistent order when serializing the object. Order is affected by the code.
		}

		@Test
		void jacksonSerializerTest() {
				HashTester<TestObject> tester = new HashTester<>(new HashStreamProviderCRC32(), new HashingJacksonSerializer<>());
				HashTester<Map<String, Object>> tester2 = new HashTester<>(new HashStreamProviderCRC32(), new HashingJacksonSerializer<Map<String, Object>>());

				Map<String, Object> testObject2 = new LinkedHashMap<>();
				testObject2.put("field2", "value2");
				testObject2.put("field1", "value1");

				Assertions.assertEquals(Hash.wrap(new byte[] { -120, -113, -92, 81 }), tester.getHash(new TestObject("value1", "value2")));
				// Jackson is able to sort the fields in a consistent order when serializing the object.
				Assertions.assertEquals(tester.getHash(new TestObject("value1", "value2")), tester2.getHash(testObject2));
		}

		private class HashTester<T> {
				private final HashStreamProvider provider;
				private final HashingSerializer<T> serializer;

				public HashTester(HashStreamProvider provider, HashingSerializer<T> serializer) {
						this.provider = provider;
						this.serializer = serializer;
				}

				public Hash getHash(T value) {
						try (HashStream hashStream = provider.createHashStream()) {
								try (DataStream dataStream = new DataStream(hashStream)) {
										if (!serializer.serialize(dataStream, value)) {
												return null;
										}
								}
								return hashStream.getHashes();
						} catch (ClassCastException e) {
								return null;
						} catch (Exception e) {
								throw new HashStreamException("Serializing value via hashingSerializer failed", e);
						}
				}
		}
}
