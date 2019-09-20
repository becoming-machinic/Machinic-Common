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

package com.becomingmachinic.common.data;

import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.Test;

class InMemoryBloomFilterTest {
	
	@Test
	void testSHA256HashProvider() throws Exception {
		try (BloomFilter<String> bloomFilter = new InMemoryBloomFilter<String>(100, 0.01, new StringSerializer(), new SHA256HashProvider())) {
			runTests(bloomFilter);
		}
	}
	
	@Test
	void testSHA512HashProvider() throws Exception {
		try (BloomFilter<String> bloomFilter = new InMemoryBloomFilter<String>(100, 0.01, new StringSerializer(), new SHA512HashProvider())) {
			runTests(bloomFilter);
		}
	}
	
	@Test
	void testHmacSHA256HashProvider() throws Exception {
		byte[] key = new byte[]{-57, -68, -10, -6, -117, -60, 105, 81, 112, -24, 82, -51, 59, -110, 61, -28, -90, 82, 25, -13, 30, 5, -34, -17, 70, -71, -78, -123, 114, 15, 125, -16};
		
		try (BloomFilter<String> bloomFilter = new InMemoryBloomFilter<String>(100, 0.01, new StringSerializer(), new HmacSHA256HashProvider(key))) {
			runTests(bloomFilter);
		}
	}
	
	@Test
	void testHmacSHA512HashProvider() throws Exception {
		byte[] key = new byte[] {-29, -47, 3, 116, -111, 27, 3, -38, -65, 3, -71, 53, 29, 73, 75, -46, 43, 40, 55, 63, -24, 28, -17, -15, 58, -47, 42, -64, -51, -128, -103, 52, -5, -51, 1, 17, -61, -94, 16, -77, -38, 120, -96, 102, -56, -109, -31, 28, 123, -67, 17, 6, 20, 9, 45, 79, -74, -114, -42, 113, 2, 104, 116, 35};
		
		try (BloomFilter<String> bloomFilter = new InMemoryBloomFilter<String>(100, 0.01, new StringSerializer(), new HmacSHA512HashProvider(key))) {
			runTests(bloomFilter);
		}
	}
	
	@Test
	void testFalsePositiveRate() throws Exception {
		double fpp = 0.01;
		int count = 100000;
		int falsePositives = 0;
		
		try (BloomFilter<String> bloomFilter = new InMemoryBloomFilter<String>(count, fpp, new StringSerializer(), new SHA256HashProvider())) {
			for(int i = 0; i < count;i++) {
				String value = String.format("test%s", i);
				if(!bloomFilter.add(value)) {
					falsePositives++;
				}
			}
			System.out.println(String.format("size: %s",bloomFilter.size()));
		}
		
		double falsePositiveRate = (falsePositives * 1.0d) / count;
		
		Assertions.assertTrue(falsePositiveRate <= fpp ,String.format("False positive rate %s is higher then requested rate %s",falsePositiveRate,fpp));
		System.out.println(String.format("falsePositiveRate: %s",falsePositiveRate));
	}
	
	private void runTests(BloomFilter<String> bloomFilter) throws MachinicDataException {
		for(int i = 0; i < 100;i++) {
			String value = String.format("test%s", i);
			Assertions.assertFalse(bloomFilter.contains(value),value);
			Assertions.assertTrue(bloomFilter.add(value),value);
			Assertions.assertTrue(bloomFilter.contains(value),value);
		}
	}
}
