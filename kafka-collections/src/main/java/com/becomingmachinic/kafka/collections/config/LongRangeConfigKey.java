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

package com.becomingmachinic.kafka.collections.config;

import java.util.Map;

import com.becomingmachinic.kafka.collections.KafkaCollectionConfigurationException;

public class LongRangeConfigKey extends ConfigKey<Long> {
	public static Long NO_DEFAULT = Long.MIN_VALUE;
	protected final Long minValue;
	protected final Long maxValue;
	
	public LongRangeConfigKey(String name, Long defaultValue, Long minValue, Long maxValue) {
		super(name, defaultValue);
		this.minValue = minValue;
		this.maxValue = maxValue;
	}
	
	@Override
	public Long getValue(Map<? extends Object, Object> providedProperties) throws KafkaCollectionConfigurationException {
		Long value = getAsLong(providedProperties.get(this.name));
		if (value != null) {
			if (this.minValue != null && value < this.minValue) {
				throw new KafkaCollectionConfigurationException("Property %s has a value of %s which is less then the minimum required value of %s", this.name, Long.toString(value), Long.toString(this.minValue));
			}
			if (this.maxValue != null && value > this.maxValue) {
				throw new KafkaCollectionConfigurationException("Property %s has a value of %s which is greater then maximum required value of %s", this.name, Long.toString(value), Long.toString(this.maxValue));
			}
		} else {
			if(NO_DEFAULT.equals(defaultValue)){
				return null;
			}
			value = this.defaultValue;
		}
		if(value == null) {
			throw new KafkaCollectionConfigurationException("Property %s is missing", this.name);
		}
		return value;
	}
}
