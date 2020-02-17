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

import java.util.ArrayList;
import java.util.List;
import java.util.Map;

import com.becomingmachinic.kafka.collections.KafkaCollectionConfigurationException;

public class StringEnumerationConfigKey extends ConfigKey<String> {
	
	protected final List<String> validValues;
	
	public StringEnumerationConfigKey(String name, String defaultValue, List<String> validValues) {
		super(name, defaultValue);
		this.validValues = new ArrayList<>();
		for(String value : validValues){
			this.validValues.add(value.toLowerCase());
		}
	}
	
	@Override
	public String getValue(Map<? extends Object, Object> providedProperties) throws KafkaCollectionConfigurationException {
		String value = getAsString(providedProperties.get(this.name));
		if (value != null) {
			if (!this.validValues.contains(value.toLowerCase())) {
				throw new KafkaCollectionConfigurationException("Property %s has a value of %s which is not valid. Value should be one of %s", this.name, value, String.join(", ", validValues));
			}
		} else {
			value = this.defaultValue;
		}
		if(value == null) {
			throw new KafkaCollectionConfigurationException("Parameter %s is required", this.name);
		}
		return value;
	}
}
