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

import com.becomingmachinic.kafka.collections.CollectionConfig;
import com.becomingmachinic.kafka.collections.KafkaCollectionConfigurationException;

public class TopicConfigKey extends ConfigKey<String> {
	
	public TopicConfigKey() {
		super(CollectionConfig.COLLECTION_TOPIC,null);
	}
	
	@Override
	public String getValue(Map<? extends Object, Object> providedProperties) throws KafkaCollectionConfigurationException {
		String value = getAsString(providedProperties.get(this.name));
		if (value != null) {
			if (!value.replaceAll("[^0-9A-Za-z_-]+", "").equals(value)) {
				throw new KafkaCollectionConfigurationException("Parameter %s contains invalid characters. It should contain alphanumeric, hyphen and underscore only",this.name);
			}
		}
		if(value == null) {
			String name = new NameConfigKey().getValue(providedProperties);
			if(name != null){
				value = name.toLowerCase() + "_collection";
			}
			
			if(value == null) {
				throw new KafkaCollectionConfigurationException("Property %s or %s is required", CollectionConfig.COLLECTION_NAME,CollectionConfig.COLLECTION_TOPIC);
			}
		}
		return value;
	}
}