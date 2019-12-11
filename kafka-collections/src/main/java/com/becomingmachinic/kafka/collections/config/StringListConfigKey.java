package com.becomingmachinic.kafka.collections.config;

import com.becomingmachinic.kafka.collections.KafkaCollectionConfigurationException;

import java.util.List;
import java.util.Map;

public class StringListConfigKey extends ConfigKey<List<String>> {
	
	public StringListConfigKey(String name, List<String> defaultValue) {
		super(name, defaultValue);
	}
	
	public List<String> getValue(Map<? extends Object, Object> providedProperties) throws KafkaCollectionConfigurationException {
		List<String> value = getAsStringList(providedProperties.get(this.name));
		if(value == null || value.isEmpty()){
			value = this.defaultValue;
		}
		if(value == null) {
			throw new KafkaCollectionConfigurationException("Parameter %s is required", this.name);
		}
		return value;
	}
}
