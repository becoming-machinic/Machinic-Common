package com.becomingmachinic.kafka.collections.config;

import com.becomingmachinic.kafka.collections.KafkaCollectionConfigurationException;

import java.util.Map;

public class BooleanConfigKey extends ConfigKey<Boolean> {
	
	public BooleanConfigKey(String name, Boolean defaultValue) {
		super(name, defaultValue);
	}
	
	public Boolean getValue(Map<? extends Object, Object> providedProperties) throws KafkaCollectionConfigurationException {
		Boolean value = getAsBoolean(providedProperties.get(this.name));
		if (value == null) {
			value = this.defaultValue;
		}
		if(value == null) {
			throw new KafkaCollectionConfigurationException("Parameter %s is required", this.name);
		}
		return value;
	}
}
