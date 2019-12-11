package com.becomingmachinic.kafka.collections.config;

import com.becomingmachinic.kafka.collections.KafkaCollectionConfigurationException;

import java.util.List;
import java.util.Map;

public class StringEnumerationConfigKey extends ConfigKey<String> {
	
	protected final List<String> validValues;
	
	public StringEnumerationConfigKey(String name, String defaultValue, List<String> validValues) {
		super(name, defaultValue);
		this.validValues = validValues;
	}
	
	public String getValue(Map<? extends Object, Object> providedProperties) throws KafkaCollectionConfigurationException {
		String value = getAsString(providedProperties.get(this.name));
		if (value != null) {
			if (!this.validValues.contains(value)) {
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
