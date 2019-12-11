package com.becomingmachinic.kafka.collections.config;

import com.becomingmachinic.kafka.collections.KafkaCollectionConfigurationException;

import java.util.Map;

public class IntegerRangeConfigKey extends ConfigKey<Integer> {
	public static Integer NO_DEFAULT = Integer.MIN_VALUE;
	
	protected final Integer minValue;
	protected final Integer maxValue;
	
	public IntegerRangeConfigKey(String name, Integer defaultValue, Integer minValue, Integer maxValue) {
		super(name, defaultValue);
		this.minValue = minValue;
		this.maxValue = maxValue;
	}
	
	public Integer getValue(Map<? extends Object, Object> providedProperties) throws KafkaCollectionConfigurationException {
		Integer value = getAsInteger(providedProperties.get(this.name));
		if (value != null) {
			if (this.minValue != null && value < this.minValue) {
				throw new KafkaCollectionConfigurationException("Property %s has a value of %s which is less then the minimum required value of %s", this.name, Long.toString(value), Long.toString(this.minValue));
			}
			if (this.maxValue != null && value > this.maxValue) {
				throw new KafkaCollectionConfigurationException("Property %s has a value of %s which is greater then maximum required value of %s", this.name, Long.toString(value), Long.toString(this.maxValue));
			}
		} else {
			if(NO_DEFAULT.equals(this.defaultValue)){
				return null;
			}
			value = this.defaultValue;
		}
		if(value == null) {
			throw new KafkaCollectionConfigurationException("Parameter %s is required", this.name);
		}
		return value;
	}
}
