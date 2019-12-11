package com.becomingmachinic.kafka.collections.config;

import com.becomingmachinic.kafka.collections.KafkaCollectionConfigurationException;

import java.util.Map;

public class LongRangeConfigKey extends ConfigKey<Long> {
	public static Long NO_DEFAULT = Long.MIN_VALUE;
	protected final Long minValue;
	protected final Long maxValue;
	
	public LongRangeConfigKey(String name, Long defaultValue, Long minValue, Long maxValue) {
		super(name, defaultValue);
		this.minValue = minValue;
		this.maxValue = maxValue;
	}
	
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
