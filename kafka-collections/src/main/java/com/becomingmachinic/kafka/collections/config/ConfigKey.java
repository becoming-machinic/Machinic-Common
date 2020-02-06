package com.becomingmachinic.kafka.collections.config;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collection;
import java.util.List;
import java.util.Map;

import com.becomingmachinic.kafka.collections.KafkaCollectionConfigurationException;

public abstract class ConfigKey<T> {
	protected final String name;
	protected final T defaultValue;
	
	public ConfigKey(String name, T defaultValue) {
		this.name = name;
		this.defaultValue = defaultValue;
	}
	
	public String getName() {
		return name;
	}
	public Object getDefaultValue() {
		return defaultValue;
	}
	
	public abstract T getValue(Map<? extends Object, Object> providedProperties) throws KafkaCollectionConfigurationException;
	
	protected static Boolean getAsBoolean(Object value) {
		if (value != null) {
			if (value instanceof Boolean) {
				return (Boolean) value;
			} else if (value instanceof String) {
				String stringValue = (String) value;
				if ("true".equalsIgnoreCase(stringValue)) {
					return Boolean.TRUE;
				} else if ("false".equalsIgnoreCase(stringValue)) {
					return Boolean.FALSE;
				}
			}
		}
		return null;
	}
	
	protected static Long getAsLong(Object value) {
		if (value != null) {
			if (value instanceof Long) {
				return (Long) value;
			} else if (value instanceof Integer) {
				return ((Integer) value).longValue();
			} else if (value instanceof String) {
				try {
					return Long.parseLong((String) value);
				} catch (Exception e) {
				}
			}
		}
		return null;
	}
	
	protected static Integer getAsInteger(Object value) {
		if (value != null) {
			if (value instanceof Integer) {
				return (Integer) value;
			} else if (value instanceof Long) {
				return ((Long) value).intValue();
			} else if (value instanceof String) {
				try {
					return Integer.parseInt((String) value);
				} catch (Exception e) {
				}
			}
		}
		return null;
	}
	
	protected static Short getAsShort(Object value) {
		if (value != null) {
			if (value instanceof Short) {
				return (Short) value;
			} else if (value instanceof Integer) {
				return ((Integer) value).shortValue();
			} else if (value instanceof Long) {
				return ((Long) value).shortValue();
			} else if (value instanceof String) {
				try {
					return Short.parseShort((String) value);
				} catch (Exception e) {
				}
			}
		}
		return null;
	}
	
	protected static String getAsString(Object value) {
		if (value != null) {
			if (value instanceof String) {
				return (String) value;
			}
		}
		return null;
	}
	
	protected static List<String> getAsStringList(Object value) {
		if (value != null) {
			if (value instanceof String) {
				return Arrays.asList((String) value);
			} else if( value instanceof String[]){
				return Arrays.asList((String[]) value);
			} else if(value instanceof Collection){
				List<String> stringList = new ArrayList<>();
				for (Object valueEntry : ((Collection<?>) value)) {
					if(valueEntry instanceof String){
						stringList.add((String)valueEntry);
					}
				}
				return stringList;
			}
		}
		return null;
	}
}
