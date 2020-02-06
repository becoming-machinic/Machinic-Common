package com.becomingmachinic.kafka.collections.config;

import java.util.Arrays;
import java.util.HashMap;
import java.util.LinkedHashMap;
import java.util.Map;
import java.util.Map.Entry;
import java.util.Set;

public class ConfigDef {
	protected final Map<String,ConfigKey<?>> configDefMap = new LinkedHashMap<>();
	
	public ConfigDef(ConfigKey<?>... configKeys){
		for(ConfigKey<?> configKey : Arrays.asList(configKeys)){
			this.configDefMap.put(configKey.getName(),configKey);
		}
	}
	
	public Map<String,Object> getConfigMap(Map<? extends Object,Object> providedProperties){
		Map<String,Object> configMap = new HashMap<>();
		for(Entry<String,ConfigKey<?>> entry : this.configDefMap.entrySet()){
			configMap.put(entry.getKey(),entry.getValue().getValue(providedProperties));
		}
		return configMap;
	}
	
	public Set<String> getKeys(){
		return this.configDefMap.keySet();
	}
}
