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
