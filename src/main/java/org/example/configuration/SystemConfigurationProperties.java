package org.example.configuration;

import java.util.Map;
import java.util.concurrent.ConcurrentHashMap;

public class SystemConfigurationProperties implements ConfigurationProperties {
    private final Map<String, Object> cache = new ConcurrentHashMap<>();

    @Override
    public Integer getInteger(String name, Integer defaultValue) {
        String key = name + "_INT";
        if (this.cache.containsKey(key)) {
            return (Integer) this.cache.get(key);
        }
        String value = this.getProperty(name);
        if (value == null) {
            this.cache.put(key, defaultValue);
            return defaultValue;
        }
        value = value.replaceAll("_", "").replaceAll(",", "");
        Integer result = Integer.parseInt(value);
        this.cache.put(key, result);
        return result;
    }

    @Override
    public String getString(String name, String defaultValue) {
        String key = name + "_STR";
        if (this.cache.containsKey(key)) {
            return (String) this.cache.get(key);
        }
        String value = this.getProperty(name);
        if (value == null) {
            this.cache.put(key, defaultValue);
            return defaultValue;
        }
        this.cache.put(key, value);
        return value;
    }

    @Override
    public Boolean getBoolean(String name, Boolean defaultValue) {
        String key = name + "_BOOL";
        if (this.cache.containsKey(key)) {
            return (Boolean) this.cache.get(key);
        }
        String value = this.getProperty(name);
        if (value == null) {
            this.cache.put(key, defaultValue);
            return defaultValue;
        }
        Boolean result = Boolean.parseBoolean(value);
        this.cache.put(key, result);
        return result;
    }

    private String getProperty(String name) {
        String value = System.getenv(name);
        if (value == null) {
            value = System.getProperty(name);
        }
        return value;
    }
}
