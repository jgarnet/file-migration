package org.example.configuration;

public interface ConfigurationProperties {
    Integer getInteger(String name, Integer defaultValue);
    String getString(String name, String defaultValue);
    Boolean getBoolean(String name, Boolean defaultValue);
}
