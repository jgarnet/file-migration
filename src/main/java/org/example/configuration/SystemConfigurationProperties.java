package org.example.configuration;

public class SystemConfigurationProperties implements ConfigurationProperties {
    @Override
    public Integer getInteger(String name, Integer defaultValue) {
        String value = this.getProperty(name);
        if (value == null) {
            return defaultValue;
        }
        value = value.replaceAll("_", "").replaceAll(",", "");
        return Integer.parseInt(value);
    }

    @Override
    public String getString(String name, String defaultValue) {
        String value = this.getProperty(name);
        if (value == null) {
            return defaultValue;
        }
        return value;
    }

    @Override
    public Boolean getBoolean(String name, Boolean defaultValue) {
        String value = this.getProperty(name);
        if (value == null) {
            return defaultValue;
        }
        return Boolean.parseBoolean(value);
    }

    private String getProperty(String name) {
        String value = System.getenv(name);
        if (value == null) {
            value = System.getProperty(name);
        }
        return value;
    }
}
