package org.example.persistence.database;

import com.zaxxer.hikari.HikariConfig;
import com.zaxxer.hikari.HikariDataSource;
import org.example.configuration.ConfigurationProperties;

import javax.sql.DataSource;

public class HikariDatabase implements Database {
    private final HikariDataSource dataSource;

    public HikariDatabase(ConfigurationProperties properties) {
        HikariConfig config = new HikariConfig();
        config.setJdbcUrl(properties.getString("DB_URL", null));
        config.setUsername(properties.getString("DB_USER", null));
        config.setPassword(properties.getString("DB_PASSWORD", null));

        config.setMaximumPoolSize(properties.getInteger("DB_POOL_SIZE", 10));
        config.setMinimumIdle(properties.getInteger("DB_MIN_IDLE", 2));
        config.setIdleTimeout(properties.getInteger("DB_IDLE_TIMEOUT", 30000));
        config.setConnectionTimeout(properties.getInteger("DB_CONN_TIMEOUT", 30000));
        config.setMaxLifetime(properties.getInteger("DB_MAX_LIFETIME", 1800000));

        this.dataSource = new HikariDataSource(config);
    }

    @Override
    public DataSource getDataSource() {
        return this.dataSource;
    }
}