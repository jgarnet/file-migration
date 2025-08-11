package org.example;

import org.example.configuration.ConfigurationProperties;
import org.example.configuration.SystemConfigurationProperties;
import org.example.lock.Lock;
import org.example.mover.FileMover;
import org.example.mover.StubFileMover;
import org.example.persistence.database.Database;
import org.example.persistence.database.HikariDatabase;
import org.example.persistence.repository.*;
import redis.clients.jedis.JedisPool;
import redis.clients.jedis.JedisPoolConfig;

public class Main {
    public static void main(String[] args) {
        // Dependencies
        ConfigurationProperties config = new SystemConfigurationProperties();
        Database database = new HikariDatabase(config);

        JedisPoolConfig jedisPoolConfig = new JedisPoolConfig();
        jedisPoolConfig.setMaxTotal(25);
        jedisPoolConfig.setMaxIdle(10);
        jedisPoolConfig.setMinIdle(2);
        jedisPoolConfig.setTestOnBorrow(true);
        JedisPool jedisPool = new JedisPool(jedisPoolConfig, config.getString("JEDIS_URL", "redis"), 6379, 2000);
        Lock lock = new Lock(jedisPool::getResource);

        FilesRepository filesRepository = new PostgresFilesRepository(database);
        MigrationFilesRepository migrationFilesRepository = new PostgresMigrationFilesRepository(database);
        MigrationRangesRepository rangesRepository = new PostgresMigrationRangesRepository(database, config);
        FileMover fileMover = new StubFileMover();

        // Global Scheduler
        GlobalScheduler globalScheduler = new GlobalScheduler(
                config,
                lock,
                filesRepository,
                migrationFilesRepository,
                rangesRepository,
                fileMover
        );
        globalScheduler.run();
    }
}