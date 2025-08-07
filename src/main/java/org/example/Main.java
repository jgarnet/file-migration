package org.example;

import org.example.configuration.ConfigurationProperties;
import org.example.configuration.SystemConfigurationProperties;
import org.example.jobs.CleanupJob;
import org.example.jobs.MigrationJob;
import org.example.jobs.RetryJob;
import org.example.jobs.SeedJob;
import org.example.lock.Lock;
import org.example.mover.FileMover;
import org.example.mover.StubFileMover;
import org.example.persistence.database.Database;
import org.example.persistence.database.HikariDatabase;
import org.example.persistence.repository.*;
import redis.clients.jedis.JedisPool;
import redis.clients.jedis.JedisPoolConfig;

import java.util.concurrent.Executors;
import java.util.concurrent.ScheduledExecutorService;
import java.util.concurrent.TimeUnit;

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

        // Schedulers
        int jobThreads = config.getInteger("JOB_THREADS", 20);
        ScheduledExecutorService jobScheduler = Executors.newScheduledThreadPool(jobThreads);
        int retryThreads = config.getInteger("RETRY_THREADS", 5);
        ScheduledExecutorService retryScheduler = Executors.newScheduledThreadPool(retryThreads);
        ScheduledExecutorService seedScheduler = Executors.newSingleThreadScheduledExecutor();
        ScheduledExecutorService cleanupScheduler = Executors.newSingleThreadScheduledExecutor();

        // jobs
        MigrationJob migrationJob = new MigrationJob(
                config,
                jobScheduler,
                rangesRepository,
                migrationFilesRepository,
                filesRepository,
                fileMover,
                lock
        );
        SeedJob seedJob = new SeedJob(
                config,
                seedScheduler,
                lock,
                rangesRepository
        );
        RetryJob retryJob = new RetryJob(
                config,
                retryScheduler,
                migrationFilesRepository,
                fileMover
        );
        CleanupJob cleanupJob = new CleanupJob(
                config,
                cleanupScheduler,
                lock,
                rangesRepository
        );

        // Initialize
        seedScheduler.submit(seedJob);
        for (int i = 0; i < jobThreads; i++) {
            jobScheduler.schedule(migrationJob, 30, TimeUnit.SECONDS);
        }
        for (int i = 0; i < retryThreads; i++) {
            retryScheduler.schedule(retryJob, 30, TimeUnit.SECONDS);
        }
        cleanupScheduler.schedule(cleanupJob, 60, TimeUnit.SECONDS);
    }
}