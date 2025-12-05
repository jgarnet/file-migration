package org.example;

import org.example.configuration.ConfigurationProperties;
import org.example.jobs.CleanupJob;
import org.example.jobs.MigrationJob;
import org.example.jobs.RetryJob;
import org.example.jobs.SeedJob;
import org.example.lock.Lock;
import org.example.logger.Logger;
import org.example.logger.SystemLogger;
import org.example.mover.FileMover;
import org.example.persistence.repository.FilesRepository;
import org.example.persistence.repository.MigrationFilesRepository;
import org.example.persistence.repository.MigrationRangesRepository;

import java.util.List;
import java.util.UUID;
import java.util.concurrent.Executors;
import java.util.concurrent.ScheduledExecutorService;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicBoolean;

public class GlobalScheduler implements Runnable {
    private static final String GLOBAL_KEY = "MIGRATION_GLOBAL_LOCK";
    private static final UUID GLOBAL_LOCK = UUID.randomUUID();
    private final Logger log = new SystemLogger();
    private final ConfigurationProperties config;
    private final Lock lock;
    private final FilesRepository filesRepository;
    private final MigrationFilesRepository migrationFilesRepository;
    private final MigrationRangesRepository rangesRepository;
    private final FileMover fileMover;
    private final ScheduledExecutorService globalScheduler;
    private ScheduledExecutorService jobScheduler;
    private ScheduledExecutorService seedScheduler;
    private ScheduledExecutorService retryScheduler;
    private ScheduledExecutorService cleanupScheduler;
    private boolean schedulersInitialized = false;
    private final AtomicBoolean shutdown;

    public GlobalScheduler(ConfigurationProperties config, Lock lock, FilesRepository filesRepository, MigrationFilesRepository migrationFilesRepository, MigrationRangesRepository rangesRepository, FileMover fileMover) {
        this.shutdown = new AtomicBoolean(false);
        this.config = config;
        this.lock = lock;
        this.filesRepository = filesRepository;
        this.migrationFilesRepository = migrationFilesRepository;
        this.rangesRepository = rangesRepository;
        this.fileMover = fileMover;
        this.globalScheduler = Executors.newSingleThreadScheduledExecutor();
    }

    @Override
    public void run() {
        if (this.config.getBoolean("ENABLE_GLOBAL_LOCKING", false)) {
            try {
                this.log.info("Running global schedule check");
                // only run job on one container during a given period to reduce load if needed
                int globalJobTtl = this.config.getInteger("GLOBAL_LOCK_TTL", 7_200);
                if (this.lock.acquireLock(GLOBAL_KEY, GLOBAL_LOCK, globalJobTtl)) {
                    if (!this.schedulersInitialized) {
                        // only initialize schedulers if not already initialized
                        // i.e. account for edge-case where container A has job lock, then re-acquires it again
                        this.log.info("Acquired global lock; scheduling jobs");
                        this.globalScheduler.submit(this::scheduleJobs);
                    } else {
                        this.log.info("Already have global lock; continuing migration");
                    }
                    // ensure after global rotating window, the global scheduler will either keep job lock or
                    // transition to another container
                    this.globalScheduler.schedule(this, globalJobTtl, TimeUnit.SECONDS);
                } else {
                    this.log.info(String.format("Global schedule lock already acquired; sleeping for %d seconds", globalJobTtl));
                    // job already locked by another container; wait to check again until lock period is over
                    this.globalScheduler.schedule(this, globalJobTtl, TimeUnit.SECONDS);
                    // in case this container had previous lock, shut down schedulers
                    this.shutdownSchedulers();
                }
            } catch (Exception e) {
                this.log.error(e);
                this.globalScheduler.schedule(this, 300, TimeUnit.SECONDS);
            }
        } else {
            this.globalScheduler.submit(this::scheduleJobs);
        }
    }

    private void scheduleJobs() {
        this.schedulersInitialized = true;

        // Schedulers
        int jobThreads = config.getInteger("JOB_THREADS", 20);
        this.jobScheduler = Executors.newScheduledThreadPool(jobThreads);
        int retryThreads = config.getInteger("RETRY_THREADS", 5);
        this.retryScheduler = Executors.newScheduledThreadPool(retryThreads);
        this.seedScheduler = Executors.newSingleThreadScheduledExecutor();
        this.cleanupScheduler = Executors.newSingleThreadScheduledExecutor();

        // Initialize
        this.seedScheduler.submit(new SeedJob(
                this.config,
                this.seedScheduler,
                this.lock,
                this.rangesRepository,
                this.shutdown
        ));
        for (int i = 0; i < jobThreads; i++) {
            this.jobScheduler.schedule(new MigrationJob(
                    this.config,
                    this.jobScheduler,
                    this.shutdown,
                    this.rangesRepository,
                    this.migrationFilesRepository,
                    this.filesRepository,
                    this.fileMover,
                    this.lock,
                    new BackoffCounter(List.of(10, 30, 60, 300))
            ), 5, TimeUnit.SECONDS);
        }
        for (int i = 0; i < retryThreads; i++) {
            this.retryScheduler.schedule(new RetryJob(
                    this.config,
                    this.retryScheduler,
                    this.migrationFilesRepository,
                    this.fileMover,
                    this.shutdown
            ), 30, TimeUnit.SECONDS);
        }
        this.cleanupScheduler.schedule(new CleanupJob(
                this.config,
                this.cleanupScheduler,
                this.lock,
                this.rangesRepository,
                this.shutdown
        ), 60, TimeUnit.SECONDS);
    }

    private void shutdownSchedulers() {
        if (this.schedulersInitialized) {
            this.jobScheduler.shutdown();
            this.seedScheduler.shutdown();
            this.retryScheduler.shutdown();
            this.cleanupScheduler.shutdown();
            this.schedulersInitialized = false;
        }
    }
}
