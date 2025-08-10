package org.example.jobs;

import org.example.configuration.ConfigurationProperties;
import org.example.lock.Lock;
import org.example.persistence.repository.MigrationRangesRepository;

import java.util.UUID;
import java.util.concurrent.ScheduledExecutorService;

public class SeedJob extends AbstractJobRunner {
    private final Lock lock;
    private final MigrationRangesRepository rangesRepository;
    private final static String LOCK_KEY = "SEED_JOB";

    public SeedJob(ConfigurationProperties config, ScheduledExecutorService scheduler, Lock lock, MigrationRangesRepository rangesRepository) {
        super(config, scheduler);
        this.lock = lock;
        this.rangesRepository = rangesRepository;
    }

    @Override
    protected void process() {
        UUID executionId = UUID.randomUUID();
        try {
            try {
                if (this.lock.acquireLock(LOCK_KEY, executionId, 1800)) {
                    this.log.info("Running file migration seed job");
                    this.rangesRepository.seedRanges(!this.rangesRepository.isInitialized());
                }
            } catch (Exception e) {
                this.log.error("Failed to seed file migration ranges");
                this.log.error(e);
            } finally {
                this.lock.releaseLock(LOCK_KEY, executionId);
                this.schedule();
            }
        } catch (Exception e) {
            this.log.error("Encountered error running file migration seed job");
            this.log.error(e);
            this.schedule(300);
        }
    }

    @Override
    protected long getDefaultDelay() {
        return this.config.getInteger("SEED_DELAY", 3600);
    }

    @Override
    protected String getName() {
        return "Seed";
    }
}
