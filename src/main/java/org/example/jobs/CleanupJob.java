package org.example.jobs;

import org.example.configuration.ConfigurationProperties;
import org.example.lock.Lock;
import org.example.persistence.repository.MigrationRangesRepository;

import java.util.UUID;
import java.util.concurrent.ScheduledExecutorService;

public class CleanupJob extends AbstractJobRunner {
    private final static String JOB_KEY = "CLEANUP_JOB";
    private final Lock lock;
    private final MigrationRangesRepository rangesRepository;

    public CleanupJob(ConfigurationProperties config, ScheduledExecutorService scheduler, Lock lock, MigrationRangesRepository rangesRepository) {
        super(config, scheduler);
        this.lock = lock;
        this.rangesRepository = rangesRepository;
    }

    @Override
    protected void process() {
        UUID executionId = UUID.randomUUID();
        try {
            if (this.lock.acquireLock(JOB_KEY, executionId, 1800)) {
                this.log.info("Running file migration cleanup job");
                try {
                    this.rangesRepository.cleanup();
                } catch (Exception e) {
                    this.log.error("Failed to run file migration cleanup job");
                    this.log.error(e);
                } finally {
                    this.lock.releaseLock(JOB_KEY, executionId);
                }
            }
        } catch (Exception e) {
            this.log.error("Encountered error running file migration cleanup job");
            this.log.error(e);
        } finally {
            this.schedule();
        }
    }

    @Override
    protected long getDefaultDelay() {
        return this.config.getInteger("CLEANUP_DELAY", 3600);
    }
}
