package org.example.jobs;

import org.apache.commons.collections4.CollectionUtils;
import org.example.configuration.ConfigurationProperties;
import org.example.file.*;
import org.example.lock.Lock;
import org.example.mover.FileMover;
import org.example.persistence.repository.FilesRepository;
import org.example.persistence.repository.MigrationFilesRepository;
import org.example.persistence.repository.MigrationRangesRepository;

import java.time.LocalDateTime;
import java.util.ArrayList;
import java.util.List;
import java.util.UUID;
import java.util.concurrent.ScheduledExecutorService;
import java.util.concurrent.atomic.AtomicBoolean;

public class MigrationJob extends AbstractJobRunner {
    private final MigrationRangesRepository rangesRepository;
    private final MigrationFilesRepository migrationFilesRepository;
    private final FilesRepository filesRepository;
    private final FileMover fileMover;
    private final Lock lock;

    public MigrationJob(ConfigurationProperties config, ScheduledExecutorService scheduler, MigrationRangesRepository rangesRepository, MigrationFilesRepository migrationFilesRepository, FilesRepository filesRepository, FileMover fileMover, Lock lock, AtomicBoolean shutdown) {
        super(config, scheduler, shutdown);
        this.rangesRepository = rangesRepository;
        this.migrationFilesRepository = migrationFilesRepository;
        this.filesRepository = filesRepository;
        this.fileMover = fileMover;
        this.lock = lock;
    }

    protected void process() {
        try {
            UUID executionId = UUID.randomUUID();
            MigrationRange job = this.rangesRepository.pickRange();
            String jobKey = String.format("FILE_MIGRATION_%d", job != null ? job.getRangeId() : -1);
            if (job != null && this.lock.acquireLock(jobKey, executionId, 1800)) {
                this.log.info("Running file migration job");
                try {
                    this.rangesRepository.saveRange(job.getRangeId(), JobStatus.PROCESSING);
                    int recordsProcessed = this.run(job.getMinId(), job.getMaxId());
                    this.log.info(String.format("Processed %d records", recordsProcessed));
                    // if no attachments processed this time, last processed id should inherit from last successful run
                    this.rangesRepository.saveRange(job.getRangeId(), JobStatus.COMPLETE);
                    // reset backoff as long as ranges are available
                    this.backoffCounter = 0;
                } catch (Exception e) {
                    this.log.error("Failed to execute file migration job");
                    this.log.error(e);
                    // release the job back to PENDING if possible
                    try {
                        this.rangesRepository.saveRange(job.getRangeId(), JobStatus.PENDING);
                    } catch (Exception ex) {
                        this.log.error(ex);
                    }
                } finally {
                    this.lock.releaseLock(jobKey, executionId);
                    this.schedule();
                }
            } else {
                // better luck next time
                if (job == null) {
                    this.log.info("No migration ranges available");
                    // no jobs available in db -- increment backoff counter
                    this.backoffCounter++;
                }
                this.schedule();
            }
        } catch (Exception e) {
            this.log.error("Encountered issue running migration job");
            this.log.error(e);
            // most likely Redis or DB connection issue -- it's possible schedule got skipped
            // schedule retry again after five minutes, just in case
            this.schedule(300);
        }
    }

    private int run(int minId, int maxId) throws Exception {
        List<SourceFile> sourceFiles = filesRepository.getSourceFiles(minId, maxId);
        if (CollectionUtils.isNotEmpty(sourceFiles)) {
            List<MigrationFile> records = new ArrayList<>(sourceFiles.size());
            int failures = 0;
            for (SourceFile sourceFile : sourceFiles) {
                try {
                    String newUri = this.fileMover.move(sourceFile);
                    records.add(new MigrationFile(
                            sourceFile.getId(),
                            sourceFile.getUri(),
                            newUri,
                            sourceFile.getName(),
                            sourceFile.getCreateDate(),
                            MigrationStatus.SUCCESS,
                            0,
                            LocalDateTime.now()
                    ));
                } catch (Exception e) {
                    failures++;
                    this.log.error(String.format("Failed to migrate file %s:", sourceFile.getUri()));
                    this.log.error(e);
                    records.add(new MigrationFile(
                            sourceFile.getId(),
                            sourceFile.getUri(),
                            null,
                            sourceFile.getName(),
                            sourceFile.getCreateDate(),
                            MigrationStatus.FAIL,
                            0,
                            LocalDateTime.now()
                    ));
                }
            }
            this.log.info(String.format("Processed %d file migrations, %d failed", records.size(), failures));
            this.migrationFilesRepository.save(records);
            return sourceFiles.size();
        }
        return 0;
    }

    @Override
    protected long getDefaultDelay() {
        return this.config.getInteger("MIGRATION_DELAY", 0);
    }

    @Override
    protected String getName() {
        return "Migration";
    }
}
