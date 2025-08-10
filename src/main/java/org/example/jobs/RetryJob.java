package org.example.jobs;

import org.apache.commons.collections4.CollectionUtils;
import org.example.configuration.ConfigurationProperties;
import org.example.file.MigrationFile;
import org.example.file.MigrationStatus;
import org.example.file.SourceFile;
import org.example.mover.FileMover;
import org.example.persistence.repository.MigrationFilesRepository;

import java.time.LocalDateTime;
import java.util.ArrayList;
import java.util.List;
import java.util.concurrent.ScheduledExecutorService;

public class RetryJob extends AbstractJobRunner {
    private final MigrationFilesRepository migrationFilesRepository;
    private final FileMover fileMover;

    public RetryJob(ConfigurationProperties config, ScheduledExecutorService scheduler, MigrationFilesRepository migrationFilesRepository, FileMover fileMover) {
        super(config, scheduler);
        this.migrationFilesRepository = migrationFilesRepository;
        this.fileMover = fileMover;
    }

    @Override
    protected void process() {
        try {
            this.log.info("Running file migration retry job");
            List<MigrationFile> failures = this.migrationFilesRepository.getFailures();
            if (CollectionUtils.isNotEmpty(failures)) {
                List<MigrationFile> processed = new ArrayList<>(failures.size());
                this.backoffCounter = 0;
                int failureCount = 0;
                for (MigrationFile record : failures) {
                    try {
                        this.fileMover.move(
                                new SourceFile(
                                        record.getId(),
                                        record.getFileName(),
                                        record.getOldUri(),
                                        LocalDateTime.now() // inconsequential at this stage; date only used in filter stage
                                )
                        );
                        processed.add(new MigrationFile(
                                record.getId(),
                                record.getOldUri(),
                                record.getNewUri(),
                                record.getFileName(),
                                record.getCreateDate(),
                                MigrationStatus.SUCCESS,
                                record.getRetryCount(),
                                LocalDateTime.now()
                        ));
                    } catch (Exception e) {
                        failureCount++;
                        this.log.error(String.format("Failed to migrate file %s:", record.getOldUri()));
                        this.log.error(e);
                        processed.add(new MigrationFile(
                                record.getId(),
                                record.getOldUri(),
                                record.getNewUri(),
                                record.getFileName(),
                                record.getCreateDate(),
                                MigrationStatus.FAIL,
                                record.getRetryCount() + 1,
                                LocalDateTime.now()
                        ));
                    }
                }
                this.log.info(String.format("Retried %d file migrations, %d failed", failures.size(), failureCount));
                this.migrationFilesRepository.saveRetries(processed);
            } else {
                this.log.info("No file migration failures detected at this time");
                this.backoffCounter++;
            }
        } catch (Exception e) {
            this.log.error("Encountered error running file migration retry job");
            this.log.error(e);
        } finally {
            this.schedule();
        }
    }

    @Override
    protected long getDefaultDelay() {
        return this.config.getInteger("RETRY_DELAY", 1800);
    }

    @Override
    protected String getName() {
        return "Retry";
    }
}
