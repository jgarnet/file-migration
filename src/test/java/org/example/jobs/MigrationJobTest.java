package org.example.jobs;

import org.example.BackoffCounter;
import org.example.configuration.ConfigurationProperties;
import org.example.file.JobStatus;
import org.example.file.MigrationRange;
import org.example.lock.Lock;
import org.example.mover.FileMover;
import org.example.persistence.repository.FilesRepository;
import org.example.persistence.repository.MigrationFilesRepository;
import org.example.persistence.repository.MigrationRangesRepository;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.mockito.Mock;
import org.mockito.Mockito;

import java.time.LocalDateTime;
import java.util.UUID;
import java.util.concurrent.ScheduledExecutorService;
import java.util.concurrent.atomic.AtomicBoolean;

import static org.mockito.ArgumentMatchers.*;
import static org.mockito.Mockito.times;
import static org.mockito.Mockito.verify;
import static org.mockito.MockitoAnnotations.openMocks;

public class MigrationJobTest {
    @Mock
    private ConfigurationProperties config;
    @Mock
    private ScheduledExecutorService scheduler;
    @Mock
    private MigrationRangesRepository rangesRepository;
    @Mock
    private MigrationFilesRepository migrationFilesRepository;
    @Mock
    private FilesRepository filesRepository;
    @Mock
    private FileMover fileMover;
    @Mock
    private Lock lock;
    @Mock
    private BackoffCounter lockCounter;

    @BeforeEach
    void setup() {
        openMocks(this);
    }

    @Test
    public void testCannotAcquireLock() throws Exception {
        Mockito.when(config.getBoolean(eq("ENABLE_JOB"), anyBoolean())).thenReturn(true);
        Mockito.when(config.getBoolean(eq("AFTER_HOURS"), anyBoolean())).thenReturn(false);
        Mockito.when(rangesRepository.pickRange()).thenReturn(new MigrationRange(
                1,
                1,
                10,
                JobStatus.PENDING,
                LocalDateTime.now()
        ));
        Mockito.when(
                lock.acquireLock(anyString(), any(UUID.class), anyInt())
        ).thenReturn(false);
        MigrationJob job = new MigrationJob(
                config,
                scheduler,
                new AtomicBoolean(),
                rangesRepository,
                migrationFilesRepository,
                filesRepository,
                fileMover,
                lock,
                lockCounter
        );
        job.run();
        verify(lockCounter, times(1)).increment();
    }
}
