package org.example.persistence.repository;

import org.example.file.JobStatus;
import org.example.file.MigrationRange;

import java.sql.SQLException;

public interface MigrationRangesRepository {
    /**
     * Saves the state of a given migration range.
     */
    void saveRange(int rangeId, JobStatus status) throws SQLException;

    /**
     * Picks the next available range to process.
     */
    MigrationRange pickRange() throws SQLException;

    /**
     * Seeds the range table based on the max attachment sequence in the source table(s).
     * If init is true, this represents the first seed -- otherwise, it will expand the range based on previous max.
     */
    void seedRanges(boolean init) throws SQLException;

    /**
     * Checks if the ranges have been seeded yet.
     */
    boolean isInitialized() throws SQLException;

    /**
     * Releases any ranges stuck in PROCESSING back to PENDING
     */
    void cleanup() throws SQLException;
}
