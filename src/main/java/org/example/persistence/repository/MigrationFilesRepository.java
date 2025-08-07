package org.example.persistence.repository;

import org.example.file.MigrationFile;

import java.sql.SQLException;
import java.util.List;

/**
 * Abstraction layer for managing the state of individual attachment migrations.
 */
public interface MigrationFilesRepository {
    void save(List<MigrationFile> records) throws SQLException;
    void saveRetries(List<MigrationFile> records) throws SQLException;
    List<MigrationFile> getFailures() throws SQLException;
}