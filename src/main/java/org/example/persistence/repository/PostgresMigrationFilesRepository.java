package org.example.persistence.repository;

import org.example.file.MigrationFile;
import org.example.file.MigrationStatus;
import org.example.persistence.database.Database;

import java.sql.*;
import java.util.ArrayList;
import java.util.List;

public class PostgresMigrationFilesRepository implements MigrationFilesRepository {
    private final static String SAVE = "INSERT INTO migration_files (file_id, old_uri, new_uri, file_name, create_date, status, migration_date) VALUES (?, ?, ?, ?, ?, ?, now()) ON CONFLICT (file_id) DO UPDATE SET new_uri = EXCLUDED.new_uri, status = EXCLUDED.status";
    private final static String SAVE_RETRIES = "UPDATE migration_files SET new_uri = ?, status = ?, retry_count = ?, last_attempt_date = NOW() WHERE file_id = ?";
    private final static String GET_FAILURES = "WITH retries AS (SELECT file_id FROM migration_files WHERE status = 'FAIL' AND retry_count < 3 ORDER BY last_attempt_date NULLS FIRST LIMIT 500 FOR UPDATE SKIP LOCKED) UPDATE migration_files SET status = 'RETRYING', last_attempt_date = NOW() FROM retries WHERE migration_files.file_id = retries.file_id RETURNING migration_files.*";
    private final Database database;

    public PostgresMigrationFilesRepository(Database database) {
        this.database = database;
    }

    @Override
    public void save(List<MigrationFile> records) throws SQLException {
        try (Connection conn = this.database.getDataSource().getConnection()) {
            conn.setAutoCommit(false);
            try {
                try (PreparedStatement statement = conn.prepareStatement(SAVE)) {
                    for (MigrationFile file : records) {
                        statement.setInt(1, file.getId());
                        statement.setString(2, file.getOldUri());
                        statement.setString(3, file.getNewUri());
                        statement.setString(4, file.getFileName());
                        statement.setTimestamp(5, Timestamp.valueOf(file.getCreateDate()));
                        statement.setString(6, file.getStatus().getValue());
                        statement.addBatch();
                    }
                    statement.executeBatch();
                }
                conn.commit();
            } catch (Exception e) {
                conn.rollback();
                throw e;
            }
        }
    }

    @Override
    public void saveRetries(List<MigrationFile> records) throws SQLException {
        try (Connection conn = this.database.getDataSource().getConnection()) {
            conn.setAutoCommit(false);
            try {
                try (PreparedStatement statement = conn.prepareStatement(SAVE_RETRIES)) {
                    for (MigrationFile file : records) {
                        statement.setString(1, file.getNewUri());
                        statement.setString(2, file.getStatus().getValue());
                        statement.setInt(3, file.getRetryCount());
                        statement.setInt(4, file.getId());
                        statement.addBatch();
                    }
                    statement.executeBatch();
                }
                conn.commit();
            } catch (Exception e) {
                conn.rollback();
                throw e;
            }
        }
    }

    @Override
    public List<MigrationFile> getFailures() throws SQLException {
        List<MigrationFile> files = new ArrayList<>();
        try (Connection conn = this.database.getDataSource().getConnection()) {
            try (PreparedStatement statement = conn.prepareStatement(GET_FAILURES)) {
                try (ResultSet resultSet = statement.executeQuery()) {
                    while (resultSet.next()) {
                        files.add(new MigrationFile(
                                resultSet.getInt("file_id"),
                                resultSet.getString("old_uri"),
                                resultSet.getString("new_uri"),
                                resultSet.getString("file_name"),
                                resultSet.getTimestamp("create_date").toLocalDateTime(),
                                MigrationStatus.from(resultSet.getString("status")),
                                resultSet.getInt("retry_count"),
                                resultSet.getTimestamp("last_attempt_date").toLocalDateTime()
                        ));
                    }
                }
            }
        }
        return files;
    }
}
