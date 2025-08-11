package org.example.persistence.repository;

import org.example.configuration.ConfigurationProperties;
import org.example.file.JobStatus;
import org.example.file.MigrationRange;
import org.example.persistence.database.Database;

import java.sql.Connection;
import java.sql.PreparedStatement;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.time.LocalDate;
import java.time.LocalDateTime;

public class PostgresMigrationRangesRepository implements MigrationRangesRepository {
    private final static String IS_INITIALIZED = "SELECT COUNT(*) AS total FROM migration_ranges";
    private final static String SEED_RANGES = "INSERT INTO migration_ranges (min_id, max_id) SELECT series.min_id, LEAST(series.min_id + ?, ?) FROM generate_series(?, ?, ?) AS series(min_id)";
    private final static String GET_FILES_RANGE = "SELECT MIN(file_id) as min_id, MAX(file_id) as max_id FROM source_files WHERE create_date >= ?";
    private final static String PICK_RANGE = "SELECT * FROM migration_ranges WHERE status = 'PENDING' ORDER BY range_id LIMIT 1";
    private final static String SAVE_RANGE = "UPDATE migration_ranges SET status = ?, last_updated = NOW() WHERE range_id = ?";
    private final static String GET_MAX_RANGE = "SELECT max(max_id) AS max_id FROM migration_ranges";
    private final static String GET_NEXT_MAX = "SELECT max(file_id) AS max_id FROM source_files WHERE file_id > ?";
    private final static String CLEANUP = "UPDATE migration_ranges SET status = 'PENDING' WHERE status = 'PROCESSING' AND last_updated < NOW() - INTERVAL '60 minutes'";
    private final Database database;
    private final int retentionPeriod;
    private final int batchSize;

    public PostgresMigrationRangesRepository(Database database, ConfigurationProperties config) {
        this.database = database;
        this.retentionPeriod = config.getInteger("RETENTION_PERIOD", 525_600);
        this.batchSize = config.getInteger("BATCH_SIZE", 10_000);
    }

    @Override
    public void saveRange(int rangeId, JobStatus status) throws SQLException {
        try (Connection connection = this.database.getDataSource().getConnection()) {
            try (PreparedStatement statement = connection.prepareStatement(SAVE_RANGE)) {
                statement.setString(1, status.getValue());
                statement.setInt(2, rangeId);
                statement.executeUpdate();
            }
        }
    }

    @Override
    public MigrationRange pickRange() throws SQLException {
        try (Connection conn = this.database.getDataSource().getConnection()) {
            try (PreparedStatement statement = conn.prepareStatement(PICK_RANGE)) {
                try (ResultSet resultSet = statement.executeQuery()) {
                    if (resultSet.next()) {
                        return new MigrationRange(
                                resultSet.getInt("range_id"),
                                resultSet.getInt("min_id"),
                                resultSet.getInt("max_id"),
                                JobStatus.from(resultSet.getString("status")),
                                resultSet.getTimestamp("last_updated").toLocalDateTime()
                        );
                    }
                }
            }
        }
        return null;
    }

    @Override
    public void seedRanges(boolean init) throws SQLException {
        try (Connection conn = this.database.getDataSource().getConnection()) {
            conn.setAutoCommit(false);
            try {
                if (init) {
                    // initializing the migration ranges based on current file size
                    java.sql.Date minDate = java.sql.Date.valueOf(this.getMinDate());
                    int min = -1, max = -1;
                    try (PreparedStatement statement = conn.prepareStatement(GET_FILES_RANGE)) {
                        statement.setDate(1, minDate);
                        try (ResultSet resultSet = statement.executeQuery()) {
                            if (resultSet.next()) {
                                min = resultSet.getInt("min_id");
                                max = resultSet.getInt("max_id");
                            }
                        }
                    }
                    // the algorithm assumes 0 is not a valid ID
                    if (min > 0 && max > 0) {
                        this.runSeedQuery(conn, min, max);
                    }
                } else {
                    int currentMax = -1;
                    // expanding the attachment ranges based on current attachment size
                    try (PreparedStatement statement = conn.prepareStatement(GET_MAX_RANGE)) {
                        try (ResultSet resultSet = statement.executeQuery()) {
                            if (resultSet.next()) {
                                currentMax = resultSet.getInt("max_id");
                            }
                        }
                    }
                    int nextMax = -1;
                    try (PreparedStatement statement = conn.prepareStatement(GET_NEXT_MAX)) {
                        statement.setInt(1, currentMax);
                        try (ResultSet resultSet = statement.executeQuery()) {
                            if (resultSet.next()) {
                                nextMax = resultSet.getInt("max_id");
                            }
                        }
                    }
                    if (currentMax != -1 && nextMax != -1) {
                        // don't do anything if max sequence has not changed
                        if (currentMax != nextMax) {
                            this.runSeedQuery(conn, currentMax + 1, nextMax);
                        }
                    }
                }
                conn.commit();
            } catch (Exception e) {
                conn.rollback();
                throw e;
            }
        }
    }

    private void runSeedQuery(Connection connection, int min, int max) throws SQLException {
        try (PreparedStatement statement = connection.prepareStatement(SEED_RANGES)) {
            statement.setInt(1, this.batchSize - 1);
            statement.setInt(2, max);
            statement.setInt(3, min);
            statement.setInt(4, max);
            statement.setInt(5, this.batchSize);
            statement.executeUpdate();
        }
    }

    private LocalDate getMinDate() {
        LocalDateTime nowMinusRetention = LocalDateTime.now().minusMinutes(this.retentionPeriod);
        return nowMinusRetention.toLocalDate().atStartOfDay().toLocalDate();
    }

    @Override
    public boolean isInitialized() throws SQLException {
        try (Connection conn = this.database.getDataSource().getConnection()) {
            try (PreparedStatement statement = conn.prepareStatement(IS_INITIALIZED)) {
                try (ResultSet resultSet = statement.executeQuery()) {
                    if (resultSet.next()) {
                        return resultSet.getInt("total") > 0;
                    }
                    return false;
                }
            }
        }
    }

    @Override
    public void cleanup() throws SQLException {
        try (Connection conn = this.database.getDataSource().getConnection()) {
            try (PreparedStatement statement = conn.prepareStatement(CLEANUP)) {
                statement.executeUpdate();
            }
        }
    }
}
