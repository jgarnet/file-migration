package org.example.persistence.repository;

import org.example.file.SourceFile;
import org.example.persistence.database.Database;

import java.sql.Connection;
import java.sql.PreparedStatement;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.util.ArrayList;
import java.util.List;

public class PostgresFilesRepository implements FilesRepository {
    private final static String QUERY = "SELECT * FROM source_files WHERE file_id BETWEEN ? AND ?";
    private final Database database;

    public PostgresFilesRepository(Database database) {
        this.database = database;
    }

    @Override
    public List<SourceFile> getSourceFiles(int minId, int maxId) throws SQLException {
        try (Connection conn = this.database.getDataSource().getConnection()) {
            List<SourceFile> files = new ArrayList<>();
            try (PreparedStatement statement = conn.prepareStatement(QUERY)) {
                statement.setInt(1, minId);
                statement.setInt(2, maxId);
                try (ResultSet resultSet = statement.executeQuery()) {
                    while (resultSet.next()) {
                        files.add(new SourceFile(
                                resultSet.getInt("file_id"),
                                resultSet.getString("file_name"),
                                resultSet.getString("file_uri"),
                                resultSet.getTimestamp("create_date").toLocalDateTime()
                        ));
                    }
                }
            }
            return files;
        }
    }
}
