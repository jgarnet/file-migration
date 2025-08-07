package org.example.persistence.repository;

import org.example.file.SourceFile;

import java.sql.SQLException;
import java.util.List;

public interface FilesRepository {
    List<SourceFile> getSourceFiles(int minId, int maxId) throws SQLException;
}
