package org.example.mover;

import org.example.exception.MigrationException;
import org.example.file.SourceFile;

public interface FileMover {
    String move(SourceFile sourceFile) throws MigrationException;
}
